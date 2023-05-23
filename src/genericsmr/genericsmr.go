package genericsmr

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"epaxos/dlog"
	"epaxos/fastrpc"
	"epaxos/genericsmrproto"
	"github.com/portmapping/go-reuse"
	"golang.org/x/sys/unix"
	"strings"
	"sync/atomic"
	"syscall"

	//"github.com/libp2p/go-reuseport"
	"epaxos/mathextra"
	"epaxos/state"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"time"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

var storage string

type RPCPair struct {
	Obj  fastrpc.Serializable
	Chan chan fastrpc.Serializable
}

type UDPRPCPair struct {
	Name string
	Obj  fastrpc.UDPaxos //fastrpc.Serializable
	Chan chan fastrpc.Serializable
}

type Propose struct {
	*genericsmrproto.Propose
	Reply *bufio.Writer
	Mutex *sync.Mutex
}

type Beacon struct {
	Rid       int32
	Timestamp int64
}

type Replica struct {
	N              int        // total number of replicas
	Id             int32      // the ID of the current replica
	PeerAddrList   []string   // array with the IP:port address of every replica
	Peers          []net.Conn // cache of connections to all other replicas
	PeerReaders    []*bufio.Reader
	PeerWriters    []*bufio.Writer
	Alive          []bool // connection status
	Listener       net.Listener
	Clients        []net.Conn
	ClientsReaders []*bufio.Reader
	ClientsWriters []*bufio.Writer

	State *state.State

	ProposeChan chan *Propose // channel for client proposals
	BeaconChan  chan *Beacon  // channel for beacons from peer replicas

	Shutdown bool

	Thrifty bool // send only as many messages as strictly required?
	Exec    bool // execute commands?
	LRead   bool // execute local reads?
	Dreply  bool // reply to client after command has been executed?
	Beacon  bool // send beacons to detect how fast are the other replicas?

	F int

	Durable bool // log to a stable store?

	StableStorage *os.File // file support for the persistent log

	PreferredPeerOrder []int32 // replicas in the preferred order of communication

	rpcTable   map[uint8]*RPCPair
	maxRpcCode uint8

	Ewma                    []float64
	ReplicasLatenciesOrders []int32

	Mutex sync.Mutex

	Stats *genericsmrproto.Stats

	lastHeardFrom           []time.Time
	deadTime                int32
	heartbeatFrequency      time.Duration
	ewmaWeight              float64
	batchFlush              bool
	batchWait               time.Duration
	buffersFull             []chan struct{}
	mostRecentRepliedBeacon []beaconresp

	lastSentBeacons   []map[int64]beaconresp
	toRemoveI         []chan int64
	curBeaconsSentLat []int64
	udprpcTable       map[uint8]*UDPRPCPair
	UDP               bool
	UDPPeerAddrList   []*net.UDPAddr
	UDPPeerAddrToID   map[string]int32

	UDPPeers  []*net.UDPConn
	UDPPeersW []*net.UDPConn

	UDPC         *net.UDPConn
	sendBuffers  chan *[fastrpc.MAXDATAGRAMLEN]byte
	resendMsgs   chan resend
	ackedTIBSLs  chan ack
	UDPEWMAMutex sync.RWMutex

	AtomicEWMA []atomic.Value
}

type ack struct {
	fastrpc.MSGReceipt
	acker int32
}

type resend struct {
	ackReceipt fastrpc.MSGReceipt
	time.Time
	to      int32
	msg     fastrpc.UDPaxos
	msgcode uint8
}

type beaconresp struct {
	got  int64
	send int64
}

/* Client API */

func (r *Replica) Ping(args *genericsmrproto.PingArgs, reply *genericsmrproto.PingReply) error {
	return nil
}

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	return nil
}

/* Utils */
func (r *Replica) FastQuorumSize() int {
	return r.F + (r.F+1)/2
}

func (r *Replica) SlowQuorumSize() int {
	return (r.N + 1) / 2
}

// Flexible Paxos
func (r *Replica) WriteQuorumSize() int {
	return r.F + 1
}

func (r *Replica) ReadQuorumSize() int {
	return r.N - r.F
}

/* Network */
func (r *Replica) connectToPeer(i int) bool {
	var b [4]byte
	bs := b[:4]

	for done := false; !done; {
		if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
			r.Peers[i] = conn
			done = true
		} else {
			time.Sleep(1e9)
		}
	}

	binary.LittleEndian.PutUint32(bs, uint32(r.Id))
	if _, err := r.Peers[i].Write(bs); err != nil {
		fmt.Println("Write id error:", err)
		return false
	}
	r.Alive[i] = true
	r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
	r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])

	log.Printf("OUT Connected to %d", i)
	return true
}

func (r *Replica) ConnectToPeers() {
	done := make(chan bool)

	// WHEN SO_REUSEPORT NOT SUPPORTED
	//if r.UDP {
	//	// allocate send buffers
	//	numBuffers := 30
	//	r.sendBuffers = make(chan *[fastrpc.MAXDATAGRAMLEN]byte, numBuffers)
	//	for i := 0; i < numBuffers; i++ {
	//		r.sendBuffers <- &[fastrpc.MAXDATAGRAMLEN]byte{}
	//	}
	//
	//	// Fix incomplete local addresses
	//	// Register them with UDP address book
	//	for i := 0; i < len(r.PeerAddrList); i++ {
	//		if strings.HasPrefix(r.PeerAddrList[i], ":") {
	//			r.PeerAddrList[i] = "127.0.0.1" + r.PeerAddrList[i]
	//		}
	//		addr, _ := net.ResolveUDPAddr("udp", r.PeerAddrList[i])
	//		r.UDPPeerAddrList[i] = addr
	//		r.UDPPeerAddrToID[addr.String()] = int32(i)
	//	}
	//	// Listen on local port
	//	var er error
	//	r.UDPC, er = net.ListenUDP("udp", r.UDPPeerAddrList[r.Id])
	//	r.UDPC.SetWriteBuffer(1024 * 1024 * 4)
	//	r.UDPC.SetReadBuffer(1024 * 1024 * 4)
	//	if er != nil {
	//		panic(er)
	//	}
	//	time.Sleep(4 * time.Second) // badbadnotgood
	//	go r.ResendLoop()
	//	go r.replicaUDPListenerSINGLE()
	//	go r.heartbeatLoop()
	//	return
	//}

	if r.UDP {
		numBuffers := 50
		r.sendBuffers = make(chan *[fastrpc.MAXDATAGRAMLEN]byte, numBuffers)
		for i := 0; i < numBuffers; i++ {
			r.sendBuffers <- &[fastrpc.MAXDATAGRAMLEN]byte{}
		}

		for i := 0; i < len(r.PeerAddrList); i++ {
			if strings.HasPrefix(r.PeerAddrList[i], ":") {
				r.PeerAddrList[i] = "127.0.0.1" + r.PeerAddrList[i]
			}
			addr, _ := net.ResolveUDPAddr("udp", r.PeerAddrList[i])
			r.UDPPeerAddrList[i] = addr
			r.UDPPeerAddrToID[addr.String()] = int32(i)
		}

		for i := 0; i < len(r.PeerAddrList); i++ {
			if int32(i) == r.Id {
				continue
			}
			conn, _ := reuse.DialUDP("udp", r.UDPPeerAddrList[r.Id], r.UDPPeerAddrList[i])
			r.UDPPeers[i] = conn.(*net.UDPConn)
			r.UDPPeers[i].SetWriteBuffer(1024 * 1024 * 4)
			r.UDPPeers[i].SetReadBuffer(1024 * 1024 * 4)
			r.AtomicEWMA[i].Store(float64(time.Second))
			go r.replicaUDPListenerMultiple(int32(i))
		}
		time.Sleep(4 * time.Second) // badbadnotgood
		go r.ResendLoop()
		go r.heartbeatLoop()
		return
	}

	go r.waitForPeerConnections(done)
	//connect to peers
	for i := 0; i < int(r.Id); i++ {
		r.connectToPeer(i)
	}
	<-done
	log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)
	log.Printf("Node list %v", r.PeerAddrList)
	for rid, reader := range r.PeerReaders {
		if int32(rid) == r.Id {
			continue
		}
		if reader == nil {
			panic("asdflajsfj")
		}
		go r.replicaListener(rid, reader)
	}
	go r.heartbeatLoop()
}

func (r *Replica) ConnectToPeersNoListeners() {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	//connect to peers
	for i := 0; i < int(r.Id); i++ {
		for done := false; !done; {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				done = true
			} else {
				time.Sleep(1e9)
			}
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
	}
	<-done
	log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)
}

func (r *Replica) waitForPeerConnection(i int) bool {
	var b [4]byte
	bs := b[:4]
	var conn net.Conn
	var err error
	conn, err = r.Listener.Accept()
	if err != nil {
		fmt.Println("Accept error:", err)
		return false
	}
	if _, err := io.ReadFull(conn, bs); err != nil {
		fmt.Println("Connection establish error:", err)
		return false
	}
	id := int32(binary.LittleEndian.Uint32(bs))
	r.Peers[id] = conn
	r.PeerReaders[id] = bufio.NewReader(conn)
	r.PeerWriters[id] = bufio.NewWriter(conn)
	r.Alive[id] = true

	log.Printf("IN Connected to %d", id)
	return true
}

/* Peer (replica) connections dispatcher */
func (r *Replica) waitForPeerConnections(done chan bool) {
	if r.UDP {
		for i := 0; i < r.N; i++ {
			if int32(i) == r.Id {
				continue
			}
			var b [4]byte
			bs := b[:4]
			if _, err := r.UDPPeers[i].Read(bs); err != nil {
				fmt.Println("Connection establish error:", err)
			}
			id := int32(binary.LittleEndian.Uint32(bs))
			if int(id) != i {
				panic("wrong addrs")
			}
			r.Alive[id] = true
			log.Printf("IN Connected to %d", id)
		}
	} else {
		r.Listener, _ = net.Listen("tcp", r.PeerAddrList[r.Id])
		for i := r.Id + 1; i < int32(r.N); i++ {
			r.waitForPeerConnection(int(i))
		}
	}
	done <- true
}

/* Client connections dispatcher */
func (r *Replica) WaitForClientConnections() {
	log.Println("Waiting for client connections")
	if r.Listener == nil {
		r.Listener, _ = net.Listen("tcp", r.PeerAddrList[r.Id])
	}
	for !r.Shutdown {
		conn, err := r.Listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		r.Mutex.Lock()
		r.Clients = append(r.Clients, conn)
		r.Mutex.Unlock()
		go r.clientListener(conn)
	}
}

func (r *Replica) heartbeatLoop() {
	timer := time.NewTimer(r.heartbeatFrequency)
	for !r.Shutdown {
		for i := int32(0); i < int32(r.N); i++ {
			if i == r.Id {
				continue
			}
			r.SendBeacon(i)
		}
		<-timer.C
		timer.Reset(r.heartbeatFrequency)
	}
}

func (r *Replica) replicaListener(rid int, reader *bufio.Reader) {
	var msgType uint8
	var err error = nil
	var gbeacon genericsmrproto.Beacon
	var gbeaconReply genericsmrproto.BeaconReply

	for err == nil && !r.Shutdown {
		if msgType, err = reader.ReadByte(); err != nil {
			break
		}
		switch uint8(msgType) {
		case genericsmrproto.GENERIC_SMR_BEACON:
			if err = gbeacon.Unmarshal(reader); err != nil {
				break
			}
			beac := &Beacon{int32(rid), gbeacon.Timestamp}
			r.ReplyBeacon(beac)
			break
		case genericsmrproto.GENERIC_SMR_BEACON_REPLY:
			if err = gbeaconReply.Unmarshal(reader); err != nil {
				break
			}
			dlog.Println("receive beacon ", gbeaconReply.Timestamp, " reply from ", rid)
			r.Mutex.Lock()
			if r.Alive[rid] == false {
				r.Alive[rid] = true
			}
			t := time.Now()
			r.lastHeardFrom[rid] = t
			r.updateLatencyWithReply(rid, gbeaconReply)
			r.Mutex.Unlock()
			break
		default:
			if rpair, present := r.rpcTable[msgType]; present {
				obj := rpair.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				rpair.Chan <- obj
			} else {
				panic("alsdjfjldskf")
				log.Fatal("Error: received unknown message type ", msgType, " from  ", rid)
			}
		}
	}
	r.Mutex.Lock()
	r.Alive[rid] = false
	r.Mutex.Unlock()
}

type msgWB struct {
	fastrpc.CollectedM
	buffers map[int32]*[fastrpc.MAXDATAGRAMLEN]byte //seq to buffer
}

type UDPOptions struct {
	Address         string
	MinPacketLength int
	MaxPacketLength int
}

func (r *Replica) connectUDP(opt UDPOptions, id int32) {
	opt = UDPOptions{
		Address:         r.UDPPeerAddrList[r.Id].String(),
		MinPacketLength: 0,
		MaxPacketLength: fastrpc.MAXDATAGRAMLEN,
	}

	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				opErr = syscall.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}
	//lc.Listen()
	//k, _ := lc.Listen()
	lp, err := lc.ListenPacket(context.Background(), "udp", opt.Address)

	if err != nil {
		panic(fmt.Sprintf("dial failed: %v", err))
	}

	conn := lp.(*net.UDPConn)
	//conn
	r.UDPPeers[id] = conn
	//r.UDPPeerAddrList[i]

	//NewPacketConn
	//conn.
	//err = net.NewPacketConn(conn).SetControlMessage(ipv4.FlagDst|ipv4.FlagInterface, true)
	//if err != nil {
	//	h.sysLog.Fatalf("set control msg failed: %v", err)
	//}
}

// assumes that r.UDPC is being listened on
func (r *Replica) replicaUDPListenerSINGLE() {
	//var gbeaconReply genericsmrproto.BeaconReply
	//ts := int64(0)
	crtmsgs := make([]map[fastrpc.TIB]msgWB, r.N) //fastrpc.CollectedM)
	mutex := make([]sync.Mutex, r.N)
	for i := 0; i < r.N; i++ {
		crtmsgs[i] = make(map[fastrpc.TIB]msgWB)
	}

	//for i := 0; i < r.N; i++ {
	//	go func() {
	buffers := make(chan *[fastrpc.MAXDATAGRAMLEN]byte, 100)
	for i := 0; i < 60; i++ {
		buffers <- &[fastrpc.MAXDATAGRAMLEN]byte{}
	}
	b := <-buffers
	bs := b[:]
	tibsl := fastrpc.MSGReceipt{}
	tib := fastrpc.TIB{}
	for {
		n, addr, err := r.UDPC.ReadFromUDP(bs)
		if err != nil {
			panic(err)
		}
		if addr == nil {
			panic("Invalid address")
		}
		switch b[0] {
		case genericsmrproto.GENERIC_SMR_BEACON:
			//go func(b *[fastrpc.MAXDATAGRAMLEN]byte, bs []byte) {
			if n != 13 {
				panic("oh no")
			}
			ts := int64((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)) |
				(uint32(bs[5])<<32 | (uint32(bs[6]) << 40) | (uint32(bs[7]) << 48) | (uint32(bs[8]) << 54)))
			senderId := int32(uint32(bs[9]) | (uint32(bs[10]) << 8) | (uint32(bs[11]) << 16) | (uint32(bs[12]) << 24))
			go func() {
				bs := <-r.sendBuffers

				if len(bs) < fastrpc.MAXDATAGRAMLEN {
					panic("Lost buffer size")
				}
				bs[0] = genericsmrproto.GENERIC_SMR_BEACON_REPLY
				bs[1] = byte(ts)
				bs[2] = byte(ts >> 8)
				bs[3] = byte(ts >> 16)
				bs[4] = byte(ts >> 24)
				bs[5] = byte(ts >> 32)
				bs[6] = byte(ts >> 40)
				bs[7] = byte(ts >> 48)
				bs[8] = byte(ts >> 56)
				bs[9] = byte(r.Id)
				bs[10] = byte(r.Id >> 8)
				bs[11] = byte(r.Id >> 16)
				bs[12] = byte(r.Id >> 24)
				if _, e := r.UDPC.WriteTo(bs[:13], r.UDPPeerAddrList[senderId]); e != nil {
					panic(e)
				}
				r.sendBuffers <- bs
			}()
			//buffers <- b
			//}(b, bs)
			//b = <-buffers
			//bs = b[:]
		case genericsmrproto.GENERIC_SMR_BEACON_REPLY:
			//go func(b *[fastrpc.MAXDATAGRAMLEN]byte, bs []byte) {
			if n != 13 {
				panic("oh no")
			}
			ts := int64((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)) |
				(uint32(bs[5])<<32 | (uint32(bs[6]) << 40) | (uint32(bs[7]) << 48) | (uint32(bs[8]) << 54)))
			senderId := int(int32(uint32(bs[9]) | (uint32(bs[10]) << 8) | (uint32(bs[11]) << 16) | (uint32(bs[12]) << 24)))
			//buffers <- b
			g := genericsmrproto.BeaconReply{ts}
			//gbeaconReply.Timestamp = ts

			go func() {
				if r.Alive[senderId] == false {
					// todo add lock for alive -- when doing check in replica
					r.Alive[senderId] = true
				}

				r.lastHeardFrom[senderId] = time.Now()
				r.UDPEWMAUpdate(senderId, g)
			}()
			//}(b, bs)
			//b = <-buffers
			//bs = b[:]
			break
		case genericsmrproto.UDP_TIBSL_ACK:
			//go func(b *[fastrpc.MAXDATAGRAMLEN]byte, bs []byte) {
			tibsl2 := fastrpc.DecodeTIBSLFromSlice(b[5:])
			id := int32((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)))
			//buffers <- b
			//log.Printf("Got ack from id %d for tibsl %d %d %d %d %d %d", id, tibsl2.T, tibsl2.I, tibsl2.BB, tibsl2.BP, tibsl2.Seq, tibsl.Last)
			go func() {
				r.ackedTIBSLs <- ack{
					MSGReceipt: tibsl2,
					acker:      id,
				}
			}()
			//}(b, bs)
			//b = <-buffers
			//bs = b[:]
			break
		default:
			tibsl = fastrpc.DecodeTIBSL(b)
			tib = tibsl.TIB
			if _, e := r.udprpcTable[tibsl.T]; !e {
				panic(fmt.Sprintf("Got unknown message code %d", tibsl.T))
			}

			if tibsl.Ack {
				go func(tibsl fastrpc.MSGReceipt, addr *net.UDPAddr) {
					bs := <-r.sendBuffers
					if len(bs) < fastrpc.MAXDATAGRAMLEN {
						panic("Lost buffer size")
					}
					bs[0] = genericsmrproto.UDP_TIBSL_ACK
					bs[1] = byte(r.Id)
					bs[2] = byte(r.Id >> 8)
					bs[3] = byte(r.Id >> 16)
					bs[4] = byte(r.Id >> 24)
					fastrpc.EncodeTIBSLFromStruct(tibsl, bs[5:])
					if _, e := r.UDPC.WriteTo(bs[:fastrpc.TIBSLLEN+5], addr); e != nil {
						panic(e)
					}
					r.sendBuffers <- bs
				}(tibsl, addr)
			}

			// fast path
			if tibsl.Last == 0 && tibsl.Seq == 0 {
				log.Printf("doing fast path msg")
				go func(tibsl fastrpc.MSGReceipt, b *[fastrpc.MAXDATAGRAMLEN]byte, bs []byte) {
					colM := fastrpc.CollectedM{
						Messages: make(map[int32][]byte),
						Last:     0,
					}
					colM.Messages[tibsl.Seq] = bs[fastrpc.TIBSLLEN:n]
					if rpair, present := r.udprpcTable[tibsl.T]; present {
						obj := rpair.Obj.NewUDP()
						if err = obj.FromStrippedDatagrams(colM); err != nil {
							panic("Could not deserialise msg")
						}
						rpair.Chan <- obj
						buffers <- b
					}
				}(tibsl, b, bs)
				b = <-buffers
				bs = b[:]
				break
			}

			// if new message, allocate
			go func(addr string) {
				id, e := r.UDPPeerAddrToID[addr]
				if !e {
					panic("bad id")
				}
				//r.UDPPeerAddrList
				mutex[id].Lock()
				crtmsgs := crtmsgs[id]
				if _, e := crtmsgs[tib]; !e {
					crtmsgs[tib] = msgWB{
						CollectedM: fastrpc.CollectedM{
							//MLen:     make(map[int32]int),
							Messages: make(map[int32][]byte),
							Last:     -1,
						},
						buffers: make(map[int32]*[fastrpc.MAXDATAGRAMLEN]byte),
						//LastRecvAt: time.Now().Add(time.Millisecond * 20),
					}
				}

				// detect last message s == l to determine length
				if tibsl.Seq == tibsl.Last {
					crtmsgs[tib] = msgWB{
						CollectedM: fastrpc.CollectedM{
							//MLen:     crtmsgs[tib].MLen,
							Messages:   crtmsgs[tib].Messages,
							Last:       tibsl.Last,
							LastRecvAt: crtmsgs[tib].LastRecvAt,
						},
						buffers: crtmsgs[tib].buffers,
					}
				}

				// add to crtmsgs[tib]
				// and strip tibsl
				//crtmsgs[tib].Messages[tibsl.Seq] = make([]byte, len(b[fastrpc.TIBSLLEN:n]))
				//s := time.Now()
				crtmsgs[tib].Messages[tibsl.Seq] = bs[fastrpc.TIBSLLEN:n]
				crtmsgs[tib].buffers[tibsl.Seq] = b
				b = <-buffers
				bs = b[:]

				//copy(crtmsgs[tib].Messages[tibsl.Seq], b[fastrpc.TIBSLLEN:n])
				//e := time.Now()
				//log.Printf("copy message took %d us", e.Sub(s).Microseconds())

				// check if got all datagrams for message and then forward on to replica
				if crtmsgs[tib].Last != -1 {
					gotAll := true
					for m := int32(0); m < crtmsgs[tib].Last; m++ {
						if _, e := crtmsgs[tib].Messages[m]; !e {
							gotAll = false
							break
						}
					}

					if gotAll {
						delete(crtmsgs, tib)
						go func(msgB msgWB, t uint8) {
							if rpair, present := r.udprpcTable[t]; present {
								obj := rpair.Obj.NewUDP()
								if err = obj.FromStrippedDatagrams(msgB.CollectedM); err != nil {
									panic("Could not deserialise msg")
								}
								rpair.Chan <- obj

								for k, _ := range msgB.buffers {
									buffers <- msgB.buffers[k]
								}
							}
						}(crtmsgs[tib], tibsl.T)
					}
				}
				mutex[id].Unlock()
				/// check if there are any messages to drop
				//now := time.Now()
				//for tibkey := range crtmsgs {
				//	if crtmsgs[tibkey].LastRecvAt.Before(now) {
				//		dlog.AgentPrintfN(r.Id, "Dropping message of type %d for instance %d", tibkey.T, tibkey.I)
				//	}
				//
				//}
				//break
			}(addr.String())
		}
	}
	//}()
	//}
}

/*
func (r *Replica) recover(rid int) {
	r.Alive[rid] = false
	if rid < int(r.id) {
		for connected := false; !connected; {
			connected = r.connectToPeer(rid)
		}
	} else {
		for connected := false; !connected; {
			connected = r.waitForPeerConnection(rid)
		}
	}
	r.Alive[rid] = true
}
*/

func (r *Replica) clientListener(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	var msgType byte //:= make([]byte, 1)
	var err error

	r.Mutex.Lock()
	log.Println("Client up ", conn.RemoteAddr(), "(", r.LRead, ")")
	r.Mutex.Unlock()

	mutex := &sync.Mutex{}

	for !r.Shutdown && err == nil {

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {

		case genericsmrproto.PROPOSE:
			propose := new(genericsmrproto.Propose)
			if err = propose.Unmarshal(reader); err != nil {
				break
			}
			if r.LRead && (propose.Command.Op == state.GET || propose.Command.Op == state.SCAN) {
				val := propose.Command.Execute(r.State)
				propreply := &genericsmrproto.ProposeReplyTS{
					TRUE,
					propose.CommandId,
					val,
					propose.Timestamp}
				r.ReplyProposeTS(propreply, writer, mutex)
			} else {
				r.ProposeChan <- &Propose{propose, writer, mutex}
			}
			break

		case genericsmrproto.READ:
			read := new(genericsmrproto.Read)
			if err = read.Unmarshal(reader); err != nil {
				break
			}
			//r.ReadChan <- read
			break

		case genericsmrproto.PROPOSE_AND_READ:
			pr := new(genericsmrproto.ProposeAndRead)
			if err = pr.Unmarshal(reader); err != nil {
				break
			}
			//r.ProposeAndReadChan <- pr
			break

		case genericsmrproto.STATS:
			r.Mutex.Lock()
			b, _ := json.Marshal(r.Stats)
			r.Mutex.Unlock()
			writer.Write(b)
			if r.batchFlush {
				break
			}
			writer.Flush()
		}
	}
	conn.Close()
	log.Println("Client down ", conn.RemoteAddr())
}

func (r *Replica) RegisterUDPRPC(name string, msgObj fastrpc.UDPaxos, notify chan fastrpc.Serializable) uint8 {
	code := r.maxRpcCode
	r.maxRpcCode++
	r.udprpcTable[code] = &UDPRPCPair{name, msgObj, notify}
	dlog.Println("registering RPC ", r.maxRpcCode)
	return code
}

func (r *Replica) RegisterRPC(msgObj fastrpc.Serializable, notify chan fastrpc.Serializable) uint8 {
	code := r.maxRpcCode
	r.maxRpcCode++
	r.rpcTable[code] = &RPCPair{msgObj, notify}
	dlog.Println("registering RPC ", r.maxRpcCode)
	return code
}

func (r *Replica) CalculateAlive() {
	r.Mutex.Lock()
	r.calcAliveInternal()
	r.Mutex.Unlock()
}

func (r *Replica) CalculateAliveUNSAFE() {
	r.calcAliveInternal()
}

func (r *Replica) calcAliveInternal() {
	for i := 0; i < r.N; i++ {
		if i == int(r.Id) || r.lastHeardFrom[i].Equal(time.Time{}) {
			continue
		} else {
			timeSinceLastMsg := time.Now().Sub(r.lastHeardFrom[i])
			if timeSinceLastMsg > time.Millisecond*time.Duration(r.deadTime) {
				r.Alive[i] = false
			}
		}
	}
}

func (r *Replica) SendMsg(peerId int32, code uint8, msg fastrpc.Serializable) {
	r.Mutex.Lock()
	r.sendMsg(peerId, code, msg)
	r.Mutex.Unlock()
}

// todo move up the stack -- upon commit or new ballot don't need to resend
// can choose different replicas to send to instead (need group knowledge)
func (r *Replica) ResendLoop() {
	ackMap := make(map[ack]struct{})
	rAck := ack{}
	needToResend := false
	//todo check if alive first before resending
	for {
		select {
		case tibslAcked := <-r.ackedTIBSLs:
			ackMap[tibslAcked] = struct{}{}
			break
		case checkResend := <-r.resendMsgs:
			s := time.Now()
			rAck.MSGReceipt = checkResend.ackReceipt
			rAck.acker = checkResend.to
			if _, e := ackMap[rAck]; !e {
				needToResend = true
				delete(ackMap, rAck)
			} else {
				needToResend = false
			}
			if !needToResend {
				break
			}
			go func() {
				b := <-r.sendBuffers
				if len(b) < fastrpc.MAXDATAGRAMLEN {
					panic("Lost buffer size")
				}
				checkResend.msg.WriteDatagrams(checkResend.msgcode, true, r.UDPPeers[rAck.acker], r.UDPPeerAddrList[checkResend.to], b)
				e := time.Now()
				dlog.AgentPrintfN(r.Id, "Resending %s message instance %d at ballot %d.%d to Replica %d. It took %d µs", r.udprpcTable[rAck.T].Name, rAck.I, rAck.BB, rAck.BP, rAck.acker, e.Sub(s).Microseconds())
				r.sendBuffers <- b
			}()
			break
		}
	}
}

func (r *Replica) SendToGroup(group []int32, code uint8, msg fastrpc.Serializable) {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	for _, aid := range group {
		if aid == r.Id || !r.Alive[aid] {
			continue
		}
		w := r.PeerWriters[aid]
		if w == nil {
			log.Printf("Connection to %d lost!\n", aid)
			return
		}

		if code == 0 {
			panic("bad rpc code")
		}
		w.WriteByte(code)
		msg.Marshal(w)
		w.Flush()
	}
}

func (r *Replica) BcastUDPMsg(peers []int32, code uint8, msg fastrpc.UDPaxos, doResend bool) {
	go func() {
		b := <-r.sendBuffers
		if len(b) < fastrpc.MAXDATAGRAMLEN {
			panic("Lost buffer size")
		}
		s := time.Now()
		receipt := msg.BcastDatagrams(code, doResend, peers, r.UDPPeers, r.UDPPeerAddrList, b)
		e := time.Now()
		dlog.AgentPrintfN(r.Id, "Sent %d datagrams for %s message, this took %d microseconds", receipt.Last+1, r.udprpcTable[code].Name, e.Sub(s).Microseconds())
		r.sendBuffers <- b
		if !doResend {
			return
		}
		//for i := 0; i < len(peers); i++ {
		//	go func(i int) {
		//		ewma := r.AtomicEWMA[peers[i]].Load().(float64)
		//		toSleep := time.Duration(ewma * 1.3)
		//		time.Sleep(toSleep)
		//		r.resendMsgs <- resend{
		//			ackReceipt: receipt,
		//			to:         peers[i],
		//			msg:        msg,
		//			msgcode:    code,
		//		}
		//	}(i)
		//}
	}()
}

func (r *Replica) SendUDPMsg(peerID int32, code uint8, msg fastrpc.UDPaxos, doResend bool) {
	go func() {
		b := <-r.sendBuffers
		if len(b) < fastrpc.MAXDATAGRAMLEN {
			panic("Lost buffer size")
		}
		s := time.Now()
		tibsls := msg.WriteDatagrams(code, doResend, r.UDPPeers[peerID], r.UDPPeerAddrList[peerID], b)
		e := time.Now()
		dlog.AgentPrintfN(r.Id, "Sent %d datagrams for %s message, this took %d microseconds", tibsls.Last+1, r.udprpcTable[code].Name, e.Sub(s).Microseconds())
		r.sendBuffers <- b
		if !doResend {
			return
		}
		//ewma := r.AtomicEWMA[peerID].Load().(float64)
		//toSleep := time.Duration(ewma * 1.3)
		//log.Printf("Sleeping for %d µs", toSleep.Microseconds())
		//time.Sleep(toSleep)
		//r.resendMsgs <- resend{
		//	ackReceipt: tibsls,
		//	to:         peerID,
		//	msg:        msg,
		//	msgcode:    code,
		//}
	}()
}

func (r *Replica) sendMsg(peerId int32, code uint8, msg fastrpc.Serializable) {
	if r.UDP {
		panic("Should not send msg")
	}
	w := r.PeerWriters[peerId]
	if w == nil {
		log.Printf("Connection to %d lost!\n", peerId)
		return
	}

	w.WriteByte(code)
	msg.Marshal(w)

	if r.checkAndHandleBatchMessaging(peerId) {
		return
	}

	w.Flush()
}

func (r *Replica) checkAndHandleBatchMessaging(peerId int32) bool {
	if r.batchFlush {
		if r.PeerWriters[peerId].Buffered() > 4000000 { // 4MB buffer size
			r.buffersFull[peerId] <- struct{}{}
		}
		return true
	}
	return false
	//todo give timer too
}

func (r *Replica) SendMsgUNSAFE(peerId int32, code uint8, msg fastrpc.Serializable) {
	r.sendMsg(peerId, code, msg)
}

func (r *Replica) SendMsgNoFlushUNSAFE(peerId int32, code uint8, msg fastrpc.Serializable) {
	w := r.PeerWriters[peerId]
	if w == nil {
		log.Printf("Connection to %d lost!\n", peerId)
		return
	}
	w.WriteByte(code)
	msg.Marshal(w)
}

func (r *Replica) SendMsgNoFlush(peerId int32, code uint8, msg fastrpc.Serializable) {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	w := r.PeerWriters[peerId]
	if w == nil {
		log.Printf("Connection to %d lost!\n", peerId)
		return
	}
	w.WriteByte(code)
	msg.Marshal(w)
}

func (r *Replica) ReplyProposeTS(reply *genericsmrproto.ProposeReplyTS, w *bufio.Writer, lock *sync.Mutex) {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	reply.Marshal(w)
	w.Flush()
}

func (r *Replica) SendBeacon(peerId int32) {
	if r.UDP {
		go func() {
			b := [13]byte{}
			bs := b[:]
			t := time.Now().UnixNano()
			bs[0] = genericsmrproto.GENERIC_SMR_BEACON
			binary.BigEndian.PutUint64(bs[1:9], uint64(t))
			//bs[1] = byte(t)
			//bs[2] = byte(t >> 8)
			//bs[3] = byte(t >> 16)
			//bs[4] = byte(t >> 24)
			//bs[5] = byte(t >> 32)
			//bs[6] = byte(t >> 40)
			//bs[7] = byte(t >> 48)
			//bs[8] = byte(t >> 56)
			//bs[9] = byte(r.Id)
			//bs[10] = byte(r.Id >> 8)
			//bs[11] = byte(r.Id >> 16)
			//bs[12] = byte(r.Id >> 24)
			if _, e := r.UDPPeers[peerId].Write(bs); e != nil {
				panic(e)
			}
		}()
		return
	}

	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	w := r.PeerWriters[peerId]
	if w == nil {
		log.Printf("Connection to %d lost!\n", peerId)
		return
	}
	//log.Println("sending beacon to", peerId)
	w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON)
	t := time.Now().UnixNano()
	beacon := &genericsmrproto.Beacon{Timestamp: t}
	beacon.Marshal(w)
	dlog.Println("send beacon ", beacon.Timestamp, " to ", peerId)

	if r.checkAndHandleBatchMessaging(peerId) {
		return
	}
	w.Flush()
}

func (r *Replica) ReplyBeacon(beacon *Beacon) {
	dlog.Println("replying beacon to ", beacon.Rid)
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	w := r.PeerWriters[beacon.Rid]
	if w == nil {
		log.Printf("Connection to %d lost!\n", beacon.Rid)
		return
	}
	//log.Println("sending beacon reply to", beacon.Rid)
	w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON_REPLY)
	rb := genericsmrproto.BeaconReply{beacon.Timestamp}
	rb.Marshal(w)
	if r.checkAndHandleBatchMessaging(beacon.Rid) {
		return
	}
	w.Flush()
}

func (r *Replica) Crash() {
	r.Mutex.Lock()
	r.Listener.Close()
	for i := 0; i < r.N; i++ {
		if int(r.Id) == i {
			continue
		} else {
			r.Peers[i].Close()
		}
	}
	r.Mutex.Unlock()
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, lread bool, dreply bool, failures int, storageParentDir string, deadTime int32, batchFlush bool, batchWait time.Duration, udp bool) *Replica {
	slidingW := 20

	r := &Replica{
		len(peerAddrList),
		int32(id),
		peerAddrList,
		make([]net.Conn, len(peerAddrList)),
		make([]*bufio.Reader, len(peerAddrList)),
		make([]*bufio.Writer, len(peerAddrList)),
		make([]bool, len(peerAddrList)),
		nil,
		nil,
		nil,
		nil,
		state.InitState(),
		make(chan *Propose, CHAN_BUFFER_SIZE),
		make(chan *Beacon, CHAN_BUFFER_SIZE),
		false,
		thrifty,
		exec,
		lread,
		dreply,
		false,
		failures,
		false,
		nil,
		make([]int32, len(peerAddrList)),
		make(map[uint8]*RPCPair),
		genericsmrproto.GENERIC_SMR_BEACON_REPLY + 1,
		make([]float64, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		sync.Mutex{},
		&genericsmrproto.Stats{make(map[string]int)},
		make([]time.Time, len(peerAddrList)),
		deadTime,
		time.Duration(250 * time.Millisecond),
		0.1,
		batchFlush,
		batchWait,
		make([]chan struct{}, len(peerAddrList)),
		make([]beaconresp, len(peerAddrList)),
		make([]map[int64]beaconresp, len(peerAddrList)),
		make([]chan int64, len(peerAddrList)),
		make([]int64, len(peerAddrList)),
		make(map[uint8]*UDPRPCPair),
		udp,
		make([]*net.UDPAddr, len(peerAddrList)),
		make(map[string]int32),
		make([]*net.UDPConn, len(peerAddrList)),
		make([]*net.UDPConn, len(peerAddrList)),
		nil,
		nil,
		make(chan resend, 1000),
		make(chan ack, 1000),
		sync.RWMutex{},
		make([]atomic.Value, len(peerAddrList)),
	}

	storage = storageParentDir

	var err error
	r.StableStorage, err =
		os.Create(fmt.Sprintf("%vstable-store-replica%d", storage, r.Id))
	//		os.OpenFile(fmt.Sprintf("%v/stable-store-replica%d", storage, r.id), os.O_RDWR|os.O_CREATE, 0755)

	if err != nil {
		log.Fatal(err)
	}

	for i := int32(0); i < int32(r.N); i++ {
		r.lastSentBeacons[i] = make(map[int64]beaconresp, slidingW)
		r.toRemoveI[i] = make(chan int64, slidingW)
		if r.Id == i {
			r.Alive[i] = true
		}
		r.Ewma[i] = 0.0
		r.ReplicasLatenciesOrders[i] = i
	}

	if r.batchFlush {
		for i := int32(0); i < int32(len(r.PeerWriters)); i++ {
			if i == r.Id {
				continue
			}
			bufferFull := r.buffersFull[i]
			go func() {
				timer := time.NewTimer(r.batchWait)
				for !r.Shutdown {
					select {
					case <-timer.C:
					case <-bufferFull:
						r.PeerFlushBuffer(i)
						timer.Reset(r.batchWait)
						break
					}
				}
			}()
		}
	}
	return r
}

func (r *Replica) CopyEWMA() []float64 {
	r.Mutex.Lock()
	cp := make([]float64, len(r.Ewma))
	copy(cp, r.Ewma)
	r.Mutex.Unlock()
	return cp
}

func testEq(a, b []int32) bool {
	// If one is nil, the other must also be nil.
	if (a == nil) != (b == nil) {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (r *Replica) RandomisePeerOrder() {
	//	rand.Seed(time.Now().UnixNano() * int64(os.Getpid()))
	r.Mutex.Lock()
	old := make([]int32, r.N)
	copy(old, r.PreferredPeerOrder)
	//for testEq(old, r.PreferredPeerOrder) {

	for i := 0; i < len(r.PreferredPeerOrder); i++ {
		r.PreferredPeerOrder[i] = int32(i)
	}
	rand.Shuffle(r.N, func(i, j int) {
		r.PreferredPeerOrder[i], r.PreferredPeerOrder[j] = r.PreferredPeerOrder[j], r.PreferredPeerOrder[i]
	})

	// move self to end (we don't ever send to selves)
	theEnd := len(r.PreferredPeerOrder) - 1
	for i := 0; i < len(r.PreferredPeerOrder); i++ {
		if r.PreferredPeerOrder[i] == r.Id {
			tmp := r.PreferredPeerOrder[theEnd]
			r.PreferredPeerOrder[theEnd] = r.PreferredPeerOrder[i]
			r.PreferredPeerOrder[i] = tmp
		}
	}

	theEnd--
	for i := 0; i < theEnd; i++ {
		if !r.Alive[r.PreferredPeerOrder[i]] {
			tmp := r.PreferredPeerOrder[theEnd]
			r.PreferredPeerOrder[theEnd] = r.PreferredPeerOrder[i]
			r.PreferredPeerOrder[i] = tmp
			theEnd--
		}
	}
	//	}
	r.Mutex.Unlock()
}

//func (r *Replica) updateLatencyWithReply(rid int, gbeaconReply genericsmrproto.BeaconReply) {
//	t := time.Now().UnixNano()
//	r.Ewma[rid] = mathextra.EwmaAdd(r.Ewma[rid], r.ewmaWeight, float64(t-gbeaconReply.Timestamp))
//	if gbeaconReply.Timestamp < r.mostRecentRepliedBeacon[rid].send {
//		return
//	}
//	r.mostRecentRepliedBeacon[rid] = beaconresp{
//		got:  t,
//		send: gbeaconReply.Timestamp,
//	}
//}

func (r *Replica) UDPEWMAUpdate(rid int, gbeaconReply genericsmrproto.BeaconReply) {
	t := time.Now().UnixNano()
	diff := t - gbeaconReply.Timestamp
	ridEwma := r.AtomicEWMA[rid].Load().(float64)
	ridEwma = mathextra.EwmaAdd(ridEwma, r.ewmaWeight, float64(diff))
	r.AtomicEWMA[rid].Store(ridEwma)
	//diffTS := time.Duration(diff) * time.Nanosecond
	//h := time.Duration(ridEwma) * time.Nanosecond
	//log.Printf("setting ewma to %d milliseoncds after %d ts", h.Milliseconds(), diffTS.Milliseconds())
	if gbeaconReply.Timestamp < r.mostRecentRepliedBeacon[rid].send {
		return
	}
	r.mostRecentRepliedBeacon[rid] = beaconresp{
		got:  t,
		send: gbeaconReply.Timestamp,
	}
}

func (r *Replica) updateLatencyWithReply(rid int, gbeaconReply genericsmrproto.BeaconReply) {
	t := time.Now().UnixNano()
	r.Ewma[rid] = mathextra.EwmaAdd(r.Ewma[rid], r.ewmaWeight, float64(t-gbeaconReply.Timestamp))
	if gbeaconReply.Timestamp < r.mostRecentRepliedBeacon[rid].send {
		return
	}
	r.mostRecentRepliedBeacon[rid] = beaconresp{
		got:  t,
		send: gbeaconReply.Timestamp,
	}
}

type LatencyOracle interface {
	GetLatency(rId int32) time.Duration
	GetPeerOrderLatency() []int32 // ascending - you will always be index 0
	GetAlive() []bool
	GetPeerLatencies() []time.Duration
}

func (r *Replica) GetLatency(rId int32) time.Duration {
	return time.Duration(r.Ewma[rId]) * time.Nanosecond
}

func (r *Replica) GetAlive() []bool {
	for i := 0; i < r.N; i++ {
		if int(r.Id) == i {
			continue
		}
		if time.Duration(r.Ewma[i]) > time.Duration(r.deadTime)*time.Millisecond {
			r.Alive[i] = false
		}
	}
	return r.Alive
}

func (r *Replica) GetPeerLatencies() []time.Duration {
	lat := make([]time.Duration, r.N)
	for rId := int32(0); rId < int32(r.N); rId++ {
		lat[rId] = r.GetLatency(rId)
	}
	return lat
}

func (r *Replica) GetAliveRandomPeerOrder() []int32 {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	reps := make([]int32, 1, r.N)
	reps[0] = r.Id
	for i := int32(0); i < int32(r.N); i++ {
		if r.Id == i || !r.Alive[i] {
			continue
		}
		//reps[i] = i
		reps = append(reps, i)
	}
	toShuff := reps[1:]
	rand.Shuffle(len(toShuff), func(i, j int) {
		toShuff[i], toShuff[j] = toShuff[j], toShuff[i]
	})
	return reps
}

// returns all alive acceptors ordered by random (including self)
func (r *Replica) GetRandomPeerOrder() []int32 {
	// returns a random preference order for sending messages, except we are always first
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	reps := make([]int32, 1, r.N)
	reps[0] = r.Id
	for i := int32(0); i < int32(r.N); i++ {
		if r.Id == i {
			continue
		}
		//reps[i] = i
		reps = append(reps, i)
	}
	toShuff := reps[1:]
	rand.Shuffle(len(toShuff), func(i, j int) {
		toShuff[i], toShuff[j] = toShuff[j], toShuff[i]
	})
	return reps
}

// returns all alive acceptors ordered by latency (including self)
func (r *Replica) GetPeerOrderLatency() []int32 {
	r.Mutex.Lock()
	peers := make([]int32, 1, r.N)
	peers[0] = r.Id
	for i := int32(0); i < int32(r.N); i++ {
		if r.Id == i {
			continue
		}
		peers = append(peers, i)
	}

	if true {

	}
	now := time.Now().UnixNano()
	pl := peers[1:]

	sort.Slice(pl, func(i, j int) bool {
		lhs := r.Ewma[pl[i]]*(1-r.ewmaWeight) + float64(now-r.mostRecentRepliedBeacon[pl[i]].send)*r.ewmaWeight
		rhs := r.Ewma[pl[j]]*(1-r.ewmaWeight) + float64(now-r.mostRecentRepliedBeacon[pl[j]].send)*r.ewmaWeight
		return lhs < rhs //now-r.mostRecentRepliedBeacon[pl[i]] < now-r.mostRecentRepliedBeacon[pl[j]]
	})

	r.Mutex.Unlock()
	return peers
}

func (r *Replica) getAlivePeers() []int32 {
	aliveReps := make([]int32, 0, r.N)
	for i, isAlive := range r.Alive {
		if isAlive {
			aliveReps = append(aliveReps, int32(i))
		}
	}
	return aliveReps
}

func (r *Replica) FromCandidatesSelectBestLatency(candidates [][]int32) []int32 {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	livingQrmsIds := r.getLivingFromCandidates(candidates)
	bestQrm := int32(-1)
	bestScore := int32(math.MaxInt32)
	for _, id := range livingQrmsIds {
		qrm := candidates[id]
		crtScore := int32(0)
		for acc := range qrm {
			crtScore += r.ReplicasLatenciesOrders[acc]
		}
		if crtScore < bestScore {
			bestScore = crtScore
			bestQrm = id
		}
	}
	if bestQrm == -1 {
		return []int32{}
	}
	return candidates[bestQrm]
}

func (r *Replica) FromCandidatesSelectRandom(candidates [][]int32) []int32 {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	livingQrmsIds := r.getLivingFromCandidates(candidates)
	if len(livingQrmsIds) == 0 {
		return []int32{}
	}
	return candidates[livingQrmsIds[rand.Intn(len(livingQrmsIds))]]
}

func (r *Replica) getLivingFromCandidates(candidates [][]int32) []int32 {
	r.calcAliveInternal()
	livingQuorums := make([]int32, 0, len(candidates))
	for i, qrm := range candidates {
		for j := 0; j < len(qrm); j++ {
			if !r.Alive[qrm[j]] && int32(qrm[j]) != r.Id { // we are assumed to be alive but will appear dead
				break
			}
		}
		livingQuorums = append(livingQuorums, int32(i))
	}
	return livingQuorums
}

func (r *Replica) ClientFlushFBuffer(cli int32) {
	r.Mutex.Lock()
	_ = r.ClientsWriters[cli].Flush()
	r.Mutex.Unlock()
}

func (r *Replica) PeerFlushBuffer(to int32) {
	r.Mutex.Lock()
	_ = r.PeerWriters[to].Flush()
	r.Mutex.Unlock()
}

func (r *Replica) replicaUDPListenerMultiple(i int32) {
	crtmsgs := make(map[fastrpc.TIB]msgWB)
	b := &[fastrpc.MAXDATAGRAMLEN]byte{}
	//bs := b[:]
	tibsl := fastrpc.MSGReceipt{}
	tib := fastrpc.TIB{}

	ts := int64(0)

	n := 0
	addr := &net.UDPAddr{}
	var err error

	validCode := false
	gotAllDatagrams := false

	//preAlloc := make(chan struct{}, 50)
	//buffers := make(chan [fastrpc.MAXDATAGRAMLEN]byte, 50)
	//go func() {
	//	for {
	//		<-preAlloc
	//		buf := [fastrpc.MAXDATAGRAMLEN]byte{}
	//		buffers <- buf
	//	}
	//}()

	//timeOut := time.Duration(0)
	now := time.Time{}

	for {
		n, addr, err = r.UDPPeers[i].ReadFromUDP((*b)[:])
		if err != nil {
			continue
			//panic(err)
		}
		if addr == nil {
			panic("Invalid address")
		}
		switch b[0] {
		case genericsmrproto.GENERIC_SMR_BEACON:
			//s := time.Now()
			if n != 13 {
				panic("oh no")
			}
			ts = int64(binary.BigEndian.Uint64((*b)[1:9]))
			go func() {
				bbs := [13]byte{genericsmrproto.GENERIC_SMR_BEACON_REPLY}
				binary.BigEndian.PutUint64(bbs[1:], uint64(ts))
				if _, e := r.UDPPeers[i].Write(bbs[:]); e != nil {
					panic(e)
				}
			}()
			//e := time.Now()
			//dlog.AgentPrintfN(r.Id, "It took %d µs to handle beacon", e.Sub(s).Microseconds())
			break
		case genericsmrproto.GENERIC_SMR_BEACON_REPLY:
			//s := time.Now()
			if n != 13 {
				panic("oh no")
			}
			ts = int64(binary.BigEndian.Uint64((*b)[1:9]))
			g := genericsmrproto.BeaconReply{ts}
			r.udpUpdateReplicaKnowledge(int(i), g)
			//e := time.Now()
			//dlog.AgentPrintfN(r.Id, "It took %d µs to handle beacon reply", e.Sub(s).Microseconds())
			break
		case genericsmrproto.UDP_TIBSL_ACK:
			//s := time.Now()
			tibsl2 := fastrpc.DecodeTIBSLFromSlice((*b)[5:])
			go func() {
				r.ackedTIBSLs <- ack{
					MSGReceipt: tibsl2,
					acker:      i,
				}
			}()
			//e := time.Now()
			//dlog.AgentPrintfN(r.Id, "It took %d µs to handle ack message", e.Sub(s).Microseconds())
			//dlog.AgentPrintfN(r.Id, "It took %d µs to handle ack of %s message with tibsl %d.%d.%d.%d from %d", e.Sub(s).Microseconds(), r.udprpcTable[tibsl2.T].Name, tibsl2.I, tibsl2.BB, tibsl2.BP, tibsl2.Seq, i)
			break
		default:
			tibsl = fastrpc.DecodeTIBSL(b)
			tib = tibsl.TIB
			if _, validCode = r.udprpcTable[tibsl.T]; !validCode {
				panic(fmt.Sprintf("Got unknown message code %d", tibsl.T))
			}

			// fast path
			if tibsl.Last == 0 && tibsl.Seq == 0 { // msg is a single datagram
				if tibsl.Ack {
					go r.sendReceiptAck(i, tibsl, addr)
				}
				//s := time.Now()
				colM := fastrpc.CollectedM{
					Messages: make(map[int32][]byte),
					Last:     0,
				}
				//colM.Messages[tibsl.Seq] = (*b)[fastrpc.TIBSLLEN:n]
				//b = &[fastrpc.MAXDATAGRAMLEN]byte{}
				colM.Messages[tibsl.Seq] = make([]byte, n-fastrpc.TIBSLLEN)
				copy(colM.Messages[tibsl.Seq], (*b)[fastrpc.TIBSLLEN:n])
				go func(tibsl fastrpc.MSGReceipt) {
					if rpair, present := r.udprpcTable[tibsl.T]; present {
						obj := rpair.Obj.NewUDP()
						if err := obj.FromStrippedDatagrams(colM); err != nil {
							panic("Could not deserialise msg")
						}
						rpair.Chan <- obj
					}
				}(tibsl)
				//e := time.Now()
				//dlog.AgentPrintfN(r.Id, "It took %d µs to fast path unmarshall this %s message with tibsl %d.%d.%d.%d", e.Sub(s).Microseconds(), r.udprpcTable[tibsl.T].Name, tibsl.I, tibsl.BB, tibsl.BP, tibsl.Seq)
				break
			}

			now = time.Now()
			//go func() { preAlloc <- struct{}{} }()
			// if new message, allocate
			if _, validCode = crtmsgs[tib]; !validCode {
				crtmsgs[tib] = msgWB{
					CollectedM: fastrpc.CollectedM{
						Messages:   make(map[int32][]byte),
						Last:       -1,
						LastRecvAt: now,
					},
					buffers: make(map[int32]*[fastrpc.MAXDATAGRAMLEN]byte),
				}
			}

			// detect last message s == l to determine length
			if tibsl.Seq == tibsl.Last {
				crtmsgs[tib] = msgWB{
					CollectedM: fastrpc.CollectedM{
						Messages:   crtmsgs[tib].Messages,
						Last:       tibsl.Last,
						LastRecvAt: crtmsgs[tib].LastRecvAt,
					},
					buffers: crtmsgs[tib].buffers,
				}
			}
			//a := [fastrpc.MAXDATAGRAMLEN]byte{}                            //<-buffers                                                 //[fastrpc.MAXDATAGRAMLEN]byte{}

			crtmsgs[tib].Messages[tibsl.Seq] = (*b)[fastrpc.TIBSLLEN:n]
			d := make(chan struct{})
			go func() {
				b = &[fastrpc.MAXDATAGRAMLEN]byte{}
				d <- struct{}{}
			}()

			if crtmsgs[tib].Last != -1 {
				gotAllDatagrams = false
				if len(crtmsgs[tib].Messages) == int(crtmsgs[tib].Last+1) {
					gotAllDatagrams = true // assume no wrong sequence numbers -- could add safety check
				}

				if gotAllDatagrams {
					if tibsl.Ack {
						go r.sendReceiptAck(i, fastrpc.MSGReceipt{
							TIB:  tibsl.TIB,
							Seq:  crtmsgs[tib].Last,
							Last: crtmsgs[tib].Last,
							Ack:  true,
						}, addr)
					}
					go func(msgB msgWB, t uint8, tibsl fastrpc.MSGReceipt) {
						if rpair, present := r.udprpcTable[t]; present {
							obj := rpair.Obj.NewUDP()
							//copy here from buffers to messages
							if err = obj.FromStrippedDatagrams(msgB.CollectedM); err != nil {
								panic("Could not deserialise msg")
							}
							//dlog.AgentPrintfN(r.Id, "Got %d message with tibsl %d.%d.%d.%d")
							rpair.Chan <- obj
						}
					}(crtmsgs[tib], tibsl.T, tibsl)
					delete(crtmsgs, tib)
				}
			}
			//e := time.Now()
			//dlog.AgentPrintfN(r.Id, "It took %d µs to slow path unmarshall this %s message with tibsl %d.%d.%d.%d", e.Sub(now).Microseconds(), r.udprpcTable[tibsl.T].Name, tibsl.I, tibsl.BB, tibsl.BP, tibsl.Seq)
			//timeOut := time.Duration(r.AtomicEWMA[i].Load().(float64)*2) * time.Nanosecond
			t := time.Millisecond * 100
			now := time.Now()
			for k := range crtmsgs {
				timeOut := crtmsgs[k].LastRecvAt.Add(t)
				if now.Before(timeOut) {
					continue
				}
				dlog.AgentPrintfN(r.Id, "Dropping %s message in instance %d at ballot %d.%d as it timed out on receiving. Received only %d datagrams.", r.udprpcTable[k.T].Name, k.I, k.BB, k.BP, len(crtmsgs[k].Messages))
				delete(crtmsgs, k)
			}
			<-d
			break
		}
	}
}

func (r *Replica) udpUpdateReplicaKnowledge(senderId int, g genericsmrproto.BeaconReply) {
	if r.Alive[senderId] == false {
		// todo add lock for alive -- when doing check in replica
		r.Alive[senderId] = true
	}

	r.lastHeardFrom[senderId] = time.Now()
	r.UDPEWMAUpdate(senderId, g)
}

func (r *Replica) sendUDPBeaconReply(ts int64, i int32, bs []byte) {
	bs[0] = genericsmrproto.GENERIC_SMR_BEACON_REPLY
	bs[1] = byte(ts)
	bs[2] = byte(ts >> 8)
	bs[3] = byte(ts >> 16)
	bs[4] = byte(ts >> 24)
	bs[5] = byte(ts >> 32)
	bs[6] = byte(ts >> 40)
	bs[7] = byte(ts >> 48)
	bs[8] = byte(ts >> 56)
	bs[9] = byte(r.Id)
	bs[10] = byte(r.Id >> 8)
	bs[11] = byte(r.Id >> 16)
	bs[12] = byte(r.Id >> 24)
	if _, e := r.UDPPeers[i].Write(bs[:13]); e != nil {
		panic(e)
	}
}

func (r *Replica) sendReceiptAck(i int32, tibsl fastrpc.MSGReceipt, addr *net.UDPAddr) {
	bs := <-r.sendBuffers
	if len(bs) < fastrpc.MAXDATAGRAMLEN {
		panic("Lost buffer size")
	}
	bs[0] = genericsmrproto.UDP_TIBSL_ACK
	bs[1] = byte(r.Id)
	bs[2] = byte(r.Id >> 8)
	bs[3] = byte(r.Id >> 16)
	bs[4] = byte(r.Id >> 24)
	fastrpc.EncodeTIBSLFromStruct(tibsl, bs[5:])
	if _, e := r.UDPPeers[i].Write(bs[:fastrpc.TIBSLLEN+5]); e != nil {
		panic(e)
	}
	r.sendBuffers <- bs
}
