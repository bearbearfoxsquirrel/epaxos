package genericsmr

import (
	"bufio"
	"dlog"
	"encoding/binary"
	"encoding/json"
	"fastrpc"
	"fmt"
	"genericsmrproto"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"state"
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

	Durable     bool     // log to a stable store?
	StableStore *os.File // file support for the persistent log

	PreferredPeerOrder []int32 // replicas in the preferred order of communication

	rpcTable map[uint8]*RPCPair
	rpcCode  uint8

	Ewma                    []float64
	ReplicasLatenciesOrders []int32

	Mutex sync.Mutex

	Stats *genericsmrproto.Stats

	lastHeardFrom      []time.Time
	deadTime           int32
	heartbeatFrequency time.Duration
	ewmaWeight         float64
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

	conn, err := r.Listener.Accept()
	if err != nil {
		fmt.Println("Accept error:", err)
		return false
		//		continue
	}
	if _, err := io.ReadFull(conn, bs); err != nil {
		fmt.Println("Connection establish error:", err)
		return false
		//		continue
	}
	id := int32(binary.LittleEndian.Uint32(bs))
	//	if id != int32(i) {
	//	return false
	//}
	r.Peers[id] = conn
	r.PeerReaders[id] = bufio.NewReader(conn)
	r.PeerWriters[id] = bufio.NewWriter(conn)
	r.Alive[id] = true

	log.Printf("IN Connected to %d", id)
	return true
}

/* Peer (replica) connections dispatcher */
func (r *Replica) waitForPeerConnections(done chan bool) {

	r.Listener, _ = net.Listen("tcp", r.PeerAddrList[r.Id])
	for i := r.Id + 1; i < int32(r.N); i++ {
		r.waitForPeerConnection(int(i))
	}

	done <- true
}

/* Client connections dispatcher */
func (r *Replica) WaitForClientConnections() {
	log.Println("Waiting for client connections")
	//numClis := 0
	for !r.Shutdown {
		conn, err := r.Listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		r.Mutex.Lock()
		r.Clients = append(r.Clients, conn)
		r.Mutex.Unlock()
		//	numClis++
		//	r.Clients =
		go r.clientListener(conn)

	}
}

func (r *Replica) heartbeatLoop() {
	timer := time.NewTimer(r.heartbeatFrequency)
	for !r.Shutdown {
		for i := int32(0); i < int32(r.N-1); i++ {
			if r.PreferredPeerOrder[i] == r.Id {
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

		go func() {
			r.Mutex.Lock()
			if r.Alive[rid] == false {
				r.Alive[rid] = true
			}
			r.lastHeardFrom[rid] = time.Now()
			r.Mutex.Unlock()
		}()

		switch uint8(msgType) {
		case genericsmrproto.GENERIC_SMR_BEACON:
			if err = gbeacon.Unmarshal(reader); err != nil {
				break
			}
			beacon := &Beacon{int32(rid), gbeacon.Timestamp}
			r.ReplyBeacon(beacon)
			break

		case genericsmrproto.GENERIC_SMR_BEACON_REPLY:
			if err = gbeaconReply.Unmarshal(reader); err != nil {
				break
			}
			dlog.Println("receive beacon ", gbeaconReply.Timestamp, " reply from ", rid)
			r.Mutex.Lock()
			r.updateLatencyRanks(rid, gbeaconReply)
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
				log.Fatal("Error: received unknown message type ", msgType, " from  ", rid)
			}
		}
	}

	r.Mutex.Lock()
	r.Alive[rid] = false
	r.Mutex.Unlock()
}

/*
func (r *Replica) recover(rid int) {
	r.Alive[rid] = false
	if rid < int(r.Id) {
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
			writer.Flush()
		}
	}
	conn.Close()
	log.Println("Client down ", conn.RemoteAddr())
}

func (r *Replica) RegisterRPC(msgObj fastrpc.Serializable, notify chan fastrpc.Serializable) uint8 {
	code := r.rpcCode
	r.rpcCode++
	r.rpcTable[code] = &RPCPair{msgObj, notify}
	dlog.Println("registering RPC ", r.rpcCode)
	return code
}

func (r *Replica) CalculateAlive() {
	r.Mutex.Lock()
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
	r.Mutex.Unlock()
}

func (r *Replica) SendMsg(peerId int32, code uint8, msg fastrpc.Serializable) {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	w := r.PeerWriters[peerId]
	if w == nil {
		log.Printf("Connection to %d lost!\n", peerId)
		return
	}

	if code == 0 {
		panic("bad rpc code")
	}
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
}

func (r *Replica) SendMsgNoFlush(peerId int32, code uint8, msg fastrpc.Serializable) {
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
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	w := r.PeerWriters[peerId]
	if w == nil {
		log.Printf("Connection to %d lost!\n", peerId)
		return
	}
	w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON)
	beacon := &genericsmrproto.Beacon{Timestamp: time.Now().UnixNano()}
	beacon.Marshal(w)
	w.Flush()
	dlog.Println("send beacon ", beacon.Timestamp, " to ", peerId)
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
	w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON_REPLY)
	rb := &genericsmrproto.BeaconReply{beacon.Timestamp}
	rb.Marshal(w)
	w.Flush()
}

func (r *Replica) Crash() {
	r.Listener.Close()
	for i := 0; i < r.N; i++ {
		if int(r.Id) == i {
			continue
		} else {
			r.Peers[i].Close()
		}
	}
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, lread bool, dreply bool, failures int, storageParentDir string, deadTime int32) *Replica {
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
		time.Duration(100 * time.Millisecond),
		0.1,
	}

	storage = storageParentDir

	var err error
	r.StableStore, err =
		os.Create(fmt.Sprintf("%v/stable-store-replica%d", storage, r.Id))
		//		os.OpenFile(fmt.Sprintf("%v/stable-store-replica%d", storage, r.Id), os.O_RDWR|os.O_CREATE, 0755)

	if err != nil {
		log.Fatal(err)
	}

	for i := int32(0); i < int32(r.N); i++ {
		if r.Id == i {
			continue
		}
		r.Ewma[i] = 0.0
		r.ReplicasLatenciesOrders[i] = i
	}

	return r
}

/*
// updates the preferred order in which to communicate with peers according to a preferred quorum
func (r *Replica) UpdatePreferredPeerOrder(quorum []int32) {
			aux := make([]int32, r.N)
			i := 0
			for _, p := range quorum {
				if p == r.Id {
					continue
				}
				aux[i] = p
				i++
			}

			for _, p := range r.PreferredPeerOrder {
				found := false
				for j := 0; j < i; j++ {
					if aux[j] == p {
						found = true
						break
					}
				}
				if !found {
					aux[i] = p
					i++
				}
			}

			r.Mutex.Lock()
			r.PreferredPeerOrder = aux
			r.Mutex.Unlock()
}
*/

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

func (r *Replica) SortPeerOrderByLatency() {
	r.Mutex.Lock()

	r.Mutex.Unlock()
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

	//if !r.Alive[r.PreferredPeerOrder[0]] {
	//	panic("why sorted dead process to top of list")
	//}
	/*npings := 20

	for j := 0; j < npings; j++ {
		for i := int32(0); i < int32(r.N); i++ {
			if i == r.Id {
				continue
			}
			r.Mutex.Lock()
			if r.Alive[i] {
				r.Mutex.Unlock()
				r.SendBeacon(i)
			} else {
				r.ReplicasLatenciesOrders[i] = math.MaxInt64
				r.Mutex.Unlock()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	quorum := make([]int32, r.N)

	r.Mutex.Lock()
	for i := int32(0); i < int32(r.N); i++ {
		pos := 0
		for j := int32(0); j < int32(r.N); j++ {
			if (r.ReplicasLatenciesOrders[j] < r.ReplicasLatenciesOrders[i]) || ((r.ReplicasLatenciesOrders[j] == r.ReplicasLatenciesOrders[i]) && (j < i)) {
				pos++
			}
		}
		quorum[pos] = int32(i)
	}
	r.Mutex.Unlock()

	r.UpdatePreferredPeerOrder(quorum)

	for i := 0; i < r.N-1; i++ {
		node := r.PreferredPeerOrder[i]
		lat := float64(r.ReplicasLatenciesOrders[node]) / float64(npings*1000000)
		log.Println(node, " -> ", lat, "ms")
	}
	*/
}

func (r *Replica) updateLatencyRanks(rid int, gbeaconReply genericsmrproto.BeaconReply) {

	r.Ewma[rid] = (1-r.ewmaWeight)*r.Ewma[rid] + r.ewmaWeight*float64(time.Now().UnixNano()-gbeaconReply.Timestamp)
	sort.Slice(r.ReplicasLatenciesOrders, func(i, j int) bool {
		return r.Ewma[i] < r.Ewma[j]
	})
}
