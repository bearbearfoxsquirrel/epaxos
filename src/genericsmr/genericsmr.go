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
	"math"
	"math/rand"
	"mathextra"
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

	Durable bool // log to a stable store?

	StableStorage *os.File // file support for the persistent log

	PreferredPeerOrder []int32 // replicas in the preferred order of communication

	rpcTable   map[uint8]*RPCPair
	maxRpcCode uint8
	//	rpcCodes   map[reflect.Type]uint8

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
	//EwmaSent          []float
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

func (r *Replica) RegisterRPC(msgObj fastrpc.Serializable, notify chan fastrpc.Serializable) uint8 {
	//	r.Mutex.Lock()
	code := r.maxRpcCode
	r.maxRpcCode++
	r.rpcTable[code] = &RPCPair{msgObj, notify}
	//r.rpcCodes[reflect.TypeOf(msgObj)] = code
	dlog.Println("registering RPC ", r.maxRpcCode)
	//	r.Mutex.Unlock()
	return code
}

func (r *Replica) CalculateAlive() {
	r.Mutex.Lock()
	r.calcAliveInternal()
	r.Mutex.Unlock()
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
	defer r.Mutex.Unlock()

	w := r.PeerWriters[peerId]
	if w == nil {
		log.Printf("Connection to %d lost!\n", peerId)
		return
	}

	if code == 0 {
		panic("bad rpc code")
	}
	if _, okie := r.rpcTable[code]; !okie {
		panic("waaaaaaaa")
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
}

func (r *Replica) SendMsgUNSAFE(peerId int32, code uint8, msg fastrpc.Serializable) {
	w := r.PeerWriters[peerId]
	if w == nil {
		log.Printf("Connection to %d lost!\n", peerId)
		return
	}

	if code == 0 {
		panic("bad rpc code")
	}
	if _, okie := r.rpcTable[code]; !okie {
		panic("waaaaaaaa")
	}
	w.WriteByte(code)
	msg.Marshal(w)

	if r.checkAndHandleBatchMessaging(peerId) {
		return
	}

	w.Flush()
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

	// for inital tell to track

	//r.lastSentBeacons[t]

	//r.EwmaSent[peerId] = mathextra.EwmaAdd(r.EwmaSent[peerId], r.ewmaWeight, float64(t))

	// if there is a signal to add new one not responded, delete oldest

	//r.lastSentBeacons[peerId][t] = beaconresp{
	//	got:  0,
	//	send: t,
	//}
	//if len(r.lastSentBeacons[peerId]) > 20 {
	//	i := <-r.toRemoveI[peerId]
	//	delete(r.lastSentBeacons[peerId], i)
	//}
	//r.toRemoveI[peerId] <- t // (i + 1) % 20

	//l := 0
	//for j := 0; j < 20; j++ {
	//	r.lastSentBeacons[peerId][j] =
	//}
	//r.curBeaconsSentLat[peerId] =

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

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, lread bool, dreply bool, failures int, storageParentDir string, deadTime int32, batchFlush bool, batchWait time.Duration) *Replica {
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
		//for j := 0; j < slidingW; j++ {
		//r.toRemoveI[i] <-
		//}

		if r.Id == i {
			r.Alive[i] = true
		}
		r.Ewma[i] = 0.0
		r.ReplicasLatenciesOrders[i] = i
		//r.mostRecentRepliedBeacon[i] = //math.MaxInt64
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

		//go func() {
		//	for !r.Shutdown {
		//		time.Sleep(r.batchWait)
		//		for i := int32(0); i < int32(len(r.ClientsWriters)); i++ {
		//			r.ClientFlushFBuffer(i)
		//		}
		//	}
		//}()
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

func (r *Replica) updateLatencyWithReply(rid int, gbeaconReply genericsmrproto.BeaconReply) {
	//if _, e := r.lastSentBeacons[rid][gbeaconReply.Timestamp]; e {
	//	r.lastSentBeacons[rid][gbeaconReply.Timestamp] = beaconresp{
	//		got:  t,
	//		send: gbeaconReply.Timestamp,
	//	}
	//}
	//r.mostRecentRepliedBeacon[rid].got-r.mostRecentRepliedBeacon[rid].send
	t := time.Now().UnixNano()
	r.Ewma[rid] = mathextra.EwmaAdd(r.Ewma[rid], r.ewmaWeight, float64(t-gbeaconReply.Timestamp)) //(1-r.ewmaWeight)*r.Ewma[rid] + r.ewmaWeight*)
	if gbeaconReply.Timestamp < r.mostRecentRepliedBeacon[rid].send {
		return
	}
	r.mostRecentRepliedBeacon[rid] = beaconresp{
		got:  t,
		send: gbeaconReply.Timestamp,
	}
	//
	//r.mostRecentRepliedBeacon[rid]
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
		//peers[i] = i
		peers = append(peers, i)
	}

	if true {

	}
	now := time.Now().UnixNano()
	pl := peers[1:]

	//latencies := make([]int64, r.N)
	//for p := 0; p < r.N; p++ {
	//	if int32(p) == r.Id {
	//		continue
	//	}
	//	for _, beac := range r.lastSentBeacons[p] {
	//		//for j := 0; j < 20; j++ {
	//		recv := beac.got
	//		if recv == 0 {
	//			recv = now
	//		}
	//		latencies[p] += recv - beac.send
	//	}
	//	latencies[p] = latencies[p] / 20
	//}

	sort.Slice(pl, func(i, j int) bool {
		lhs := r.Ewma[pl[i]]*(1-r.ewmaWeight) + float64(now-r.mostRecentRepliedBeacon[pl[i]].send)*r.ewmaWeight
		rhs := r.Ewma[pl[j]]*(1-r.ewmaWeight) + float64(now-r.mostRecentRepliedBeacon[pl[j]].send)*r.ewmaWeight
		return lhs < rhs //now-r.mostRecentRepliedBeacon[pl[i]] < now-r.mostRecentRepliedBeacon[pl[j]]
	})
	////
	//lat := make([]float64, r.N)
	//for i := 0; i < r.N; i++ {
	//	lat[i] = r.Ewma[peers[i]]*(1-r.ewmaWeight) + float64(now-r.mostRecentRepliedBeacon[peers[i]].send)*r.ewmaWeight
	//}
	//dlog.AgentPrintfN(r.Id, "for this we send to %v", peers)
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
