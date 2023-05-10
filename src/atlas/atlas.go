package atlas

import (
	"encoding/binary"
	"epaxos/CommitExecutionComparator"
	"epaxos/dlog"
	"epaxos/epaxosproto"
	"epaxos/fastrpc"
	"epaxos/genericsmr"
	"epaxos/genericsmrproto"
	"epaxos/state"
	"io"
	"sync"
	"time"
)

const MAX_INSTANCE = 10 * 1024 * 1024

const MAX_DEPTH_DEP = 10
const TRUE = uint8(1)
const FALSE = uint8(0)
const ADAPT_TIME_SEC = 10

const COMMIT_GRACE_PERIOD = 10 * 1e9 // 10 second(s)

const BF_K = 4
const BF_M_N = 32.0

const HT_INIT_SIZE = 200000

// FIXME main differences with the original code base
// - fix N=3 case
// - add vbal variable (TLA spec. is wrong)
// - remove checkpoints (need to fix them first)
// - remove short commits (with N>7 propagating committed dependencies is necessary)
// - must run with thriftiness on (recovery is incorrect otherwise)
// - when conflicts are transitive skip waiting prior commuting commands

// todo for atlas
// change message names
// change quorums
// change dependency calculation code -- find f+1 messages that have same deps - if f+1 responses have the same deps, then we can take fast path
// change the deps sent to

// Let X1,...,X2 f+1 be 2 f + 1 sets. Let popular(X1,...,X2 f+1) = {x | x appears in at least f +1 of the sets}
// That is, the proposer takes the fast path only if every dependency vy computed by any of the dependency service nodes was computed by a majority of the dependency service nodes.

// not todo
// change recovery protocol
// change execution protocol - its only simpler

// init deps := commands that are currently in the start phase conflicts(c) = {id \notin start : conflict(c, cmd[id])}.

// we want a list of the last instance for each replica that conflicts with our all commands in the current one
var cpMarker []state.Command
var cpcounter = 0

type ToRecord struct {
	CommitExecutionComparator.InstanceID
	time.Time
}

type Replica struct {
	*genericsmr.Replica
	prepareChan           chan fastrpc.Serializable
	preAcceptChan         chan fastrpc.Serializable
	acceptChan            chan fastrpc.Serializable
	commitChan            chan fastrpc.Serializable
	prepareReplyChan      chan fastrpc.Serializable
	preAcceptReplyChan    chan fastrpc.Serializable
	preAcceptOKChan       chan fastrpc.Serializable
	acceptReplyChan       chan fastrpc.Serializable
	tryPreAcceptChan      chan fastrpc.Serializable
	tryPreAcceptReplyChan chan fastrpc.Serializable
	prepareRPC            uint8
	prepareReplyRPC       uint8
	preAcceptRPC          uint8
	preAcceptReplyRPC     uint8
	acceptRPC             uint8
	acceptReplyRPC        uint8
	commitRPC             uint8
	tryPreAcceptRPC       uint8
	tryPreAcceptReplyRPC  uint8
	InstanceSpace         [][]*Instance // the space of all instances (used and not yet used)
	crtInstance           []int32       // highest active instance numbers that this replica knows about
	CommittedUpTo         []int32       // highest committed instance per replica that this replica knows about
	ExecedUpTo            []int32       // instance up to which all commands have been executed (including iteslf)
	exec                  *Exec
	conflicts             []map[state.Key]int32
	//maxSeqPerKey          map[state.Key]int32
	maxSeq             int32
	latestCPReplica    int32
	latestCPInstance   int32
	clientMutex        *sync.Mutex // for synchronizing when sending replies to clients from multiple go-routines
	instancesToRecover chan *instanceId
	IsLeader           bool // does this replica think it is the leader
	maxRecvBallot      int32
	batchWait          int
	//transconf             bool
	emulatedSS        bool
	emulatedWriteTime time.Duration
	cmpCommitAndExec  bool
	commitExecComp    *CommitExecutionComparator.CommitExecutionComparator
	commitExecMutex   *sync.Mutex
	sepExecThread     bool
	sendToFastestQrm  bool
}

func (r *Replica) CloseUp() {
	//panic("implement me")
}

type Instance struct {
	Cmds        []state.Command
	bal, vbal   int32
	Status      int8
	Deps        []int32
	lb          *LeaderBookkeeping
	proposeTime int64
	Exec        struct {
		Index, Lowlink int
		id             *instanceId
	}
}

type instanceId struct {
	replica  int32
	instance int32
}

type LeaderBookkeeping struct {
	clientProposals []*genericsmr.Propose
	ballot          int32
	allEqual        bool
	preAcceptOKs    int
	acceptOKs       int
	nacks           int
	initialDeps     []int32
	committedDeps   []int32
	prepareReplies  []*epaxosproto.PrepareReply
	preparing       bool
	possibleQuorum  []bool
	tpaReps         int
	tpaAccepted     bool
	lastTriedBallot int32
	cmds            []state.Command
	status          int8
	mergeddeps      []int32
	depsMap         []map[int32]int32
}

func (b *LeaderBookkeeping) addReportedDeps(deps []int32, rid int32) {
	for l := int32(0); l < int32(len(deps)); l++ {
		dep := deps[l]
		if _, e := b.depsMap[l][dep]; !e {
			b.depsMap[l][dep] = 1
			continue
		}
		b.depsMap[l][dep] = b.depsMap[l][dep] + 1
	}
}

func NewReplica(replica *genericsmr.Replica, peerAddrList []string, thrifty bool, exec bool, lread bool, dreply bool, beacon bool, durable bool, batchWait int, transconf bool, emulatedSS bool, emulatedWriteTime time.Duration, cmpCommitExec bool, cmpCommitExecLoc string, sepExecThread bool, deadTime int32, sendToFastQrm bool) *Replica {
	r := &Replica{
		replica,
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*2),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0, 0, 0, 0,
		make([][]*Instance, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		nil,
		make([]map[state.Key]int32, len(peerAddrList)),
		//make(map[state.Key]int32),
		0,
		0,
		-1,
		new(sync.Mutex),
		make(chan *instanceId, genericsmr.CHAN_BUFFER_SIZE),
		false,
		-1,
		batchWait,
		//transconf,
		emulatedSS,
		emulatedWriteTime,
		cmpCommitExec,
		nil,
		new(sync.Mutex),
		sepExecThread,
		sendToFastQrm}

	r.Beacon = beacon
	r.Durable = durable

	if !thrifty {
		panic("must run with thriftiness on")
	}

	for i := 0; i < r.N; i++ {
		r.InstanceSpace[i] = make([]*Instance, MAX_INSTANCE) // FIXME
		r.crtInstance[i] = -1
		r.ExecedUpTo[i] = -1
		r.CommittedUpTo[i] = -1
		r.conflicts[i] = make(map[state.Key]int32, HT_INIT_SIZE)
	}

	r.exec = &Exec{r, make([]*Instance, 100)}
	r.Exec = exec

	cpMarker = make([]state.Command, 0)

	//register RPCs
	r.prepareRPC = r.RegisterRPC(new(epaxosproto.Prepare), r.prepareChan)
	r.prepareReplyRPC = r.RegisterRPC(new(epaxosproto.PrepareReply), r.prepareReplyChan)
	r.preAcceptRPC = r.RegisterRPC(new(epaxosproto.PreAccept), r.preAcceptChan)
	r.preAcceptReplyRPC = r.RegisterRPC(new(epaxosproto.PreAcceptReply), r.preAcceptReplyChan)
	r.acceptRPC = r.RegisterRPC(new(epaxosproto.Accept), r.acceptChan)
	r.acceptReplyRPC = r.RegisterRPC(new(epaxosproto.AcceptReply), r.acceptReplyChan)
	r.commitRPC = r.RegisterRPC(new(epaxosproto.Commit), r.commitChan)
	r.tryPreAcceptRPC = r.RegisterRPC(new(epaxosproto.TryPreAccept), r.tryPreAcceptChan)
	r.tryPreAcceptReplyRPC = r.RegisterRPC(new(epaxosproto.TryPreAcceptReply), r.tryPreAcceptReplyChan)

	r.Stats.M["weird"], r.Stats.M["conflicted"], r.Stats.M["slow"], r.Stats.M["fast"], r.Stats.M["totalCommitTime"], r.Stats.M["totalBatching"], r.Stats.M["totalBatchingSize"] = 0, 0, 0, 0, 0, 0, 0

	if cmpCommitExec {
		r.commitExecComp = CommitExecutionComparator.CommitExecutionComparatorNew(cmpCommitExecLoc)
	}

	go r.run()

	return r
}

// append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable || r.emulatedSS {
		return
	}

	b := make([]byte, 9+r.N*4)
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.bal))
	binary.LittleEndian.PutUint32(b[4:8], uint32(inst.vbal))
	b[8] = byte(inst.Status)
	l := 9
	for _, dep := range inst.Deps {
		binary.LittleEndian.PutUint32(b[l:l+4], uint32(dep))
		l += 4
	}
	r.StableStorage.Write(b[:])
}

// write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command) {
	if !r.Durable || r.emulatedSS {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStorage))
	}
}
func (r *Replica) sync() {
	if !r.Durable {
		return
	}
	//log.Println("synced")
	if r.emulatedSS {
		time.Sleep(r.emulatedWriteTime)
	} else {
		_ = r.StableStorage.Sync()
	}
}

/* Clock goroutine */
var fastClockChan chan bool
var slowClockChan chan bool

func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(time.Duration(r.batchWait) * time.Millisecond) // ms
		fastClockChan <- true
	}
}

func (r *Replica) slowClock() {
	for !r.Shutdown {
		time.Sleep(150 * 1e6) // 150 ms
		slowClockChan <- true
	}
}

func (r *Replica) stopAdapting() {
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	r.Beacon = false
	time.Sleep(1000 * 1000 * 1000)
	r.Mutex.Lock()
	for i := 0; i < r.N-1; i++ {
		min := i
		for j := i + 1; j < r.N-1; j++ {
			if r.Ewma[r.PreferredPeerOrder[j]] < r.Ewma[r.PreferredPeerOrder[min]] {
				min = j
			}
		}
		aux := r.PreferredPeerOrder[i]
		r.PreferredPeerOrder[i] = r.PreferredPeerOrder[min]
		r.PreferredPeerOrder[min] = aux
	}
	//log.Println(r.PreferredPeerOrder)
	r.Mutex.Unlock()
}

func (r *Replica) BatchingEnabled() bool {
	return r.batchWait > 0
}

/*
**********************************

	Main event processing loop

***********************************
*/
func (r Replica) replicaLoop() {
	onOffProposeChan := r.ProposeChan

	for !r.Shutdown {
		select {
		case propose := <-onOffProposeChan:
			//got a Propose from a client
			dlog.Println("Handle propose from client now")
			r.handlePropose(propose)
			//deactivate new proposals channel to prioritize the handling of other protocol messages,
			//and to allow commands to accumulate for batching
			if r.BatchingEnabled() {
				onOffProposeChan = nil
			}
			break
		case <-fastClockChan:
			//activate new proposals channel
			onOffProposeChan = r.ProposeChan
			break
		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*epaxosproto.Prepare)
			//got a Prepare message
			dlog.Println("Received Prepare %d.%d w. ballot %d\n", prepare.Replica, prepare.Instance, prepare.Ballot)
			panic("not implemented")
			//r.handlePrepare(prepare)
			break
		case preAcceptS := <-r.preAcceptChan:
			preAccept := preAcceptS.(*epaxosproto.PreAccept)
			//got a PreAccept message
			dlog.Println("Received PreAccept %d.%d w. ballot %d\n", preAccept.Replica, preAccept.Instance, preAccept.Ballot)
			r.handlePreAccept(preAccept)
			break
		case acceptS := <-r.acceptChan:
			accept := acceptS.(*epaxosproto.Accept)
			//got an Accept message
			dlog.Println("Received Accept %d.%d w. ballot %d\n", accept.Replica, accept.Instance, accept.Ballot)
			r.handleAccept(accept)
			break
		case commitS := <-r.commitChan:
			commit := commitS.(*epaxosproto.Commit)
			//got a Commit message
			dlog.Println("Received Commit %d.%d\n", commit.Replica, commit.Instance)
			r.handleCommit(commit)
			break
		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*epaxosproto.PrepareReply)
			//got a Prepare reply
			dlog.Println("Received PrepareReply %d.%d w. ballot %d from %d\n", prepareReply.Replica, prepareReply.Instance, prepareReply.Ballot, prepareReply.AcceptorId)
			panic("not implemented")
			//r.handlePrepareReply(prepareReply)
			break
		case preAcceptReplyS := <-r.preAcceptReplyChan:
			preAcceptReply := preAcceptReplyS.(*epaxosproto.PreAcceptReply)
			//got a PreAccept reply
			dlog.Println("Received PreAcceptReply %d.%d w. ballot %d\n", preAcceptReply.Replica, preAcceptReply.Instance, preAcceptReply.Ballot)
			r.handlePreAcceptReply(preAcceptReply)
			break
		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*epaxosproto.AcceptReply)
			//got an Accept reply
			dlog.Println("Received AcceptReply %d.%d w. ballot %d\n", acceptReply.Replica, acceptReply.Instance, acceptReply.Ballot)
			r.handleAcceptReply(acceptReply)
			break
		case beacon := <-r.BeaconChan:
			dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
			r.ReplyBeacon(beacon)
			break
		case <-slowClockChan:
			if r.Beacon {
				dlog.Printf("weird %d; conflicted %d; slow %d; fast %d\n", r.Stats.M["weird"], r.Stats.M["conflicted"], r.Stats.M["slow"], r.Stats.M["fast"])
				for q := int32(0); q < int32(r.N); q++ {
					if q == r.Id {
						continue
					}
					r.SendBeacon(q)
				}
			}
			break
			//case <-r.instancesToRecover:
			//	panic("not implemented")
			//r.startRecoveryForInstance(iid.replica, iid.instance)
		}
	}
}

func (r *Replica) run() {
	r.ConnectToPeers()

	r.RandomisePeerOrder()

	if r.Exec && r.sepExecThread {
		go r.executeCommands()
	}

	slowClockChan = make(chan bool, 1)
	fastClockChan = make(chan bool, 1)
	go r.slowClock()

	//Enabled fast clock when batching
	if r.BatchingEnabled() {
		go r.fastClock()
	}

	//if r.Beacon {
	//	go r.stopAdapting()
	//}

	go r.WaitForClientConnections()
	go r.replicaLoop()
}

/***********************************
  Command execution thread        *
************************************/

func (r *Replica) executeCommands() {
	const SLEEP_TIME_NS = 1e6
	problemInstance := make([]int32, r.N)
	timeout := make([]uint64, r.N)
	for q := 0; q < r.N; q++ {
		problemInstance[q] = -1
		timeout[q] = 0
	}

	for !r.Shutdown {
		executed := false
		for q := int32(0); q < int32(r.N); q++ {
			for inst := r.ExecedUpTo[q] + 1; inst <= r.crtInstance[q]; inst++ {
				if r.InstanceSpace[q][inst] != nil && r.InstanceSpace[q][inst].Status == epaxosproto.EXECUTED {
					if inst == r.ExecedUpTo[q]+1 {
						r.ExecedUpTo[q] = inst
					}
					continue
				}
				if r.InstanceSpace[q][inst] == nil || r.InstanceSpace[q][inst].Status < epaxosproto.COMMITTED || r.InstanceSpace[q][inst].Cmds == nil {
					if inst == problemInstance[q] {
						timeout[q] += SLEEP_TIME_NS
						if timeout[q] >= COMMIT_GRACE_PERIOD {
							for k := problemInstance[q]; k <= r.crtInstance[q]; k++ {
								dlog.Printf("Recovering instance %d.%d", q, k)
								r.instancesToRecover <- &instanceId{q, k}
							}
							timeout[q] = 0
						}
					} else {
						problemInstance[q] = inst
						timeout[q] = 0
					}
					break
				}
				if ok := r.exec.executeCommand(int32(q), inst); ok {
					executed = true
					if inst == r.ExecedUpTo[q]+1 {
						r.ExecedUpTo[q] = inst
					}
				}
			}
		}
		if !executed {
			r.Mutex.Lock()
			r.Mutex.Unlock() // FIXME for cache coherence
			time.Sleep(SLEEP_TIME_NS)
		}
	}
}

/* Ballot helper functions */
func isInitialBallot(ballot int32, replica int32, instance int32) bool {
	return ballot == replica
}

func (r *Replica) makeBallot(replica int32, instance int32) {
	lb := r.InstanceSpace[replica][instance].lb
	n := r.Id
	if r.Id != replica {
		n += int32(r.N)
	}
	if r.IsLeader {
		for n < r.maxRecvBallot {
			n += int32(r.N)
		}
	}
	lb.lastTriedBallot = n
	dlog.Printf("Last tried ballot in %d.%d is %d\n", replica, instance, lb.lastTriedBallot)
}

/**********************************************************************
                   inter-replica communication
***********************************************************************/

func (r *Replica) replyPrepare(replicaId int32, reply *epaxosproto.PrepareReply) {
	dlog.Printf("Sending PrepareReply %d.%d w. ballot=%d, status=%d to %d\n", reply.Replica, reply.Instance, reply.Ballot, reply.Status, replicaId)
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyPreAccept(replicaId int32, reply *epaxosproto.PreAcceptReply) {
	dlog.Printf("Sending ReplyPreAccept %d.%d w. ballot=%d, deps=%d, seq=%d, committedDeps=%d to %d\n", reply.Replica, reply.Instance, reply.Ballot, reply.Deps, reply.Seq, reply.CommittedDeps, replicaId)
	r.SendMsg(replicaId, r.preAcceptReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *epaxosproto.AcceptReply) {
	dlog.Printf("Sending AcceptReply %d.%d w. ballot=%d to %d\n", reply.Replica, reply.Instance, reply.Ballot, replicaId)
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

func (r *Replica) replyTryPreAccept(replicaId int32, reply *epaxosproto.TryPreAcceptReply) {
	dlog.Printf("Sending TryPreAcceptReply %d.%d w. ballot=%d to %d\n", reply.Replica, reply.Instance, reply.Ballot, replicaId)
	r.SendMsg(replicaId, r.tryPreAcceptReplyRPC, reply)
}

func (r *Replica) bcastPrepare(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	args := &epaxosproto.Prepare{r.Id, replica, instance, lb.lastTriedBallot}
	//r.CalculateAlive()
	var order []int32
	if r.sendToFastestQrm {
		order = r.GetPeerOrderLatency()
	} else {
		order = r.GetRandomPeerOrder()
	}
	if len(order) < r.SlowQuorumSize() {
		r.SendToGroup(order, r.prepareRPC, args)
		//n := r.N - 1
		//q := r.id
		//for sent := 0; sent < n; {
		//	q = (q + 1) % int32(r.N)
		//	if q == r.id {
		//		dlog.Printf("Not enough replicas alive! %v", r.Alive)
		//		break
		//	}
		//	if !r.Alive[q] {
		//		continue
		//	}
		//	dlog.Printf("Sending Prepare %d.%d w. ballot %d to %d\n", replica, instance, lb.lastTriedBallot, q)
		//	r.SendMsg(q, r.prepareRPC, args)
		//	sent++
		//}
	} else {
		r.SendToGroup(order[:r.SlowQuorumSize()], r.prepareRPC, args)
	}

}

func (r *Replica) FastQrmSize() int {
	return r.N/2 + r.F
}

func (r *Replica) SlowQrmSize() int {
	return r.F + 1
}

func (r *Replica) bcastPreAccept(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("PreAccept bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	pa := new(epaxosproto.PreAccept)
	pa.LeaderId = r.Id
	pa.Replica = replica
	pa.Instance = instance
	pa.Ballot = lb.lastTriedBallot
	pa.Command = lb.cmds
	pa.Deps = lb.initialDeps

	var order []int32
	if r.sendToFastestQrm {
		order = r.GetPeerOrderLatency()
	} else {
		order = r.GetRandomPeerOrder()
	}
	if len(order) < r.FastQrmSize() {
		r.SendToGroup(order, r.preAcceptRPC, pa)
	} else {
		println("sending to %v", order[:r.FastQrmSize()])
		r.SendToGroup(order[:r.FastQrmSize()], r.preAcceptRPC, pa)
	}
}

func (r *Replica) bcastAccept(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Accept bcast failed:", err)
		}
	}()

	lb := r.InstanceSpace[replica][instance].lb
	ea := new(epaxosproto.Accept)
	ea.LeaderId = r.Id
	ea.Replica = replica
	ea.Instance = instance
	ea.Ballot = lb.lastTriedBallot
	ea.Deps = lb.mergeddeps

	var order []int32
	if r.sendToFastestQrm {
		order = r.GetPeerOrderLatency()
	} else {
		order = r.GetRandomPeerOrder()
	}

	if len(order) < r.SlowQrmSize() {
		n := r.SlowQrmSize()
		order = order[:n]
		dlog.Printf("Sending Accept %d.%d w. ballot %d \n", replica, instance, lb.lastTriedBallot)
		r.SendToGroup(order, r.acceptRPC, ea)
	} else {
		r.SendToGroup(order[:r.SlowQrmSize()], r.acceptRPC, ea)
	}

}

func (r *Replica) bcastCommit(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Commit bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	ec := new(epaxosproto.Commit)
	ec.LeaderId = r.Id
	ec.Replica = replica
	ec.Instance = instance
	ec.Command = lb.cmds
	ec.Deps = lb.mergeddeps
	ec.Ballot = lb.ballot

	//	r.CalculateAlive()
	//	r.RandomisePeerOrder()
	peers := r.GetRandomPeerOrder()
	r.SendToGroup(peers, r.commitRPC, ec)
	//for q := 0; q < r.N-1; q++ {
	//
	////	if !r.Alive[r.PreferredPeerOrder[q]] {
	////		continue
	////	}
	//	dlog.Printf("Sending Commit %d.%d to %d\n", replica, instance, r.PreferredPeerOrder[q])
	//	r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, ec)
	//}
}

/******************************************************************
              Helper functions
*******************************************************************/

func (r *Replica) clearHashtables() {
	for q := 0; q < r.N; q++ {
		r.conflicts[q] = make(map[state.Key]int32, HT_INIT_SIZE)
	}
}

func (r *Replica) updateCommitted(replica int32) {
	r.Mutex.Lock()
	for r.InstanceSpace[replica][r.CommittedUpTo[replica]+1] != nil &&
		(r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == epaxosproto.COMMITTED ||
			r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == epaxosproto.EXECUTED) {

		r.CommittedUpTo[replica] = r.CommittedUpTo[replica] + 1
	}
	r.Mutex.Unlock()
}

func (r *Replica) updateConflicts(cmds []state.Command, replica int32, instance int32) {
	for i := 0; i < len(cmds); i++ {
		if d, present := r.conflicts[replica][cmds[i].K]; present {
			if d < instance {
				r.conflicts[replica][cmds[i].K] = instance
			}
		} else {
			r.conflicts[replica][cmds[i].K] = instance
		}
	}
}

func (r *Replica) updateAttributes(cmds []state.Command, deps []int32, replica int32, instance int32) ([]int32, bool) {
	changed := false
	for rid := 0; rid < r.N; rid++ {
		if r.Id != replica && int32(rid) == replica { // why?????????????????????????????????????????????????????????????????????????????/
			// does this not mean that we don't conflict with our own commands?????
			continue
		}
		for ci := 0; ci < len(cmds); ci++ {
			if d, present := (r.conflicts[rid])[cmds[ci].K]; present {
				if d > deps[rid] {
					deps[rid] = d
					changed = true
					break
				}
			}
		}
	}
	return deps, changed
}

/**********************************************************************

                           PHASE 1

***********************************************************************/

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	//TODO!! Handle client retries
	batchSize := len(r.ProposeChan) + 1
	r.Mutex.Lock()
	r.Stats.M["totalBatching"]++
	r.Stats.M["totalBatchingSize"] += batchSize
	r.Mutex.Unlock()

	r.crtInstance[r.Id]++

	dlog.Printf("Starting %d.%d w. %s (batch=%d)\n", r.Id, r.crtInstance[r.Id], propose.Command.String(), batchSize)

	cmds := make([]state.Command, batchSize)
	proposals := make([]*genericsmr.Propose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose
	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}

	r.startPhase1(cmds, r.Id, r.crtInstance[r.Id], r.Id, proposals)

	cpcounter += len(cmds)

}

func (r *Replica) startPhase1(cmds []state.Command, replica int32, instance int32, ballot int32, proposals []*genericsmr.Propose) {
	deps := make([]int32, r.N)
	for q := 0; q < r.N; q++ {
		deps[q] = -1
	}
	deps, _ = r.updateAttributes(cmds, deps, replica, instance)
	committedDeps := make([]int32, r.N)
	for i := 0; i < r.N; i++ {
		committedDeps[i] = -1
	}

	inst := r.newInstance(replica, instance, cmds, ballot, ballot, epaxosproto.PREACCEPTED, deps)
	r.InstanceSpace[replica][instance] = inst

	r.updateConflicts(cmds, replica, instance)

	inst.lb = r.newLeaderBookkeeping(proposals, deps, committedDeps, deps, ballot, cmds, epaxosproto.PREACCEPTED)

	r.recordInstanceMetadata(r.InstanceSpace[r.Id][instance])
	r.recordCommands(cmds)
	r.sync()

	dlog.Printf("Phase1Start in %d.%d w. (ballot=%d, deps=%d)\n", replica, instance, ballot, deps)
	r.bcastPreAccept(replica, instance)

}

func (r *Replica) handlePreAccept(preAccept *epaxosproto.PreAccept) {
	inst := r.InstanceSpace[preAccept.Replica][preAccept.Instance]

	if preAccept.Instance > r.crtInstance[preAccept.Replica] {
		r.crtInstance[preAccept.Replica] = preAccept.Instance
		dlog.Printf("New crtInstance %d.%d", preAccept.Replica, preAccept.Instance)
	}

	if inst == nil {
		inst = r.newInstanceDefault(preAccept.Replica, preAccept.Instance)
		r.InstanceSpace[preAccept.Replica][preAccept.Instance] = inst
	}

	// differ from original code: follow TLA
	if inst != nil && preAccept.Ballot < inst.bal {
		dlog.Printf("Smaller ballot %d < %d\n", preAccept.Ballot, inst.bal)
		dlog.Printf("Current state deps=%d, committedUpTo=%d\n", inst.Deps, r.CommittedUpTo)
		return
	}

	inst.bal = preAccept.Ballot

	// differ from original code: TLA fix
	if inst.Status >= epaxosproto.ACCEPTED {
		//reordered handling of commit/accept and pre-accept
		if inst.Cmds == nil {
			panic("this shouldn't happen????")
			r.InstanceSpace[preAccept.LeaderId][preAccept.Instance].Cmds = preAccept.Command
			r.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance)
			r.recordCommands(preAccept.Command)
			r.sync()
		}
	} else {
		deps, changed := r.updateAttributes(preAccept.Command, preAccept.Deps, preAccept.Replica, preAccept.Instance)
		status := epaxosproto.PREACCEPTED_EQ
		if changed {
			status = epaxosproto.PREACCEPTED
		}
		//	initialDeps := make([]int32, len(inst.Deps))
		//	copy(initialDeps, inst.Deps)
		inst.Cmds = preAccept.Command
		inst.Deps = deps
		inst.bal = preAccept.Ballot
		inst.vbal = preAccept.Ballot
		inst.Status = status

		r.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance)
		r.recordInstanceMetadata(r.InstanceSpace[preAccept.Replica][preAccept.Instance])
		r.recordCommands(preAccept.Command)
		r.sync()
	}

	reply := &epaxosproto.PreAcceptReply{
		preAccept.Replica,
		preAccept.Instance,
		inst.bal,
		inst.vbal,
		-1,
		inst.Deps,
		r.CommittedUpTo,
		inst.Status}
	r.replyPreAccept(preAccept.LeaderId, reply)
}

func (r *Replica) handlePreAcceptReply(pareply *epaxosproto.PreAcceptReply) {
	inst := r.InstanceSpace[pareply.Replica][pareply.Instance]
	lb := inst.lb

	if pareply.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = pareply.Ballot
	}

	if lb.lastTriedBallot > pareply.Ballot {
		dlog.Printf("In %d.%d, message in late \n", pareply.Replica, pareply.Instance)
		return
	}

	if lb.lastTriedBallot < pareply.Ballot {
		dlog.Printf("In %d.%d, another active leader w. ballot %d \n", pareply.Replica, pareply.Instance, pareply.Ballot)
		lb.nacks++
		if lb.nacks+1 > r.N>>1 {
			if r.IsLeader {
				r.makeBallot(pareply.Replica, pareply.Instance)
				r.recordInstanceMetadata(inst)
				r.sync()
				dlog.Printf("Retrying with ballot %d \n", lb.lastTriedBallot)
				r.bcastPrepare(pareply.Replica, pareply.Instance)
			}
		}
		return
	}

	if lb.status != epaxosproto.PREACCEPTED && lb.status != epaxosproto.PREACCEPTED_EQ {
		dlog.Printf("In %d.%d, already status=%d \n", pareply.Replica, pareply.Instance, lb.status)
		return
	}

	inst.lb.preAcceptOKs++

	// wouldn't get back a response
	if pareply.VBallot > lb.ballot {
		lb.ballot = pareply.VBallot
		lb.mergeddeps = pareply.Deps
		lb.status = pareply.Status
	}

	isInitialBallot := isInitialBallot(lb.lastTriedBallot, pareply.Replica, pareply.Instance)

	// differ from original code: (r.N <= 3 && !r.Thrifty) not inline with SOSP Section 4.4
	//seq, deps, allEqual := r.mergeAttributes(lb.seq, lb.deps, pareply.Seq, pareply.Deps)
	//if r.N <= 3 && r.Thrifty {
	//	no need to check for equality
	//} else {

	lb.addReportedDeps(pareply.Deps, pareply.Replica)
	// differ from original code: TLA fix
	if lb.status <= epaxosproto.PREACCEPTED_EQ {
		lb.mergeddeps = pareply.Deps
	}

	//precondition := inst.lb.allEqual && allCommitted && isInitialBallot
	if inst.lb.preAcceptOKs < (r.FastQrmSize() - 1) {
		dlog.Printf("Not enough pre-accept replies in %d.%d (preAcceptOk=%d, slowQuorumSize=%d)\n", pareply.Replica, pareply.Instance, lb.preAcceptOKs, r.Replica.SlowQuorumSize())
		return
	}

	//allConfsFAcked := r.CanCommitFastPathDeps(lb.allDeps)
	mergedDeps, fastPathOK := r.GetFinalDeps(lb)
	lb.mergeddeps = mergedDeps
	inst.lb.allEqual = fastPathOK
	if !fastPathOK {
		r.Mutex.Lock()
		r.Stats.M["conflicted"]++
		r.Mutex.Unlock()
	}

	if fastPathOK {
		r.checkAndRecordCommit(pareply.Replica, pareply.Instance)
		dlog.Printf("Fast path %d.%d, w. deps %d\n", pareply.Replica, pareply.Instance, pareply.Deps)
		lb.status = epaxosproto.COMMITTED
		inst.Status = lb.status
		inst.bal = lb.ballot
		inst.Cmds = lb.cmds

		inst.Deps = lb.mergeddeps
		lb.committedDeps = lb.mergeddeps
		r.recordInstanceMetadata(inst)
		r.sync()

		r.updateCommitted(pareply.Replica)
		if inst.lb.clientProposals != nil && !r.Dreply {
			// give clients the all clear
			dlog.Println("Commited value being sent to clients")
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ReplyProposeTS(
					&genericsmrproto.ProposeReplyTS{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL(),
						inst.lb.clientProposals[i].Timestamp},
					inst.lb.clientProposals[i].Reply,
					inst.lb.clientProposals[i].Mutex)
			}
		}

		//fast learn was here
		r.bcastCommit(pareply.Replica, pareply.Instance)

		r.Mutex.Lock()
		r.Stats.M["fast"]++
		if inst.proposeTime != 0 {
			r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
		}
		r.Mutex.Unlock()
		if r.Exec && !r.sepExecThread {
			r.executeCommands()
		}
	} else {
		// } else if inst.lb.preAcceptOKs >= r.N/2 && !precondition {
		dlog.Printf("Slow path %d.%d (inst.lb.fastPathOK=%t,  isInitialBallot=%t)\n", pareply.Replica, pareply.Instance, fastPathOK, isInitialBallot)
		lb.status = epaxosproto.ACCEPTED

		inst.Status = lb.status
		inst.bal = lb.ballot
		inst.Cmds = lb.cmds
		inst.Deps = lb.mergeddeps
		r.recordInstanceMetadata(inst)
		r.recordCommands(inst.Cmds)
		r.sync()

		r.bcastAccept(pareply.Replica, pareply.Instance)

		r.Mutex.Lock()
		r.Stats.M["slow"]++
		r.Mutex.Unlock()
	}
}

/**********************************************************************

                       PHASE 2

***********************************************************************/

func (r *Replica) handleAccept(accept *epaxosproto.Accept) {
	inst := r.InstanceSpace[accept.Replica][accept.Instance]

	if accept.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = accept.Ballot
	}

	if accept.Instance > r.crtInstance[accept.Replica] {
		r.crtInstance[accept.Replica] = accept.Instance
		dlog.Printf("New crtInstance %d.%d", accept.Replica, accept.Instance)
	}

	if inst == nil {
		inst = r.newInstanceDefault(accept.Replica, accept.Instance)
		r.InstanceSpace[accept.Replica][accept.Instance] = inst
	}

	if accept.Ballot < inst.bal {
		dlog.Printf("Smaller ballot %d < %d\n", accept.Ballot, inst.bal)
	} else if inst.Status >= epaxosproto.COMMITTED {
		dlog.Printf("Already committed / executed \n")
	} else {
		inst.Deps = accept.Deps
		//inst.Seq = accept.Seq
		inst.bal = accept.Ballot
		inst.vbal = accept.Ballot
		r.recordInstanceMetadata(r.InstanceSpace[accept.Replica][accept.Instance])
		r.recordCommands(inst.Cmds)
		r.sync()
	}

	reply := &epaxosproto.AcceptReply{accept.Replica, accept.Instance, inst.bal}
	r.replyAccept(accept.LeaderId, reply)

}

func (r *Replica) handleAcceptReply(areply *epaxosproto.AcceptReply) {
	inst := r.InstanceSpace[areply.Replica][areply.Instance]
	lb := inst.lb

	if areply.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = areply.Ballot
	}

	if lb.status != epaxosproto.ACCEPTED {
		// we've move on, these are delayed replies, so just ignore
		dlog.Println("Delayed reply")
		return
	}

	if lb.lastTriedBallot != areply.Ballot {
		dlog.Println("Wrong ballot")
		return
	}

	if areply.Ballot > lb.lastTriedBallot {
		dlog.Printf("In %d.%d, another leader w. ballot %d \n", areply.Replica, areply.Instance, areply.Ballot)
		lb.nacks++
		if lb.nacks+1 > r.N>>1 {
			if r.IsLeader {
				r.makeBallot(areply.Replica, areply.Instance)
				r.recordInstanceMetadata(inst)
				r.sync()
				dlog.Printf("Retrying with ballot %d \n", lb.lastTriedBallot)
				r.bcastPrepare(areply.Replica, areply.Instance)
			}
		}
		return
	}

	inst.lb.acceptOKs++

	if inst.lb.acceptOKs+1 > r.N/2 {
		r.checkAndRecordCommit(areply.Replica, areply.Instance)
		lb.status = epaxosproto.COMMITTED
		inst.Status = epaxosproto.COMMITTED
		r.updateCommitted(areply.Replica)
		r.recordInstanceMetadata(inst)
		r.recordCommands(inst.Cmds)
		r.sync() //is this necessary here?

		if inst.lb.clientProposals != nil && !r.Dreply {
			// give clients the all clear
			dlog.Println("Sending slowly commited value to clients")
			for i := 0; i < len(inst.lb.clientProposals); i++ {

				r.ReplyProposeTS(
					&genericsmrproto.ProposeReplyTS{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL(),
						inst.lb.clientProposals[i].Timestamp},
					inst.lb.clientProposals[i].Reply,
					inst.lb.clientProposals[i].Mutex)
			}
		}

		r.bcastCommit(areply.Replica, areply.Instance)
		r.Mutex.Lock()
		if inst.proposeTime != 0 {
			r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
		}
		r.Mutex.Unlock()
		if r.Exec && !r.sepExecThread {
			r.executeCommands()
		}
	} else {
		dlog.Println("Not enough")
	}
}

/**********************************************************************

                           COMMIT

***********************************************************************/

func (r *Replica) handleCommit(commit *epaxosproto.Commit) {
	inst := r.InstanceSpace[commit.Replica][commit.Instance]

	if commit.Instance > r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance
		dlog.Printf("New crtInstance %d.%d", commit.Replica, commit.Instance)
	}

	if commit.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = commit.Ballot
	}

	if inst == nil {
		r.InstanceSpace[commit.Replica][commit.Instance] = r.newInstanceDefault(commit.Replica, commit.Instance)
		inst = r.InstanceSpace[commit.Replica][commit.Instance]
	}

	if inst.Status >= epaxosproto.COMMITTED {
		dlog.Printf("Already committed \n")
		return
	}

	if commit.Ballot < inst.bal {
		dlog.Printf("Smaller ballot %d < %d\n", commit.Ballot, inst.bal)
		return
	}

	dlog.Printf("Committing %d.%d \n", commit.Replica, commit.Instance)

	// FIXME timeout on client side?
	if commit.Replica == r.Id {
		if len(commit.Command) == 1 && commit.Command[0].Op == state.NONE && inst.lb.clientProposals != nil {
			for _, p := range inst.lb.clientProposals {
				dlog.Printf("In %d.%d, re-proposing %s \n", commit.Replica, commit.Instance, p.Command.String())
				r.ProposeChan <- p
			}
			inst.lb.clientProposals = nil
		}
	}

	inst.bal = commit.Ballot
	inst.vbal = commit.Ballot
	inst.Cmds = commit.Command
	inst.Deps = commit.Deps
	r.checkAndRecordCommit(commit.Replica, commit.Instance)
	inst.Status = epaxosproto.COMMITTED

	r.updateConflicts(commit.Command, commit.Replica, commit.Instance)
	r.updateCommitted(commit.Replica)
	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
	r.recordCommands(commit.Command)
	if r.Exec && !r.sepExecThread {
		r.executeCommands()
	}
}

func (r *Replica) checkAndRecordCommit(log int32, inst int32) {
	if r.cmpCommitAndExec {
		id := CommitExecutionComparator.InstanceID{Log: log, Seq: inst}
		now := time.Now()

		if !r.sepExecThread {
			r.commitExecComp.RecordCommit(id, now)
		} else {
			r.commitExecMutex.Lock()
			r.commitExecComp.RecordCommit(id, now)
			r.commitExecMutex.Unlock()
		}

	}
}

func (r *Replica) newInstanceDefault(replica int32, instance int32) *Instance {
	return r.newInstance(replica, instance, nil, -1, -1, epaxosproto.NONE, nil)
}

func (r *Replica) newInstance(replica int32, instance int32, cmds []state.Command, cballot int32, lballot int32, status int8, deps []int32) *Instance {
	return &Instance{
		Cmds:        cmds,
		bal:         cballot,
		vbal:        lballot,
		Status:      status,
		Deps:        deps,
		lb:          nil,
		proposeTime: time.Now().UnixNano(),
		Exec: struct {
			Index, Lowlink int
			id             *instanceId
		}{
			Index:   0,
			Lowlink: 0,
			id: &instanceId{
				replica:  replica,
				instance: instance,
			},
		},
	}
}

func (r *Replica) newLeaderBookkeepingDefault() *LeaderBookkeeping {
	return r.newLeaderBookkeeping(nil, r.newNilDeps(), r.newNilDeps(), r.newNilDeps(), 0, nil, epaxosproto.NONE)
}

func (r *Replica) newLeaderBookkeeping(p []*genericsmr.Propose, originalDeps []int32, committedDeps []int32, deps []int32, lastTriedBallot int32, cmds []state.Command, status int8) *LeaderBookkeeping {
	lb := &LeaderBookkeeping{p, -1, true, 0, 0, 0, originalDeps, committedDeps, nil, false, make([]bool, r.N), 0, false, lastTriedBallot, cmds, status, make([]int32, r.N), make([]map[int32]int32, r.N)}
	for l := 0; l < len(lb.depsMap); l++ {
		lb.depsMap[l] = make(map[int32]int32)
	}
	return lb
}

func (r *Replica) newNilDeps() []int32 {
	nildeps := make([]int32, r.N)
	for i := 0; i < r.N; i++ {
		nildeps[i] = -1
	}
	return nildeps
}

func (r *Replica) GetFinalDeps(bookkeeping *LeaderBookkeeping) ([]int32, bool) { // []instance
	// for each rid, check the max instance with f responses
	// as far as I am aware this is the implementation of deps for when we execute all previous instances to max one reported as dep
	// this limits the dep list to only n deps.
	// at least one dep will have f reporting due to quorums.
	fDeps := make([]int32, r.N)
	fastPath := true
	for l := 0; l < len(bookkeeping.depsMap); l++ {
		max := int32(-1)
		for i, count := range bookkeeping.depsMap[l] {
			if count <= int32(r.F) {
				fastPath = false
				continue
			}
			if i < max {
				continue
			}
			max = i
		}
		fDeps[l] = max
	}
	return fDeps, fastPath
}

//func (r *Replica) CanCommitFastPathDeps(deps map[instanceId]int32) bool {
//	for _, count := range deps {
//		if count < int32(r.F) {
//			return false
//		}
//	}
//	return true
//}
