package lwc

import (
	"dlog"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"lwcproto"
	"math"
	"math/rand"
	"state"
	"time"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

const COMMIT_GRACE_PERIOD = 10 * 1e9 // 10 second(s)
const SLEEP_TIME_NS = 1e6

type RetryInfo struct {
	backedoff        bool
	InstToPrep       int32
	attemptedConfBal lwcproto.ConfigBal
	backoffus        int32
}

type BackoffInfo struct {
	minBackoff     int32
	maxInitBackoff int32
	maxBackoff     int32
}

type ProposalTuples struct {
	cmd      []state.Command
	proposal []*genericsmr.Propose
}

type ProposalInfo struct {
	inst             int32
	proposingConfBal lwcproto.ConfigBal
}
type Replica struct {
	*genericsmr.Replica // extends a generic Paxos replica
	configChan          chan fastrpc.Serializable
	prepareChan         chan fastrpc.Serializable
	acceptChan          chan fastrpc.Serializable
	commitChan          chan fastrpc.Serializable
	commitShortChan     chan fastrpc.Serializable
	prepareReplyChan    chan fastrpc.Serializable
	acceptReplyChan     chan fastrpc.Serializable
	//instancesToRecover  chan int32
	prepareRPC          uint8
	acceptRPC           uint8
	commitRPC           uint8
	commitShortRPC      uint8
	prepareReplyRPC     uint8
	acceptReplyRPC      uint8
	instanceSpace       []*Instance // the space of all instances (used and not yet used)
	crtInstance         int32       // highest active instance number that this replica knows about
	crtConfig           int32
	Shutdown            bool
	counter             int
	flush               bool
	executedUpTo        int32
	batchWait           int
	maxBalInc           int16
	maxBeganInstances   int32
	beginNew            chan struct{}
	crtOpenedInstances  []int32
	proposableInstances chan ProposalInfo
	valuesToPropose     chan ProposalTuples
	noopInstance        chan ProposalInfo
	noopWaitUs          int32
	retryInstance       chan RetryInfo
	BackoffManager      BackoffManager
	alwaysNoop          bool
}

type ProposerStatus int
type AcceptorStatus int

const (
	NOT_BEGUN ProposerStatus = iota
	BACKING_OFF
	PREPARING
	READY_TO_PROPOSE
	PROPOSING
	CLOSED
)

const (
	NOT_STARTED AcceptorStatus = iota
	PREPARED
	ACCEPTED
	COMMITTED
)

type AcceptorBookkeeping struct {
	status   AcceptorStatus
	cmds     []state.Command
	curBal   lwcproto.Ballot
	vConfBal lwcproto.ConfigBal
}

type Instance struct {
	abk *AcceptorBookkeeping
	pbk *ProposingBookkeeping
}

func (q QuorumInfo) quorumCount() int32 {
	return int32(len(q.aids))
}

func (q *QuorumInfo) quorumClear() {
	q.aids = make(map[int32]struct{})
}

func NewQuorumInfo(qType QuorumType) *QuorumInfo {
	qrmInfo := new(QuorumInfo)
	*qrmInfo = QuorumInfo{
		qrmType: qType,
		//	oks:     0,
		nacks: 0,
		aids:  make(map[int32]struct{}),
	}
	return qrmInfo
}

func (q *QuorumInfo) quorumAdd(aid int32) {
	if _, exists := q.aids[aid]; exists {
		dlog.Println("Already added to quorum")
		return
	}
	q.aids[aid] = struct{}{} // assumes that only correct aids are added
}

type QuorumType int

const (
	PROMISE QuorumType = iota
	ACCEPTANCE
)

type QuorumInfo struct {
	qrmType QuorumType
	oks     int
	nacks   int
	aids    map[int32]struct{}
}

type ProposingBookkeeping struct {
	status             ProposerStatus
	proposalInfos      map[lwcproto.ConfigBal]*QuorumInfo
	maxKnownBal        lwcproto.Ballot
	maxAcceptedConfBal lwcproto.ConfigBal // highest maxAcceptedConfBal at which a command was accepted
	cmds               []state.Command    // the accepted command
	propCurConfBal     lwcproto.ConfigBal // highest this replica maxAcceptedConfBal tried so far
	clientProposals    []*genericsmr.Propose
}

type CurrentBackoff struct {
	backoffTimeUs    int32
	preemptedConfBal lwcproto.ConfigBal
	attemptNo        int32
}

type BackoffManager struct {
	currentBackoffs map[int32]CurrentBackoff
	BackoffInfo
	sig *chan RetryInfo
}

func NewBackoffManager(minBO, maxInitBO, maxBO int32, signalChan *chan RetryInfo) BackoffManager {
	rand.Seed(time.Now().UTC().UnixNano())
	//	if minBO > maxInitBO || minBO > maxBO {
	//		panic("Incorrect set up times for backoffs")
	//}
	return BackoffManager{
		currentBackoffs: make(map[int32]CurrentBackoff),
		BackoffInfo: BackoffInfo{
			minBackoff:     minBO,
			maxInitBackoff: maxInitBO,
			maxBackoff:     maxBO,
		},
		sig: signalChan,
	}
}

// range specification, note that min <= max
type IntRange struct {
	min, max int
}

// get next random value within the interval including min and max
func (ir *IntRange) NextRandom(r *rand.Rand) int {
	return r.Intn(ir.max-ir.min+1) + ir.min
}

func (bm *BackoffManager) BeginBackoff(inst int32, attemptedConfBal lwcproto.ConfigBal) {
	curBackoff, exists := bm.currentBackoffs[inst]

	if exists {
		if curBackoff.preemptedConfBal.GreaterThan(attemptedConfBal) || curBackoff.preemptedConfBal.Equal(attemptedConfBal) {
			dlog.Println("Ignoring backoff request as already backing off instance for this conf-bal or a greater one")
			return
		}
	}

	var curAttempt int32
	var prevBackoff int32
	if exists {
		curAttempt = curBackoff.attemptNo + 1
		prevBackoff = curBackoff.backoffTimeUs
	} else {
		curAttempt = 0
		//prevBackoff = bm.minBackoff //
		prevBackoff = 0 //  (rand.Int31() % bm.maxInitBackoff) + bm.minBackoff
	}

	factor := 10.
	t := float64(curAttempt) + rand.Float64()
	tmp := math.Pow(2, t) * math.Tanh(math.Sqrt(factor*t))
	tmp *= 1000 + float64(bm.minBackoff)
	next := int32(tmp) - prevBackoff

	if next > bm.maxBackoff {
		next = bm.maxBackoff //- prevBackoff
	}

	log.Printf("Began backoff of %dus for instance %d on conf-bal %d.%d.%d", next, inst, attemptedConfBal.Config, attemptedConfBal.Number, attemptedConfBal.PropID)

	//if (next < bm.minBackoff) {panic("Too low backoff")}

	bm.currentBackoffs[inst] = CurrentBackoff{
		backoffTimeUs:    next,
		preemptedConfBal: attemptedConfBal,
		attemptNo:        curAttempt,
	}

	go func() {
		timer := time.NewTimer(time.Duration(next) * time.Microsecond)

		<-timer.C
		*bm.sig <- RetryInfo{
			InstToPrep:       inst,
			attemptedConfBal: attemptedConfBal,
			backoffus:        next,
			backedoff:        true,
		}
	}()
}

func (bm *BackoffManager) StillRelevant(backoff RetryInfo) bool {
	curBackoff, exists := bm.currentBackoffs[backoff.InstToPrep]

	if !exists {
		return false
	} else {
		return backoff.attemptedConfBal.Equal(curBackoff.preemptedConfBal) //should update so that inst also has bal backed off
	}
}

func (bm *BackoffManager) ClearBackoff(inst int32) {
	delete(bm.currentBackoffs, inst)
}

func (r *Replica) noopStillRelevant(inst int32) bool {
	return r.instanceSpace[inst].pbk.cmds == nil
}

//type

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, lread bool, dreply bool, durable bool, batchWait int, f int, crtConfig int32, storageLoc string, maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool) *Replica {
	retryInstances := make(chan RetryInfo, maxOpenInstances*100000)
	r := &Replica{
		Replica:          genericsmr.NewReplica(id, peerAddrList, thrifty, exec, lread, dreply, f, storageLoc),
		configChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitShortChan:  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptReplyChan:  make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		//	instancesToRecover:  nil,
		prepareRPC:          0,
		acceptRPC:           0,
		commitRPC:           0,
		commitShortRPC:      0,
		prepareReplyRPC:     0,
		acceptReplyRPC:      0,
		instanceSpace:       make([]*Instance, 15*1024*1024),
		crtInstance:         -1, //get from storage
		crtConfig:           crtConfig,
		Shutdown:            false,
		counter:             0,
		flush:               true,
		executedUpTo:        -1, //get from storage
		batchWait:           batchWait,
		maxBalInc:           100,
		maxBeganInstances:   maxOpenInstances,
		beginNew:            make(chan struct{}, maxOpenInstances),
		crtOpenedInstances:  make([]int32, maxOpenInstances),
		proposableInstances: make(chan ProposalInfo, maxOpenInstances*1000),
		valuesToPropose:     make(chan ProposalTuples, 5000),
		noopInstance:        make(chan ProposalInfo, maxOpenInstances*1000),
		noopWaitUs:          noopwait,
		retryInstance:       retryInstances,
		BackoffManager:      NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, &retryInstances),
	}

	r.Durable = durable

	r.prepareRPC = r.RegisterRPC(new(lwcproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(lwcproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(lwcproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(lwcproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(lwcproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(lwcproto.AcceptReply), r.acceptReplyChan)

	//r.submitNewConfig()
	go r.run()

	return r
}

func (r *Replica) recordNewConfig(config int32) {
	if !r.Durable {
		return
	}

	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(config))
	r.StableStore.Write(b[:])
}

//append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	var b [12]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(r.crtConfig))
	binary.LittleEndian.PutUint32(b[4:8], uint32(inst.abk.curBal.Number))
	binary.LittleEndian.PutUint32(b[8:12], uint32(inst.abk.curBal.PropID))
	r.StableStore.Write(b[:])
}

//write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command) {
	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

//sync with the stable store
func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

func (r *Replica) replyPrepare(replicaId int32, reply *lwcproto.PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *lwcproto.AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

/* Clock goroutine */

var fastClockChan chan bool

func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(time.Duration(r.batchWait) * time.Millisecond) // ms
		fastClockChan <- true
	}
}

func (r *Replica) BatchingEnabled() bool {
	return r.batchWait > 0
}

/* ============= */

/* Main event processing loop */

func (r *Replica) run() {

	r.ConnectToPeers()

	r.RandomisePeerOrder()

	if r.Exec {
		go r.executeCommands()
	}

	fastClockChan = make(chan bool, 1)

	//Enabled fast clock when batching

	if r.BatchingEnabled() {
		go r.fastClock()
	}

	onOffProposeChan := r.ProposeChan

	//if r.crtConfig > 1 {
	// send to all
	//}

	go r.WaitForClientConnections()

	for i := 0; i < int(r.maxBeganInstances); i++ {
		r.beginNew <- struct{}{}
		r.crtOpenedInstances[i] = -1
	}

	newPeerOrderTimer := time.NewTimer(1 * time.Second)

	for !r.Shutdown {

		select {

		case <-newPeerOrderTimer.C:
			go r.RandomisePeerOrder()
			dlog.Println("Recomputing closest peers")
			newPeerOrderTimer.Reset(100 * time.Millisecond)
			break

		case propose := <-onOffProposeChan:
			//got a Propose from a clien
			r.handlePropose(propose)

			//r.tryAccept(propose)
			//deactivate new proposals channel to prioritize the handling of other protocol messages,
			//and to allow commands to accumulate for batching
			if r.BatchingEnabled() {
				onOffProposeChan = nil
			}
			break

		case <-r.beginNew:
			r.beginNextInstance()
			break

		case noopInfo := <-r.noopInstance:
			//panic("Oh no op")
			inst := r.instanceSpace[noopInfo.inst]
			pbk := inst.pbk

			if pbk.status == READY_TO_PROPOSE && pbk.propCurConfBal.Equal(noopInfo.proposingConfBal) {
				pbk.status = PROPOSING
				pbk.cmds = state.NOOP()
				// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
				r.acceptorAcceptOnConfBal(noopInfo.inst, pbk.propCurConfBal, inst.pbk.cmds)
				r.proposerCheckAndHandleNewAcceptedValue(noopInfo.inst, r.Id, pbk.propCurConfBal, inst.pbk.cmds)
				r.bcastAccept(noopInfo.inst)
			} else {

			}

			break
		case next := <-r.retryInstance:
			if r.BackoffManager.StillRelevant(next) || !next.backedoff {
				inst := r.instanceSpace[next.InstToPrep]

				if !next.backedoff {
					if inst == nil {
						r.instanceSpace[next.InstToPrep] = r.makeEmptyInstance()
						inst = r.instanceSpace[next.InstToPrep]
					}
				}

				if inst.pbk.status != BACKING_OFF {
					return
				}

				r.beginNewPropConfigBal(next.InstToPrep)

				nextConfBal := r.instanceSpace[next.InstToPrep].pbk.propCurConfBal
				r.acceptorPrepareOnConfBal(next.InstToPrep, nextConfBal)
				r.bcastPrepare(next.InstToPrep)
				dlog.Printf("Proposing next conf-bal %d.%d.%d to instance %d\n", nextConfBal.Config, nextConfBal.Number, nextConfBal.PropID, next.InstToPrep)
			} else {
				dlog.Printf("Skipping retry of instance %d due to preempted again or closed\n", next.InstToPrep)
			}
			break
		case <-fastClockChan:
			//activate new proposals channel
			onOffProposeChan = r.ProposeChan
			break

		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*lwcproto.Prepare)
			//got a Prepare message
			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
			r.handlePrepare(prepare)
			break

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*lwcproto.Accept)
			//got an Accept message
			dlog.Printf("Received Accept Request from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
			r.handleAccept(accept)
			// if new or equal backoff again
			break

		case commitS := <-r.commitChan:
			commit := commitS.(*lwcproto.Commit)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommit(commit)
			break

		case commitS := <-r.commitShortChan:
			commit := commitS.(*lwcproto.CommitShort)
			//got a Commit message
			dlog.Printf("Received short Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommitShort(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*lwcproto.PrepareReply)
			//got a Prepare reply
			dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
			r.handlePrepareReply(prepareReply)
			break

		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*lwcproto.AcceptReply)
			//got an Accept reply
			dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
			r.handleAcceptReply(acceptReply)
			break
		}
	}
}

func (r *Replica) bcastPrepare(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Prepare bcast failed:", err)
		}
	}()

	args := &lwcproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurConfBal}

	n := r.N - 1

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.prepareRPC, args)
		sent++
		if sent >= n {
			break
		}
	}

}

var pa lwcproto.Accept

func (r *Replica) bcastAccept(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()

	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.ConfigBal = r.instanceSpace[instance].pbk.propCurConfBal
	pa.Command = r.instanceSpace[instance].pbk.cmds
	args := &pa

	n := r.N - 1

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.acceptRPC, args)
		sent++
		if sent >= n {
			break
		}
	}

}

var pc lwcproto.Commit
var pcs lwcproto.CommitShort

func (r *Replica) bcastCommit(instance int32, confBal lwcproto.ConfigBal, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.ConfigBal = confBal
	pc.Command = command

	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.ConfigBal = confBal
	pcs.Count = int32(len(command))
	//argsShort := &pcs

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, &pc)
		sent++
	}

}

func (r *Replica) getNextProposingConfigBal(instance int32) lwcproto.ConfigBal {
	pbk := r.instanceSpace[instance].pbk
	min := pbk.maxKnownBal.Number + 1
	max := min + r.maxBalInc
	if max < 0 {
		max = math.MaxInt16
	}
	next := rand.Intn(int(max)-int(min)+1) + int(min)
	if int16(next) < min {
		panic("Trying outdated conf-bal?")
	}

	next -= int(r.Id)
	//	r.instanceSpace[instance].curBal = lwcproto.Ballot{int16(next), int16(r.Id)}

	proposingConfBal := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: lwcproto.Ballot{int16(next), int16(r.Id)},
	}
	//pbk.propCurConfBal = proposingConfBal
	//pbk.maxKnownBal = r.instanceSpace[instance].curBal
	dlog.Printf("For instance", instance, "now incrementing to new conf-bal", proposingConfBal)
	return proposingConfBal
}

func (r *Replica) incToNextOpenInstance() {
	r.crtInstance++

	if r.instanceSpace[r.crtInstance] != nil {
		str := fmt.Sprintf("Oh no, we've tried to begin an instance already begun %d", r.crtInstance)
		panic(str)
	}
}

func (r *Replica) makeEmptyInstance() *Instance {
	return &Instance{
		abk: &AcceptorBookkeeping{
			status: NOT_STARTED,
			cmds:   nil,
			curBal: lwcproto.Ballot{-1, -1},
			vConfBal: lwcproto.ConfigBal{
				Config: -1,
				Ballot: lwcproto.Ballot{-1, -1},
			},
		},
		pbk: &ProposingBookkeeping{
			status:        NOT_BEGUN,
			proposalInfos: make(map[lwcproto.ConfigBal]*QuorumInfo),
			maxKnownBal:   lwcproto.Ballot{-1, -1},
			maxAcceptedConfBal: lwcproto.ConfigBal{
				Config: -1,
				Ballot: lwcproto.Ballot{-1, -1},
			},
			cmds: nil,
			propCurConfBal: lwcproto.ConfigBal{
				Config: -1,
				Ballot: lwcproto.Ballot{-1, -1},
			},
			clientProposals: nil,
		},
	}
}

func (r *Replica) beginNewPropConfigBal(inst int32) {
	pbk := r.instanceSpace[inst].pbk
	pbk.status = PREPARING
	nextConfBal := r.getNextProposingConfigBal(inst)
	pbk.proposalInfos[nextConfBal] = new(QuorumInfo)

	pbk.propCurConfBal = nextConfBal
	pbk.maxKnownBal = nextConfBal.Ballot

	pbk.proposalInfos[nextConfBal] = NewQuorumInfo(PROMISE)
}

func (r *Replica) beginNextInstance() {
	r.incToNextOpenInstance()

	foundFree := false
	for i := 0; i < len(r.crtOpenedInstances); i++ {
		// allow for an openable instance
		if r.crtOpenedInstances[i] == -1 {
			r.crtOpenedInstances[i] = r.crtInstance
			foundFree = true
			break
		}
	}

	if !foundFree {
		panic("Opened up too many instances or closed instances not considered closed")
	}

	r.instanceSpace[r.crtInstance] = r.makeEmptyInstance()
	curInst := r.instanceSpace[r.crtInstance]

	//propConfBal := r.getNextProposingConfigBal(r.crtInstance)
	//curInst.pbk.propCurConfBal = propConfBal
	//	curInst.pbk.maxKnownBal = propConfBal.Ballot
	r.beginNewPropConfigBal(r.crtInstance)

	r.acceptorPrepareOnConfBal(r.crtInstance, curInst.pbk.propCurConfBal)
	//	pbk := curInst.pbk

	r.bcastPrepare(r.crtInstance)
	log.Printf("Opened new instance %d\n", r.crtInstance)
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	batchSize := len(r.ProposeChan) + 1
	dlog.Printf("Batched %d\n", batchSize)

	proposals := make([]*genericsmr.Propose, batchSize)
	cmds := make([]state.Command, batchSize)
	proposals[0] = propose
	cmds[0] = propose.Command

	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		proposals[i] = prop
		cmds[i] = prop.Command
	}

	proposalsToSubmit := ProposalTuples{
		cmd:      cmds,
		proposal: proposals,
	}

	sortedOut := false

	for !sortedOut {
		select {
		case proposalInfo := <-r.proposableInstances:

			inst := r.instanceSpace[proposalInfo.inst]
			pbk := inst.pbk

			if pbk.status == READY_TO_PROPOSE && proposalInfo.proposingConfBal.Equal(pbk.propCurConfBal) {
				pbk.cmds = cmds
				pbk.clientProposals = proposals
				pbk.status = PROPOSING
				// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
				r.acceptorAcceptOnConfBal(proposalInfo.inst, pbk.propCurConfBal, inst.pbk.cmds)
				r.proposerCheckAndHandleNewAcceptedValue(proposalInfo.inst, r.Id, pbk.propCurConfBal, inst.pbk.cmds)
				r.bcastAccept(proposalInfo.inst)
				sortedOut = true
			}
			break
		default:
			r.valuesToPropose <- proposalsToSubmit
			sortedOut = true
			break
		}
	}
}

func (r *Replica) acceptorPrepareOnConfBal(inst int32, confBal lwcproto.ConfigBal) {
	r.instanceSpace[inst].abk.status = PREPARED
	if confBal.Config > r.crtConfig {
		r.recordNewConfig(confBal.Config)
		r.sync()
		r.crtConfig = confBal.Config
		dlog.Printf("Joined higher Config %d", r.crtConfig)
	}

	dlog.Printf("Acceptor Preparing Config-Ballot %d.%d.%d ", confBal.Config, confBal.Number, confBal.PropID)
	r.instanceSpace[inst].abk.curBal = confBal.Ballot
}

func (r *Replica) acceptorAcceptOnConfBal(inst int32, confBal lwcproto.ConfigBal, cmds []state.Command) {
	abk := r.instanceSpace[inst].abk
	abk.status = ACCEPTED

	if confBal.Config > r.crtConfig {
		r.recordNewConfig(confBal.Config)
		r.crtConfig = confBal.Config
		dlog.Printf("Joined higher Config %d", r.crtConfig)
	}

	dlog.Printf("Acceptor Accepting Config-Ballot %d.%d.%d ", confBal.Config, confBal.Number, confBal.PropID)
	abk.curBal = confBal.Ballot
	abk.vConfBal = confBal
	abk.cmds = cmds

	r.recordInstanceMetadata(r.instanceSpace[inst])
	r.recordCommands(cmds)
	r.sync()

}

func (r *Replica) proposerCheckAndHandlePreempt(inst int32, preemptingConfigBal lwcproto.ConfigBal) bool {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if preemptingConfigBal.GreaterThan(pbk.propCurConfBal) {
		if pbk.status == PROPOSING {
			r.checkAndRequeueInstanceClientProposals(inst)
		}

		pbk.status = BACKING_OFF
		pbk.maxKnownBal = preemptingConfigBal.Ballot
		r.BackoffManager.BeginBackoff(inst, preemptingConfigBal)
		return true
	} else {
		return false
	}
}

func (r *Replica) handlePrepare(prepare *lwcproto.Prepare) {
	r.checkAndHandleNewlyReceivedInstance(prepare.Instance)
	inst := r.instanceSpace[prepare.Instance]

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed instance %d, returning commit \n", prepare.Instance)
		var pc lwcproto.Commit
		pc.LeaderId = prepare.LeaderId
		pc.Instance = prepare.Instance
		pc.ConfigBal = inst.abk.vConfBal
		pc.Command = inst.abk.cmds
		args := &pc
		r.SendMsg(pc.LeaderId, r.commitRPC, args)
		return
	}

	minAcceptableConfBal := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: inst.abk.curBal,
	}

	if minAcceptableConfBal.GreaterThan(prepare.ConfigBal) {
		dlog.Printf("Already prepared on higher Config-Ballot %d.%d.%d < %d.%d.%d", prepare.Config, prepare.Number, prepare.PropID, minAcceptableConfBal.Config, minAcceptableConfBal.Number, minAcceptableConfBal.PropID)
		// return preempt
	} else if prepare.ConfigBal.GreaterThan(minAcceptableConfBal) {
		dlog.Printf("Preparing on prepared on new Config-Ballot %d.%d.%d", prepare.Config, prepare.Number, prepare.PropID)

		r.acceptorPrepareOnConfBal(prepare.Instance, prepare.ConfigBal)
		r.proposerCheckAndHandlePreempt(prepare.Instance, prepare.ConfigBal)
	} else {
		// msg reordering
		dlog.Printf("Config-Ballot %d.%d.%d already joined, returning same promise", prepare.Config, prepare.Number, prepare.PropID)
	}

	newConfigBal := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: inst.abk.curBal,
	}

	var preply = &lwcproto.PrepareReply{
		Instance:   prepare.Instance,
		ConfigBal:  newConfigBal,
		VConfigBal: inst.abk.vConfBal,
		AcceptorId: r.Id,
		Command:    inst.abk.cmds,
	}

	r.replyPrepare(prepare.LeaderId, preply)
}

type ProposerAccValHandler int

const (
	IGNORED ProposerAccValHandler = iota
	NEW_VAL
	ACKED
	CHOSEN
)

func (r *Replica) proposerCheckAndHandleNewAcceptedValue(inst int32, aid int32, accepted lwcproto.ConfigBal, val []state.Command) ProposerAccValHandler {
	if accepted.IsZeroVal() {
		return IGNORED
	}

	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	newVal := false
	if accepted.GreaterThan(pbk.maxAcceptedConfBal) {
		newVal = true

		if pbk.clientProposals != nil {
			if !cmdsEqual(pbk.cmds, val) {
				r.valuesToPropose <- ProposalTuples{pbk.cmds, pbk.clientProposals}
				pbk.clientProposals = nil
			}
		}

		pbk.maxAcceptedConfBal = accepted
		pbk.cmds = val
	}

	if accepted.Ballot.GreaterThan(pbk.maxKnownBal) {
		pbk.maxKnownBal = accepted.Ballot
	}

	qrm, exists := pbk.proposalInfos[accepted]
	if !exists {
		//qrm = new(QuorumInfo)
		qrm = NewQuorumInfo(ACCEPTANCE)
	}

	qrm.quorumAdd(aid)

	// not assumed local acceptor has accepted it
	if int(qrm.quorumCount()) >= r.WriteQuorumSize() {
		return CHOSEN
	} else if newVal {
		return NEW_VAL
	} else {
		return ACKED
	}
}

func (r *Replica) handlePrepareReply(preply *lwcproto.PrepareReply) {
	// early learning
	inst := r.instanceSpace[preply.Instance]
	pbk := r.instanceSpace[preply.Instance].pbk

	if inst.abk.status == COMMITTED {
		dlog.Println("Inst already known to be chosen")
		return
	}

	valWhatDone := r.proposerCheckAndHandleNewAcceptedValue(preply.Instance, preply.AcceptorId, preply.VConfigBal, preply.Command)

	if valWhatDone == CHOSEN {
		dlog.Printf("Preparing instance recognised as chosen (instance %d), returning commit \n", preply.Instance)
		if preply.ConfigBal.IsZeroVal() {
			panic("Why we commit zero ballot")
		}
		r.acceptorCommit(preply.Instance, preply.ConfigBal, preply.Command)
		r.proposerCloseCommit(preply.Instance, preply.ConfigBal)
		r.bcastCommit(preply.Instance, preply.ConfigBal, preply.Command)
		return
	}

	if pbk.propCurConfBal.GreaterThan(preply.ConfigBal) || pbk.status != PREPARING {
		dlog.Printf("Message in late \n")
		return
	}

	if r.proposerCheckAndHandlePreempt(preply.Instance, preply.ConfigBal) {
		//pbk.nacks++
		//	if pbk.nacks+1 > r.N>>1 { // no qrm
		// we could send promise also through a prepare reply to the owner of the preempting confbal
		dlog.Printf("Another active proposer using config-ballot %d.%d.%d greater than mine\n", preply.ConfigBal)
		if preply.ConfigBal.Config > r.crtConfig {
			r.recordNewConfig(preply.ConfigBal.Config)
			r.sync()
		}
		return
	}

	qrm := pbk.proposalInfos[pbk.propCurConfBal]
	qrm.quorumAdd(preply.AcceptorId)

	if int(qrm.quorumCount()+1) >= r.Replica.ReadQuorumSize() {
		pbk.status = READY_TO_PROPOSE

		dlog.Println("Can now propose in instance", preply.Instance)
		qrm.quorumClear()
		qrm.qrmType = ACCEPTANCE

		if inst.pbk.maxAcceptedConfBal.IsZeroVal() {
			select {
			case cmds := <-r.valuesToPropose:
				pbk.cmds = cmds.cmd
				pbk.clientProposals = cmds.proposal
				break
			default:
				if r.shouldProposeNoop(preply.Instance) { //not mem safe but it doesn't really matter

					go func() {
						noopTimer := time.NewTimer(time.Duration(r.noopWaitUs) * time.Microsecond)

						instNoop := preply.Instance
						instPropConfBal := inst.pbk.propCurConfBal
						if len(r.proposableInstances) > 1000 {
							for i := 0; i < 800; i++ {
								<-r.proposableInstances
							}

						}

						r.proposableInstances <- ProposalInfo{
							inst:             instNoop,
							proposingConfBal: instPropConfBal,
						}

						if len(r.noopInstance) > 1000 {
							for i := 0; i < 800; i++ {
								<-r.noopInstance
							}

						}

						<-noopTimer.C // this no op timer will go off if no client value is received during the timeout
						r.noopInstance <- ProposalInfo{
							inst:             instNoop,
							proposingConfBal: instPropConfBal,
						}
					}()
				} else {
					instNoop := preply.Instance
					instPropConfBal := inst.pbk.propCurConfBal
					if len(r.proposableInstances) > 1000 {
						for i := 0; i < 800; i++ {
							<-r.proposableInstances
						}

					}

					r.proposableInstances <- ProposalInfo{
						inst:             instNoop,
						proposingConfBal: instPropConfBal,
					}
				}
				return
			}
		}
	}
	pbk.status = PROPOSING
	// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
	r.acceptorAcceptOnConfBal(preply.Instance, preply.ConfigBal, inst.pbk.cmds)
	r.proposerCheckAndHandleNewAcceptedValue(preply.Instance, r.Id, preply.ConfigBal, inst.pbk.cmds)

	r.bcastAccept(preply.Instance)
}

func (r *Replica) shouldProposeNoop(inst int32) bool {
	if r.alwaysNoop {
		return true
	}
	for i := inst; i <= r.crtInstance; i++ {
		if r.instanceSpace[i] != nil {
			if r.instanceSpace[i].abk.status >= ACCEPTED {
				return true
			}
		}
	}
	return false
}

func (r *Replica) checkAndHandleNewlyReceivedInstance(instance int32) {
	inst := r.instanceSpace[instance]

	if inst == nil {
		if instance > r.crtInstance {
			r.crtInstance = instance
		}
		r.instanceSpace[instance] = r.makeEmptyInstance()
	}

}

func (r *Replica) handleAccept(accept *lwcproto.Accept) {
	r.checkAndHandleNewlyReceivedInstance(accept.Instance)
	inst := r.instanceSpace[accept.Instance]

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed \n")
		var pc lwcproto.Commit
		pc.LeaderId = accept.LeaderId
		pc.Instance = accept.Instance
		pc.ConfigBal = inst.abk.vConfBal
		pc.Command = inst.abk.cmds
		args := &pc
		r.SendMsg(pc.LeaderId, r.commitRPC, args)
		return
	}

	minAcceptableConfBal := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: lwcproto.Ballot{-1, -1},
	}

	// should always be unless there is a restart
	if accept.ConfigBal.GreaterThan(minAcceptableConfBal) {
		r.acceptorAcceptOnConfBal(accept.Instance, accept.ConfigBal, accept.Command)

		dlog.Printf("Accepted instance %d on conf-ball %d.%d.%d", accept.Instance, accept.Config, accept.Number, accept.PropID)

		//check proposer qrm
		accValState := r.proposerCheckAndHandleNewAcceptedValue(accept.Instance, r.Id, accept.ConfigBal, accept.Command)
		accValState = r.proposerCheckAndHandleNewAcceptedValue(accept.Instance, int32(accept.PropID), accept.ConfigBal, accept.Command)

		// here is where we can add fast learning bit - also add acceptance by config-bal's owner
		if accValState == CHOSEN {
			r.acceptorCommit(accept.Instance, accept.ConfigBal, accept.Command)
			r.proposerCloseCommit(accept.Instance, accept.ConfigBal)
			r.bcastCommit(accept.Instance, accept.ConfigBal, accept.Command)
			//do chosen
		}

		r.proposerCheckAndHandlePreempt(accept.Instance, accept.ConfigBal)
	} else if minAcceptableConfBal.GreaterThan(accept.ConfigBal) {
		dlog.Printf("Returning preempt for config-ballot %d.%d.%d < %d.%d.%d in Instance %d\n", accept.Config, accept.Number, accept.PropID, minAcceptableConfBal.Config, minAcceptableConfBal.Number, minAcceptableConfBal.PropID, accept.Instance)
	} else {
		dlog.Printf("Already acknowledged accept request but will return again", accept.Config, accept.Number, accept.PropID, minAcceptableConfBal.Config, minAcceptableConfBal.Number, minAcceptableConfBal.PropID, accept.Instance)
	}

	replyConfBal := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: inst.abk.curBal,
	}

	areply := &lwcproto.AcceptReply{accept.Instance, r.Id, replyConfBal, accept.ConfigBal}
	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) checkIfOpenedInstanceAndSignalNew(inst int32) {
	// was instance opened by us
	for i := 0; i < len(r.crtOpenedInstances); i++ {
		// allow for an openable instance
		if r.crtOpenedInstances[i] == inst {
			r.crtOpenedInstances[i] = -1
			r.beginNew <- struct{}{}
			break
		}
	}

}

func cmdsEqual(a, b []state.Command) bool {
	if len(a) != len(b) {
		return false
	} else {
		for i := 0; i < len(a); i++ {
			if a[i].String() != b[i].String() {
				return false
			}
		}
		return true
	}
}

func (r *Replica) handleAcceptReply(areply *lwcproto.AcceptReply) {
	// could modify to have record of all ballots
	inst := r.instanceSpace[areply.Instance]
	pbk := r.instanceSpace[areply.Instance].pbk

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed ")
		return
	}

	accepted := areply.Cur.Equal(areply.Req)
	preempted := areply.Cur.GreaterThan(areply.Req)
	if accepted {
		dlog.Printf("Acceptance of instance %d at %d.%d.%d by Acceptor %d received\n", areply.Instance, areply.Cur.Config, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId)
		// pbk.cmds should be correct value - proposer only gets to this
		// stage if they learn of any previously chosen value and propose it again themselves
		qrmState := r.proposerCheckAndHandleNewAcceptedValue(areply.Instance, areply.AcceptorId, areply.Cur, pbk.cmds)
		if qrmState == CHOSEN {
			// set acceptor chosen
			inst.abk.status = COMMITTED
			inst.abk.curBal = pbk.propCurConfBal.Ballot
			inst.abk.vConfBal = pbk.propCurConfBal
			inst.abk.cmds = pbk.cmds

			r.recordInstanceMetadata(r.instanceSpace[areply.Instance])
			//r.sync()

			if !cmdsEqual(inst.pbk.cmds, inst.abk.cmds) {
				panic("Commands somehow been overwritten")
			}

			r.proposerCloseCommit(areply.Instance, areply.Cur)
		}
	} else if preempted {
		if areply.Cur.Config > r.crtConfig {
			r.recordNewConfig(areply.Cur.Config)
			r.sync()
			r.crtConfig = areply.Cur.Config
		}
		r.proposerCheckAndHandlePreempt(areply.Instance, areply.Cur)
	} else {
		msg := fmt.Sprintf("Somehow cur Conf-Bal of %d is %d.%d.%d when we requested %d.%d.%d for acceptance",
			areply.AcceptorId, areply.Cur.Config, areply.Cur.Number, areply.Cur.PropID,
			areply.Req.Config, areply.Req.Number, areply.Req.PropID)
		panic(msg)
	}
}

func (r *Replica) checkAndRequeueInstanceClientProposals(instance int32) {
	inst := r.instanceSpace[instance]
	if inst.pbk.clientProposals != nil && !cmdsEqual(inst.abk.cmds, inst.pbk.cmds) {
		r.valuesToPropose <- ProposalTuples{inst.pbk.cmds, inst.pbk.clientProposals}
		inst.pbk.cmds = inst.abk.cmds
		inst.pbk.clientProposals = nil
	}
}

func (r *Replica) proposerCloseCommit(inst int32, chosenAt lwcproto.ConfigBal) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	pbk.status = CLOSED

	r.BackoffManager.ClearBackoff(inst)
	if chosenAt.Ballot.GreaterThan(pbk.maxKnownBal) {
		pbk.maxKnownBal = chosenAt.Ballot
	}

	if chosenAt.GreaterThan(pbk.maxAcceptedConfBal) {
		pbk.maxAcceptedConfBal = chosenAt
	}

	if pbk.clientProposals != nil && !r.Dreply {
		// give client the all clear
		for i := 0; i < len(pbk.cmds); i++ {
			propreply := &genericsmrproto.ProposeReplyTS{
				TRUE,
				pbk.clientProposals[i].CommandId,
				state.NIL(),
				pbk.clientProposals[i].Timestamp}
			r.ReplyProposeTS(propreply, pbk.clientProposals[i].Reply, pbk.clientProposals[i].Mutex)
		}

	}
	r.checkAndRequeueInstanceClientProposals(inst)
	r.checkIfOpenedInstanceAndSignalNew(inst)
}

func (r *Replica) acceptorCommit(instance int32, chosenAt lwcproto.ConfigBal, cmds []state.Command) {
	inst := r.instanceSpace[instance]
	abk := inst.abk
	dlog.Printf("Committing (crtInstance=%d)\n", instance)
	inst.abk.status = COMMITTED
	shouldSync := false
	if r.crtConfig < chosenAt.Config {
		r.recordNewConfig(chosenAt.Config)
		r.crtConfig = chosenAt.Config
		shouldSync = true
	}
	abk.curBal = chosenAt.Ballot
	abk.vConfBal = chosenAt
	abk.cmds = cmds

	r.recordInstanceMetadata(inst)
	r.recordCommands(cmds)
	if shouldSync {
		r.sync()
	}
}

func (r *Replica) handleCommit(commit *lwcproto.Commit) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	inst := r.instanceSpace[commit.Instance]
	r.checkIfOpenedInstanceAndSignalNew(commit.Instance)

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed \n")
		return
	}

	r.acceptorCommit(commit.Instance, commit.ConfigBal, commit.Command)
	r.proposerCloseCommit(commit.Instance, commit.ConfigBal)
}

func (r *Replica) handleCommitShort(commit *lwcproto.CommitShort) {
	panic("oh nooooooooo")
	/*	r.checkIfOpenedInstanceAndSignalNew(commit.Instance)

		inst := r.instanceSpace[commit.Instance]
		if inst == nil {
			if commit.Instance > r.crtInstance {
				r.crtInstance = commit.Instance
			}

			dlog.Printf("Commit short received but nothing recorded \n")
			r.instanceSpace[r.crtInstance] = new(Instance)
			*r.instanceSpace[commit.Instance] = r.makeEmptyInstance()
			inst = r.instanceSpace[commit.Instance]

			if commit.Config > r.crtConfig {
				r.recordNewConfig(commit.Config)
				r.sync()
				r.crtConfig = commit.Config
			}


			inst.abk.curBal = commit.ConfigBal.Ballot

			// start preparing next bal to recover value
			r.resetProposerToPreparing(commit.Instance, commit.ConfigBal)
			nextConfBal := r.getNextProposingConfigBal(commit.Instance)
			inst.pbk.propCurConfBal = nextConfBal

			// acceptor prepare
			r.acceptorPrepareOnConfBal(commit.Instance, inst.pbk.propCurConfBal)
			r.bcastPrepare(commit.Instance)
			return
		}

		if inst.status == COMMITTED {
			dlog.Printf("Already committed \n")
			return
		}

		crtAcceptorConfBal := lwcproto.ConfigBal{
			Config: r.crtConfig,
			Ballot: inst.abk.curBal,
		}

		if crtAcceptorConfBal.GreaterThan(commit.ConfigBal) {
			dlog.Printf("Smaller than crt %d.%d.%d < %d.%d.%d\n", commit.ConfigBal, crtAcceptorConfBal)
			return
		} else if commit.ConfigBal.GreaterThan(crtAcceptorConfBal) {
			if commit.Config > r.crtConfig {
				r.recordNewConfig(commit.Config)
				r.sync()
				r.crtConfig = commit.Config
			}


			inst.abk.curBal = commit.ConfigBal.Ballot

			// start preparing next bal to recover value
			r.resetProposerToPreparing(commit.Instance, commit.ConfigBal)
			nextConfBal := r.getNextProposingConfigBal(commit.Instance)
			inst.pbk.propCurConfBal = nextConfBal

			// acceptor prepare
			r.acceptorPrepareOnConfBal(commit.Instance, inst.pbk.propCurConfBal)
			r.bcastPrepare(commit.Instance)

			return
		} else {
			dlog.Printf("Committing \n")
			r.instanceSpace[commit.Instance].status = COMMITTED
			r.instanceSpace[commit.Instance].abk.curBal = commit.ConfigBal.Ballot
			r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
			r.recordCommands(r.instanceSpace[commit.Instance].abk.cmds)

			if inst.pbk.clientProposals != nil && !r.Dreply {
				// give client the all clear
				for i := 0; i < len(inst.pbk.cmds); i++ {
					propreply := &genericsmrproto.ProposeReplyTS{
						TRUE,
						inst.pbk.clientProposals[i].CommandId,
						state.NIL(),
						inst.pbk.clientProposals[i].Timestamp}
					r.ReplyProposeTS(propreply, inst.pbk.clientProposals[i].Reply, inst.pbk.clientProposals[i].Mutex)
				}
			}
		}*/
}

func (r *Replica) executeCommands() {

	timeout := int64(0)
	problemInstance := int32(0)

	for !r.Shutdown {
		executed := false

		// FIXME idempotence
		for i := r.executedUpTo + 1; i <= r.crtInstance; i++ {
			inst := r.instanceSpace[i]
			if inst != nil && inst.abk.status == COMMITTED { //&& inst.abk.cmds != nil {
				for j := 0; j < len(inst.abk.cmds); j++ {
					dlog.Printf("Executing " + inst.abk.cmds[j].String())
					if r.Dreply && inst.pbk != nil && inst.pbk.clientProposals != nil && !cmdsEqual(inst.abk.cmds, state.NOOP()) {
						val := inst.abk.cmds[j].Execute(r.State)
						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							inst.pbk.clientProposals[j].CommandId,
							val,
							inst.pbk.clientProposals[j].Timestamp}
						r.ReplyProposeTS(propreply, inst.pbk.clientProposals[j].Reply, inst.pbk.clientProposals[j].Mutex)
					} else if inst.abk.cmds[j].Op == state.PUT {
						inst.abk.cmds[j].Execute(r.State)
					}
				}
				executed = true
				r.executedUpTo++
				dlog.Printf("Executed up to %d (crtInstance=%d)", r.executedUpTo, r.crtInstance)
				//r.instanceSpace[i] = nil
			} else {
				if i == problemInstance {
					timeout += SLEEP_TIME_NS
					if timeout >= COMMIT_GRACE_PERIOD {
						for k := problemInstance; k <= r.crtInstance; k++ {
							dlog.Printf("Recovering instance %d \n", k)
							r.retryInstance <- RetryInfo{
								backedoff:  false,
								InstToPrep: k,
								attemptedConfBal: lwcproto.ConfigBal{
									-1,
									lwcproto.Ballot{-1, -1},
								},
								backoffus: 0,
							}

							//	panic(":O oh no, theres nothing to do")
							//	r.instancesToRecover <- k
						}
						problemInstance = 0
						timeout = 0
					}
				} else {
					problemInstance = i
					timeout = 0
				}
				break
			}
		}

		if !executed {
			r.Mutex.Lock()
			r.Mutex.Unlock() // FIXME for cache coherence
			time.Sleep(SLEEP_TIME_NS)
		}
	}

}
