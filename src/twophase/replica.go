package twophase

import (
	"epaxos/acceptor"
	"epaxos/batching"
	"epaxos/dlog"
	"epaxos/fastrpc"
	"epaxos/genericsmr"
	"epaxos/lwcproto"
	"epaxos/proposerstate"
	"epaxos/stablestore"
	"epaxos/state"
	"epaxos/stats"
	"epaxos/stdpaxosproto"
	"epaxos/twophase/learner"
	"epaxos/twophase/logfmt"
	"epaxos/twophase/proposer"
	"math"
	"os"
	"sync"
	"time"
)

type ConcurrentFile struct {
	*os.File
	sync.Mutex
}

func (f *ConcurrentFile) Sync() error {
	dlog.AgentPrintfN(1, "acq sync lock")
	f.Mutex.Lock()
	dlog.AgentPrintfN(1, "release sync lock")
	defer f.Mutex.Unlock()
	return f.File.Sync()
}

func (f *ConcurrentFile) Write(b []byte) (int, error) {
	dlog.AgentPrintfN(1, "acq write lock")
	f.Mutex.Lock()
	dlog.AgentPrintfN(1, "release write lock")
	defer f.Mutex.Unlock()
	return f.File.Write(b)
}

func (f *ConcurrentFile) WriteAt(b []byte, off int64) (int, error) {
	dlog.AgentPrintfN(1, "acq write 2 lock")
	f.Mutex.Lock()
	dlog.AgentPrintfN(1, "release write 2 lock")
	defer f.Mutex.Unlock()
	return f.File.WriteAt(b, off)
}

type ProposalTuples struct {
	cmd      []state.Command
	proposal []*genericsmr.Propose
}

type TimeoutInfo struct {
	inst    int32
	ballot  stdpaxosproto.Ballot
	phase   proposer.ProposerStatus
	msgCode uint8
	msg     fastrpc.Serializable
}

type ProposalInfo struct {
	inst         int32
	proposingBal stdpaxosproto.Ballot
	batching.ProposalBatch
	PrevSleeps int32
}

func (pinfo *ProposalInfo) popBatch() batching.ProposalBatch {
	b := pinfo.ProposalBatch
	pinfo.ProposalBatch = nil
	return b
}

func (pinfo *ProposalInfo) putBatch(bat batching.ProposalBatch) {
	pinfo.ProposalBatch = bat
}

type Replica struct {
	PrepareBroadcaster
	ValueBroadcaster

	proposer.Executor
	learner.Learner

	ProposeBatchOracle

	proposer.Proposer
	proposer.ProposerInstanceQuorumaliser
	proposer.LearnerQuorumaliser
	proposer.AcceptorQrmInfo

	*genericsmr.Replica           // extends a generic Paxos replica
	configChan                    chan fastrpc.Serializable
	prepareChan                   chan fastrpc.Serializable
	acceptChan                    chan fastrpc.Serializable
	commitChan                    chan fastrpc.Serializable
	commitShortChan               chan fastrpc.Serializable
	prepareReplyChan              chan fastrpc.Serializable
	acceptReplyChan               chan fastrpc.Serializable
	stateChan                     chan fastrpc.Serializable
	stateChanRPC                  uint8
	prepareRPC                    uint8
	acceptRPC                     uint8
	commitRPC                     uint8
	commitShortRPC                uint8
	prepareReplyRPC               uint8
	acceptReplyRPC                uint8
	instanceSpace                 []*proposer.PBK // the space of all instances (used and not yet used)
	Shutdown                      bool
	counter                       int
	flush                         bool
	maxBatchWait                  int
	crtOpenedInstances            []int32
	proposableInstances           chan struct{}
	noopWaitUs                    int32
	RetryInstance                 chan proposer.RetryInfo
	alwaysNoop                    bool
	lastTimeClientChosen          time.Time
	lastOpenProposalTime          time.Time
	timeSinceLastProposedInstance time.Time
	fastLearn                     bool
	whenCrash                     time.Duration
	howLongCrash                  time.Duration
	whoCrash                      int32
	timeoutMsgs                   chan TimeoutInfo
	timeout                       time.Duration
	catchupBatchSize              int32
	catchingUp                    bool
	lastSettleBatchInst           int32
	flushCommit                   bool
	group1Size                    int
	nextRecoveryBatchPoint        int32
	recoveringFrom                int32
	commitCatchUp                 bool
	maxBatchSize                  int
	TimeseriesStats               *stats.TimeseriesStats
	InstanceStats                 *stats.InstanceStats
	ProposalStats                 *stats.ProposalStats
	sendProposerState             bool
	proposerState                 proposerstate.State
	acceptor.Acceptor
	stablestore.StableStore
	proactivelyPrepareOnPreempt bool
	noopWait                    time.Duration
	messageFilterIn             chan *messageFilterComm
	isAccMsgFilter              bool
	expectedBatchedRequests     int32
	sendPreparesToAllAcceptors  bool
	PrepareResponsesRPC
	AcceptResponsesRPC
	batchProposedObservers []ProposedObserver
	proposedBatcheNumber   map[int32]int32
	promiseLeases          chan acceptor.PromiseLease
	classesLeased          map[int32]stdpaxosproto.Ballot
	iWriteAhead            int32
	writeAheadAcceptor     bool
	tryInitPropose         chan proposer.RetryInfo
	sendFastestQrm         bool
	nudge                  chan chan batching.ProposalBatch
	bcastCommit            bool
	nopreempt              bool
	bcastAcceptance        bool
	syncAcceptor           bool
	disklessNOOP           bool

	disklessNOOPPromisesAwaiting map[int32]chan struct{}
	disklessNOOPPromises         map[int32]map[stdpaxosproto.Ballot]map[int32]struct{}
	forceDisklessNOOP            bool

	ClientBatcher               proposer.BatchManager
	instanceProposeValueTimeout *InstanceProposeValueTimeout
	nextNoopEnd                 time.Time
	nextNoop                    *time.Timer
	noops                       int
	resetTo                     chan time.Duration
	noopCancel                  chan struct{}
	bcastAcceptDisklessNOOP     bool
	maxLearnt                   int32
	maxValueInst                int32
}

func (r *Replica) HasAcked(q int32, instance int32, ballot stdpaxosproto.Ballot) bool {
	//if instance > r.GetCrtInstance() {
	//	panic("Being asked for an instance we haven't started yet")
	//}
	return r.instanceSpace[instance].Qrms[lwcproto.ConfigBal{-1, ballot}].HasAcknowledged(q)
}

func (r *Replica) GetAckersGroup(instance int32, ballot stdpaxosproto.Ballot) []int32 {
	//if instance > r.GetCrtInstance() {
	//	panic("Being asked for an instance we haven't started yet")
	//}
	consensusG := r.GetConsensusGroup(instance, ballot)
	acked := make([]int32, 0, len(consensusG))
	for _, aid := range consensusG {
		if !r.instanceSpace[instance].Qrms[lwcproto.ConfigBal{-1, ballot}].HasAcknowledged(aid) {
			acked = append(acked, aid)
		}
	}
	return acked
}

func (r *Replica) GetConsensusGroup(instance int32, ballot stdpaxosproto.Ballot) []int32 {
	//if instance > r.GetCrtInstance() {
	//	panic("Being asked for an instance we haven't started yet")
	//}
	return r.AcceptorQrmInfo.GetGroup(instance)
}

type MyBatchLearner interface {
	Learn(bat batching.ProposalBatch)
}

const MAXPROPOSABLEINST = 1000

//const CHAN_BUFFER_SIZE = 200000

func (r *Replica) CloseUp() {

}

/* Clock goroutine */
var fastClockChan chan bool

func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(time.Duration(r.maxBatchWait) * time.Millisecond) // ms
		dlog.Println("sending fast clock")
		fastClockChan <- true
	}
}

func (r *Replica) BatchingEnabled() bool {
	return r.maxBatchWait > 0
}

type InstanceProposeValueTimeout struct {
	proposer.ProposedClientValuesManager
	nextBatch                  proposer.CurBatch
	sleepingInsts              map[int32]time.Time
	constructedAwaitingBatches []batching.ProposalBatch
	chosenBatches              map[int32]struct{}
}

// noop timers reordering seems to cause long delays if there are lots of sleeping instances

func (r *Replica) updateNoopTimer() {
	if len(r.instanceProposeValueTimeout.sleepingInsts) == 0 {
		dlog.AgentPrintfN(r.Id, "No more instances to noop so clearing noop timer")
		return
	}
	dlog.AgentPrintfN(r.Id, "More instances to noop so setting next timeout")
	dlog.AgentPrintfN(r.Id, "Next Noop expires in %d milliseconds", r.noopWait.Milliseconds())
	go func() {
		<-time.After(r.noopWait)
		r.proposableInstances <- struct{}{}
	}()
}

func (r *Replica) noLongerSleepingInstance(inst int32) {
	man := r.instanceProposeValueTimeout
	if _, e := man.sleepingInsts[inst]; !e {
		//dlog.AgentPrintfN(r.Id, "Cannot stop instance is it is not sleeping")
		return
	}
	delete(man.sleepingInsts, inst)
}

func (r *Replica) run() {
	r.ConnectToPeers()
	r.RandomisePeerOrder()
	go r.WaitForClientConnections()

	fastClockChan = make(chan bool, 1)
	doner := make(chan struct{})
	if r.Id == r.whoCrash {
		go func() {
			t := time.NewTimer(r.whenCrash)
			<-t.C
			doner <- struct{}{}
		}()
	}

	startGetEvent := time.Now()
	doWhat := ""
	startEvent := time.Now()
	endEvent := time.Now()
	startInstSig := r.Proposer.GetStartInstanceSignaller()
	for !r.Shutdown {
		startGetEvent = time.Now()
		select {
		case <-r.proposableInstances:
			startEvent = time.Now()
			doWhat = "handle instance value proposing wait timeout"
			dlog.AgentPrintfN(r.Id, "Value proposing wait for some instance(s) has timed out")
			now := time.Now()
			if len(r.instanceProposeValueTimeout.sleepingInsts) == 0 {
				break
			}
			instToProposeUpTo := int32(-1)
			for i, t := range r.instanceProposeValueTimeout.sleepingInsts {
				if instToProposeUpTo > i {
					continue
				}
				if t.Add(r.noopWait).After(now) && r.maxLearnt < i {
					// noop hasn't expired and there isn't any chosen values
					continue
				}
				instToProposeUpTo = i
			}
			//get point of where to go from then skip up to that
			for i, _ := range r.instanceProposeValueTimeout.sleepingInsts {
				if i > instToProposeUpTo {
					continue
				}
				pbk := r.instanceSpace[i]
				if pbk == nil {
					panic("????")
				}
				dlog.AgentPrintfN(r.Id, "Rechecking whether to propose in instance %d", i)
				r.noLongerSleepingInstance(i)
				if pbk.Status != proposer.READY_TO_PROPOSE {
					// remove
					dlog.AgentPrintfN(r.Id, "Decided to not propose in instance %d as we are no longer on ballot %d.%d", i, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
					continue
				}
				r.tryPropose(i, 2)
			}
			//for len(r.instanceProposeValueTimeout.sleepingInsts) > 0 {
			//	min := r.instanceProposeValueTimeout.getMinimumSleepingInstance()
			//
			//}
			r.updateNoopTimer()
			break
		case clientRequest := <-r.ProposeChan:
			startEvent = time.Now()
			doWhat = "handle client request"
			r.ClientBatcher.AddProposal(clientRequest, r.ProposeChan)
			if len(r.instanceProposeValueTimeout.sleepingInsts) == 0 {
				dlog.AgentPrintfN(r.Id, "No instances to propose to propose batch to")
				break
			}
			min := r.instanceProposeValueTimeout.getMinimumSleepingInstance()
			r.noLongerSleepingInstance(min)
			r.tryPropose(min, 1)
			break
		case stateS := <-r.stateChan:
			startEvent = time.Now()
			doWhat = "receive state"
			recvState := stateS.(*proposerstate.State)
			r.handleState(recvState)
			break
		case <-startInstSig:
			startEvent = time.Now()
			doWhat = "startGetEvent new instance"
			r.beginNextInstance()
			break
		case t := <-r.tryInitPropose:
			startEvent = time.Now()
			doWhat = "attempt initially to propose values in instance"
			r.tryInitaliseForPropose(t.Inst, t.AttemptedBal.Ballot)
			break
		case lease := <-r.promiseLeases:
			startEvent = time.Now()
			doWhat = "handle promise lease"
			r.updateLeases(lease)
			break
		//case <-c:
		//	startEvent = time.Now()
		//	doWhat = "print timeseries stats"
		//	r.TimeseriesStats.PrintAndReset()
		//	break
		case next := <-r.RetryInstance:
			startEvent = time.Now()
			doWhat = "handle new ballot request"
			dlog.Println("Checking whether to retry a proposal")
			r.tryNextAttempt(next)
			break
		case prepareS := <-r.prepareChan:
			startEvent = time.Now()
			doWhat = "handle prepare request"
			prepare := prepareS.(*stdpaxosproto.Prepare)
			//got a Prepare message
			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
			r.handlePrepare(prepare)
			break
		case acceptS := <-r.acceptChan:
			startEvent = time.Now()
			doWhat = "handle accept request"
			accept := acceptS.(*stdpaxosproto.Accept)
			//got an Accept message
			dlog.Printf("Received Accept Request from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
			r.handleAccept(accept)
			break
		case commitS := <-r.commitChan:
			startEvent = time.Now()
			doWhat = "handle Commit"
			commit := commitS.(*stdpaxosproto.Commit)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommit(commit)
			break
		case commitS := <-r.commitShortChan:
			startEvent = time.Now()
			doWhat = "handle Commit short"
			commit := commitS.(*stdpaxosproto.CommitShort)
			//got a Commit message
			dlog.Printf("Received short Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommitShort(commit)
			break
		case prepareReplyS := <-r.prepareReplyChan:
			startEvent = time.Now()
			doWhat = "handle prepare reply"
			prepareReply := prepareReplyS.(*stdpaxosproto.PrepareReply)
			//got a Prepare reply
			dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
			r.HandlePrepareReply(prepareReply)
			break
		case acceptReplyS := <-r.acceptReplyChan:
			startEvent = time.Now()
			doWhat = "handle accept reply"
			acceptReply := acceptReplyS.(*stdpaxosproto.AcceptReply)
			//got an Accept reply
			dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
			r.handleAcceptReply(acceptReply)
			break
		}
		endEvent = time.Now()
		dlog.AgentPrintfN(r.Id, "It took %d µs to receive event %s", startEvent.Sub(startGetEvent).Microseconds(), doWhat)
		dlog.AgentPrintfN(r.Id, "It took %d µs to %s", endEvent.Sub(startEvent).Microseconds(), doWhat)
	}
}

func (man *InstanceProposeValueTimeout) getMinimumSleepingInstance() int32 {
	min := int32(math.MaxInt32)
	for inst := range man.sleepingInsts {
		if inst >= min {
			continue
		}
		min = inst
	}
	if min == int32(math.MaxInt32) {
		panic("No sleeping batches found")
	}
	return min
}

func (r *Replica) bcastPrepare(instance int32) {
	r.PrepareBroadcaster.Bcast(instance, r.instanceSpace[instance].PropCurBal.Ballot)
}

// var pa stdpaxosproto.Accept
func (r *Replica) bcastAccept(instance int32) {
	pbk := r.instanceSpace[instance]
	r.ValueBroadcaster.BcastAccept(instance, pbk.PropCurBal.Ballot, pbk.WhoseCmds, pbk.Cmds)
}

func (r *Replica) bcastCommitToAll(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) {
	r.ValueBroadcaster.BcastCommit(instance, ballot, command, whose)
}

func (r *Replica) beginNextInstance() {
	// if in accept phase try propose, else try prepare?
	opened := r.Proposer.StartNextInstance(&r.instanceSpace)
	for _, i := range opened {
		pbk := r.instanceSpace[i]
		switch pbk.Status {
		case proposer.PREPARING:
			//dlog.AgentPrintfN(r.Id, "Starting slow track instance %d", i)
			prepMsg := getPrepareMessage(r.Id, i, pbk)
			dlog.AgentPrintfN(r.Id, "Opened new instance %d, with ballot %d.%d", i, prepMsg.Number, prepMsg.PropID)
			r.bcastPrepare(i)
			break
		case proposer.READY_TO_PROPOSE:
			// todo move to single instance manager
			//dlog.AgentPrintfN(r.Id, "Starting fast track instance %d", i)
			r.ProposerInstanceQuorumaliser.StartPromiseQuorumOnCurBal(pbk, i)
			r.tryPropose(i, 0)
			break
		}
	}

}

func getPrepareMessage(id int32, inst int32, curInst *proposer.PBK) *stdpaxosproto.Prepare {
	prepMsg := &stdpaxosproto.Prepare{
		LeaderId: id,
		Instance: inst,
		Ballot:   curInst.PropCurBal.Ballot,
	}
	return prepMsg
}

// show at several scales th throughput latency graph
// compare approaches on failure and restarting
// compare the throughput latency difference
func (r *Replica) tryNextAttempt(next proposer.RetryInfo) {
	inst := r.instanceSpace[next.Inst]
	if !r.Proposer.DecideRetry(inst, next) {
		return
	}
	dlog.AgentPrintfN(r.Id, "Retry needed as backoff expired for instance %d", next.Inst)
	r.Proposer.StartNextProposal(inst, next.Inst)
	r.bcastPrepare(next.Inst)
}

func (r *Replica) handlePrepare(prepare *stdpaxosproto.Prepare) {
	dlog.AgentPrintfN(r.Id, logfmt.ReceivePrepareFmt(prepare))
	if int32(prepare.PropID) == r.Id {
		if r.syncAcceptor {
			panic("should not receive promise request this way")
		}
		if !r.writeAheadAcceptor {
			dlog.AgentPrintfN(r.Id, "Giving Prepare in instance %d at ballot %d.%d to acceptor as it is needed for safety", prepare.Instance, prepare.Number, prepare.PropID)
		} else {
			dlog.AgentPrintfN(r.Id, "Giving Prepare in instance %d at ballot %d.%d to acceptor as it can form a quorum", prepare.Instance, prepare.Number, prepare.PropID)
		}
		acceptorHandlePrepareLocal(r.Id, r.Acceptor, r.Learner, r.Replica, prepare, r.PrepareResponsesRPC, r.prepareReplyChan)
		return
	}

	if r.AcceptorQrmInfo.IsInGroup(prepare.Instance, r.Id) {
		dlog.AgentPrintfN(r.Id, "Giving Prepare for instance %d at ballot %d.%d to acceptor as it can form a quorum", prepare.Instance, prepare.Number, prepare.PropID)
		if r.syncAcceptor {
			acceptorSyncHandlePrepare(r.Id, r.Learner, r.Acceptor, prepare, r.PrepareResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.nopreempt)
		} else {
			acceptorHandlePrepareFromRemote(r.Id, r.Learner, r.Acceptor, prepare, r.PrepareResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.nopreempt)
		}
	}

	if r.Proposer.LearnOfBallot(&r.instanceSpace, prepare.Instance, lwcproto.ConfigBal{Config: -1, Ballot: prepare.Ballot}, stdpaxosproto.PROMISE) {
		r.noLongerSleepingInstance(prepare.Instance)
		pCurBal := r.instanceSpace[prepare.Instance].PropCurBal
		if !pCurBal.IsZero() {
			dlog.AgentPrintfN(r.Id, "Prepare Received from Replica %d in instance %d at ballot %d.%d preempted our ballot %d.%d",
				prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID, pCurBal.Number, pCurBal.PropID)
		}
	}
	r.instanceProposeValueTimeout.ProposedClientValuesManager.LearnOfBallot(r.instanceSpace[prepare.Instance], prepare.Instance, lwcproto.ConfigBal{Config: -1, Ballot: prepare.Ballot}, r.ClientBatcher)
}

// func (r *Replica) learn
func (r *Replica) proposerWittnessAcceptedValue(inst int32, aid int32, accepted stdpaxosproto.Ballot, val []*state.Command, whoseCmds int32) bool {
	if accepted.IsZero() {
		return false
	}
	r.Proposer.LearnOfBallotAccepted(&r.instanceSpace, inst, lwcproto.ConfigBal{Config: -1, Ballot: accepted}, whoseCmds)
	pbk := r.instanceSpace[inst]
	//if pbk.Status == proposer.CLOSED {
	//	return false
	//}
	r.instanceProposeValueTimeout.ProposedClientValuesManager.LearnOfBallotValue(pbk, inst, lwcproto.ConfigBal{Config: -1, Ballot: accepted}, val, whoseCmds, r.ClientBatcher)

	newVal := false
	if accepted.GreaterThan(pbk.ProposeValueBal.Ballot) {
		setProposingValue(pbk, whoseCmds, accepted, val)
		newVal = true
	}
	return newVal
}

func setProposingValue(pbk *proposer.PBK, whoseCmds int32, bal stdpaxosproto.Ballot, val []*state.Command) {
	pbk.WhoseCmds = whoseCmds
	pbk.ProposeValueBal = lwcproto.ConfigBal{Config: -1, Ballot: bal}
	pbk.Cmds = val
}

func (r *Replica) HandlePromise(preply *stdpaxosproto.PrepareReply) {
	dlog.AgentPrintfN(r.Id, "Promise recorded on instance %d at ballot %d.%d from Replica %d with value ballot %d.%d and whose commands %d",
		preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.AcceptorId, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd)
	//if r.doPatientProposals {
	//	r.patientProposals.gotPromise(preply.Instance, preply.Req, preply.AcceptorId)
	//}
	pbk := r.instanceSpace[preply.Instance]
	if r.disklessNOOP && (pbk.Status == proposer.READY_TO_PROPOSE || pbk.Status == proposer.PREPARING) {
		if _, e := r.disklessNOOPPromises[preply.Instance]; !e {
			r.disklessNOOPPromises[preply.Instance] = make(map[stdpaxosproto.Ballot]map[int32]struct{})
		}
		if _, e := r.disklessNOOPPromises[preply.Instance][preply.Cur]; !e {
			r.disklessNOOPPromises[preply.Instance][preply.Cur] = make(map[int32]struct{})
		}
		r.disklessNOOPPromises[preply.Instance][preply.Cur][preply.AcceptorId] = struct{}{}
		//if c, ec := r.disklessNOOPPromisesAwaiting[preply.Instance]; ec && r.GotPromisesFromAllInGroup(preply.Instance, preply.Cur) {
		//	c <- struct{}{}
		//}
	}
	if pbk.Status != proposer.PREPARING {
		return
	}
	qrm := pbk.Qrms[pbk.PropCurBal]
	qrm.AddToQuorum(preply.AcceptorId)
	if !qrm.QuorumReached() {
		return
	}
	dlog.AgentPrintfN(r.Id, "Promise Quorum reached in instance %d at ballot %d.%d",
		preply.Instance, preply.Cur.Number, preply.Cur.PropID)
	r.tryInitaliseForPropose(preply.Instance, preply.Req)
}

//func (r *Replica) UpdateValueMaxInstance(inst int32) {
//if r.maxValueInst > inst {
//	return
//}
//r.maxValueInst = inst
//go func() {r.proposableInstances <- struct{}{}}()
//}

func (r *Replica) UpdateMaxLearntInstance(inst int32) {
	if r.maxLearnt > inst {
		return
	}
	r.maxLearnt = inst
	go func() { r.proposableInstances <- struct{}{} }()
}
func (r *Replica) HandlePrepareReply(preply *stdpaxosproto.PrepareReply) {
	// pbk should not be nil
	pbk := r.instanceSpace[preply.Instance]
	dlog.AgentPrintfN(r.Id, logfmt.ReceivePrepareReplyFmt(preply))
	if pbk.Status == proposer.CLOSED {
		return
	}

	if learnerCheckChosen(r.Learner, preply.Instance, preply.Cur, "Prepare Reply", preply.AcceptorId, r.commitRPC, r.Replica, r.Id) {
		return
	}

	if !preply.VBal.IsZero() {
		r.UpdateMaxLearntInstance(preply.Instance)
		r.Learner.ProposalValue(preply.Instance, preply.VBal, preply.Command, preply.WhoseCmd)
		r.Learner.ProposalAccepted(preply.Instance, preply.VBal, preply.AcceptorId)
		// todo make part of learn value
		r.ProposerAddToQuorum(preply.Instance, preply.VBal, preply.AcceptorId, pbk)
		if r.Learner.IsChosen(preply.Instance) && r.Learner.HasLearntValue(preply.Instance) { // newly learnt
			dlog.AgentPrintfN(r.Id, "From prepare replies %d", proposer.LearntInlineFmt(preply.Instance, preply.VBal, r.Proposer, preply.WhoseCmd))
			r.proposerCloseCommit(preply.Instance, preply.VBal, preply.Command, preply.WhoseCmd)
			r.bcastCommitToAll(preply.Instance, preply.VBal, preply.Command, preply.WhoseCmd)
			return
		}
		r.Proposer.LearnOfBallotAccepted(&r.instanceSpace, preply.Instance, lwcproto.ConfigBal{-1, preply.VBal}, preply.WhoseCmd)
	}
	if preply.VBal.GreaterThan(pbk.ProposeValueBal.Ballot) {
		setProposingValue(pbk, preply.WhoseCmd, preply.VBal, preply.Command)
	}

	if preply.Req.GreaterThan(pbk.PropCurBal.Ballot) {
		panic("Some how got a promise on a future proposal")
	}
	if preply.Req.GreaterThan(preply.Cur) {
		panic("somehow acceptor did not promise us")
	}

	// IS PREEMPT?
	if preply.Cur.GreaterThan(preply.Req) {
		isNewPreempted := r.Proposer.LearnOfBallot(&r.instanceSpace, preply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: preply.Cur}, stdpaxosproto.PROMISE)
		r.noLongerSleepingInstance(preply.Instance)
		if isNewPreempted {
			r.instanceProposeValueTimeout.ProposedClientValuesManager.LearnOfBallot(r.instanceSpace[preply.Instance], preply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: preply.Cur}, r.ClientBatcher)
			pCurBal := r.instanceSpace[preply.Instance].PropCurBal
			dlog.AgentPrintfN(r.Id, "Prepare Reply Received from Replica %d in instance %d at with current ballot %d.%d preempted our ballot %d.%d", preply.AcceptorId, preply.Instance, preply.Cur.Number, preply.Cur.PropID, pCurBal.Number, pCurBal.PropID)
		}
		// Proactively try promise
		if r.AcceptorQrmInfo.IsInGroup(preply.Instance, r.Id) && r.proactivelyPrepareOnPreempt && isNewPreempted && int32(preply.Req.PropID) != r.Id {
			newPrep := &stdpaxosproto.Prepare{
				LeaderId: int32(preply.Cur.PropID),
				Instance: preply.Instance,
				Ballot:   preply.Cur,
			}
			acceptorHandlePrepareFromRemote(r.Id, r.Learner, r.Acceptor, newPrep, r.PrepareResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.nopreempt)
		}
		return
	}
	// IS PROMISE.
	r.HandlePromise(preply)
}

func (r *Replica) ProposerAddToQuorum(inst int32, bal stdpaxosproto.Ballot, aid int32, pbk *proposer.PBK) {
	if pbk.Qrms[lwcproto.ConfigBal{-1, bal}] == nil {
		r.LearnerQuorumaliser.TrackProposalAcceptance(pbk, inst, lwcproto.ConfigBal{-1, bal})
	}
	pbk.Qrms[lwcproto.ConfigBal{-1, bal}].AddToQuorum(aid)
}

func (r *Replica) tryInitaliseForPropose(inst int32, ballot stdpaxosproto.Ballot) {
	pbk := r.instanceSpace[inst]
	if !pbk.PropCurBal.Ballot.Equal(ballot) || pbk.Status != proposer.PREPARING {
		return
	}

	qrm := pbk.Qrms[pbk.PropCurBal]
	if (!qrm.HasAcknowledged(r.Id) && !r.writeAheadAcceptor) || (r.writeAheadAcceptor && !r.isLeased(inst, pbk.PropCurBal.Ballot)) {
		dlog.AgentPrintfN(r.Id, "Not safe to send accept requests for instance %d, need to wait until a lease or promise from our acceptor is received", inst)
		go func() {
			time.Sleep(5 * time.Millisecond) // todo replace with check upon next message to see if try propose again or clean up this info
			r.tryInitPropose <- proposer.RetryInfo{
				Inst:           inst,
				AttemptedBal:   lwcproto.ConfigBal{Config: -1, Ballot: ballot},
				PreempterBal:   lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}},
				PreempterAt:    0,
				Prev:           0,
				TimesPreempted: 0,
			}
		}()
		return
	}

	if !pbk.ProposeValueBal.IsZero() && pbk.WhoseCmds != r.Id && pbk.ClientProposals != nil {
		pbk.ClientProposals = nil // at this point, our client proposal will not be chosen
	}

	pbk.Status = proposer.READY_TO_PROPOSE
	r.tryPropose(inst, 0)
}

func (r *Replica) tryPropose(inst int32, priorAttempts int) {
	pbk := r.instanceSpace[inst]
	if pbk.Status != proposer.READY_TO_PROPOSE {
		panic("asjfalskdjf")
	}
	dlog.AgentPrintfN(r.Id, "Attempting to propose value in instance %d", inst)
	qrm := pbk.Qrms[pbk.PropCurBal]
	qrm.StartAcceptanceQuorum()

	if pbk.ProposeValueBal.IsZero() {
		b := r.ClientBatcher.GetBatchToPropose()
		if b != nil {
			pbk.ClientProposals = b
			setProposingValue(pbk, r.Id, pbk.PropCurBal.Ballot, pbk.ClientProposals.GetCmds())
		} else {
			if priorAttempts == 0 {
				r.BeginWaitingForClientProposals(inst, pbk)
				return
			}
			man := r.instanceProposeValueTimeout
			if len(man.nextBatch.Cmds) > 0 {
				b = &batching.Batch{
					Proposals: man.nextBatch.ClientVals,
					Cmds:      man.nextBatch.Cmds,
					Uid:       man.nextBatch.Uid,
				}
				dlog.AgentPrintfN(r.Id, "Assembled partial batch with UID %d (length %d values)", b.GetUID(), len(b.GetCmds()))
				man.nextBatch.Cmds = make([]*state.Command, 0, man.nextBatch.MaxLength)
				man.nextBatch.ClientVals = make([]*genericsmr.Propose, 0, man.nextBatch.MaxLength)
				man.nextBatch.Uid += 1
				pbk.ClientProposals = b
				setProposingValue(pbk, r.Id, pbk.PropCurBal.Ballot, pbk.ClientProposals.GetCmds())
			} else {
				if r.isProposingDisklessNOOP(inst) {
					setProposingValue(pbk, -1, pbk.ProposeValueBal.Ballot, state.DisklessNOOPP())
				}
				setProposingValue(pbk, -1, pbk.PropCurBal.Ballot, state.NOOPP())
			}
		}
	}

	if pbk.Cmds == nil {
		panic("there must be something to propose")
	}
	pbk.SetNowProposing()
	b := r.Proposer.GetBalloter()
	dlog.AgentPrintfN(r.Id, proposer.ProposingValue(r.Id, pbk, inst, int32(b.GetAttemptNumber(pbk.ProposeValueBal.Number)), r.isProposingDisklessNOOP(inst)))

	r.Learner.ProposalValue(inst, pbk.PropCurBal.Ballot, pbk.Cmds, pbk.WhoseCmds)
	if pbk.ClientProposals != nil {
		r.Executor.ProposedBatch(inst, pbk.ClientProposals)
	}
	r.bcastAccept(inst)
	r.Proposer.LearnOfBallotAccepted(&r.instanceSpace, inst, pbk.PropCurBal, pbk.WhoseCmds)
}

func (r *Replica) isProposingDisklessNOOP(inst int32) bool {
	pbk := r.instanceSpace[inst]
	if pbk.PropCurBal.Ballot.Number == 0 {
		return true
	}
	return r.disklessNOOP && pbk.WhoseCmds == -1 && pbk.Status >= proposer.PROPOSING && r.GotPromisesFromAllInGroup(inst, pbk.PropCurBal.Ballot)
}

func (r *Replica) BeginWaitingForClientProposals(inst int32, pbk *proposer.PBK) {
	r.instanceProposeValueTimeout.sleepingInsts[inst] = time.Now()
	if len(r.instanceProposeValueTimeout.sleepingInsts) > 1 {
		dlog.AgentPrintfN(r.Id, "No client value to propose in instance %d at ballot %d.%d. Queued instance for checking again.", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
		return
	}
	dlog.AgentPrintfN(r.Id, "No client values to propose in instance %d at ballot %d.%d. Waiting %d ms before checking again", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID, r.noopWait.Milliseconds())
	go func() {
		<-time.After(r.noopWait)
		r.proposableInstances <- struct{}{}
	}()
}

func (r *Replica) checkAndHandleNewlyReceivedInstance(instance int32, ballot stdpaxosproto.Ballot) {
	if instance < 0 {
		return
	}
	r.Proposer.LearnOfBallot(&r.instanceSpace, instance, lwcproto.ConfigBal{Config: -1, Ballot: ballot}, stdpaxosproto.PROMISE)
}

func (r *Replica) handleAccept(accept *stdpaxosproto.Accept) {
	dlog.AgentPrintfN(r.Id, logfmt.ReceiveAcceptFmt(accept))
	//r.checkAndHandleNewlyReceivedInstance(accept.Instance)
	r.Proposer.LearnOfBallot(&r.instanceSpace, accept.Instance, lwcproto.ConfigBal{-1, accept.Ballot}, stdpaxosproto.ACCEPTANCE)
	r.UpdateMaxLearntInstance(accept.Instance)
	r.Learner.ProposalValue(accept.Instance, accept.Ballot, accept.Command, accept.WhoseCmd)
	if r.Learner.IsChosen(accept.Instance) && r.Learner.HasLearntValue(accept.Instance) {
		// tell proposer and acceptor of learnt
		cB, _, _ := r.Learner.GetChosen(accept.Instance)
		r.proposerCloseCommit(accept.Instance, cB, accept.Command, accept.WhoseCmd)
		return
	}

	pbk := r.instanceSpace[accept.Instance]
	if accept.Ballot.GreaterThan(pbk.ProposeValueBal.Ballot) {
		setProposingValue(pbk, accept.WhoseCmd, accept.Ballot, accept.Command)
	}

	if r.Proposer.LearnOfBallot(&r.instanceSpace, accept.Instance, lwcproto.ConfigBal{Config: -1, Ballot: accept.Ballot}, stdpaxosproto.ACCEPTANCE) {
		pCurBal := r.instanceSpace[accept.Instance].PropCurBal
		r.noLongerSleepingInstance(accept.Instance)
		if !pCurBal.IsZero() {
			dlog.AgentPrintfN(r.Id, "Accept Received from Replica %d in instance %d at ballot %d.%d preempted our ballot %d.%d",
				accept.PropID, accept.Instance, accept.Number, accept.PropID, pCurBal.Number, pCurBal.PropID)
		}
	}
	r.Proposer.LearnOfBallotAccepted(&r.instanceSpace, accept.Instance, lwcproto.ConfigBal{Config: -1, Ballot: accept.Ballot}, accept.WhoseCmd)
	// todo change with ballot value <- still safe rn cause accepted proposers do not affect safety but would be better interface design

	if accept.LeaderId == -2 {
		dlog.AgentPrintfN(r.Id, "Not passing Accept for instance %d at ballot %d.%d to acceptor as we are passive observers", accept.Instance, accept.Ballot.Number, accept.Ballot.PropID)
		return
	}
	if !r.AcceptorQrmInfo.IsInGroup(accept.Instance, r.Id) {
		dlog.AgentPrintfN(r.Id, "Not passing Accept for instance %d at ballot %d.%d to acceptor as we cannot form an acceptance quorum", accept.Instance, accept.Ballot.Number, accept.Ballot.PropID)
		return
	}

	r.acceptorHandleAcceptRequest(accept)
}

func (r *Replica) acceptorHandleAcceptRequest(accept *stdpaxosproto.Accept) {
	if int32(accept.PropID) == r.Id {
		acceptorHandleAcceptLocal(r.Id, r.Acceptor, accept, r.AcceptResponsesRPC, r.Replica, r.bcastAcceptance, r.acceptReplyChan)
		return
	}
	acceptorHandleAccept(r.Id, r.Learner, r.Acceptor, accept, r.AcceptResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.bcastAcceptance, r.acceptReplyChan, r.nopreempt, r.bcastAcceptDisklessNOOP)
}

func (r *Replica) handleAcceptance(areply *stdpaxosproto.AcceptReply) {
	r.Learner.ProposalAccepted(areply.Instance, areply.Cur, areply.AcceptorId)
	//r.UpdateValueMaxInstance(areply.Instance)
	dlog.AgentPrintfN(r.Id, "Acceptance recorded on proposal instance %d at ballot %d.%d from Replica %d with whose commands %d",
		areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId, areply.WhoseCmd)
	if r.Learner.IsChosen(areply.Instance) && r.Learner.HasLearntValue(areply.Instance) {
		_, cV, cWC := r.Learner.GetChosen(areply.Instance)
		r.proposerCloseCommit(areply.Instance, areply.Cur, cV, cWC)
	}
}

func (r *Replica) handleAcceptReply(areply *stdpaxosproto.AcceptReply) {
	//r.Proposer.LearnOfBallot(&r.instanceSpace, areply.Instance, lwcproto.ConfigBal{-1, areply.Cur}, areply.CurPhase)
	r.checkAndHandleNewlyReceivedInstance(areply.Instance, areply.Cur)
	dlog.AgentPrintfN(r.Id, logfmt.ReceiveAcceptReplyFmt(areply))
	pbk := r.instanceSpace[areply.Instance]

	if learnerCheckChosen(r.Learner, areply.Instance, areply.Cur, "Accept Reply", areply.AcceptorId, r.commitRPC, r.Replica, r.Id) {
		return
	}

	// PREEMPTED
	if areply.Cur.GreaterThan(areply.Req) {
		pCurBal := r.instanceSpace[areply.Instance].PropCurBal
		dlog.AgentPrintfN(r.Id, "Accept Reply received from Replica %d in instance %d with current ballot %d.%d preempted our ballot %d.%d",
			areply.AcceptorId, areply.Instance, areply.Cur.Number, areply.Cur.PropID, pCurBal.Number, pCurBal.PropID)
		r.Proposer.LearnOfBallot(&r.instanceSpace, areply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur}, areply.CurPhase)
		r.instanceProposeValueTimeout.ProposedClientValuesManager.LearnOfBallot(pbk, areply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur}, r.ClientBatcher)
		r.noLongerSleepingInstance(areply.Instance)
		return
	}
	if areply.Req.GreaterThan(pbk.PropCurBal.Ballot) && areply.Req.PropID == int16(r.Id) {
		panic("got a future acceptance??")
	}
	if areply.Req.GreaterThan(areply.Cur) {
		panic("Acceptor didn't accept request")
	}

	// ACCEPTED
	r.handleAcceptance(areply)
}

func (r *Replica) proposerCloseCommit(inst int32, chosenAt stdpaxosproto.Ballot, chosenVal []*state.Command, whoseCmd int32) {
	pbk := r.instanceSpace[inst]
	r.UpdateMaxLearntInstance(inst)
	//r.UpdateValueMaxInstance(inst)
	if pbk.Status == proposer.CLOSED {
		return
	}
	if r.Id == int32(chosenAt.PropID) {
		r.bcastCommitToAll(inst, chosenAt, chosenVal, whoseCmd)
	}
	r.Proposer.LearnBallotChosen(&r.instanceSpace, inst, lwcproto.ConfigBal{Config: -1, Ballot: chosenAt}, whoseCmd) // todo add client value chosen log
	if whoseCmd == r.Id {
		if _, e := r.instanceProposeValueTimeout.chosenBatches[pbk.ClientProposals.GetUID()]; e {
			dlog.AgentPrintfN(r.Id, proposer.LearntBatchAgainFmt(inst, chosenAt, r.Proposer, whoseCmd, pbk.ClientProposals))
		} else {
			dlog.AgentPrintfN(r.Id, proposer.LearntBatchFmt(inst, chosenAt, r.Proposer, whoseCmd, pbk.ClientProposals))
		}
		r.instanceProposeValueTimeout.chosenBatches[pbk.ClientProposals.GetUID()] = struct{}{}
	} else {
		dlog.AgentPrintfN(r.Id, proposer.LearntFmt(inst, chosenAt, r.Proposer, whoseCmd))
	}
	r.instanceProposeValueTimeout.ProposedClientValuesManager.ValueChosen(pbk, inst, whoseCmd, chosenVal, r.ClientBatcher)

	r.noLongerSleepingInstance(inst)
	setProposingValue(pbk, whoseCmd, chosenAt, chosenVal)
	r.Executor.Learnt(inst, chosenVal, whoseCmd)
}

// todo make it so proposer acceptor and learner all guard on chosen
func (r *Replica) handleCommit(commit *stdpaxosproto.Commit) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance, commit.Ballot)
	dlog.AgentPrintfN(r.Id, logfmt.ReceiveCommitFmt(commit))

	if r.Learner.IsChosen(commit.Instance) && r.Learner.HasLearntValue(commit.Instance) {
		dlog.AgentPrintfN(r.Id, "Ignoring Commit for instance %d as it is already learnt", commit.Instance)
		return
	}
	r.Learner.ProposalValue(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
	r.Learner.ProposalChosen(commit.Instance, commit.Ballot)
	r.proposerCloseCommit(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
}

// if commited at one ballot, only need one ack from a higher ballot to be chosen
func (r *Replica) handleCommitShort(commit *stdpaxosproto.CommitShort) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance, commit.Ballot)
	dlog.AgentPrintfN(r.Id, logfmt.ReceiveCommitShortFmt(commit))
	pbk := r.instanceSpace[commit.Instance]
	r.Learner.ProposalChosen(commit.Instance, commit.Ballot)
	if !r.Learner.HasLearntValue(commit.Instance) {
		dlog.AgentPrintfN(r.Id, "Cannot learn instance %d at ballot %d.%d as we do not have a value for it", commit.Instance, commit.Ballot.Number, commit.Ballot.PropID)
		return
	}
	if pbk.Cmds == nil {
		panic("We don't have any record of the value to be committed")
	}
	r.proposerCloseCommit(commit.Instance, commit.Ballot, pbk.Cmds, commit.WhoseCmd)
}

func (r *Replica) handleState(state *proposerstate.State) {
	r.Proposer.LearnOfBallot(&r.instanceSpace, state.CurrentInstance, lwcproto.ConfigBal{-2, stdpaxosproto.Ballot{-2, int16(state.ProposerID)}}, stdpaxosproto.PROMISE)
}

func (r *Replica) updateLeases(lease acceptor.PromiseLease) {
	dlog.AgentPrintfN(r.Id, "Received lease for from instance %d to %d, until ballots %d.%d", lease.From, lease.From+r.iWriteAhead, lease.MaxBalPromises.Number, lease.MaxBalPromises.PropID)
	r.classesLeased[lease.From/r.iWriteAhead] = lease.MaxBalPromises
}

func (r *Replica) isLeased(inst int32, ballot stdpaxosproto.Ballot) bool {
	bLeased, e := r.classesLeased[inst/r.iWriteAhead]
	if !e {
		return false
	}
	if ballot.GreaterThan(bLeased) {
		return false
	}
	return true
}

func (r *Replica) GotPromisesFromAllInGroup(instance int32, ballot stdpaxosproto.Ballot) bool {
	g := r.GetGroup(instance)

	promisers := r.disklessNOOPPromises[instance][ballot]
	for _, a := range g {
		if _, e := promisers[a]; !e {
			return false
		}
	}
	return true
}

func (r *Replica) noInstancesWaiting(inst int32) bool {
	for i := inst + 1; i <= r.Proposer.GetCrtInstance(); i++ {
		pbk := r.instanceSpace[i]
		if !pbk.ProposeValueBal.IsZero() {
			return false
		}
	}
	return true
}
