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
	balloter2 "epaxos/twophase/balloter"
	"epaxos/twophase/exec"
	"epaxos/twophase/learner"
	"epaxos/twophase/logfmt"
	"epaxos/twophase/proposalmanager"
	"math"
	"math/rand"
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
	phase   proposalmanager.ProposerStatus
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

	exec.Executor
	learner.Learner

	proposalmanager.CrtInstanceOracle
	ProposeBatchOracle

	balloter *balloter2.Balloter

	proposalmanager.Proposer
	proposalmanager.ProposerInstanceQuorumaliser
	proposalmanager.LearnerQuorumaliser
	proposalmanager.AcceptorQrmInfo

	batchLearners []MyBatchLearner
	noopLearners  []proposalmanager.NoopLearner

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
	instanceSpace                 []*proposalmanager.PBK // the space of all instances (used and not yet used)
	Shutdown                      bool
	counter                       int
	flush                         bool
	maxBatchWait                  int
	crtOpenedInstances            []int32
	proposableInstances           chan struct{} //*time.Timer
	noopWaitUs                    int32
	retryInstance                 chan proposalmanager.RetryInfo
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
	doStats                       bool
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
	//doPatientProposals     bool
	//patientProposals
	startInstanceSig   chan struct{}
	doEager            bool
	promiseLeases      chan acceptor.PromiseLease
	classesLeased      map[int32]stdpaxosproto.Ballot
	iWriteAhead        int32
	writeAheadAcceptor bool
	tryInitPropose     chan proposalmanager.RetryInfo
	sendFastestQrm     bool
	nudge              chan chan batching.ProposalBatch
	bcastCommit        bool
	nopreempt          bool
	bcastAcceptance    bool
	syncAcceptor       bool
	disklessNOOP       bool

	disklessNOOPPromisesAwaiting map[int32]chan struct{}
	disklessNOOPPromises         map[int32]map[stdpaxosproto.Ballot]map[int32]struct{}
	forceDisklessNOOP            bool

	clientBatcher               ClientRequestBatcher
	instanceProposeValueTimeout *InstanceProposeValueTimeout
	nextNoopEnd                 time.Time
	nextNoop                    *time.Timer
	noops                       int
	resetTo                     chan time.Duration
	noopCancel                  chan struct{}
	execSig                     proposalmanager.ExecOpenInstanceSignal
	bcastAcceptDisklessNOOP     bool
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
	if r.doStats {
		r.TimeseriesStats.Close()
		r.InstanceStats.Close()
		r.ProposalStats.CloseOutput()
	}
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
	ProposedClientValuesManager
	nextBatch                  curBatch
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

	var c chan struct{}
	if r.doStats {
		r.TimeseriesStats.GoClock()
		c = r.TimeseriesStats.C
	} else {
		c = make(chan struct{})
	}

	startGetEvent := time.Now()
	doWhat := ""

	startEvent := time.Now()
	endEvent := time.Now()
	for !r.Shutdown {
		startGetEvent = time.Now()
		select {
		case <-r.proposableInstances:
			startEvent = time.Now()
			doWhat = "handle instance proposing timeout"
			dlog.AgentPrintfN(r.Id, "Got notification to noop")
			if len(r.instanceProposeValueTimeout.sleepingInsts) == 0 {
				break
			}
			for len(r.instanceProposeValueTimeout.sleepingInsts) > 0 {
				min := r.instanceProposeValueTimeout.getMinimumSleepingInstance()
				r.noLongerSleepingInstance(min)
				pbk := r.instanceSpace[min]
				if pbk == nil {
					panic("????")
				}
				dlog.AgentPrintfN(r.Id, "Rechecking whether to propose in instance %d", min)
				if pbk.Status != proposalmanager.READY_TO_PROPOSE {
					dlog.AgentPrintfN(r.Id, "Decided to not propose in instance %d as we are no longer on ballot %d.%d", min, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
					break
				}
				r.updateNoopTimer()
				r.tryPropose(min, 2)
			}
			break
		case clientRequest := <-r.ProposeChan:
			startEvent = time.Now()
			doWhat = "handle client request"
			if r.clientBatcher.CurrentBatchLen() == 0 && !r.doEager {
				go func() { r.startInstanceSig <- struct{}{} }()
			}
			r.clientBatcher.AddProposal(clientRequest, r.ProposeChan)
			if len(r.instanceProposeValueTimeout.sleepingInsts) == 0 {
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
		case <-r.startInstanceSig:
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
		case <-c:
			startEvent = time.Now()
			doWhat = "print timeseries stats"
			r.TimeseriesStats.PrintAndReset()
			break
		case next := <-r.retryInstance:
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
		panic("asldfjdslk")
	}
	return min
}

func (r *Replica) bcastPrepareUDP(instance int32, ballot stdpaxosproto.Ballot) {
	args := &stdpaxosproto.Prepare{r.Id, instance, ballot}
	//pbk := r.instanceSpace[instance]
	sentTo := make([]int32, 0, r.N)
	sendMsgs := make([]int32, 0, r.N)
	if r.sendPreparesToAllAcceptors {
		inGroup := false
		for _, i := range r.AcceptorQrmInfo.GetGroup(instance) {
			if i == r.Id {
				inGroup = true
				continue
			}
			sentTo = append(sentTo, i)
			sendMsgs = append(sendMsgs, i)
		}
		if !r.writeAheadAcceptor || inGroup {
			r.sendPrepareToSelf(args)
			sentTo = append(sentTo, r.Id)
		}
	} else {
		panic("Not yet implemented")
	}

	//} else {
	//	sentTo = pbk.Qrms[pbk.PropCurBal].Broadcast(r.prepareRPC, args)
	//}
	r.Replica.BcastUDPMsg(sendMsgs, r.prepareRPC, args, false)

	if r.doStats {
		instID := stats.InstanceID{
			Log: 0,
			Seq: instance,
		}
		r.InstanceStats.RecordOccurrence(instID, "My Phase 1 Proposals", 1)
	}
	dlog.AgentPrintfN(r.Id, "Broadcasting Prepare for instance %d at ballot %d.%d to replicas %v", args.Instance, args.Number, args.PropID, sentTo)
}

func (r *Replica) bcastPrepareTCP(instance int32, ballot stdpaxosproto.Ballot) {
	args := &stdpaxosproto.Prepare{r.Id, instance, ballot}
	//pbk := r.instanceSpace[instance]
	var sentTo []int32
	if r.sendPreparesToAllAcceptors {
		sentTo = make([]int32, 0, r.N)
		r.Replica.Mutex.Lock()
		inGroup := false
		for _, i := range r.AcceptorQrmInfo.GetGroup(instance) {
			if i == r.Id {
				inGroup = true
				continue
			}
			r.Replica.SendMsgUNSAFE(i, r.prepareRPC, args)
			sentTo = append(sentTo, i)
		}
		r.Replica.Mutex.Unlock()
		if !r.writeAheadAcceptor || inGroup {
			r.sendPrepareToSelf(args)
			sentTo = append(sentTo, r.Id)
		}
	} else {
		panic("not implemented")
		//sentTo = pbk.Qrms[pbk.PropCurBal].Broadcast(r.prepareRPC, args)
	}

	if r.doStats {
		instID := stats.InstanceID{
			Log: 0,
			Seq: instance,
		}
		r.InstanceStats.RecordOccurrence(instID, "My Phase 1 Proposals", 1)
	}
	dlog.AgentPrintfN(r.Id, "Broadcasting Prepare for instance %d at ballot %d.%d to replicas %v", args.Instance, args.Number, args.PropID, sentTo)
}

func (r *Replica) bcastPrepare(instance int32) {
	if r.UDP {
		r.bcastPrepareUDP(instance, r.instanceSpace[instance].PropCurBal.Ballot)
		return
	}
	r.bcastPrepareTCP(instance, r.instanceSpace[instance].PropCurBal.Ballot) // todo do as interface
}

func (r *Replica) sendPrepareToSelf(args *stdpaxosproto.Prepare) {
	if r.syncAcceptor {
		promise := acceptorSyncHandlePrepareLocal(r.Id, r.Acceptor, args, r.PrepareResponsesRPC)
		if promise == nil {
			return
		}
		r.HandlePromise(promise)
	} else {
		go func(prep *stdpaxosproto.Prepare) {
			r.prepareChan <- prep
		}(args)
	}
}

func (r *Replica) bcastAcceptUDP(instance int32, ballot stdpaxosproto.Ballot, whosecmds int32, cmds []*state.Command) {
	// -1 = ack, -2 = passive observe, -3 = diskless accept
	pa := stdpaxosproto.Accept{
		LeaderId: -1,
		Instance: instance,
		Ballot:   ballot,    //r.instanceSpace[instance].PropCurBal.Ballot,
		WhoseCmd: whosecmds, //r.instanceSpace[instance].WhoseCmds,
		Command:  cmds,      //r.instanceSpace[instance].Cmds,
	}
	paPassive := pa
	paPassive.LeaderId = -2

	sendC := r.F + 1
	if !r.Thrifty {
		sendC = r.F*2 + 1
	}

	// todo add check to see if alive??
	pa.LeaderId = -1
	disklessNOOP := r.isProposingDisklessNOOP(instance)
	if disklessNOOP {
		pa.LeaderId = -3
	}
	// todo order peerlist by latency or random
	// first try to send to self
	sentTo := make([]int32, 0, r.N)
	sendMsgsActive := make([]int32, 0, r.N)
	sendMsgsPassive := make([]int32, 0, r.N)
	acceptorGroup := r.AcceptorQrmInfo.GetGroup(instance)
	rand.Shuffle(len(acceptorGroup), func(i, j int) {
		tmp := acceptorGroup[i]
		acceptorGroup[i] = acceptorGroup[j]
		acceptorGroup[j] = tmp
	})
	selfInGroup := false
	isBcastingAccept := r.bcastAcceptance || (disklessNOOP && r.bcastAcceptDisklessNOOP)
	// send to those in group except self
	for _, peer := range acceptorGroup {
		if peer == r.Id {
			selfInGroup = true
			sentTo = append(sentTo, r.Id)
			break
		}
	}

	for _, peer := range acceptorGroup {
		if peer == r.Id {
			continue
		}
		if len(sentTo) >= sendC {
			if !isBcastingAccept {
				break
			}
			//r.Replica.SendUDPMsg(peer, r.acceptRPC, &paPassive, true)
			sentTo = append(sentTo, peer)
			sendMsgsPassive = append(sendMsgsActive, peer)
			continue
		}
		//r.Replica.SendUDPMsg(peer, r.acceptRPC, &pa, true)
		sendMsgsActive = append(sendMsgsActive, peer)
		sentTo = append(sentTo, peer)
	}

	// send to those not in group
	if isBcastingAccept {
		for peer := int32(0); peer < int32(r.N); peer++ {
			if r.AcceptorQrmInfo.IsInGroup(instance, peer) {
				continue
			}
			sendMsgsPassive = append(sendMsgsPassive, peer)
			sentTo = append(sentTo, peer)
		}
	}
	r.Replica.BcastUDPMsg(sendMsgsActive, r.acceptRPC, &pa, true)
	r.Replica.BcastUDPMsg(sendMsgsPassive, r.acceptRPC, &paPassive, true)
	if selfInGroup {
		go func() { r.acceptChan <- &pa }()
	}
	if r.doStats {
		instID := stats.InstanceID{
			Log: 0,
			Seq: instance,
		}
		r.InstanceStats.RecordOccurrence(instID, "My Phase 2 Proposals", 1)
	}
	dlog.AgentPrintfN(r.Id, "Broadcasting Accept for instance %d with whose commands %d at ballot %d.%d to Replicas %v (active acceptors %v)", pa.Instance, pa.WhoseCmd, pa.Number, pa.PropID, sentTo, sendMsgsActive)
}

func (r *Replica) bcastAcceptTCP(instance int32, ballot stdpaxosproto.Ballot, whosecmds int32, cmds []*state.Command) {
	// -1 = ack, -2 = passive observe, -3 = diskless accept
	pa := stdpaxosproto.Accept{
		LeaderId: -1,
		Instance: instance,
		Ballot:   ballot,
		WhoseCmd: whosecmds,
		Command:  cmds,
	}
	args := &pa

	sendC := r.F + 1
	if !r.Thrifty {
		sendC = r.F*2 + 1
	}

	// todo add check to see if alive??

	pa.LeaderId = -1
	disklessNOOP := r.isProposingDisklessNOOP(instance) // will not update after proposal made
	// todo order peerlist by latency or random
	// -1 = ack, -2 = passive observe, -3 = diskless accept
	// first try to send to self
	sentTo := make([]int32, 0, r.N)
	acceptorGroup := r.AcceptorQrmInfo.GetGroup(instance)
	rand.Shuffle(len(acceptorGroup), func(i, j int) {
		tmp := acceptorGroup[i]
		acceptorGroup[i] = acceptorGroup[j]
		acceptorGroup[j] = tmp
	})
	selfInGroup := false
	r.Replica.Mutex.Lock()
	isBcastingAccept := r.bcastAcceptance || (disklessNOOP && r.bcastAcceptDisklessNOOP)
	// send to those in group except self
	for _, peer := range acceptorGroup {
		if peer == r.Id {
			selfInGroup = true
			sentTo = append(sentTo, r.Id)
			break
		}
	}

	for _, peer := range acceptorGroup {
		if peer == r.Id {
			continue
		}
		if len(sentTo) >= sendC {
			if !isBcastingAccept {
				break
			}
			pa.LeaderId = -2
		}
		r.Replica.SendMsgUNSAFE(peer, r.acceptRPC, args)
		sentTo = append(sentTo, peer)
	}

	// send to those not in group
	if isBcastingAccept {
		pa.LeaderId = -2
		for peer := int32(0); peer < int32(r.N); peer++ {
			if r.AcceptorQrmInfo.IsInGroup(instance, peer) {
				continue
			}
			r.Replica.SendMsgUNSAFE(peer, r.acceptRPC, args)
			sentTo = append(sentTo, peer)
		}
	}
	r.Replica.Mutex.Unlock()

	if selfInGroup {
		pa.LeaderId = -1
		if disklessNOOP {
			pa.LeaderId = -3
		}
		go func() { r.acceptChan <- args }()
	}
	instID := stats.InstanceID{
		Log: 0,
		Seq: instance,
	}
	if r.doStats {
		r.InstanceStats.RecordOccurrence(instID, "My Phase 2 Proposals", 1)
	}
	dlog.AgentPrintfN(r.Id, "Broadcasting Accept for instance %d with whose commands %d at ballot %d.%d to Replicas %v", pa.Instance, pa.WhoseCmd, pa.Number, pa.PropID, sentTo)
}

// var pa stdpaxosproto.Accept
func (r *Replica) bcastAccept(instance int32) {
	pbk := r.instanceSpace[instance]
	if r.UDP {
		r.bcastAcceptUDP(instance, pbk.PropCurBal.Ballot, pbk.WhoseCmds, pbk.Cmds)
		return
	}
	go r.bcastAcceptTCP(instance, pbk.PropCurBal.Ballot, pbk.WhoseCmds, pbk.Cmds)
}

func (r *Replica) bcastCommitUDP(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) {
	var pc stdpaxosproto.Commit
	var pcs stdpaxosproto.CommitShort

	if len(r.instanceSpace[instance].Cmds) == 0 {
		panic("fkjdlkfj;dlfj")
	}

	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.WhoseCmd = whose //r.instanceSpace[instance].WhoseCmds
	pc.MoreToCome = 0
	pc.Command = command

	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = ballot
	pcs.WhoseCmd = whose // r.instanceSpace[instance].WhoseCmds
	pcs.Count = int32(len(command))
	argsShort := pcs
	// promises will not update after status == closed

	if r.bcastAcceptance || (r.disklessNOOP && r.bcastAcceptDisklessNOOP && r.GotPromisesFromAllInGroup(instance, ballot) && pc.WhoseCmd == -1) {
		if !r.bcastCommit {
			return
		}
		list := make([]int32, 0, r.N)
		for q := int32(0); q < int32(r.N); q++ {
			if q == r.Id {
				continue
			}
			//r.SendUDPMsg(q, r.commitShortRPC, &argsShort, true)
			list = append(list, q)
		}
		dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID)
		r.Replica.BcastUDPMsg(list, r.commitShortRPC, &argsShort, true)
		return
	}

	sList := make([]int32, 0, r.N)
	list := make([]int32, 0, r.N)
	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id {
			continue
		}

		inQrm := r.instanceSpace[instance].Qrms[lwcproto.ConfigBal{Config: -1, Ballot: ballot}].HasAcknowledged(q)
		if inQrm {
			sList = append(sList, q)
			//dlog.AgentPrintfN(r.Id, "Broadcasting Commit short for instance %d learnt with whose commands %d, at ballot %d.%d to ", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID, q)
			//r.SendUDPMsg(q, r.commitShortRPC, &argsShort, true)
		} else {
			list = append(list, q)
			//dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d to ", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID, q)
			//r.SendUDPMsg(q, r.commitRPC, &pc, true)
		}
	}
	r.Replica.BcastUDPMsg(list, r.commitRPC, &pc, true)
	r.Replica.BcastUDPMsg(sList, r.commitShortRPC, &argsShort, true)

	dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID)
}

func (r *Replica) bcastCommitTCP(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) {
	var pc stdpaxosproto.Commit
	var pcs stdpaxosproto.CommitShort
	if len(r.instanceSpace[instance].Cmds) == 0 {
		panic("fkjdlkfj;dlfj")
	}

	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.WhoseCmd = whose //r.instanceSpace[instance].WhoseCmds
	pc.MoreToCome = 0
	pc.Command = command

	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = ballot
	pcs.WhoseCmd = whose //r.instanceSpace[instance].WhoseCmds
	pcs.Count = int32(len(command))
	argsShort := pcs
	if r.bcastAcceptance || (r.disklessNOOP && r.bcastAcceptDisklessNOOP && r.GotPromisesFromAllInGroup(instance, ballot) && pc.WhoseCmd == -1) {
		if !r.bcastCommit {
			return
		}
		r.Replica.Mutex.Lock()
		for q := int32(0); q < int32(r.N); q++ {
			if q == r.Id {
				continue
			}
			r.SendMsgUNSAFE(q, r.commitShortRPC, &argsShort)
		}
		r.Replica.Mutex.Unlock()
		dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID)
		return
	}

	r.Replica.Mutex.Lock()
	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id {
			continue
		}

		inQrm := r.instanceSpace[instance].Qrms[lwcproto.ConfigBal{Config: -1, Ballot: ballot}].HasAcknowledged(q)
		if inQrm {
			dlog.AgentPrintfN(r.Id, "Broadcasting Commit short for instance %d learnt with whose commands %d, at ballot %d.%d to ", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID, q)
			r.SendMsgUNSAFE(q, r.commitShortRPC, &argsShort)
		} else {
			dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d to ", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID, q)
			r.SendMsgUNSAFE(q, r.commitRPC, &pc)
		}
	}
	r.Replica.Mutex.Unlock()
	dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID)
}

func (r *Replica) bcastCommitToAll(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) {
	if r.UDP {
		go r.bcastCommitUDP(instance, ballot, command, whose)
		return
	}
	go r.bcastCommitTCP(instance, ballot, command, whose)
}

func (r *Replica) beginNextInstance() {
	// if in accept phase try propose, else try prepare?
	r.Proposer.StartNextInstance(&r.instanceSpace)
	// todo change to for loop
	inst := r.Proposer.GetCrtInstance()
	curInst := r.instanceSpace[inst]
	prepMsg := getPrepareMessage(r.Id, inst, curInst)
	dlog.AgentPrintfN(r.Id, "Opened new instance %d, with ballot %d.%d", inst, prepMsg.Number, prepMsg.PropID)
	r.bcastPrepare(inst)
}

func getPrepareMessage(id int32, inst int32, curInst *proposalmanager.PBK) *stdpaxosproto.Prepare {
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
func (r *Replica) tryNextAttempt(next proposalmanager.RetryInfo) {
	inst := r.instanceSpace[next.Inst]
	if !r.Proposer.DecideRetry(inst, next) {
		return
	}

	dlog.AgentPrintfN(r.Id, "Retry needed as backoff expired for instance %d", next.Inst)
	r.Proposer.StartNextProposal(inst, next.Inst)
	nextBallot := inst.PropCurBal

	if r.doStats {
		r.ProposalStats.Open(stats.InstanceID{Log: 0, Seq: next.Inst}, nextBallot)
		r.InstanceStats.RecordOccurrence(stats.InstanceID{Log: 0, Seq: next.Inst}, "My Phase 1 Proposals", 1)
	}
	//if r.doPatientProposals {
	//	r.patientProposals.startedProposal(next.Inst, inst.PropCurBal.Ballot)
	//}
	r.bcastPrepare(next.Inst)
}

//func (r *Replica) recordStatsPreempted(inst int32, pbk *proposalmanager.PBK) {
//	if pbk.Status != proposalmanager.BACKING_OFF && r.doStats {
//		id := stats.InstanceID{Log: 0, Seq: inst}
//		if pbk.Status == proposalmanager.PREPARING || pbk.Status == proposalmanager.READY_TO_PROPOSE {
//			r.InstanceStats.RecordOccurrence(id, "My Phase 1 Preempted", 1)
//			r.TimeseriesStats.Update("My Phase 1 Preempted", 1)
//			r.ProposalStats.CloseAndOutput(id, pbk.PropCurBal, stats.HIGHERPROPOSALONGOING)
//		} else if pbk.Status == proposalmanager.PROPOSING {
//			r.InstanceStats.RecordOccurrence(id, "My Phase 2 Preempted", 1)
//			r.TimeseriesStats.Update("My Phase 2 Preempted", 1)
//			r.ProposalStats.CloseAndOutput(id, pbk.PropCurBal, stats.HIGHERPROPOSALONGOING)
//		}
//	}
//}

func (r *Replica) handlePrepare(prepare *stdpaxosproto.Prepare) {
	dlog.AgentPrintfN(r.Id, "Replica received a Prepare from Replica %d in instance %d at ballot %d.%d", prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID)
	if int32(prepare.PropID) == r.Id {
		if r.syncAcceptor {
			panic("should not receive promise request this way")
		}
		if !r.writeAheadAcceptor {
			dlog.AgentPrintfN(r.Id, "Giving Prepare in instance %d at ballot %d.%d to acceptor as it is needed for safety", prepare.Instance, prepare.Number, prepare.PropID)
		} else {
			dlog.AgentPrintfN(r.Id, "Giving Prepare in instance %d at ballot %d.%d to acceptor as it can form a quorum", prepare.Instance, prepare.Number, prepare.PropID)
		}
		acceptorHandlePrepareLocal(r.Id, r.Acceptor, r.Replica, prepare, r.PrepareResponsesRPC, r.prepareReplyChan)
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

	//if r.instanceSpace[prepare.Instance].Status == proposalmanager.CLOSED {
	//	return
	//}

	//if r.doPatientProposals {
	//	r.patientProposals.learnOfProposal(prepare.Instance, prepare.Ballot)
	//}

	if r.Proposer.LearnOfBallot(&r.instanceSpace, prepare.Instance, lwcproto.ConfigBal{Config: -1, Ballot: prepare.Ballot}, stdpaxosproto.PROMISE) {
		r.noLongerSleepingInstance(prepare.Instance)
		pCurBal := r.instanceSpace[prepare.Instance].PropCurBal
		if !pCurBal.IsZero() {
			dlog.AgentPrintfN(r.Id, "Prepare Received from Replica %d in instance %d at ballot %d.%d preempted our ballot %d.%d",
				prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID, pCurBal.Number, pCurBal.PropID)
		}
	}
	r.instanceProposeValueTimeout.ProposedClientValuesManager.learnOfBallot(r.instanceSpace[prepare.Instance], prepare.Instance, lwcproto.ConfigBal{Config: -1, Ballot: prepare.Ballot}, &r.clientBatcher)
}

// func (r *Replica) learn
func (r *Replica) proposerWittnessAcceptedValue(inst int32, aid int32, accepted stdpaxosproto.Ballot, val []*state.Command, whoseCmds int32) bool {
	if accepted.IsZero() {
		return false
	}
	r.Proposer.LearnOfBallotAccepted(&r.instanceSpace, inst, lwcproto.ConfigBal{Config: -1, Ballot: accepted}, whoseCmds)
	pbk := r.instanceSpace[inst]
	//if pbk.Status == proposalmanager.CLOSED {
	//	return false
	//}
	r.instanceProposeValueTimeout.ProposedClientValuesManager.learnOfBallotValue(pbk, inst, lwcproto.ConfigBal{Config: -1, Ballot: accepted}, val, whoseCmds, &r.clientBatcher)

	newVal := false
	if accepted.GreaterThan(pbk.ProposeValueBal.Ballot) {
		setProposingValue(pbk, whoseCmds, accepted, val)
		newVal = true
	}
	return newVal
}

func setProposingValue(pbk *proposalmanager.PBK, whoseCmds int32, bal stdpaxosproto.Ballot, val []*state.Command) {
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
	if r.disklessNOOP && (pbk.Status == proposalmanager.READY_TO_PROPOSE || pbk.Status == proposalmanager.PREPARING) {
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
	if pbk.Status != proposalmanager.PREPARING {
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

func (r *Replica) HandlePrepareReply(preply *stdpaxosproto.PrepareReply) {
	pbk := r.instanceSpace[preply.Instance]
	dlog.AgentPrintfN(r.Id, "Replica received a Prepare Reply from Replica %d in instance %d at requested ballot %d.%d and current ballot %d.%d", preply.AcceptorId, preply.Instance, preply.Req.Number, preply.Req.PropID, preply.Cur.Number, preply.Cur.PropID)
	if r.instanceSpace[preply.Instance].Status == proposalmanager.CLOSED {
		return
	}

	r.Learner.ProposalValue(preply.Instance, preply.VBal, preply.Command, preply.WhoseCmd)
	r.Learner.ProposalAccepted(preply.Instance, preply.VBal, preply.AcceptorId)
	if !preply.VBal.IsZero() && int32(preply.VBal.PropID) == r.Id {
		r.instanceSpace[preply.Instance].Qrms[lwcproto.ConfigBal{-1, preply.VBal}].AddToQuorum(preply.AcceptorId)
	}
	if r.Learner.IsChosen(preply.Instance) && r.Learner.HasLearntValue(preply.Instance) {
		if r.instanceSpace[preply.Instance].Qrms[lwcproto.ConfigBal{-1, preply.VBal}] == nil {
			r.LearnerQuorumaliser.TrackProposalAcceptance(r.instanceSpace[preply.Instance], preply.Instance, lwcproto.ConfigBal{-1, preply.VBal})
			//r.instanceSpace[preply.Instance].Qrms[lwcproto.ConfigBal{-1, preply.VBal}].StartAcceptanceQuorum()
		}
		r.instanceSpace[preply.Instance].Qrms[lwcproto.ConfigBal{-1, preply.VBal}].AddToQuorum(preply.AcceptorId)
		cB, cV, cWC := r.Learner.GetChosen(preply.Instance)
		r.proposerCloseCommit(preply.Instance, cB, cV, cWC)
		dlog.AgentPrintfN(r.Id, "From prepare replies %d", logfmt.LearntInlineFmt(preply.Instance, cB, r.balloter, cWC))
		r.bcastCommitToAll(preply.Instance, cB, cV, cWC)
		return
	}

	if preply.VBal.GreaterThan(pbk.ProposeValueBal.Ballot) {
		setProposingValue(pbk, preply.WhoseCmd, preply.VBal, preply.Command)
	}

	if pbk.Status == proposalmanager.CLOSED {
		dlog.AgentPrintfN(r.Id, "Discarding Prepare Reply from Replica %d in instance %d at requested ballot %d.%d because it's already chosen", preply.AcceptorId, preply.Instance, preply.Req.Number, preply.Req.PropID)
		return
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
			r.instanceProposeValueTimeout.ProposedClientValuesManager.learnOfBallot(r.instanceSpace[preply.Instance], preply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: preply.Cur}, &r.clientBatcher)
			//if r.doPatientProposals {
			//	r.patientProposals.learnOfProposal(preply.Instance, preply.Cur)
			//}

			pCurBal := r.instanceSpace[preply.Instance].PropCurBal
			dlog.AgentPrintfN(r.Id, "Prepare Reply Received from Replica %d in instance %d at with current ballot %d.%d preempted our ballot %d.%d",
				preply.AcceptorId, preply.Instance, preply.Cur.Number, preply.Cur.PropID, pCurBal.Number, pCurBal.PropID)
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

func (r *Replica) tryInitaliseForPropose(inst int32, ballot stdpaxosproto.Ballot) {
	pbk := r.instanceSpace[inst]
	if !pbk.PropCurBal.Ballot.Equal(ballot) || pbk.Status != proposalmanager.PREPARING {
		return
	}

	qrm := pbk.Qrms[pbk.PropCurBal]
	if (!qrm.HasAcknowledged(r.Id) && !r.writeAheadAcceptor) || (r.writeAheadAcceptor && !r.isLeased(inst, pbk.PropCurBal.Ballot)) {
		dlog.AgentPrintfN(r.Id, "Not safe to send accept requests for instance %d, need to wait until a lease or promise from our acceptor is received", inst)
		go func() {
			time.Sleep(5 * time.Millisecond) // todo replace with check upon next message to see if try propose again or clean up this info
			r.tryInitPropose <- proposalmanager.RetryInfo{
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

	if r.doStats {
		id := stats.InstanceID{
			Log: 0,
			Seq: inst,
		}
		r.InstanceStats.RecordComplexStatEnd(id, "Phase 1", "Success")
	}

	if !pbk.ProposeValueBal.IsZero() && pbk.WhoseCmds != r.Id && pbk.ClientProposals != nil {
		pbk.ClientProposals = nil // at this point, our client proposal will not be chosen
	}

	pbk.Status = proposalmanager.READY_TO_PROPOSE
	//timeDelay := time.Duration(0)
	//if r.doPatientProposals {
	//	timeDelay = r.patientProposals.getTimeToDelayProposal(inst, pbk.PropCurBal.Ballot)
	//}
	//if timeDelay <= 0 { //* time.Millisecond {
	//	dlog.AgentPrintfN(r.Id, "Decided not to sleep instance %d as quorum acquired after last expected response", inst) // or not doing patient
	r.tryPropose(inst, 0)
	//return
	//}
	//dlog.AgentPrintfN(r.Id, "Sleeping instance %d at ballot %d.%d for %d microseconds before trying to propose value", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID, timeDelay.Microseconds())
	//go func(bal stdpaxosproto.Ballot) {
	//	timer := time.NewTimer(timeDelay)
	//	<-timer.C
	//	r.proposableInstances <- ProposalInfo{
	//		inst:          inst,
	//		proposingBal:  bal,
	//		ProposalBatch: nil,
	//		PrevSleeps:    0,
	//	}
	//}(pbk.PropCurBal.Ballot)
	return
}

func (r *Replica) tryPropose(inst int32, priorAttempts int) {
	pbk := r.instanceSpace[inst]
	if pbk.Status != proposalmanager.READY_TO_PROPOSE {
		panic("asjfalskdjf")
	}
	dlog.AgentPrintfN(r.Id, "Attempting to propose value in instance %d", inst)
	qrm := pbk.Qrms[pbk.PropCurBal]
	qrm.StartAcceptanceQuorum()

	if !pbk.ProposeValueBal.IsZero() {
		if r.doStats {
			r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Previous Value ProposedBatch", 1)
			r.TimeseriesStats.Update("Times Previous Value ProposedBatch", 1)
			r.ProposalStats.RecordPreviousValueProposed(stats.InstanceID{0, inst}, pbk.PropCurBal, len(pbk.Cmds))
		}
	} else {
		b := r.clientBatcher.GetBatchToPropose()
		if b != nil {
			pbk.ClientProposals = b
			setProposingValue(pbk, r.Id, pbk.PropCurBal.Ballot, pbk.ClientProposals.GetCmds())
		} else {
			if priorAttempts == 0 {
				r.BeginWaitingForClientProposals(inst, pbk)
				return
			}
			man := r.instanceProposeValueTimeout
			if len(man.nextBatch.cmds) > 0 {
				b = &batching.Batch{
					Proposals: man.nextBatch.clientVals,
					Cmds:      man.nextBatch.cmds,
					Uid:       man.nextBatch.uid,
				}
				dlog.AgentPrintfN(r.Id, "Assembled partial batch with UID %d (length %d values)", b.GetUID(), len(b.GetCmds()))
				man.nextBatch.cmds = make([]*state.Command, 0, man.nextBatch.maxLength)
				man.nextBatch.clientVals = make([]*genericsmr.Propose, 0, man.nextBatch.maxLength)
				man.nextBatch.uid += 1
				pbk.ClientProposals = b
				setProposingValue(pbk, r.Id, pbk.PropCurBal.Ballot, pbk.ClientProposals.GetCmds())
			} else {
				setProposingValue(pbk, -1, pbk.PropCurBal.Ballot, state.NOOPP())
			}
		}
	}

	if pbk.Cmds == nil {
		panic("there must be something to propose")
	}
	pbk.SetNowProposing()
	if pbk.WhoseCmds == -1 {
		if r.isProposingDisklessNOOP(inst) { // no diskless noop should be always bcastaccept as we will have always verified there is no conflict (at least with synchronous acceptor)
			dlog.AgentPrintfN(r.Id, "Proposing diskless noop in instance %d at ballot %d.%d", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
		} else {
			dlog.AgentPrintfN(r.Id, "Proposing persistent noop in instance %d at ballot %d.%d", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
		}
	} else {
		if pbk.WhoseCmds == r.Id {
			if r.balloter.GetAttemptNumber(pbk.ProposeValueBal.Number) > 1 {
				dlog.AgentPrintfN(r.Id, "Proposing again batch with UID %d (length %d values) in instance %d at ballot %d.%d", pbk.ClientProposals.GetUID(), len(pbk.ClientProposals.GetCmds()), inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
			} else {
				dlog.AgentPrintfN(r.Id, "Proposing batch with UID %d (length %d values) in instance %d at ballot %d.%d", pbk.ClientProposals.GetUID(), len(pbk.ClientProposals.GetCmds()), inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
			}
		} else {
			dlog.AgentPrintfN(r.Id, "Proposing previous value from ballot %d.%d with whose command %d in instance %d at ballot %d.%d",
				pbk.ProposeValueBal.Number, pbk.ProposeValueBal.PropID, pbk.WhoseCmds, inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
		}
	}

	r.Learner.ProposalValue(inst, pbk.PropCurBal.Ballot, pbk.Cmds, pbk.WhoseCmds)
	if pbk.ClientProposals != nil {
		r.Executor.ProposedBatch(inst, pbk.ClientProposals)
	}
	r.bcastAccept(inst)
}

func (r *Replica) isProposingDisklessNOOP(inst int32) bool {
	pbk := r.instanceSpace[inst]
	return r.disklessNOOP && pbk.WhoseCmds == -1 && pbk.Status >= proposalmanager.PROPOSING && r.GotPromisesFromAllInGroup(inst, pbk.PropCurBal.Ballot)
}

func (r *Replica) BeginWaitingForClientProposals(inst int32, pbk *proposalmanager.PBK) {
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

func (r *Replica) checkAndHandleNewlyReceivedInstance(instance int32) {
	if instance < 0 {
		return
	}
	r.Proposer.LearnOfBallot(&r.instanceSpace, instance, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
}

func (r *Replica) handleAccept(accept *stdpaxosproto.Accept) {
	dlog.AgentPrintfN(r.Id, "Replica received Accept from Replica %d in instance %d at ballot %d.%d", accept.PropID, accept.Instance, accept.Number, accept.PropID)
	r.checkAndHandleNewlyReceivedInstance(accept.Instance)
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
	dlog.AgentPrintfN(r.Id, "Acceptance recorded on proposal instance %d at ballot %d.%d from Replica %d with whose commands %d",
		areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId, areply.WhoseCmd)
	if int32(areply.Req.PropID) == r.Id {
		r.instanceSpace[areply.Instance].Qrms[lwcproto.ConfigBal{-1, areply.Cur}].AddToQuorum(areply.AcceptorId)
	}
	if r.Learner.IsChosen(areply.Instance) && r.Learner.HasLearntValue(areply.Instance) {
		_, cV, cWC := r.Learner.GetChosen(areply.Instance)
		r.proposerCloseCommit(areply.Instance, areply.Cur, cV, cWC)
	}
}

func (r *Replica) handleAcceptReply(areply *stdpaxosproto.AcceptReply) {
	r.checkAndHandleNewlyReceivedInstance(areply.Instance)
	dlog.AgentPrintfN(r.Id, "Replica received Accept Reply from Replica %d in instance %d at requested ballot %d.%d and current ballot %d.%d", areply.AcceptorId, areply.Instance, areply.Req.Number, areply.Req.PropID, areply.Cur.Number, areply.Cur.PropID)
	pbk := r.instanceSpace[areply.Instance]

	// PREEMPTED
	if areply.Cur.GreaterThan(areply.Req) {
		pCurBal := r.instanceSpace[areply.Instance].PropCurBal
		dlog.AgentPrintfN(r.Id, "Accept Reply received from Replica %d in instance %d with current ballot %d.%d preempted our ballot %d.%d",
			areply.AcceptorId, areply.Instance, areply.Cur.Number, areply.Cur.PropID, pCurBal.Number, pCurBal.PropID)
		r.Proposer.LearnOfBallot(&r.instanceSpace, areply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur}, areply.CurPhase)
		r.instanceProposeValueTimeout.ProposedClientValuesManager.learnOfBallot(pbk, areply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur}, &r.clientBatcher)
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
	if pbk.Status == proposalmanager.CLOSED {
		return
	}
	if r.Id == int32(chosenAt.PropID) {
		r.bcastCommitToAll(inst, chosenAt, chosenVal, whoseCmd)
	}
	r.Proposer.LearnBallotChosen(&r.instanceSpace, inst, lwcproto.ConfigBal{Config: -1, Ballot: chosenAt}) // todo add client value chosen log
	//if !pbk.PropCurBal.IsZero() && r.doPatientProposals {
	//	r.patientProposals.stoppingProposals(inst, pbk.PropCurBal.Ballot)
	//}
	if whoseCmd == r.Id {
		if _, e := r.instanceProposeValueTimeout.chosenBatches[pbk.ClientProposals.GetUID()]; e {
			dlog.AgentPrintfN(r.Id, logfmt.LearntBatchAgainFmt(inst, chosenAt, r.balloter, whoseCmd, pbk.ClientProposals))
		} else {
			dlog.AgentPrintfN(r.Id, logfmt.LearntBatchFmt(inst, chosenAt, r.balloter, whoseCmd, pbk.ClientProposals))
		}
		r.instanceProposeValueTimeout.chosenBatches[pbk.ClientProposals.GetUID()] = struct{}{}
	} else {
		dlog.AgentPrintfN(r.Id, logfmt.LearntFmt(inst, chosenAt, r.balloter, whoseCmd))
	}
	r.instanceProposeValueTimeout.ProposedClientValuesManager.valueChosen(pbk, inst, whoseCmd, chosenVal, &r.clientBatcher)

	r.noLongerSleepingInstance(inst)
	setProposingValue(pbk, whoseCmd, chosenAt, chosenVal)
	r.Executor.Learnt(inst, chosenVal, whoseCmd)
	if r.execSig == nil {
		return
	}
	r.execSig.CheckExec(r.Proposer, &r.Executor)
}

// todo make it so proposer acceptor and learner all guard on chosen
func (r *Replica) handleCommit(commit *stdpaxosproto.Commit) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	dlog.AgentPrintfN(r.Id, "Replica received Commit from Replica %d in instance %d at ballot %d.%d with whose commands %d", commit.PropID, commit.Instance, commit.Ballot.Number, commit.Ballot.PropID, commit.WhoseCmd)

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
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	dlog.AgentPrintfN(r.Id, "Replica received Commit Short from Replica %d in instance %d at ballot %d.%d with whose commands %d", commit.PropID, commit.Instance, commit.Ballot.Number, commit.Ballot.PropID, commit.WhoseCmd)
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
