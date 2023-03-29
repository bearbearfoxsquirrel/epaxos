package paxos

//
//import (
//	"epaxos/acceptor"
//	"epaxos/batching"
//	"epaxos/dlog"
//	"epaxos/fastrpc"
//	"epaxos/genericsmr"
//	"epaxos/instanceagentmapper"
//	"epaxos/lwcproto"
//	"epaxos/proposerstate"
//	"epaxos/quorumsystem"
//	"epaxos/stablestore"
//	"epaxos/state"
//	"epaxos/stats"
//	"epaxos/stdpaxosproto"
//	"epaxos/twophase"
//	"epaxos/twophase/aceptormessagefilter"
//	balloter2 "epaxos/twophase/balloter"
//	_const "epaxos/twophase/const"
//	"epaxos/twophase/exec"
//	"epaxos/twophase/learner"
//	"epaxos/twophase/logfmt"
//	"epaxos/twophase/proposer"
//	"fmt"
//	"math"
//	"math/rand"
//	"os"
//	"sync"
//	"time"
//)
//
//// batching and executing
//// non leader batches up proposals and send them to the leader
//
//// leader takes batches, and proposes them. Informs followers that their batch has been proposed
//
//// non-leader gets that notification and makes a note of it
//
//// when executing htey can detetect the client requests and inform them
//
//// things to do
//// monitor leader failure -- signal new ballot
//// implement multiinstance promises
//// implement promise leases
//// implement client request forwarding and tracking
//// add option for inital leader -- simple check on an int
//// similar to promise leases could implement promise len as a parameter/generalisation of both 1 instance and all instances
//// as promises approach us, we then issue a new promise request
//
//// we can use previous decisisions to inform us of future ones ---- don't have to have to be immediately up to it. We can have a buffer so that pipelining is enabled.
//// for example -- if we learnt that instance 1 is decided by proposer p5, then instance 11 is owned by proposer p5.
//// could this lead to a gossiped multi leader setup???
//// n round robin, pipelined promises, map of owners, when we move to the next instance we begin a timeout on the previous ones of owners (or do we maintain a rythym for the system?)
//// when it times out we begin a new promise on that instance. If we decide it, then we become the owner of ballot one in instance i+n for however many are pipelined.
//// note we want the pipeline of ownership to be given long enough so that proposers do not have to limit how many concurrent propsosals they can handle
//// but short enough so that 4 message delays are needed for a minimal number of failed rounds
//// you don't have the round robin decided, we simply see if there is an onwer on that instance, if there is then move on if there isn't then attempt (non-ownership is decided either by timeout or by being ahead of the timeline -- this should failover quickly and help manage moving load)
//// graceful handover? if not failed, can send notifications for stating that we are not going to be proposing and they should take over.
//// safe method for handover without promises needed?
//
//type ConcurrentFile struct {
//	*os.File
//	sync.Mutex
//}
//
//func (f *ConcurrentFile) Sync() error {
//	//dlog.AgentPrintfN(1, "acq sync lock")
//	f.Mutex.Lock()
//	//dlog.AgentPrintfN(1, "release sync lock")
//	defer f.Mutex.Unlock()
//	return f.File.Sync()
//}
//
//func (f *ConcurrentFile) Write(b []byte) (int, error) {
//	//dlog.AgentPrintfN(1, "acq write lock")
//	f.Mutex.Lock()
//	//dlog.AgentPrintfN(1, "release write lock")
//	defer f.Mutex.Unlock()
//	return f.File.Write(b)
//}
//
//func (f *ConcurrentFile) WriteAt(b []byte, off int64) (int, error) {
//	//dlog.AgentPrintfN(1, "acq write 2 lock")
//	f.Mutex.Lock()
//	//dlog.AgentPrintfN(1, "release write 2 lock")
//	defer f.Mutex.Unlock()
//	return f.File.WriteAt(b, off)
//}
//
//type ProposalTuples struct {
//	cmd      []state.Command
//	proposal []*genericsmr.Propose
//}
//
//type TimeoutInfo struct {
//	inst    int32
//	ballot  stdpaxosproto.Ballot
//	phase   proposer.ProposerStatus
//	msgCode uint8
//	msg     fastrpc.Serializable
//}
//
//type ProposalInfo struct {
//	inst         int32
//	proposingBal stdpaxosproto.Ballot
//	batching.ProposalBatch
//	PrevSleeps int32
//}
//
//func (pinfo *ProposalInfo) popBatch() batching.ProposalBatch {
//	b := pinfo.ProposalBatch
//	pinfo.ProposalBatch = nil
//	return b
//}
//
//func (pinfo *ProposalInfo) putBatch(bat batching.ProposalBatch) {
//	pinfo.ProposalBatch = bat
//}
//
//type Replica2 struct {
//	exec.SimpleExecutor
//	learner.Learner
//
//	proposer.CrtInstanceOracle
//	twophase.ProposeBatchOracle
//
//	balloter *balloter2.Balloter
//
//	proposer.Proposer
//	proposer.ProposerInstanceQuorumaliser
//	proposer.LearnerQuorumaliser
//	proposer.AcceptorQrmInfo
//
//	batchLearners []MyBatchLearner
//	noopLearners  []proposer.NoopLearner
//
//	*genericsmr.Replica           // extends a generic Paxos replica
//	configChan                    chan fastrpc.Serializable
//	prepareChan                   chan fastrpc.Serializable
//	acceptChan                    chan fastrpc.Serializable
//	commitChan                    chan fastrpc.Serializable
//	commitShortChan               chan fastrpc.Serializable
//	prepareReplyChan              chan fastrpc.Serializable
//	acceptReplyChan               chan fastrpc.Serializable
//	stateChan                     chan fastrpc.Serializable
//	stateChanRPC                  uint8
//	prepareRPC                    uint8
//	acceptRPC                     uint8
//	commitRPC                     uint8
//	commitShortRPC                uint8
//	prepareReplyRPC               uint8
//	acceptReplyRPC                uint8
//	instanceSpace                 []*proposer.PBK // the space of all instances (used and not yet used)
//	Shutdown                      bool
//	counter                       int
//	maxBatchWait                  int
//	crtOpenedInstances            []int32
//	proposableInstances           chan struct{} //*time.Timer
//	noopWaitUs                    int32
//	retryInstance                 chan proposer.RetryInfo
//	alwaysNoop                    bool
//	lastTimeClientChosen          time.Time
//	lastOpenProposalTime          time.Time
//	timeSinceLastProposedInstance time.Time
//	fastLearn                     bool
//	whenCrash                     time.Duration
//	howLongCrash                  time.Duration
//	whoCrash                      int32
//	timeoutMsgs                   chan TimeoutInfo
//	timeout                       time.Duration
//	catchupBatchSize              int32
//	catchingUp                    bool
//	lastSettleBatchInst           int32
//	flushCommit                   bool
//	group1Size                    int
//	nextRecoveryBatchPoint        int32
//	recoveringFrom                int32
//	commitCatchUp                 bool
//	maxBatchSize                  int
//	doStats                       bool
//	sendProposerState             bool
//	proposerState                 proposerstate.State
//	acceptor.Acceptor
//	stablestore.StableStore
//	proactivelyPrepareOnPreempt bool
//	noopWait                    time.Duration
//	expectedBatchedRequests     int32
//	sendPreparesToAllAcceptors  bool
//	twophase.PrepareResponsesRPC
//	twophase.AcceptResponsesRPC
//	proposedBatcheNumber map[int32]int32
//	doPatientProposals   bool
//	startInstanceSig     chan struct{}
//	doEager              bool
//	promiseLeases        chan acceptor.PromiseLease
//	classesLeased        map[int32]stdpaxosproto.Ballot
//	iWriteAhead          int32
//	writeAheadAcceptor   bool
//	tryInitPropose       chan proposer.RetryInfo
//	sendFastestQrm       bool
//	nudge                chan chan batching.ProposalBatch
//	bcastCommit          bool
//	nopreempt            bool
//	bcastAcceptance      bool
//	syncAcceptor         bool
//	disklessNOOP         bool
//
//	disklessNOOPPromisesAwaiting map[int32]chan struct{}
//	disklessNOOPPromises         map[int32]map[stdpaxosproto.Ballot]map[int32]struct{}
//
//	clientRequestManager    *ClientRequestBatcher
//	nextNoopEnd             time.Time
//	nextNoop                *time.Timer
//	noops                   int
//	resetTo                 chan time.Duration
//	execSig                 proposer.ExecOpenInstanceSignal
//	bcastAcceptDisklessNOOP bool
//}
//
//type ClientRequestBatcher struct {
//	twophase.ProposedClientValuesManager
//	nextBatch                  curBatch
//	sleepingInsts              map[int32]time.Time
//	constructedAwaitingBatches []batching.ProposalBatch
//	chosenBatches              map[int32]struct{}
//}
//
//type MyBatchLearner interface {
//	Learn(bat batching.ProposalBatch)
//}
//
//const MAXPROPOSABLEINST = 1000
//
////const CHAN_BUFFER_SIZE = 200000
//
//func NewBaselineTwoPhaseReplica(id int, replica *genericsmr.Replica, durable bool, batchWait int, storageLoc string,
//	maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool,
//	factor float64, whoCrash int32, whenCrash time.Duration, howlongCrash time.Duration, emulatedSS bool,
//	emulatedWriteTime time.Duration, catchupBatchSize int32, timeout time.Duration, group1Size int, flushCommit bool,
//	softFac bool, doStats bool, statsParentLoc string, commitCatchup bool, deadTime int32, batchSize int,
//	constBackoff bool, requeueOnPreempt bool, tsStatsFilename string, instStatsFilename string,
//	propsStatsFilename string, sendProposerState bool, proactivePreemptOnNewB bool, batchingAcceptor bool,
//	maxAccBatchWait time.Duration, sendPreparesToAllAcceptors bool, minimalProposers bool, timeBasedBallots bool,
//	mappedProposers bool, dynamicMappedProposers bool, bcastAcceptance bool, mappedProposersNum int32,
//	instsToOpenPerBatch int32, doEager bool, sendFastestQrm bool, useGridQrms bool, minimalAcceptors bool,
//	minimalAcceptorNegatives bool, prewriteAcceptor bool, doPatientProposals bool, sendFastestAccQrm bool, forwardInduction bool,
//	q1 bool, bcastCommit bool, nopreempt bool, pam bool, pamloc string, syncaceptor bool, disklessNOOP bool, forceDisklessNOOP bool, eagerByExec bool, bcastAcceptDisklessNoop bool, eagerByExecFac float32) *Replica2 {
//
//	r := &Replica2{
//		bcastAcceptDisklessNOOP:      bcastAcceptDisklessNoop,
//		disklessNOOPPromises:         make(map[int32]map[stdpaxosproto.Ballot]map[int32]struct{}),
//		disklessNOOPPromisesAwaiting: make(map[int32]chan struct{}),
//		//forceDisklessNOOP:            forceDisklessNOOP,
//		disklessNOOP:                 disklessNOOP,
//		syncAcceptor:                 syncaceptor,
//		nopreempt:                    nopreempt,
//		bcastCommit:                  bcastCommit,
//		nudge:                        make(chan chan batching.ProposalBatch, maxOpenInstances*int32(replica.N)),
//		sendFastestQrm:               sendFastestAccQrm,
//		Replica:                      replica,
//		proposedBatcheNumber:         make(map[int32]int32),
//		bcastAcceptance:              bcastAcceptance,
//		stateChan:                    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		configChan:                   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		prepareChan:                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		acceptChan:                   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		commitChan:                   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		commitShortChan:              make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		prepareReplyChan:             make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		acceptReplyChan:              make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
//		tryInitPropose:               make(chan proposer.RetryInfo, 100),
//		prepareRPC:                   0,
//		acceptRPC:                    0,
//		commitRPC:                    0,
//		commitShortRPC:               0,
//		prepareReplyRPC:              0,
//		acceptReplyRPC:               0,
//		instanceSpace:                make([]*proposer.PBK, _const.ISpaceLen),
//		Shutdown:                     false,
//		counter:                      0,
//		//flush:                        true,
//		maxBatchWait:                 batchWait,
//		crtOpenedInstances:           make([]int32, maxOpenInstances),
//		proposableInstances:          make(chan struct{}, MAXPROPOSABLEINST*replica.N),
//		noopWaitUs:                   noopwait,
//		retryInstance:                make(chan proposer.RetryInfo, maxOpenInstances*10000),
//		alwaysNoop:                   alwaysNoop,
//		fastLearn:                    false,
//		whoCrash:                     whoCrash,
//		whenCrash:                    whenCrash,
//		howLongCrash:                 howlongCrash,
//		timeoutMsgs:                  make(chan TimeoutInfo, 5000),
//		timeout:                      timeout,
//		catchupBatchSize:             catchupBatchSize,
//		lastSettleBatchInst:          -1,
//		catchingUp:                   false,
//		flushCommit:                  flushCommit,
//		commitCatchUp:                commitCatchup,
//		maxBatchSize:                 batchSize,
//		doStats:                      doStats,
//		sendProposerState:            sendProposerState,
//		noopWait:                     time.Duration(noopwait) * time.Microsecond,
//		proactivelyPrepareOnPreempt:  proactivePreemptOnNewB,
//		expectedBatchedRequests:      200,
//		sendPreparesToAllAcceptors:   sendPreparesToAllAcceptors,
//		startInstanceSig:             make(chan struct{}, 100),
//		doEager:                      doEager,
//		clientRequestManager: &ClientRequestBatcher{
//			ProposedClientValuesManager: nil,
//			nextBatch:                   curBatch{},
//			sleepingInsts:               nil,
//			constructedAwaitingBatches:  nil,
//			chosenBatches:               nil,
//		},
//	}
//
//	pids := make([]int32, r.N)
//	ids := make([]int, r.N)
//	for i := range pids {
//		pids[i] = int32(i)
//		ids[i] = i
//	}
//
//	//var amf aceptormessagefilter.AcceptorMessageFilter = nil
//	////r.isAccMsgFilter = minimalAcceptorNegatives
//	//if minimalAcceptorNegatives {
//	//	if useGridQrms {
//	//		panic("incompatible options")
//	//	}
//	//	amf = aceptormessagefilter.MinimalAcceptorFilterNew(&instanceagentmapper.InstanceNegativeAcceptorSetMapper{
//	//		Acceptors: pids,
//	//		F:         int32(r.F),
//	//		N:         int32(r.N),
//	//	})
//	//}
//	//if amf != nil {
//	//	messageFilter := messageFilterRoutine{
//	//		AcceptorMessageFilter: amf,
//	//		aid:                   r.Id,
//	//		messageFilterIn:       make(chan *messageFilterComm, 1000),
//	//	}
//	//	r.messageFilterIn = messageFilter.messageFilterIn
//	//	go messageFilter.startFilter()
//	//}
//
//	if r.UDP {
//		r.prepareRPC = r.RegisterUDPRPC("prepare request", new(stdpaxosproto.Prepare), r.prepareChan)
//		r.acceptRPC = r.RegisterUDPRPC("accept request", new(stdpaxosproto.Accept), r.acceptChan)
//		r.commitRPC = r.RegisterUDPRPC("commit", new(stdpaxosproto.Commit), r.commitChan)
//		r.commitShortRPC = r.RegisterUDPRPC("commit short", new(stdpaxosproto.CommitShort), r.commitShortChan)
//		r.prepareReplyRPC = r.RegisterUDPRPC("prepare reply", new(stdpaxosproto.PrepareReply), r.prepareReplyChan)
//		r.acceptReplyRPC = r.RegisterUDPRPC("accept reply", new(stdpaxosproto.AcceptReply), r.acceptReplyChan)
//		//r.stateChanRPC = r.RegisterUDPRPC(new(proposerstate.State), r.stateChan)
//	} else {
//		r.prepareRPC = r.RegisterRPC(new(stdpaxosproto.Prepare), r.prepareChan)
//		r.acceptRPC = r.RegisterRPC(new(stdpaxosproto.Accept), r.acceptChan)
//		r.commitRPC = r.RegisterRPC(new(stdpaxosproto.Commit), r.commitChan)
//		r.commitShortRPC = r.RegisterRPC(new(stdpaxosproto.CommitShort), r.commitShortChan)
//		r.prepareReplyRPC = r.RegisterRPC(new(stdpaxosproto.PrepareReply), r.prepareReplyChan)
//		r.acceptReplyRPC = r.RegisterRPC(new(stdpaxosproto.AcceptReply), r.acceptReplyChan)
//		r.stateChanRPC = r.RegisterRPC(new(proposerstate.State), r.stateChan)
//	}
//
//	r.PrepareResponsesRPC = twophase.PrepareResponsesRPC{
//		PrepareReply: r.prepareReplyRPC,
//		Commit:       r.commitRPC,
//	}
//	r.AcceptResponsesRPC = twophase.AcceptResponsesRPC{
//		AcceptReply: r.acceptReplyRPC,
//		Commit:      r.commitRPC,
//	}
//
//	//if leaderbased {
//	//
//	//}
//
//	if prewriteAcceptor {
//		r.iWriteAhead = 10000000
//		r.promiseLeases = make(chan acceptor.PromiseLease, r.iWriteAhead)
//		r.classesLeased = make(map[int32]stdpaxosproto.Ballot)
//		r.writeAheadAcceptor = true
//		if batchingAcceptor {
//			r.StableStore = &ConcurrentFile{
//				File:  r.StableStorage,
//				Mutex: sync.Mutex{},
//			}
//			r.Acceptor = acceptor.PrewrittenBatcherAcceptorNew(r.StableStore, durable, emulatedSS,
//				emulatedWriteTime, int32(id), maxAccBatchWait, pids, r.prepareReplyRPC, r.acceptReplyRPC, r.commitRPC, r.commitShortRPC, commitCatchup, r.promiseLeases, r.iWriteAhead)
//
//		} else {
//			r.StableStore = r.StableStorage
//			r.Acceptor = acceptor.PrewritePromiseAcceptorNew(r.StableStore, durable, emulatedSS, emulatedWriteTime, int32(id),
//				r.prepareReplyRPC, r.acceptReplyRPC, r.commitRPC, r.commitShortRPC, commitCatchup, r.promiseLeases, r.iWriteAhead, proactivePreemptOnNewB, r.disklessNOOP)
//		}
//	} else {
//		if batchingAcceptor {
//			r.StableStore = &ConcurrentFile{
//				File:  r.StableStorage,
//				Mutex: sync.Mutex{},
//			}
//
//			r.Acceptor = acceptor.BetterBatchingAcceptorNew(r.StableStore, durable, emulatedSS,
//				emulatedWriteTime, int32(id), maxAccBatchWait, pids, r.prepareReplyRPC, r.acceptReplyRPC, r.commitRPC, r.commitShortRPC, commitCatchup)
//		} else {
//			r.StableStore = r.StableStorage
//			r.Acceptor = acceptor.StandardAcceptorNew(r.StableStore, durable, emulatedSS, emulatedWriteTime, int32(id),
//				r.prepareReplyRPC, r.acceptReplyRPC, r.commitRPC, r.commitShortRPC, commitCatchup)
//		}
//
//	}
//
//	//if r.doStats {
//	//	phaseStarts := []string{"Fast Quorum", "Slow Quorum"}
//	//	phaseEnds := []string{"Success", "Failure"}
//	//	phaseRes := map[string][]string{
//	//		"Fast Quorum": phaseEnds,
//	//		"Slow Quorum": phaseEnds,
//	//	}
//	//	phaseNegs := map[string]string{
//	//		"Fast Quorum": "Failure",
//	//		"Slow Quorum": "Failure",
//	//	}
//	//	//phaseCStats := []stats.MultiStartMultiOutStatConstructor{
//	//	//	{"Phase 1", phaseStarts, phaseRes, phaseNegs, true},
//	//	//	{"Phase 2", phaseStarts, phaseRes, phaseNegs, true},
//	//	//}
//	//
//	//	//r.InstanceStats = stats.InstanceStatsNew(statsParentLoc+fmt.Sprintf("/%s", instStatsFilename), stats.DefaultIMetrics{}.Get(), phaseCStats)
//	//	//r.ProposalStats = stats.ProposalStatsNew([]string{"Phase 1 Fast Quorum", "Phase 1 Slow Quorum", "Phase 2 Fast Quorum", "Phase 2 Slow Quorum"}, statsParentLoc+fmt.Sprintf("/%s", propsStatsFilename))
//	//	//r.TimeseriesStats = stats.TimeseriesStatsNew(stats.DefaultTSMetrics{}.Get(), statsParentLoc+fmt.Sprintf("/%s", tsStatsFilename), time.Second)
//	//}
//
//	// Quorum system
//	var qrm quorumsystem.SynodQuorumSystemConstructor
//	qrm = &quorumsystem.SynodCountingQuorumSystemConstructor{
//		F:                r.F,
//		Thrifty:          r.Thrifty,
//		Replica:          r.Replica,
//		//BroadcastFastest: sendFastestQrm,
//		AllAids:          pids,
//		//SendAllAcceptors: false,
//	}
//	if useGridQrms {
//		qrm = &quorumsystem.SynodGridQuorumSystemConstructor{
//			F:                r.F,
//			Replica:          r.Replica,
//			Thrifty:          r.Thrifty,
//			//BroadcastFastest: sendFastestQrm,
//		}
//	}
//
//	var instancequormaliser proposer.InstanceQuormaliser
//	instancequormaliser = &proposer.Standard{
//		SynodQuorumSystemConstructor: qrm,
//		Aids:                         pids,
//		MyID:                         r.Id,
//	}
//
//	// LEARNER GROUPS SET UP
//	var aqc learner.AQConstructor
//	stdaqc := learner.GetStandardGroupAQConstructorr(pids, qrm.(quorumsystem.SynodQuorumSystemConstructor), r.Id)
//	aqc = &stdaqc
//
//	// Random but determininstic proposer acceptor mapping -- 2f+1
//	if minimalAcceptors {
//		laqc := learner.GetMinimalGroupAQConstructorr(int32(r.N), int32(r.F), pids, qrm.(quorumsystem.AcceptanceQuorumsConstructor), r.Id)
//		aqc = &laqc
//		var mapper instanceagentmapper.InstanceAgentMapper
//		if useGridQrms {
//			mapper = &instanceagentmapper.InstanceAcceptorGridMapper{
//				Acceptors: pids,
//				F:         int32(r.F),
//				N:         int32(r.N),
//			}
//		} else {
//			mapper = &instanceagentmapper.InstanceAcceptorSetMapper{
//				Acceptors: pids,
//				F:         int32(r.F),
//				N:         int32(r.N),
//			}
//		}
//
//		instancequormaliser = &proposer.Minimal{
//			AcceptorMapper:               mapper,
//			SynodQuorumSystemConstructor: qrm,
//			MapperCache:                  make(map[int32][]int32),
//			MyID:                         r.Id,
//		}
//	}
//
//	if pam {
//		pamapping := instanceagentmapper.ReadFromFile(pamloc)
//		amapping := instanceagentmapper.GetAMap(pamapping)
//		//pmapping := instanceagentmapper.GetPMap(pamapping)
//		learner.GetStaticDefinedAQConstructor(amapping, qrm.(quorumsystem.SynodQuorumSystemConstructor))
//		instancequormaliser = &proposer.StaticMapped{
//			AcceptorMapper:               instanceagentmapper.FixedInstanceAgentMapping{Groups: amapping},
//			SynodQuorumSystemConstructor: qrm,
//			MyID:                         r.Id,
//		}
//
//		if doPatientProposals {
//			panic("option not implemented")
//		}
//	}
//
//	l := learner.GetBcastAcceptLearner(aqc)
//	r.Learner = &l
//	r.SimpleExecutor = exec.GetNewExecutor(int32(id), replica, r.StableStore, r.Dreply)
//
//	r.ProposerInstanceQuorumaliser = instancequormaliser
//	r.AcceptorQrmInfo = instancequormaliser
//	r.LearnerQuorumaliser = instancequormaliser
//
//	if group1Size <= r.N-r.F {
//		r.group1Size = r.N - r.F
//	} else {
//		r.group1Size = group1Size
//	}
//
//	r.clientRequestManager.ProposedClientValuesManager = ProposedClientValuesManagerNew(r.Id, r.TimeseriesStats, r.doStats)
//
//	balloter := &balloter2.Balloter{r.Id, int32(r.N), 10000, time.Time{}, timeBasedBallots}
//	r.balloter = balloter
//
//	backoffManager := proposer.BackoffManagerNew(minBackoff, maxInitBackoff, maxBackoff, r.retryInstance, factor, softFac, constBackoff, r.Id)
//	var instanceManager proposer.SingleInstanceManager = proposer.SimpleInstanceManagerNew(r.Id, backoffManager, balloter, doStats,
//		r.ProposerInstanceQuorumaliser, r.TimeseriesStats, r.ProposalStats, r.InstanceStats)
//
//	var awaitingGroup ProposerGroupGetter = SimpleProposersAwaitingGroupGetterNew(pids)
//	var minimalGroupGetter *MinimalProposersAwaitingGroup
//	if minimalProposers {
//		minimalShouldMaker := proposer.MinimalProposersShouldMakerNew(int16(r.Id), r.F)
//		instanceManager = proposer.MinimalProposersInstanceManagerNew(instanceManager.(*proposer.SimpleInstanceManager), minimalShouldMaker)
//		minimalGroupGetter = MinimalProposersAwaitingGroupNew(awaitingGroup.(*SimpleProposersAwaitingGroup), minimalShouldMaker, int32(r.F))
//		awaitingGroup = minimalGroupGetter
//	}
//
//	// SET UP SIGNAL
//	sig := proposer.SimpleSigNew(r.startInstanceSig, r.Id)
//	var openInstSig proposer.OpenInstSignal = sig
//	var ballotInstSig proposer.BallotOpenInstanceSignal = sig
//	if doEager || eagerByExec {
//		eSig := proposer.EagerSigNew(openInstSig.(*proposer.SimpleSig), maxOpenInstances)
//		openInstSig = eSig
//		ballotInstSig = eSig
//	}
//
//	if eagerByExec {
//		eESig := proposer.EagerExecUpToSigNew(openInstSig.(*proposer.EagerSig), float32(r.N), eagerByExecFac)
//		openInstSig = eESig
//		ballotInstSig = eESig
//		r.execSig = eESig
//	}
//
//	simpleGlobalManager := proposer.SimpleProposalManagerNew(r.Id, openInstSig, ballotInstSig, instanceManager, backoffManager)
//	var globalManager proposer.Proposer = simpleGlobalManager
//
//	if instsToOpenPerBatch < 1 {
//		panic("Will not open any instances")
//	}
//	if instsToOpenPerBatch > 1 && (mappedProposers || dynamicMappedProposers || doEager) {
//		panic("incompatible options")
//	}
//
//	r.noopLearners = []proposer.NoopLearner{}
//	// PROPOSER QUORUMS
//	if mappedProposers || dynamicMappedProposers || pam {
//		var agentMapper instanceagentmapper.InstanceAgentMapper
//		agentMapper = &instanceagentmapper.DetRandInstanceSetMapper{
//			Ids: pids,
//			G:   mappedProposersNum,
//			N:   int32(r.N),
//		}
//		if pam { // PAM get from file the proposer mappings
//			pamap := instanceagentmapper.ReadFromFile(pamloc)
//			pmap := instanceagentmapper.GetPMap(pamap)
//			agentMapper = &instanceagentmapper.FixedInstanceAgentMapping{Groups: pmap}
//		}
//
//		globalManager = proposer.MappedProposersProposalManagerNew(r.startInstanceSig, simpleGlobalManager, instanceManager, agentMapper)
//		mappedGroupGetter := MappedProposersAwaitingGroupNew(agentMapper)
//		if dynamicMappedProposers {
//			dAgentMapper := &proposer.DynamicInstanceSetMapper{
//				DetRandInstanceSetMapper: *agentMapper.(*instanceagentmapper.DetRandInstanceSetMapper),
//			}
//			globalManager = proposer.DynamicMappedProposerManagerNew(r.startInstanceSig, simpleGlobalManager, instanceManager, dAgentMapper, int32(r.N), int32(r.F))
//			mappedGroupGetter = MappedProposersAwaitingGroupNew(dAgentMapper)
//			r.noopLearners = []proposer.NoopLearner{globalManager.(*proposer.DynamicMappedGlobalManager)}
//		}
//		if minimalProposers {
//			awaitingGroup = MinimalMappedProposersAwaitingGroupNew(*minimalGroupGetter, *mappedGroupGetter)
//		}
//	}
//
//	if instsToOpenPerBatch > 1 {
//		openInstSig = proposer.HedgedSigNew(r.Id, r.startInstanceSig)
//		globalManager = proposer.HedgedBetsProposalManagerNew(r.Id, simpleGlobalManager, int32(r.N), instsToOpenPerBatch)
//	}
//
//	r.CrtInstanceOracle = globalManager
//	r.Proposer = globalManager
//	r.Durable = durable
//
//	r.doPatientProposals = doPatientProposals
//	if r.doPatientProposals {
//		r.patientProposals = patientProposals{
//			myId:                r.Id,
//			promisesRequestedAt: make(map[int32]map[stdpaxosproto.Ballot]time.Time),
//			pidsPropRecv:        make(map[int32]map[int32]struct{}),
//			doPatient:           true,
//			//Ewma:                make([]float64, r.N),
//			ProposerGroupGetter: awaitingGroup,
//			closed:              make(map[int32]struct{}),
//		}
//	}
//	go r.run()
//	return r
//}
//
//func (r *Replica2) CloseUp() {
//	if r.doStats {
//		r.TimeseriesStats.Close()
//		r.InstanceStats.Close()
//		r.ProposalStats.CloseOutput()
//	}
//}
//
///* Clock goroutine */
//var fastClockChan chan bool
//
//func (r *Replica2) fastClock() {
//	for !r.Shutdown {
//		time.Sleep(time.Duration(r.maxBatchWait) * time.Millisecond) // ms
//		dlog.Println("sending fast clock")
//		fastClockChan <- true
//	}
//}
//
//func (r *Replica2) BatchingEnabled() bool {
//	return r.maxBatchWait > 0
//}
//
//type curBatch struct {
//	maxLength  int
//	cmds       []*state.Command
//	clientVals []*genericsmr.Propose
//	//next       int
//	uid int32
//}
//
//// noop timers reordering seems to cause long delays if there are lots of sleeping instances
//
//func (r *Replica2) updateNoopTimer() {
//	if len(r.clientRequestManager.sleepingInsts) == 0 {
//		dlog.AgentPrintfN(r.Id, "No more instances to noop so clearing noop timer")
//		return
//	}
//	dlog.AgentPrintfN(r.Id, "More instances to noop so setting next timeout")
//	dlog.AgentPrintfN(r.Id, "Next Noop expires in %d milliseconds", r.noopWait.Milliseconds())
//	go func() {
//		<-time.After(r.noopWait)
//		r.proposableInstances <- struct{}{}
//	}()
//}
//
//func (r *Replica2) noLongerSleepingInstance(inst int32) {
//	man := r.clientRequestManager
//	if _, e := man.sleepingInsts[inst]; !e {
//		//dlog.AgentPrintfN(r.Id, "Cannot stop instance is it is not sleeping")
//		return
//	}
//	delete(man.sleepingInsts, inst)
//}
//
//func (man *ClientRequestBatcher) PutBatch(batch batching.ProposalBatch) {
//	if _, e := man.chosenBatches[batch.GetUID()]; e {
//		return
//	}
//	man.constructedAwaitingBatches = append(man.constructedAwaitingBatches, batch)
//	// put back in
//	// not if batch is chosen
//}
//
//func remove(s []batching.ProposalBatch, i int) []batching.ProposalBatch {
//	s[i] = s[len(s)-1]
//	return s[:len(s)-1]
//}
//
//func (man *ClientRequestBatcher) GetFullBatchToPropose() batching.ProposalBatch {
//	var bat batching.ProposalBatch = nil
//	batID := int32(math.MaxInt32)
//	selI := -1
//	for i, b := range man.constructedAwaitingBatches {
//		if _, e := man.chosenBatches[b.GetUID()]; e {
//			continue
//		}
//		if b.GetUID() > batID {
//			continue
//		}
//		bat = b
//		batID = b.GetUID()
//		selI = i
//	}
//
//	if selI != -1 {
//		man.constructedAwaitingBatches = remove(man.constructedAwaitingBatches, selI)
//	}
//	return bat
//}
//
//func (r *Replica2) run() {
//	r.ConnectToPeers()
//	r.RandomisePeerOrder()
//	go r.WaitForClientConnections()
//
//	fastClockChan = make(chan bool, 1)
//
//	doner := make(chan struct{})
//	if r.Id == r.whoCrash {
//		go func() {
//			t := time.NewTimer(r.whenCrash)
//			<-t.C
//			doner <- struct{}{}
//		}()
//	}
//
//	var c chan struct{}
//	if r.doStats {
//		r.TimeseriesStats.GoClock()
//		c = r.TimeseriesStats.C
//	} else {
//		c = make(chan struct{})
//	}
//
//	man := r.clientRequestManager
//	man.sleepingInsts = make(map[int32]time.Time)
//	man.chosenBatches = make(map[int32]struct{})
//	man.constructedAwaitingBatches = make([]batching.ProposalBatch, 0, 100)
//	man.nextBatch = curBatch{
//		maxLength:  r.maxBatchSize,
//		cmds:       make([]*state.Command, 0, r.maxBatchSize),
//		clientVals: make([]*genericsmr.Propose, 0, r.maxBatchSize),
//		uid:        0,
//	}
//
//	startGetEvent := time.Now()
//	//endGetEvent := time.Now()
//	doWhat := ""
//
//	startEvent := time.Now()
//	endEvent := time.Now()
//	for !r.Shutdown {
//		startGetEvent = time.Now()
//		select {
//		case <-r.proposableInstances:
//			startEvent = time.Now()
//			doWhat = "handle instance proposing timeout"
//			dlog.AgentPrintfN(r.Id, "Got notification to noop")
//			if len(r.clientRequestManager.sleepingInsts) == 0 {
//				break
//			}
//
//			for len(r.clientRequestManager.sleepingInsts) > 0 {
//				min := r.clientRequestManager.getMinimumSleepingInstance()
//				r.noLongerSleepingInstance(min)
//				pbk := r.instanceSpace[min]
//				if pbk == nil {
//					panic("????")
//				}
//				dlog.AgentPrintfN(r.Id, "Rechecking whether to propose in instance %d", min)
//				//	/pbk.PropCurBal.Ballot.GreaterThan(retry.proposingBal) ||/
//				if pbk.Status != proposer.READY_TO_PROPOSE {
//					dlog.AgentPrintfN(r.Id, "Decided to not propose in instance %d as we are no longer on ballot %d.%d", min, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
//					break
//				}
//				r.updateNoopTimer()
//				r.tryPropose(min, 2)
//			}
//			break
//		case clientRequest := <-r.ProposeChan:
//			startEvent = time.Now()
//			doWhat = "handle client request"
//			if len(man.nextBatch.cmds) == 0 && !r.doEager {
//				go func() { r.startInstanceSig <- struct{}{} }()
//			}
//			// add batch value
//			man.addToBatch(clientRequest)
//			r.getAllValuesInChan(man)
//			if len(man.nextBatch.cmds) != man.nextBatch.maxLength {
//				break
//			}
//			batch := r.addBatchToQueue(man)
//			dlog.AgentPrintfN(r.Id, "Assembled full batch with UID %d (length %d values)", batch.Uid, len(batch.GetCmds()))
//			r.startNextBatch(man)
//
//			if len(man.sleepingInsts) == 0 {
//				// add to pipeline
//				//go func() { r.startInstanceSig <- struct{}{} }()
//				dlog.AgentPrintfN(r.Id, "No instances to propose batch within")
//				break
//			}
//
//			min := r.clientRequestManager.getMinimumSleepingInstance()
//			r.noLongerSleepingInstance(min)
//			r.tryPropose(min, 1)
//			//if len(r.clientRequestManager.sleepingInsts) == 0 {
//			//	break
//			//}
//
//			break
//		case stateS := <-r.stateChan:
//			startEvent = time.Now()
//			doWhat = "receive state"
//			recvState := stateS.(*proposerstate.State)
//			r.handleState(recvState)
//			break
//		case <-r.startInstanceSig:
//			startEvent = time.Now()
//			doWhat = "startGetEvent new instance"
//			r.beginNextInstance()
//			break
//		case t := <-r.tryInitPropose:
//			startEvent = time.Now()
//			doWhat = "attempt initially to propose values in instance"
//			r.tryInitaliseForPropose(t.Inst, t.AttemptedBal.Ballot)
//			break
//		case lease := <-r.promiseLeases:
//			startEvent = time.Now()
//			doWhat = "handle promise lease"
//			r.updateLeases(lease)
//			break
//		case <-c:
//			startEvent = time.Now()
//			doWhat = "print timeseries stats"
//			r.TimeseriesStats.PrintAndReset()
//			break
//		case next := <-r.retryInstance:
//			startEvent = time.Now()
//			doWhat = "handle new ballot request"
//			dlog.Println("Checking whether to retry a proposal")
//			r.tryNextAttempt(next)
//			break
//		case prepareS := <-r.prepareChan:
//			startEvent = time.Now()
//			doWhat = "handle prepare request"
//			prepare := prepareS.(*stdpaxosproto.Prepare)
//			//got a Prepare message
//			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
//			//if !r.checkAndHandlecatchupRequest(prepare) {
//			r.handlePrepare(prepare)
//			//}
//			break
//		case acceptS := <-r.acceptChan:
//			startEvent = time.Now()
//			doWhat = "handle accept request"
//			accept := acceptS.(*stdpaxosproto.Accept)
//			//got an Accept message
//			dlog.Printf("Received Accept Request from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
//			r.handleAccept(accept)
//			break
//		case commitS := <-r.commitChan:
//			startEvent = time.Now()
//			doWhat = "handle commit"
//			commit := commitS.(*stdpaxosproto.Commit)
//			//got a Commit message
//			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
//			r.handleCommit(commit)
//			//r.checkAndHandlecatchupResponse(commit)
//			break
//		case commitS := <-r.commitShortChan:
//			startEvent = time.Now()
//			doWhat = "handle commit short"
//			commit := commitS.(*stdpaxosproto.CommitShort)
//			//got a Commit message
//			dlog.Printf("Received short Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
//			r.handleCommitShort(commit)
//			break
//		case prepareReplyS := <-r.prepareReplyChan:
//			startEvent = time.Now()
//			doWhat = "handle prepare reply"
//			prepareReply := prepareReplyS.(*stdpaxosproto.PrepareReply)
//			//got a Prepare reply
//			dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
//			r.HandlePrepareReply(prepareReply)
//			break
//		case acceptReplyS := <-r.acceptReplyChan:
//			startEvent = time.Now()
//			doWhat = "handle accept reply"
//			acceptReply := acceptReplyS.(*stdpaxosproto.AcceptReply)
//			//got an Accept reply
//			dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
//			r.handleAcceptReply(acceptReply)
//			break
//			//case proposeableInst := <-r.proposableInstances:
//			//	r.recheckInstanceToPropose(proposeableInst)
//			//	break
//		}
//		endEvent = time.Now()
//		dlog.AgentPrintfN(r.Id, "It took %d µs to receive event %s", startEvent.Sub(startGetEvent).Microseconds(), doWhat)
//		dlog.AgentPrintfN(r.Id, "It took %d µs to %s", endEvent.Sub(startEvent).Microseconds(), doWhat)
//	}
//}
//
//func (r *Replica2) addBatchToQueue(man *ClientRequestBatcher) batching.Batch {
//	batch := batching.Batch{
//		man.nextBatch.clientVals,
//		man.nextBatch.cmds,
//		man.nextBatch.uid,
//	}
//	man.constructedAwaitingBatches = append(man.constructedAwaitingBatches, &batch)
//	return batch
//}
//
//func (r *Replica2) startNextBatch(man *ClientRequestBatcher) {
//	man.nextBatch.cmds = make([]*state.Command, 0, man.nextBatch.maxLength)
//	man.nextBatch.clientVals = make([]*genericsmr.Propose, 0, man.nextBatch.maxLength)
//	man.nextBatch.uid += 1
//}
//
//func (r *Replica2) getAllValuesInChan(man *ClientRequestBatcher) {
//	if len(man.nextBatch.cmds) < man.nextBatch.maxLength {
//		l := len(r.ProposeChan)
//		for i := 0; i < l; i++ {
//			v := <-r.ProposeChan
//			man.addToBatch(v)
//			if len(man.nextBatch.cmds) == man.nextBatch.maxLength {
//				break
//			}
//		}
//	}
//}
//
//func (man *ClientRequestBatcher) addToBatch(clientRequest *genericsmr.Propose) {
//	man.nextBatch.cmds = append(man.nextBatch.cmds, &clientRequest.Command)
//	man.nextBatch.clientVals = append(man.nextBatch.clientVals, clientRequest)
//}
//
//func (man *ClientRequestBatcher) getMinimumSleepingInstance() int32 {
//	min := int32(math.MaxInt32)
//	for inst := range man.sleepingInsts {
//		if inst >= min {
//			continue
//		}
//		min = inst
//	}
//	if min == int32(math.MaxInt32) {
//		panic("asldfjdslk")
//	}
//	return min
//}
//
//func (r *Replica2) bcastPrepareUDP(instance int32, ballot stdpaxosproto.Ballot) {
//	args := &stdpaxosproto.Prepare{r.Id, instance, ballot}
//	//pbk := r.instanceSpace[instance]
//	sentTo := make([]int32, 0, r.N)
//	sendMsgs := make([]int32, 0, r.N)
//	if r.sendPreparesToAllAcceptors {
//		inGroup := false
//		for _, i := range r.AcceptorQrmInfo.GetGroup(instance) {
//			if i == r.Id {
//				inGroup = true
//				continue
//			}
//			sentTo = append(sentTo, i)
//			sendMsgs = append(sendMsgs, i)
//		}
//		if !r.writeAheadAcceptor || inGroup {
//			r.sendPrepareToSelf(args)
//			sentTo = append(sentTo, r.Id)
//		}
//	} else {
//		panic("Not yet implemented")
//	}
//
//	//} else {
//	//	sentTo = pbk.Qrms[pbk.PropCurBal].Broadcast(r.prepareRPC, args)
//	//}
//	r.Replica.BcastUDPMsg(sendMsgs, r.prepareRPC, args, false)
//
//	if r.doStats {
//		instID := stats.InstanceID{
//			Log: 0,
//			Seq: instance,
//		}
//		r.InstanceStats.RecordOccurrence(instID, "My Phase 1 Proposals", 1)
//	}
//	dlog.AgentPrintfN(r.Id, "Broadcasting Prepare for instance %d at ballot %d.%d to replicas %v", args.Instance, args.Number, args.PropID, sentTo)
//}
//
//func (r *Replica2) bcastPrepareTCP(instance int32, ballot stdpaxosproto.Ballot) {
//	args := &stdpaxosproto.Prepare{r.Id, instance, ballot}
//	pbk := r.instanceSpace[instance]
//	var sentTo []int32
//	if r.sendPreparesToAllAcceptors {
//		sentTo = make([]int32, 0, r.N)
//		r.Replica.Mutex.Lock()
//		inGroup := false
//		for _, i := range r.AcceptorQrmInfo.GetGroup(instance) {
//			if i == r.Id {
//				inGroup = true
//				continue
//			}
//			r.Replica.SendMsgUNSAFE(i, r.prepareRPC, args)
//			sentTo = append(sentTo, i)
//		}
//		r.Replica.Mutex.Unlock()
//		if !r.writeAheadAcceptor || inGroup {
//			r.sendPrepareToSelf(args)
//			sentTo = append(sentTo, r.Id)
//		}
//	} else {
//		sentTo = pbk.Qrms[pbk.PropCurBal].Broadcast(r.prepareRPC, args)
//	}
//
//	if r.doStats {
//		instID := stats.InstanceID{
//			Log: 0,
//			Seq: instance,
//		}
//		r.InstanceStats.RecordOccurrence(instID, "My Phase 1 Proposals", 1)
//	}
//	dlog.AgentPrintfN(r.Id, "Broadcasting Prepare for instance %d at ballot %d.%d to replicas %v", args.Instance, args.Number, args.PropID, sentTo)
//}
//
//func (r *Replica2) bcastPrepare(instance int32) {
//	if r.UDP {
//		r.bcastPrepareUDP(instance, r.instanceSpace[instance].PropCurBal.Ballot)
//		return
//	}
//	r.bcastPrepareTCP(instance, r.instanceSpace[instance].PropCurBal.Ballot) // todo do as interface
//}
//
//func (r *Replica2) sendPrepareToSelf(args *stdpaxosproto.Prepare) {
//	if r.syncAcceptor {
//		promise := acceptorSyncHandlePrepareLocal(r.Id, r.Acceptor, args, r.PrepareResponsesRPC)
//		if promise == nil {
//			return
//		}
//		r.HandlePromise(promise)
//	} else {
//		go func(prep *stdpaxosproto.Prepare) {
//			r.prepareChan <- prep
//		}(args)
//	}
//}
//
//func (r *Replica2) bcastAcceptUDP(instance int32, ballot stdpaxosproto.Ballot, whosecmds int32, cmds []*state.Command) {
//	// -1 = ack, -2 = passive observe, -3 = diskless accept
//	pa := stdpaxosproto.Accept{
//		LeaderId: -1,
//		Instance: instance,
//		Ballot:   ballot,    //r.instanceSpace[instance].PropCurBal.Ballot,
//		WhoseCmd: whosecmds, //r.instanceSpace[instance].WhoseCmds,
//		Command:  cmds,      //r.instanceSpace[instance].Cmds,
//	}
//	paPassive := pa
//	paPassive.LeaderId = -2
//
//	sendC := r.F + 1
//	if !r.Thrifty {
//		sendC = r.F*2 + 1
//	}
//
//	// todo add check to see if alive??
//	pa.LeaderId = -1
//	disklessNOOP := r.isProposingDisklessNOOP(instance)
//	if disklessNOOP {
//		pa.LeaderId = -3
//	}
//	// todo order peerlist by latency or random
//	// first try to send to self
//	sentTo := make([]int32, 0, r.N)
//	sendMsgsActive := make([]int32, 0, r.N)
//	sendMsgsPassive := make([]int32, 0, r.N)
//	acceptorGroup := r.AcceptorQrmInfo.GetGroup(instance)
//	rand.Shuffle(len(acceptorGroup), func(i, j int) {
//		tmp := acceptorGroup[i]
//		acceptorGroup[i] = acceptorGroup[j]
//		acceptorGroup[j] = tmp
//	})
//	selfInGroup := false
//	isBcastingAccept := r.bcastAcceptance || (disklessNOOP && r.bcastAcceptDisklessNOOP)
//	// send to those in group except self
//	for _, peer := range acceptorGroup {
//		if peer == r.Id {
//			selfInGroup = true
//			sentTo = append(sentTo, r.Id)
//			break
//		}
//	}
//
//	for _, peer := range acceptorGroup {
//		if peer == r.Id {
//			continue
//		}
//		if len(sentTo) >= sendC {
//			if !isBcastingAccept {
//				break
//			}
//			//r.Replica2.SendUDPMsg(peer, r.acceptRPC, &paPassive, true)
//			sentTo = append(sentTo, peer)
//			sendMsgsPassive = append(sendMsgsActive, peer)
//			continue
//		}
//		//r.Replica2.SendUDPMsg(peer, r.acceptRPC, &pa, true)
//		sendMsgsActive = append(sendMsgsActive, peer)
//		sentTo = append(sentTo, peer)
//	}
//
//	// send to those not in group
//	if isBcastingAccept {
//		for peer := int32(0); peer < int32(r.N); peer++ {
//			if r.AcceptorQrmInfo.IsInGroup(instance, peer) {
//				continue
//			}
//			sendMsgsPassive = append(sendMsgsPassive, peer)
//			sentTo = append(sentTo, peer)
//		}
//	}
//	r.Replica.BcastUDPMsg(sendMsgsActive, r.acceptRPC, &pa, true)
//	r.Replica.BcastUDPMsg(sendMsgsPassive, r.acceptRPC, &paPassive, true)
//	if selfInGroup {
//		go func() { r.acceptChan <- &pa }()
//	}
//	if r.doStats {
//		instID := stats.InstanceID{
//			Log: 0,
//			Seq: instance,
//		}
//		r.InstanceStats.RecordOccurrence(instID, "My Phase 2 Proposals", 1)
//	}
//	dlog.AgentPrintfN(r.Id, "Broadcasting Accept for instance %d with whose commands %d at ballot %d.%d to Replicas %v (active acceptors %v)", pa.Instance, pa.WhoseCmd, pa.Number, pa.PropID, sentTo, sendMsgsActive)
//}
//
//func (r *Replica2) bcastAcceptTCP(instance int32, ballot stdpaxosproto.Ballot, whosecmds int32, cmds []*state.Command) {
//	// -1 = ack, -2 = passive observe, -3 = diskless accept
//	pa := stdpaxosproto.Accept{
//		LeaderId: -1,
//		Instance: instance,
//		Ballot:   ballot,
//		WhoseCmd: whosecmds,
//		Command:  cmds,
//	}
//	args := &pa
//
//	sendC := r.F + 1
//	if !r.Thrifty {
//		sendC = r.F*2 + 1
//	}
//
//	// todo add check to see if alive??
//
//	pa.LeaderId = -1
//	disklessNOOP := r.isProposingDisklessNOOP(instance) // will not update after proposal made
//	// todo order peerlist by latency or random
//	// -1 = ack, -2 = passive observe, -3 = diskless accept
//	// first try to send to self
//	sentTo := make([]int32, 0, r.N)
//	acceptorGroup := r.AcceptorQrmInfo.GetGroup(instance)
//	rand.Shuffle(len(acceptorGroup), func(i, j int) {
//		tmp := acceptorGroup[i]
//		acceptorGroup[i] = acceptorGroup[j]
//		acceptorGroup[j] = tmp
//	})
//	selfInGroup := false
//	r.Replica.Mutex.Lock()
//	isBcastingAccept := r.bcastAcceptance || (disklessNOOP && r.bcastAcceptDisklessNOOP)
//	// send to those in group except self
//	for _, peer := range acceptorGroup {
//		if peer == r.Id {
//			selfInGroup = true
//			sentTo = append(sentTo, r.Id)
//			break
//		}
//	}
//
//	for _, peer := range acceptorGroup {
//		if peer == r.Id {
//			continue
//		}
//		if len(sentTo) >= sendC {
//			if !isBcastingAccept {
//				break
//			}
//			pa.LeaderId = -2
//		}
//		r.Replica.SendMsgUNSAFE(peer, r.acceptRPC, args)
//		sentTo = append(sentTo, peer)
//	}
//
//	// send to those not in group
//	if isBcastingAccept {
//		pa.LeaderId = -2
//		for peer := int32(0); peer < int32(r.N); peer++ {
//			if r.AcceptorQrmInfo.IsInGroup(instance, peer) {
//				continue
//			}
//			r.Replica.SendMsgUNSAFE(peer, r.acceptRPC, args)
//			sentTo = append(sentTo, peer)
//		}
//	}
//	r.Replica.Mutex.Unlock()
//
//	if selfInGroup {
//		pa.LeaderId = -1
//		if disklessNOOP {
//			pa.LeaderId = -3
//		}
//		go func() { r.acceptChan <- args }()
//	}
//	instID := stats.InstanceID{
//		Log: 0,
//		Seq: instance,
//	}
//	if r.doStats {
//		r.InstanceStats.RecordOccurrence(instID, "My Phase 2 Proposals", 1)
//	}
//	dlog.AgentPrintfN(r.Id, "Broadcasting Accept for instance %d with whose commands %d at ballot %d.%d to Replicas %v", pa.Instance, pa.WhoseCmd, pa.Number, pa.PropID, sentTo)
//}
//
//// var pa stdpaxosproto.Accept
//func (r *Replica2) bcastAccept(instance int32) {
//	pbk := r.instanceSpace[instance]
//	if r.UDP {
//		r.bcastAcceptUDP(instance, pbk.PropCurBal.Ballot, pbk.WhoseCmds, pbk.Cmds)
//		return
//	}
//	go r.bcastAcceptTCP(instance, pbk.PropCurBal.Ballot, pbk.WhoseCmds, pbk.Cmds)
//}
//
//func (r *Replica2) bcastCommitUDP(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) {
//	var pc stdpaxosproto.Commit
//	var pcs stdpaxosproto.CommitShort
//
//	if len(r.instanceSpace[instance].Cmds) == 0 {
//		panic("fkjdlkfj;dlfj")
//	}
//
//	pc.LeaderId = r.Id
//	pc.Instance = instance
//	pc.Ballot = ballot
//	pc.WhoseCmd = whose //r.instanceSpace[instance].WhoseCmds
//	pc.MoreToCome = 0
//	pc.Command = command
//
//	pcs.LeaderId = r.Id
//	pcs.Instance = instance
//	pcs.Ballot = ballot
//	pcs.WhoseCmd = whose // r.instanceSpace[instance].WhoseCmds
//	pcs.Count = int32(len(command))
//	argsShort := pcs
//	// promises will not update after status == closed
//
//	if r.bcastAcceptance || (r.disklessNOOP && r.bcastAcceptDisklessNOOP && r.GotPromisesFromAllInGroup(instance, ballot) && pc.WhoseCmd == -1) {
//		if !r.bcastCommit {
//			return
//		}
//		list := make([]int32, 0, r.N)
//		for q := int32(0); q < int32(r.N); q++ {
//			if q == r.Id {
//				continue
//			}
//			//r.SendUDPMsg(q, r.commitShortRPC, &argsShort, true)
//			list = append(list, q)
//		}
//		dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID)
//		return
//		r.Replica.BcastUDPMsg(list, r.commitShortRPC, &argsShort, true)
//	}
//
//	sList := make([]int32, 0, r.N)
//	list := make([]int32, 0, r.N)
//	for q := int32(0); q < int32(r.N); q++ {
//		if q == r.Id {
//			continue
//		}
//
//		inQrm := r.instanceSpace[instance].Qrms[lwcproto.ConfigBal{Config: -1, Ballot: ballot}].HasAcknowledged(q)
//		if inQrm {
//			sList = append(sList, q)
//			//dlog.AgentPrintfN(r.Id, "Broadcasting Commit short for instance %d learnt with whose commands %d, at ballot %d.%d to ", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID, q)
//			//r.SendUDPMsg(q, r.commitShortRPC, &argsShort, true)
//		} else {
//			list = append(list, q)
//			//dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d to ", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID, q)
//			//r.SendUDPMsg(q, r.commitRPC, &pc, true)
//		}
//	}
//	r.Replica.BcastUDPMsg(list, r.commitRPC, &pc, true)
//	r.Replica.BcastUDPMsg(sList, r.commitShortRPC, &argsShort, true)
//
//	dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID)
//}
//
//func (r *Replica2) bcastCommitTCP(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) {
//	var pc stdpaxosproto.Commit
//	var pcs stdpaxosproto.CommitShort
//	if len(r.instanceSpace[instance].Cmds) == 0 {
//		panic("fkjdlkfj;dlfj")
//	}
//
//	pc.LeaderId = r.Id
//	pc.Instance = instance
//	pc.Ballot = ballot
//	pc.WhoseCmd = whose //r.instanceSpace[instance].WhoseCmds
//	pc.MoreToCome = 0
//	pc.Command = command
//
//	pcs.LeaderId = r.Id
//	pcs.Instance = instance
//	pcs.Ballot = ballot
//	pcs.WhoseCmd = whose //r.instanceSpace[instance].WhoseCmds
//	pcs.Count = int32(len(command))
//	argsShort := pcs
//	if r.bcastAcceptance || (r.disklessNOOP && r.bcastAcceptDisklessNOOP && r.GotPromisesFromAllInGroup(instance, ballot) && pc.WhoseCmd == -1) {
//		if !r.bcastCommit {
//			return
//		}
//		r.Replica.Mutex.Lock()
//		for q := int32(0); q < int32(r.N); q++ {
//			if q == r.Id {
//				continue
//			}
//			r.SendMsgUNSAFE(q, r.commitShortRPC, &argsShort)
//		}
//		r.Replica.Mutex.Unlock()
//		dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID)
//		return
//	}
//
//	r.Replica.Mutex.Lock()
//	for q := int32(0); q < int32(r.N); q++ {
//		if q == r.Id {
//			continue
//		}
//
//		inQrm := r.instanceSpace[instance].Qrms[lwcproto.ConfigBal{Config: -1, Ballot: ballot}].HasAcknowledged(q)
//		if inQrm {
//			dlog.AgentPrintfN(r.Id, "Broadcasting Commit short for instance %d learnt with whose commands %d, at ballot %d.%d to ", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID, q)
//			r.SendMsgUNSAFE(q, r.commitShortRPC, &argsShort)
//		} else {
//			dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d to ", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID, q)
//			r.SendMsgUNSAFE(q, r.commitRPC, &pc)
//		}
//	}
//	r.Replica.Mutex.Unlock()
//	dlog.AgentPrintfN(r.Id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID)
//}
//
//func (r *Replica2) bcastCommitToAll(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) {
//	if r.UDP {
//		go r.bcastCommitUDP(instance, ballot, command, whose)
//		return
//	}
//	go r.bcastCommitTCP(instance, ballot, command, whose)
//}
//
//func (r *Replica2) beginNextInstance() {
//	//do2 := func (inst int32) {
//	// get batch
//	// start proposal
//	// try propose
//	//}
//
//	do := func(inst int32) {
//		curInst := r.instanceSpace[inst]
//		r.Proposer.StartNextProposal(curInst, inst)
//
//		if r.doStats {
//			r.InstanceStats.RecordOpened(stats.InstanceID{0, inst}, time.Now())
//			r.TimeseriesStats.Update("Instances Opened", 1)
//			r.ProposalStats.Open(stats.InstanceID{0, inst}, curInst.PropCurBal)
//		}
//
//		prepMsg := getPrepareMessage(r.Id, inst, curInst)
//		dlog.AgentPrintfN(r.Id, "Opened new instance %d, with ballot %d.%d", inst, prepMsg.Number, prepMsg.PropID)
//		if r.doPatientProposals {
//			r.patientProposals.startedProposal(inst, curInst.PropCurBal.Ballot)
//		}
//		r.bcastPrepare(inst)
//	}
//	r.Proposer.StartNextInstance(&r.instanceSpace, do)
//}
//
//func getPrepareMessage(id int32, inst int32, curInst *proposer.PBK) *stdpaxosproto.Prepare {
//	prepMsg := &stdpaxosproto.Prepare{
//		LeaderId: id,
//		Instance: inst,
//		Ballot:   curInst.PropCurBal.Ballot,
//	}
//	return prepMsg
//}
//
//// show at several scales th throughput latency graph
//// compare approaches on failure and restarting
//// compare the throughput latency difference
//func (r *Replica2) tryNextAttempt(next proposer.RetryInfo) {
//	inst := r.instanceSpace[next.Inst]
//	if !r.Proposer.DecideRetry(inst, next) {
//		return
//	}
//
//	dlog.AgentPrintfN(r.Id, "Retry needed as backoff expired for instance %d", next.Inst)
//	r.Proposer.StartNextProposal(inst, next.Inst)
//	nextBallot := inst.PropCurBal
//
//	if r.doStats {
//		r.ProposalStats.Open(stats.InstanceID{Log: 0, Seq: next.Inst}, nextBallot)
//		r.InstanceStats.RecordOccurrence(stats.InstanceID{Log: 0, Seq: next.Inst}, "My Phase 1 Proposals", 1)
//	}
//	if r.doPatientProposals {
//		r.patientProposals.startedProposal(next.Inst, inst.PropCurBal.Ballot)
//	}
//	r.bcastPrepare(next.Inst)
//}
//
//func (r *Replica2) recordStatsPreempted(inst int32, pbk *proposer.PBK) {
//	if pbk.Status != proposer.BACKING_OFF && r.doStats {
//		id := stats.InstanceID{Log: 0, Seq: inst}
//		if pbk.Status == proposer.PREPARING || pbk.Status == proposer.READY_TO_PROPOSE {
//			r.InstanceStats.RecordOccurrence(id, "My Phase 1 Preempted", 1)
//			r.TimeseriesStats.Update("My Phase 1 Preempted", 1)
//			r.ProposalStats.CloseAndOutput(id, pbk.PropCurBal, stats.HIGHERPROPOSALONGOING)
//		} else if pbk.Status == proposer.PROPOSING {
//			r.InstanceStats.RecordOccurrence(id, "My Phase 2 Preempted", 1)
//			r.TimeseriesStats.Update("My Phase 2 Preempted", 1)
//			r.ProposalStats.CloseAndOutput(id, pbk.PropCurBal, stats.HIGHERPROPOSALONGOING)
//		}
//	}
//}
//
//func (r *Replica2) handlePrepare(prepare *stdpaxosproto.Prepare) {
//	dlog.AgentPrintfN(r.Id, "Replica2 received a Prepare from Replica2 %d in instance %d at ballot %d.%d", prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID)
//	if int32(prepare.PropID) == r.Id {
//		if r.syncAcceptor {
//			panic("should not receive promise request this way")
//		}
//		if !r.writeAheadAcceptor {
//			dlog.AgentPrintfN(r.Id, "Giving Prepare in instance %d at ballot %d.%d to acceptor as it is needed for safety", prepare.Instance, prepare.Number, prepare.PropID)
//		} else {
//			dlog.AgentPrintfN(r.Id, "Giving Prepare in instance %d at ballot %d.%d to acceptor as it can form a quorum", prepare.Instance, prepare.Number, prepare.PropID)
//		}
//		acceptorHandlePrepareLocal(r.Id, r.Acceptor, r.Replica, prepare, r.PrepareResponsesRPC, r.prepareReplyChan)
//		return
//	}
//
//	if r.AcceptorQrmInfo.IsInGroup(prepare.Instance, r.Id) {
//		dlog.AgentPrintfN(r.Id, "Giving Prepare for instance %d at ballot %d.%d to acceptor as it can form a quorum", prepare.Instance, prepare.Number, prepare.PropID)
//		if r.syncAcceptor {
//			acceptorSyncHandlePrepare(r.Id, r.Learner, r.Acceptor, prepare, r.PrepareResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.nopreempt)
//		} else {
//			acceptorHandlePrepareFromRemote(r.Id, r.Learner, r.Acceptor, prepare, r.PrepareResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.nopreempt)
//		}
//	}
//
//	//if r.instanceSpace[prepare.Instance].Status == proposer.CLOSED {
//	//	return
//	//}
//
//	if r.doPatientProposals {
//		r.patientProposals.learnOfProposal(prepare.Instance, prepare.Ballot)
//	}
//
//	if r.Proposer.LearnOfBallot(&r.instanceSpace, prepare.Instance, lwcproto.ConfigBal{Config: -1, Ballot: prepare.Ballot}, stdpaxosproto.PROMISE) {
//		r.noLongerSleepingInstance(prepare.Instance)
//		pCurBal := r.instanceSpace[prepare.Instance].PropCurBal
//		if !pCurBal.IsZero() {
//			dlog.AgentPrintfN(r.Id, "Prepare Received from Replica2 %d in instance %d at ballot %d.%d preempted our ballot %d.%d",
//				prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID, pCurBal.Number, pCurBal.PropID)
//		}
//	}
//	r.clientRequestManager.ProposedClientValuesManager.learnOfBallot(r.instanceSpace[prepare.Instance], prepare.Instance, lwcproto.ConfigBal{Config: -1, Ballot: prepare.Ballot}, r.clientRequestManager)
//}
//
//// func (r *Replica2) learn
//func (r *Replica2) proposerWittnessAcceptedValue(inst int32, aid int32, accepted stdpaxosproto.Ballot, val []*state.Command, whoseCmds int32) bool {
//	if accepted.IsZero() {
//		return false
//	}
//	r.Proposer.LearnOfBallotAccepted(&r.instanceSpace, inst, lwcproto.ConfigBal{Config: -1, Ballot: accepted}, whoseCmds)
//	pbk := r.instanceSpace[inst]
//	//if pbk.Status == proposer.CLOSED {
//	//	return false
//	//}
//	r.clientRequestManager.ProposedClientValuesManager.learnOfBallotValue(pbk, inst, lwcproto.ConfigBal{Config: -1, Ballot: accepted}, val, whoseCmds, r.clientRequestManager)
//
//	newVal := false
//	if accepted.GreaterThan(pbk.ProposeValueBal.Ballot) {
//		setProposingValue(pbk, whoseCmds, accepted, val)
//		newVal = true
//	}
//	return newVal
//}
//
//func setProposingValue(pbk *proposer.PBK, whoseCmds int32, bal stdpaxosproto.Ballot, val []*state.Command) {
//	pbk.WhoseCmds = whoseCmds
//	pbk.ProposeValueBal = lwcproto.ConfigBal{Config: -1, Ballot: bal}
//	pbk.Cmds = val
//}
//
//func (r *Replica2) HandlePromise(preply *stdpaxosproto.PrepareReply) {
//	dlog.AgentPrintfN(r.Id, "Promise recorded on instance %d at ballot %d.%d from Replica2 %d with value ballot %d.%d and whose commands %d",
//		preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.AcceptorId, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd)
//	if r.doPatientProposals {
//		r.patientProposals.gotPromise(preply.Instance, preply.Req, preply.AcceptorId)
//	}
//	pbk := r.instanceSpace[preply.Instance]
//	if r.disklessNOOP && (pbk.Status == proposer.READY_TO_PROPOSE || pbk.Status == proposer.PREPARING) {
//		if _, e := r.disklessNOOPPromises[preply.Instance]; !e {
//			r.disklessNOOPPromises[preply.Instance] = make(map[stdpaxosproto.Ballot]map[int32]struct{})
//		}
//		if _, e := r.disklessNOOPPromises[preply.Instance][preply.Cur]; !e {
//			r.disklessNOOPPromises[preply.Instance][preply.Cur] = make(map[int32]struct{})
//		}
//		r.disklessNOOPPromises[preply.Instance][preply.Cur][preply.AcceptorId] = struct{}{}
//		//if c, ec := r.disklessNOOPPromisesAwaiting[preply.Instance]; ec && r.GotPromisesFromAllInGroup(preply.Instance, preply.Cur) {
//		//	c <- struct{}{}
//		//}
//	}
//	if pbk.Status != proposer.PREPARING {
//		return
//	}
//	qrm := pbk.Qrms[pbk.PropCurBal]
//	qrm.AddToQuorum(preply.AcceptorId)
//	if !qrm.QuorumReached() {
//		return
//	}
//	dlog.AgentPrintfN(r.Id, "Promise Quorum reached in instance %d at ballot %d.%d",
//		preply.Instance, preply.Cur.Number, preply.Cur.PropID)
//	r.tryInitaliseForPropose(preply.Instance, preply.Req)
//}
//
//func (r *Replica2) HandlePrepareReply(preply *stdpaxosproto.PrepareReply) {
//	pbk := r.instanceSpace[preply.Instance]
//	dlog.AgentPrintfN(r.Id, "Replica2 received a Prepare Reply from Replica2 %d in instance %d at requested ballot %d.%d and current ballot %d.%d", preply.AcceptorId, preply.Instance, preply.Req.Number, preply.Req.PropID, preply.Cur.Number, preply.Cur.PropID)
//	if r.instanceSpace[preply.Instance].Status == proposer.CLOSED {
//		return
//	}
//
//	r.Learner.ProposalValue(preply.Instance, preply.VBal, preply.Command, preply.WhoseCmd)
//	r.Learner.ProposalAccepted(preply.Instance, preply.VBal, preply.AcceptorId)
//	if !preply.VBal.IsZero() && int32(preply.VBal.PropID) == r.Id {
//		r.instanceSpace[preply.Instance].Qrms[lwcproto.ConfigBal{-1, preply.VBal}].AddToQuorum(preply.AcceptorId)
//	}
//	if r.Learner.IsChosen(preply.Instance) && r.Learner.HasLearntValue(preply.Instance) {
//		cB, cV, cWC := r.Learner.GetChosen(preply.Instance)
//		r.proposerCloseCommit(preply.Instance, cB, cV, cWC)
//		dlog.AgentPrintfN(r.Id, "From prepare replies %d", logfmt.LearntInlineFmt(preply.Instance, cB, r.balloter, cWC))
//		r.bcastCommitToAll(preply.Instance, cB, cV, cWC)
//		return
//	}
//
//	if preply.VBal.GreaterThan(pbk.ProposeValueBal.Ballot) {
//		setProposingValue(pbk, preply.WhoseCmd, preply.VBal, preply.Command)
//	}
//
//	if pbk.Status == proposer.CLOSED {
//		dlog.AgentPrintfN(r.Id, "Discarding Prepare Reply from Replica2 %d in instance %d at requested ballot %d.%d because it's already chosen", preply.AcceptorId, preply.Instance, preply.Req.Number, preply.Req.PropID)
//		return
//	}
//
//	if preply.Req.GreaterThan(pbk.PropCurBal.Ballot) {
//		panic("Some how got a promise on a future proposal")
//	}
//	if preply.Req.GreaterThan(preply.Cur) {
//		panic("somehow acceptor did not promise us")
//	}
//
//	// IS PREEMPT?
//	if preply.Cur.GreaterThan(preply.Req) {
//		isNewPreempted := r.Proposer.LearnOfBallot(&r.instanceSpace, preply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: preply.Cur}, stdpaxosproto.PROMISE)
//		r.noLongerSleepingInstance(preply.Instance)
//		if isNewPreempted {
//			r.clientRequestManager.ProposedClientValuesManager.learnOfBallot(r.instanceSpace[preply.Instance], preply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: preply.Cur}, r.clientRequestManager)
//			if r.doPatientProposals {
//				r.patientProposals.learnOfProposal(preply.Instance, preply.Cur)
//			}
//
//			pCurBal := r.instanceSpace[preply.Instance].PropCurBal
//			dlog.AgentPrintfN(r.Id, "Prepare Reply Received from Replica2 %d in instance %d at with current ballot %d.%d preempted our ballot %d.%d",
//				preply.AcceptorId, preply.Instance, preply.Cur.Number, preply.Cur.PropID, pCurBal.Number, pCurBal.PropID)
//		}
//		// Proactively try promise
//		if r.AcceptorQrmInfo.IsInGroup(preply.Instance, r.Id) && r.proactivelyPrepareOnPreempt && isNewPreempted && int32(preply.Req.PropID) != r.Id {
//			newPrep := &stdpaxosproto.Prepare{
//				LeaderId: int32(preply.Cur.PropID),
//				Instance: preply.Instance,
//				Ballot:   preply.Cur,
//			}
//			acceptorHandlePrepareFromRemote(r.Id, r.Learner, r.Acceptor, newPrep, r.PrepareResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.nopreempt)
//		}
//		return
//	}
//	// IS PROMISE.
//	r.HandlePromise(preply)
//}
//
//func (r *Replica2) tryInitaliseForPropose(inst int32, ballot stdpaxosproto.Ballot) {
//	pbk := r.instanceSpace[inst]
//	if !pbk.PropCurBal.Ballot.Equal(ballot) || pbk.Status != proposer.PREPARING {
//		return
//	}
//
//	qrm := pbk.Qrms[pbk.PropCurBal]
//	if (!qrm.HasAcknowledged(r.Id) && !r.writeAheadAcceptor) || (r.writeAheadAcceptor && !r.isLeased(inst, pbk.PropCurBal.Ballot)) {
//		dlog.AgentPrintfN(r.Id, "Not safe to send accept requests for instance %d, need to wait until a lease or promise from our acceptor is received", inst)
//		go func() {
//			time.Sleep(5 * time.Millisecond) // todo replace with check upon next message to see if try propose again or clean up this info
//			r.tryInitPropose <- proposer.RetryInfo{
//				Inst:           inst,
//				AttemptedBal:   lwcproto.ConfigBal{Config: -1, Ballot: ballot},
//				PreempterBal:   lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}},
//				PreempterAt:    0,
//				Prev:           0,
//				TimesPreempted: 0,
//			}
//		}()
//		return
//	}
//
//	if r.doStats {
//		id := stats.InstanceID{
//			Log: 0,
//			Seq: inst,
//		}
//		r.InstanceStats.RecordComplexStatEnd(id, "Phase 1", "Success")
//	}
//
//	if !pbk.ProposeValueBal.IsZero() && pbk.WhoseCmds != r.Id && pbk.ClientProposals != nil {
//		pbk.ClientProposals = nil // at this point, our client proposal will not be chosen
//	}
//
//	pbk.Status = proposer.READY_TO_PROPOSE
//	//timeDelay := time.Duration(0)
//	//if r.doPatientProposals {
//	//	timeDelay = r.patientProposals.getTimeToDelayProposal(inst, pbk.PropCurBal.Ballot)
//	//}
//	//if timeDelay <= 0 { //* time.Millisecond {
//	//	dlog.AgentPrintfN(r.Id, "Decided not to sleep instance %d as quorum acquired after last expected response", inst) // or not doing patient
//	r.tryPropose(inst, 0)
//	//return
//	//}
//	//dlog.AgentPrintfN(r.Id, "Sleeping instance %d at ballot %d.%d for %d microseconds before trying to propose value", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID, timeDelay.Microseconds())
//	//go func(bal stdpaxosproto.Ballot) {
//	//	timer := time.NewTimer(timeDelay)
//	//	<-timer.C
//	//	r.proposableInstances <- ProposalInfo{
//	//		inst:          inst,
//	//		proposingBal:  bal,
//	//		ProposalBatch: nil,
//	//		PrevSleeps:    0,
//	//	}
//	//}(pbk.PropCurBal.Ballot)
//	return
//}
//
//func (r *Replica2) tryPropose(inst int32, priorAttempts int) {
//	pbk := r.instanceSpace[inst]
//	if pbk.Status != proposer.READY_TO_PROPOSE {
//		panic("asjfalskdjf")
//	}
//	dlog.AgentPrintfN(r.Id, "Attempting to propose value in instance %d", inst)
//	qrm := pbk.Qrms[pbk.PropCurBal]
//	qrm.StartAcceptanceQuorum()
//
//	if !pbk.ProposeValueBal.IsZero() {
//		if r.doStats {
//			r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Previous Value ProposedBatch", 1)
//			r.TimeseriesStats.Update("Times Previous Value ProposedBatch", 1)
//			r.ProposalStats.RecordPreviousValueProposed(stats.InstanceID{0, inst}, pbk.PropCurBal, len(pbk.Cmds))
//		}
//	} else {
//		b := r.clientRequestManager.GetFullBatchToPropose()
//		if b != nil {
//			pbk.ClientProposals = b
//			setProposingValue(pbk, r.Id, pbk.PropCurBal.Ballot, pbk.ClientProposals.GetCmds())
//		} else {
//			if priorAttempts == 0 {
//				r.BeginWaitingForClientProposals(inst, pbk)
//				return
//			}
//			man := r.clientRequestManager
//			if len(man.nextBatch.cmds) > 0 {
//				b = &batching.Batch{
//					Proposals: man.nextBatch.clientVals,
//					Cmds:      man.nextBatch.cmds,
//					Uid:       man.nextBatch.uid,
//				}
//				dlog.AgentPrintfN(r.Id, "Assembled partial batch with UID %d (length %d values)", b.GetUID(), len(b.GetCmds()))
//				man.nextBatch.cmds = make([]*state.Command, 0, man.nextBatch.maxLength)
//				man.nextBatch.clientVals = make([]*genericsmr.Propose, 0, man.nextBatch.maxLength)
//				man.nextBatch.uid += 1
//				pbk.ClientProposals = b
//				setProposingValue(pbk, r.Id, pbk.PropCurBal.Ballot, pbk.ClientProposals.GetCmds())
//			} else {
//				setProposingValue(pbk, -1, pbk.PropCurBal.Ballot, state.NOOPP())
//			}
//		}
//	}
//
//	if pbk.Cmds == nil {
//		panic("there must be something to propose")
//	}
//	pbk.SetNowProposing()
//	if pbk.WhoseCmds == -1 {
//		if r.isProposingDisklessNOOP(inst) { // no diskless noop should be always bcastaccept as we will have always verified there is no conflict (at least with synchronous acceptor)
//			dlog.AgentPrintfN(r.Id, "Proposing diskless noop in instance %d at ballot %d.%d", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
//		} else {
//			dlog.AgentPrintfN(r.Id, "Proposing persistent noop in instance %d at ballot %d.%d", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
//		}
//	} else {
//		if pbk.WhoseCmds == r.Id {
//			if r.balloter.GetAttemptNumber(pbk.ProposeValueBal.Number) > 1 {
//				dlog.AgentPrintfN(r.Id, "Proposing again batch with UID %d (length %d values) in instance %d at ballot %d.%d", pbk.ClientProposals.GetUID(), len(pbk.ClientProposals.GetCmds()), inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
//			} else {
//				dlog.AgentPrintfN(r.Id, "Proposing batch with UID %d (length %d values) in instance %d at ballot %d.%d", pbk.ClientProposals.GetUID(), len(pbk.ClientProposals.GetCmds()), inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
//			}
//		} else {
//			dlog.AgentPrintfN(r.Id, "Proposing previous value from ballot %d.%d with whose command %d in instance %d at ballot %d.%d",
//				pbk.ProposeValueBal.Number, pbk.ProposeValueBal.PropID, pbk.WhoseCmds, inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
//		}
//	}
//
//	r.Learner.ProposalValue(inst, pbk.PropCurBal.Ballot, pbk.Cmds, pbk.WhoseCmds)
//	if pbk.ClientProposals != nil {
//		r.SimpleExecutor.ProposedBatch(inst, pbk.ClientProposals)
//	}
//	r.bcastAccept(inst)
//}
//
//func (r *Replica2) isProposingDisklessNOOP(inst int32) bool {
//	pbk := r.instanceSpace[inst]
//	return r.disklessNOOP && pbk.WhoseCmds == -1 && pbk.Status >= proposer.PROPOSING && r.GotPromisesFromAllInGroup(inst, pbk.PropCurBal.Ballot)
//}
//
//func (r *Replica2) BeginWaitingForClientProposals(inst int32, pbk *proposer.PBK) {
//	r.clientRequestManager.sleepingInsts[inst] = time.Now()
//	if len(r.clientRequestManager.sleepingInsts) > 1 {
//		dlog.AgentPrintfN(r.Id, "No client value to propose in instance %d at ballot %d.%d. Queued instance for checking again.", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
//		return
//	}
//	dlog.AgentPrintfN(r.Id, "No client values to propose in instance %d at ballot %d.%d. Waiting %d ms before checking again", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID, r.noopWait.Milliseconds())
//	go func() {
//		<-time.After(r.noopWait)
//		r.proposableInstances <- struct{}{}
//	}()
//}
//
//func (r *Replica2) checkAndHandleNewlyReceivedInstance(instance int32) {
//	if instance < 0 {
//		return
//	}
//	r.Proposer.LearnOfBallot(&r.instanceSpace, instance, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
//}
//
//func (r *Replica2) handleAccept(accept *stdpaxosproto.Accept) {
//	dlog.AgentPrintfN(r.Id, "Replica2 received Accept from Replica2 %d in instance %d at ballot %d.%d", accept.PropID, accept.Instance, accept.Number, accept.PropID)
//	r.checkAndHandleNewlyReceivedInstance(accept.Instance)
//	r.Learner.ProposalValue(accept.Instance, accept.Ballot, accept.Command, accept.WhoseCmd)
//	if r.Learner.IsChosen(accept.Instance) && r.Learner.HasLearntValue(accept.Instance) {
//		// tell proposer and acceptor of learnt
//		cB, _, _ := r.Learner.GetChosen(accept.Instance)
//		r.proposerCloseCommit(accept.Instance, cB, accept.Command, accept.WhoseCmd)
//		return
//	}
//
//	pbk := r.instanceSpace[accept.Instance]
//	if accept.Ballot.GreaterThan(pbk.ProposeValueBal.Ballot) {
//		setProposingValue(pbk, accept.WhoseCmd, accept.Ballot, accept.Command)
//	}
//
//	if r.Proposer.LearnOfBallot(&r.instanceSpace, accept.Instance, lwcproto.ConfigBal{Config: -1, Ballot: accept.Ballot}, stdpaxosproto.ACCEPTANCE) {
//		pCurBal := r.instanceSpace[accept.Instance].PropCurBal
//		r.noLongerSleepingInstance(accept.Instance)
//		if !pCurBal.IsZero() {
//			dlog.AgentPrintfN(r.Id, "Accept Received from Replica2 %d in instance %d at ballot %d.%d preempted our ballot %d.%d",
//				accept.PropID, accept.Instance, accept.Number, accept.PropID, pCurBal.Number, pCurBal.PropID)
//		}
//	}
//
//	if accept.LeaderId == -2 {
//		dlog.AgentPrintfN(r.Id, "Not passing Accept for instance %d at ballot %d.%d to acceptor as we are passive observers", accept.Instance, accept.Ballot.Number, accept.Ballot.PropID)
//		return
//	}
//	if !r.AcceptorQrmInfo.IsInGroup(accept.Instance, r.Id) {
//		dlog.AgentPrintfN(r.Id, "Not passing Accept for instance %d at ballot %d.%d to acceptor as we cannot form an acceptance quorum", accept.Instance, accept.Ballot.Number, accept.Ballot.PropID)
//		return
//	}
//
//	r.acceptorHandleAcceptRequest(accept)
//}
//
//func (r *Replica2) acceptorHandleAcceptRequest(accept *stdpaxosproto.Accept) {
//	if int32(accept.PropID) == r.Id {
//		acceptorHandleAcceptLocal(r.Id, r.Acceptor, accept, r.AcceptResponsesRPC, r.Replica, r.bcastAcceptance, r.acceptReplyChan)
//		return
//	}
//	acceptorHandleAccept(r.Id, r.Learner, r.Acceptor, accept, r.AcceptResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.bcastAcceptance, r.acceptReplyChan, r.nopreempt, r.bcastAcceptDisklessNOOP)
//}
//
//func (r *Replica2) handleAcceptance(areply *stdpaxosproto.AcceptReply) {
//	r.Learner.ProposalAccepted(areply.Instance, areply.Cur, areply.AcceptorId)
//	dlog.AgentPrintfN(r.Id, "Acceptance recorded on proposal instance %d at ballot %d.%d from Replica2 %d with whose commands %d",
//		areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId, areply.WhoseCmd)
//	if int32(areply.Req.PropID) == r.Id {
//		r.instanceSpace[areply.Instance].Qrms[lwcproto.ConfigBal{-1, areply.Cur}].AddToQuorum(areply.AcceptorId)
//	}
//	if r.Learner.IsChosen(areply.Instance) && r.Learner.HasLearntValue(areply.Instance) {
//		_, cV, cWC := r.Learner.GetChosen(areply.Instance)
//		r.proposerCloseCommit(areply.Instance, areply.Cur, cV, cWC)
//	}
//}
//
//func (r *Replica2) handleAcceptReply(areply *stdpaxosproto.AcceptReply) {
//	r.checkAndHandleNewlyReceivedInstance(areply.Instance)
//	dlog.AgentPrintfN(r.Id, "Replica2 received Accept Reply from Replica2 %d in instance %d at requested ballot %d.%d and current ballot %d.%d", areply.AcceptorId, areply.Instance, areply.Req.Number, areply.Req.PropID, areply.Cur.Number, areply.Cur.PropID)
//	pbk := r.instanceSpace[areply.Instance]
//
//	// PREEMPTED
//	if areply.Cur.GreaterThan(areply.Req) {
//		pCurBal := r.instanceSpace[areply.Instance].PropCurBal
//		dlog.AgentPrintfN(r.Id, "Accept Reply received from Replica2 %d in instance %d with current ballot %d.%d preempted our ballot %d.%d",
//			areply.AcceptorId, areply.Instance, areply.Cur.Number, areply.Cur.PropID, pCurBal.Number, pCurBal.PropID)
//		r.Proposer.LearnOfBallot(&r.instanceSpace, areply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur}, areply.CurPhase)
//		r.clientRequestManager.ProposedClientValuesManager.learnOfBallot(pbk, areply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur}, r.clientRequestManager)
//		r.noLongerSleepingInstance(areply.Instance)
//		return
//	}
//	if areply.Req.GreaterThan(pbk.PropCurBal.Ballot) && areply.Req.PropID == int16(r.Id) {
//		panic("got a future acceptance??")
//	}
//	if areply.Req.GreaterThan(areply.Cur) {
//		panic("Acceptor didn't accept request")
//	}
//
//	// ACCEPTED
//	r.handleAcceptance(areply)
//}
//
//func (r *Replica2) proposerCloseCommit(inst int32, chosenAt stdpaxosproto.Ballot, chosenVal []*state.Command, whoseCmd int32) {
//	pbk := r.instanceSpace[inst]
//	if pbk.Status == proposer.CLOSED {
//		return
//	}
//	if r.Id == int32(chosenAt.PropID) {
//		r.bcastCommitToAll(inst, chosenAt, chosenVal, whoseCmd)
//	}
//	r.Proposer.LearnBallotChosen(&r.instanceSpace, inst, lwcproto.ConfigBal{Config: -1, Ballot: chosenAt}) // todo add client value chosen log
//	if !pbk.PropCurBal.IsZero() && r.doPatientProposals {
//		r.patientProposals.stoppingProposals(inst, pbk.PropCurBal.Ballot)
//	}
//	if whoseCmd == r.Id {
//		if _, e := r.clientRequestManager.chosenBatches[pbk.ClientProposals.GetUID()]; e {
//			dlog.AgentPrintfN(r.Id, logfmt.LearntBatchAgainFmt(inst, chosenAt, r.balloter, whoseCmd, pbk.ClientProposals))
//		} else {
//			dlog.AgentPrintfN(r.Id, logfmt.LearntBatchFmt(inst, chosenAt, r.balloter, whoseCmd, pbk.ClientProposals))
//		}
//		r.clientRequestManager.chosenBatches[pbk.ClientProposals.GetUID()] = struct{}{}
//	} else {
//		dlog.AgentPrintfN(r.Id, logfmt.LearntFmt(inst, chosenAt, r.balloter, whoseCmd))
//	}
//	r.clientRequestManager.ProposedClientValuesManager.valueChosen(pbk, inst, whoseCmd, chosenVal, r.clientRequestManager)
//
//	r.noLongerSleepingInstance(inst)
//	setProposingValue(pbk, whoseCmd, chosenAt, chosenVal)
//	r.SimpleExecutor.Learnt(inst, chosenVal, whoseCmd)
//	if r.execSig == nil {
//		return
//	}
//	r.execSig.CheckExec(r.Proposer, &r.SimpleExecutor)
//}
//
//// todo make it so proposer acceptor and learner all guard on chosen
//func (r *Replica2) handleCommit(commit *stdpaxosproto.Commit) {
//	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
//	dlog.AgentPrintfN(r.Id, "Replica2 received Commit from Replica2 %d in instance %d at ballot %d.%d with whose commands %d", commit.PropID, commit.Instance, commit.Ballot.Number, commit.Ballot.PropID, commit.WhoseCmd)
//
//	if r.Learner.IsChosen(commit.Instance) && r.Learner.HasLearntValue(commit.Instance) {
//		dlog.AgentPrintfN(r.Id, "Ignoring commit for instance %d as it is already learnt", commit.Instance)
//		return
//	}
//	r.Learner.ProposalValue(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
//	r.Learner.ProposalChosen(commit.Instance, commit.Ballot)
//	r.proposerCloseCommit(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
//}
//
//// if commited at one ballot, only need one ack from a higher ballot to be chosen
//func (r *Replica2) handleCommitShort(commit *stdpaxosproto.CommitShort) {
//	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
//	dlog.AgentPrintfN(r.Id, "Replica2 received Commit Short from Replica2 %d in instance %d at ballot %d.%d with whose commands %d", commit.PropID, commit.Instance, commit.Ballot.Number, commit.Ballot.PropID, commit.WhoseCmd)
//	pbk := r.instanceSpace[commit.Instance]
//	r.Learner.ProposalChosen(commit.Instance, commit.Ballot)
//	if !r.Learner.HasLearntValue(commit.Instance) {
//		dlog.AgentPrintfN(r.Id, "Cannot learn instance %d at ballot %d.%d as we do not have a value for it", commit.Instance, commit.Ballot.Number, commit.Ballot.PropID)
//		return
//	}
//	if pbk.Cmds == nil {
//		panic("We don't have any record of the value to be committed")
//	}
//	r.proposerCloseCommit(commit.Instance, commit.Ballot, pbk.Cmds, commit.WhoseCmd)
//}
//
//func (r *Replica2) handleState(state *proposerstate.State) {
//	r.Proposer.LearnOfBallot(&r.instanceSpace, state.CurrentInstance, lwcproto.ConfigBal{-2, stdpaxosproto.Ballot{-2, int16(state.ProposerID)}}, stdpaxosproto.PROMISE)
//}
//
//func (r *Replica2) updateLeases(lease acceptor.PromiseLease) {
//	dlog.AgentPrintfN(r.Id, "Received lease for from instance %d to %d, until ballots %d.%d", lease.From, lease.From+r.iWriteAhead, lease.MaxBalPromises.Number, lease.MaxBalPromises.PropID)
//	r.classesLeased[lease.From/r.iWriteAhead] = lease.MaxBalPromises
//}
//
//func (r *Replica2) isLeased(inst int32, ballot stdpaxosproto.Ballot) bool {
//	bLeased, e := r.classesLeased[inst/r.iWriteAhead]
//	if !e {
//		return false
//	}
//	if ballot.GreaterThan(bLeased) {
//		return false
//	}
//	return true
//}
//
//func (r *Replica2) GotPromisesFromAllInGroup(instance int32, ballot stdpaxosproto.Ballot) bool {
//	g := r.GetGroup(instance)
//
//	promisers := r.disklessNOOPPromises[instance][ballot]
//	for _, a := range g {
//		if _, e := promisers[a]; !e {
//			return false
//		}
//	}
//	return true
//}
//
//func (r *Replica2) noInstancesWaiting(inst int32) bool {
//	for i := inst + 1; i <= r.Proposer.GetCrtInstance(); i++ {
//		pbk := r.instanceSpace[i]
//		if !pbk.ProposeValueBal.IsZero() {
//			return false
//		}
//	}
//	return true
//}
