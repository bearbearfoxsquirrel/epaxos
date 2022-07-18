package twophase

import (
	"acceptor"
	"batching"
	"dlog"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"instanceagentmapper"
	"lwcproto"
	"os"
	"proposerstate"
	"quorumsystem"
	"stablestore"
	"state"
	"stats"
	"stdpaxosproto"
	"sync"
	"time"
	"twophase/aceptormessagefilter"
	"twophase/proposalmanager"
)

type ConcurrentFile struct {
	*os.File
	sync.Mutex
}

func (f *ConcurrentFile) Sync() error {
	f.Mutex.Lock()
	defer f.Mutex.Unlock()
	return f.File.Sync()
}

func (f *ConcurrentFile) Write(b []byte) (int, error) {
	f.Mutex.Lock()
	defer f.Mutex.Unlock()
	return f.File.Write(b)
}

func (f *ConcurrentFile) WriteAt(b []byte, off int64) (int, error) {
	f.Mutex.Lock()
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
	proposalmanager.CrtInstanceOracle
	ProposeBatchOracle

	proposalmanager.GlobalInstanceManager
	ProposedClientValuesManager

	proposalmanager.ProposerQuorumaliser
	proposalmanager.LearnerQuorumaliser
	proposalmanager.AcceptorQrmInfo

	Queueing

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
	executedUpTo                  int32
	maxBatchWait                  int
	crtOpenedInstances            []int32
	proposableInstances           chan ProposalInfo
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
	bcastAcceptance bool
	bcastAcceptLearning
	batchProposedObservers []ProposedObserver
	proposedBatcheNumber   map[int32]int32
	doPatientProposals     bool
	patientProposals
	startInstanceSig   chan struct{}
	doEager            bool
	promiseLeases      chan acceptor.PromiseLease
	classesLeased      map[int32]stdpaxosproto.Ballot
	iWriteAhead        int32
	writeAheadAcceptor bool
	tryInitPropose     chan proposalmanager.RetryInfo
	sendFastestQrm     bool
	nudge              chan struct{}
	bcastCommit        bool
}

type ProposalLatencyEstimator struct {
}

type messageFilterComm struct {
	inst int32
	ret  chan bool
}

type messageFilterRoutine struct {
	aceptormessagefilter.AcceptorMessageFilter
	aid             int32
	messageFilterIn chan *messageFilterComm
}

func (m *messageFilterRoutine) startFilter() {
	for {
		req := <-m.messageFilterIn
		req.ret <- m.AcceptorMessageFilter.ShouldFilterMessage(m.aid, req.inst)
	}
}

type MyBatchLearner interface {
	Learn(bat batching.ProposalBatch)
}

const MAXPROPOSABLEINST = 1000

//const CHAN_BUFFER_SIZE = 200000

func NewBaselineTwoPhaseReplica(id int, replica *genericsmr.Replica, durable bool, batchWait int, storageLoc string,
	maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool,
	factor float64, whoCrash int32, whenCrash time.Duration, howlongCrash time.Duration, emulatedSS bool,
	emulatedWriteTime time.Duration, catchupBatchSize int32, timeout time.Duration, group1Size int, flushCommit bool,
	softFac bool, doStats bool, statsParentLoc string, commitCatchup bool, deadTime int32, batchSize int,
	constBackoff bool, requeueOnPreempt bool, tsStatsFilename string, instStatsFilename string,
	propsStatsFilename string, sendProposerState bool, proactivePreemptOnNewB bool, batchingAcceptor bool,
	maxAccBatchWait time.Duration, sendPreparesToAllAcceptors bool, minimalProposers bool, timeBasedBallots bool,
	mappedProposers bool, dynamicMappedProposers bool, bcastAcceptance bool, mappedProposersNum int,
	instsToOpenPerBatch int32, doEager bool, sendFastestQrm bool, useGridQrms bool, minimalAcceptors bool,
	minimalAcceptorNegatives bool, prewriteAcceptor bool, doPatientProposals bool, sendFastestAccQrm bool, forwardInduction bool, q1 bool, bcastCommit bool) *Replica {

	r := &Replica{
		bcastCommit:                 bcastCommit,
		nudge:                       make(chan struct{}, maxOpenInstances),
		sendFastestQrm:              sendFastestAccQrm,
		Replica:                     replica,
		proposedBatcheNumber:        make(map[int32]int32),
		bcastAcceptance:             bcastAcceptance,
		stateChan:                   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		configChan:                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareChan:                 make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptChan:                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitChan:                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitShortChan:             make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareReplyChan:            make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptReplyChan:             make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		tryInitPropose:              make(chan proposalmanager.RetryInfo, 100),
		prepareRPC:                  0,
		acceptRPC:                   0,
		commitRPC:                   0,
		commitShortRPC:              0,
		prepareReplyRPC:             0,
		acceptReplyRPC:              0,
		instanceSpace:               make([]*proposalmanager.PBK, 15*1024*1024),
		Shutdown:                    false,
		counter:                     0,
		flush:                       true,
		executedUpTo:                -1, //get from storage
		maxBatchWait:                batchWait,
		crtOpenedInstances:          make([]int32, maxOpenInstances),
		proposableInstances:         make(chan ProposalInfo, MAXPROPOSABLEINST),
		noopWaitUs:                  noopwait,
		retryInstance:               make(chan proposalmanager.RetryInfo, maxOpenInstances*10000),
		alwaysNoop:                  alwaysNoop,
		fastLearn:                   false,
		whoCrash:                    whoCrash,
		whenCrash:                   whenCrash,
		howLongCrash:                howlongCrash,
		timeoutMsgs:                 make(chan TimeoutInfo, 5000),
		timeout:                     timeout,
		catchupBatchSize:            catchupBatchSize,
		lastSettleBatchInst:         -1,
		catchingUp:                  false,
		flushCommit:                 flushCommit,
		commitCatchUp:               commitCatchup,
		maxBatchSize:                batchSize,
		doStats:                     doStats,
		sendProposerState:           sendProposerState,
		noopWait:                    time.Duration(noopwait) * time.Microsecond,
		proactivelyPrepareOnPreempt: proactivePreemptOnNewB,
		expectedBatchedRequests:     200,
		sendPreparesToAllAcceptors:  sendPreparesToAllAcceptors,
		startInstanceSig:            make(chan struct{}, 100),
		doEager:                     doEager,
	}

	if r.bcastAcceptance {
		r.bcastAcceptLearning = bcastAcceptLearning{
			vals:           make(map[int32]map[lwcproto.ConfigBal][]*state.Command),
			chosen:         make(map[int32]map[lwcproto.ConfigBal]struct{}),
			chosenVal:      make(map[int32][]*state.Command),
			instanceClosed: make(map[int32]struct{}),
		}
	}

	pids := make([]int32, r.N)
	ids := make([]int, r.N)
	for i := range pids {
		pids[i] = int32(i)
		ids[i] = i
	}

	var amf aceptormessagefilter.AcceptorMessageFilter = nil
	r.isAccMsgFilter = minimalAcceptorNegatives
	if minimalAcceptorNegatives {
		if useGridQrms {
			panic("incompatible options")
		}
		amf = aceptormessagefilter.MinimalAcceptorFilterNew(&instanceagentmapper.InstanceNegativeAcceptorSetMapper{
			Acceptors: ids,
			F:         r.F,
			N:         r.N,
		})
	}
	if amf != nil {
		messageFilter := messageFilterRoutine{
			AcceptorMessageFilter: amf,
			aid:                   r.Id,
			messageFilterIn:       make(chan *messageFilterComm, 1000),
		}
		r.messageFilterIn = messageFilter.messageFilterIn
		go messageFilter.startFilter()
	}

	r.prepareRPC = r.RegisterRPC(new(stdpaxosproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(stdpaxosproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(stdpaxosproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(stdpaxosproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(stdpaxosproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(stdpaxosproto.AcceptReply), r.acceptReplyChan)
	r.stateChanRPC = r.RegisterRPC(new(proposerstate.State), r.stateChan)

	r.PrepareResponsesRPC = PrepareResponsesRPC{
		prepareReply: r.prepareReplyRPC,
		commit:       r.commitRPC,
	}
	r.AcceptResponsesRPC = AcceptResponsesRPC{
		acceptReply: r.acceptReplyRPC,
		commit:      r.commitRPC,
	}

	if prewriteAcceptor {
		r.iWriteAhead = 1000000
		r.promiseLeases = make(chan acceptor.PromiseLease, r.iWriteAhead)
		r.classesLeased = make(map[int32]stdpaxosproto.Ballot)
		r.writeAheadAcceptor = true
		if batchingAcceptor {
			r.StableStore = &ConcurrentFile{
				File:  r.StableStorage,
				Mutex: sync.Mutex{},
			}
			r.Acceptor = acceptor.PrewrittenBatcherAcceptorNew(r.StableStore, durable, emulatedSS,
				emulatedWriteTime, int32(id), maxAccBatchWait, pids, r.prepareReplyRPC, r.acceptReplyRPC, r.commitRPC, r.commitShortRPC, commitCatchup, r.promiseLeases, r.iWriteAhead)

		} else {
			r.StableStore = r.StableStorage
			r.Acceptor = acceptor.PrewritePromiseAcceptorNew(r.StableStore, durable, emulatedSS, emulatedWriteTime, int32(id),
				r.prepareReplyRPC, r.acceptReplyRPC, r.commitRPC, r.commitShortRPC, commitCatchup, r.promiseLeases, r.iWriteAhead, proactivePreemptOnNewB)
		}
	} else {
		if batchingAcceptor {
			r.StableStore = &ConcurrentFile{
				File:  r.StableStorage,
				Mutex: sync.Mutex{},
			}

			r.Acceptor = acceptor.BetterBatchingAcceptorNew(r.StableStore, durable, emulatedSS,
				emulatedWriteTime, int32(id), maxAccBatchWait, pids, r.prepareReplyRPC, r.acceptReplyRPC, r.commitRPC, r.commitShortRPC, commitCatchup)
		} else {
			r.StableStore = r.StableStorage
			r.Acceptor = acceptor.StandardAcceptorNew(r.StableStore, durable, emulatedSS, emulatedWriteTime, int32(id),
				r.prepareReplyRPC, r.acceptReplyRPC, r.commitRPC, r.commitShortRPC, commitCatchup)
		}

	}

	if r.doStats {
		phaseStarts := []string{"Fast Quorum", "Slow Quorum"}
		phaseEnds := []string{"Success", "Failure"}
		phaseRes := map[string][]string{
			"Fast Quorum": phaseEnds,
			"Slow Quorum": phaseEnds,
		}
		phaseNegs := map[string]string{
			"Fast Quorum": "Failure",
			"Slow Quorum": "Failure",
		}
		phaseCStats := []stats.MultiStartMultiOutStatConstructor{
			{"Phase 1", phaseStarts, phaseRes, phaseNegs, true},
			{"Phase 2", phaseStarts, phaseRes, phaseNegs, true},
		}

		r.InstanceStats = stats.InstanceStatsNew(statsParentLoc+fmt.Sprintf("/%s", instStatsFilename), stats.DefaultIMetrics{}.Get(), phaseCStats)
		r.ProposalStats = stats.ProposalStatsNew([]string{"Phase 1 Fast Quorum", "Phase 1 Slow Quorum", "Phase 2 Fast Quorum", "Phase 2 Slow Quorum"}, statsParentLoc+fmt.Sprintf("/%s", propsStatsFilename))
		r.TimeseriesStats = stats.TimeseriesStatsNew(stats.DefaultTSMetrics{}.Get(), statsParentLoc+fmt.Sprintf("/%s", tsStatsFilename), time.Second)
	}

	var qrm quorumsystem.SynodQuorumSystemConstructor
	qrm = &quorumsystem.SynodCountingQuorumSystemConstructor{
		F:                r.F,
		Thrifty:          r.Thrifty,
		Replica:          r.Replica,
		BroadcastFastest: sendFastestQrm,
		AllAids:          ids,
		SendAllAcceptors: false,
	}
	if useGridQrms {
		qrm = &quorumsystem.SynodGridQuorumSystemConstructor{
			F:                r.F,
			Replica:          r.Replica,
			Thrifty:          r.Thrifty,
			BroadcastFastest: sendFastestQrm,
		}
	}

	var proposerQrms proposalmanager.Quormaliser
	proposerQrms = &proposalmanager.Standard{
		SynodQuorumSystemConstructor: qrm,
		Aids:                         ids,
		MyID:                         r.Id,
	}

	if minimalAcceptors {
		var mapper instanceagentmapper.InstanceAgentMapper
		if useGridQrms {
			mapper = &instanceagentmapper.InstanceAcceptorGridMapper{
				Acceptors: ids,
				F:         r.F,
				N:         r.N,
			}
		} else {
			mapper = &instanceagentmapper.InstanceAcceptorSetMapper{
				Acceptors: ids,
				F:         r.F,
				N:         r.N,
			}
		}

		proposerQrms = &proposalmanager.Minimal{
			AcceptorMapper:               mapper,
			SynodQuorumSystemConstructor: qrm,
			MapperCache:                  make(map[int32][]int),
			MyID:                         r.Id,
		}
	}
	r.ProposerQuorumaliser = proposerQrms
	r.AcceptorQrmInfo = proposerQrms
	r.LearnerQuorumaliser = proposerQrms

	if group1Size <= r.N-r.F {
		r.group1Size = r.N - r.F
	} else {
		r.group1Size = group1Size
	}

	balloter := proposalmanager.Balloter{r.Id, int32(r.N), 10000, time.Time{}, timeBasedBallots}

	if !q1 {
		var q Queueing = ProposingChosenUniqueueQNew(r.Id, 200)                 //ChosenUniqueQNew(r.Id, 200)
		var batchProposeOracle ProposeBatchOracle = q.(*ProposingChosenUniqueQ) //chosenQ
		r.ProposeBatchOracle = batchProposeOracle
		r.batchProposedObservers = make([]ProposedObserver, 0)
		r.batchLearners = []MyBatchLearner{q.(*ProposingChosenUniqueQ)}
		r.Queueing = q
		r.ProposedClientValuesManager = ProposedClientValuesManagerNew(r.Id, r.TimeseriesStats, r.doStats, q)
	} else {
		var q Queueing = ChosenUniqueQNew(r.Id, 200)
		var batchProposeOracle ProposeBatchOracle = q.(*ChosenUniqueQ) //chosenQ
		r.ProposeBatchOracle = batchProposeOracle
		r.batchProposedObservers = make([]ProposedObserver, 0)
		r.batchLearners = []MyBatchLearner{q.(*ChosenUniqueQ)}
		r.Queueing = q
		r.ProposedClientValuesManager = ProposedClientValuesManagerNew(r.Id, r.TimeseriesStats, r.doStats, q)
	}

	// SET UP SIGNAL
	var openInstSig proposalmanager.OpenInstSignal = proposalmanager.SimpleSigNew(r.startInstanceSig, r.Id)
	if doEager {
		openInstSig = proposalmanager.EagerSigNew(openInstSig.(*proposalmanager.SimpleSig), maxOpenInstances)
	}
	backoffManager := proposalmanager.BackoffManagerNew(minBackoff, maxInitBackoff, maxBackoff, r.retryInstance, factor, softFac, constBackoff)
	var instanceManager proposalmanager.SingleInstanceManager = proposalmanager.SimpleInstanceManagerNew(r.Id, backoffManager, balloter, doStats, r.ProposerQuorumaliser, r.TimeseriesStats, r.ProposalStats, r.InstanceStats)

	var awaitingGroup ProposerGroupGetter = SimpleProposersAwaitingGroupGetterNew(ids)
	var minimalGroupGetter *MinimalProposersAwaitingGroup
	if minimalProposers {
		minimalShouldMaker := proposalmanager.MinimalProposersShouldMakerNew(int16(r.Id), r.F)
		instanceManager = proposalmanager.MinimalProposersInstanceManagerNew(instanceManager.(*proposalmanager.SimpleInstanceManager), minimalShouldMaker)
		minimalGroupGetter = MinimalProposersAwaitingGroupNew(awaitingGroup.(*SimpleProposersAwaitingGroup), minimalShouldMaker, int32(r.F))
		awaitingGroup = minimalGroupGetter
	}

	simpleGlobalManager := proposalmanager.SimpleProposalManagerNew(r.Id, openInstSig, instanceManager, backoffManager)
	var globalManager proposalmanager.GlobalInstanceManager = simpleGlobalManager

	if instsToOpenPerBatch < 1 {
		panic("Will not open any instances")
	}
	if instsToOpenPerBatch > 1 && (mappedProposers || dynamicMappedProposers || doEager) {
		panic("incompatible options")
	}

	r.noopLearners = []proposalmanager.NoopLearner{}
	if mappedProposers || dynamicMappedProposers {
		agentMapper := &instanceagentmapper.InstanceSetMapper{
			Ids: ids,
			G:   mappedProposersNum,
			N:   r.N,
		}
		globalManager = proposalmanager.MappedProposersProposalManagerNew(simpleGlobalManager, instanceManager, agentMapper)
		mappedGroupGetter := MappedProposersAwaitingGroupNew(agentMapper)
		if dynamicMappedProposers {
			dAgentMapper := &proposalmanager.DynamicInstanceSetMapper{
				InstanceSetMapper: *agentMapper,
			}
			globalManager = proposalmanager.DynamicMappedProposerManagerNew(simpleGlobalManager, instanceManager, dAgentMapper, int32(r.N), r.F)
			mappedGroupGetter = MappedProposersAwaitingGroupNew(dAgentMapper)
			r.noopLearners = []proposalmanager.NoopLearner{globalManager.(*proposalmanager.DynamicMappedGlobalManager)}
		}
		if minimalProposers {
			awaitingGroup = MinimalMappedProposersAwaitingGroupNew(*minimalGroupGetter, *mappedGroupGetter)
		}
	}

	if instsToOpenPerBatch > 1 {
		openInstSig = proposalmanager.HedgedSigNew(r.Id, r.startInstanceSig)
		globalManager = proposalmanager.HedgedBetsProposalManagerNew(r.Id, simpleGlobalManager, int32(r.N), instsToOpenPerBatch)
	}

	r.CrtInstanceOracle = globalManager
	r.GlobalInstanceManager = globalManager
	r.Durable = durable

	r.doPatientProposals = doPatientProposals
	if r.doPatientProposals {
		r.patientProposals = patientProposals{
			myId:                r.Id,
			promisesRequestedAt: make(map[int32]map[stdpaxosproto.Ballot]time.Time),
			pidsPropRecv:        make(map[int32]map[int32]struct{}),
			doPatient:           true,
			Ewma:                make([]float64, r.N),
			ProposerGroupGetter: awaitingGroup,
			closed:              make(map[int32]struct{}),
		}
	}

	go r.run()
	return r
}

func (r *Replica) CloseUp() {
	if r.doStats {
		r.TimeseriesStats.Close()
		r.InstanceStats.Close()
		r.ProposalStats.CloseOutput()
	}
}

func (r *Replica) recordExecutedUpTo() {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(r.executedUpTo))
	r.StableStore.WriteAt(b[:], 4)
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

func (r *Replica) run() {
	r.ConnectToPeers()
	r.RandomisePeerOrder()

	fastClockChan = make(chan bool, 1)
	if r.BatchingEnabled() {
		onBatch := func() { go func() { r.startInstanceSig <- struct{}{} }() }
		if r.doEager {
			onBatch = func() {}
		}
		go batching.StartBatching(r.Id, r.ProposeChan, r.Queueing.GetTail(), r.expectedBatchedRequests, r.maxBatchSize, time.Duration(r.maxBatchWait)*time.Millisecond, onBatch, r.nudge, r.doEager)
	}

	go r.WaitForClientConnections()

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

	//var stateGo *time.Timer
	//var stateGoC <-chan time.Time
	//if r.sendProposerState {
	//	stateGo = time.NewTimer(200 * time.Millisecond)
	//	stateGoC = stateGo.C
	//}

	for !r.Shutdown {
		select {
		//case <-stateGoC:
		//for i := int32(0); i < int32(r.N); i++ {
		//	if i == r.Id {
		//		continue
		//	}
		//	msg := proposerstate.State{
		//		ProposerID:      r.Id,
		//		CurrentInstance: r.CrtInstanceOracle.GetCrtInstance(),
		//	}
		//	//dlog.AgentPrintfN(r.id, "Sending current state %d to all other proposers", r.crtInstance)
		//	r.SendMsg(i, r.stateChanRPC, &msg)
		//}
		//stateGo.Reset(time.Duration(200) * time.Millisecond)
		//break
		case stateS := <-r.stateChan:
			recvState := stateS.(*proposerstate.State)
			r.handleState(recvState)
			break
		case <-r.startInstanceSig:
			r.beginNextInstance()
			break
		case t := <-r.tryInitPropose:
			r.tryInitaliseForPropose(t.Inst, t.AttemptedBal.Ballot)
			break
		case lease := <-r.promiseLeases:
			r.updateLeases(lease)
			break
		case <-c:
			r.TimeseriesStats.PrintAndReset()
			break
		//case maybeTimedout := <-r.timeoutMsgs:
		//r.retryBallot(maybeTimedout)
		//break
		case next := <-r.retryInstance:
			dlog.Println("Checking whether to retry a proposal")
			r.tryNextAttempt(next)
			break
		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*stdpaxosproto.Prepare)
			//got a Prepare message
			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
			//if !r.checkAndHandlecatchupRequest(prepare) {
			r.handlePrepare(prepare)
			//}
			break
		case acceptS := <-r.acceptChan:
			accept := acceptS.(*stdpaxosproto.Accept)
			//got an Accept message
			dlog.Printf("Received Accept Request from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
			r.handleAccept(accept)
			break
		case commitS := <-r.commitChan:
			commit := commitS.(*stdpaxosproto.Commit)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommit(commit)
			//r.checkAndHandlecatchupResponse(commit)
			break
		case commitS := <-r.commitShortChan:
			commit := commitS.(*stdpaxosproto.CommitShort)
			//got a Commit message
			dlog.Printf("Received short Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommitShort(commit)
			break
		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*stdpaxosproto.PrepareReply)
			//got a Prepare reply
			dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
			r.handlePrepareReply(prepareReply)
			break
		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*stdpaxosproto.AcceptReply)
			//got an Accept reply
			dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
			r.handleAcceptReply(acceptReply)
			break
		case proposeableInst := <-r.proposableInstances:
			r.recheckInstanceToPropose(proposeableInst)
			break
		}
	}
}

//func (r *Replica) retryBallot(maybeTimedout TimeoutInfo) {
//	pbk := r.instanceSpace[maybeTimedout.inst]
//	if pbk.PropCurBal.Equal(lwcproto.ConfigBal{Config: -1, Ballot: maybeTimedout.ballot}) && pbk.Status == maybeTimedout.phase {
//		if r.doStats {
//			r.TimeseriesStats.Update("Message Timeouts", 1)
//			id := stats.InstanceID{
//				Log: 0,
//				Seq: maybeTimedout.inst,
//			}
//			if maybeTimedout.phase == proposalmanager.PREPARING {
//				r.InstanceStats.RecordOccurrence(id, "Phase 1 Timeout", 1)
//			} else if maybeTimedout.phase == proposalmanager.PROPOSING {
//				r.InstanceStats.RecordOccurrence(id, "Phase 2 Timeout", 1)
//			}
//		}
//		group := pbk.Qrms[pbk.PropCurBal].Broadcast(maybeTimedout.msgCode, maybeTimedout.msg)
//		phase := "prepare"
//		if maybeTimedout.phase == proposalmanager.PROPOSING {
//			phase = "accept request"
//		}
//		dlog.AgentPrintfN(r.Id, "Phase %s message in instance %d at ballot %d.%d timedout. Broadcasting to %v", phase, maybeTimedout.inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID, group)
//		//r.beginTimeout(maybeTimedout.inst, maybeTimedout.ballot, maybeTimedout.phase, r.timeout, maybeTimedout.msgCode, maybeTimedout.msg)
//	}
//}

func (r *Replica) sendSinglePrepare(instance int32, to int32) {
	// cheats - DecideRetry really be a special recovery message but lazzzzzyyyy
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast preempted:", err)
		}
	}()
	args := &stdpaxosproto.Prepare{r.Id, instance, r.instanceSpace[instance].PropCurBal.Ballot}
	//dlog.Printf("send prepare to %d\n", to)
	r.SendMsg(to, r.prepareRPC, args)
	//r.beginTimeout(args.Instance, args.Ballot, proposalmanager.PREPARING, r.timeout*5, r.prepareRPC, args)
}

////func (r *Replica) beginTimeout(inst int32, attempted stdpaxosproto.Ballot, onWhatPhase proposalmanager.ProposerStatus, timeout time.Duration, msgcode uint8, msg fastrpc.Serializable) {
////	time.AfterFunc(timeout, func() {
////		r.timeoutMsgs <- TimeoutInfo{
////			inst:    inst,
////			ballot:  attempted,
////			phase:   onWhatPhase,
////			msgCode: msgcode,
////			msg:     msg,
////		}
////	})
//}

func (r *Replica) isSlowestSlowerThanMedian(sent []int) bool {
	slowestLat := float64(-1)
	ewma := r.CopyEWMA()
	for _, v := range sent {
		if ewma[v] > slowestLat {
			slowestLat = ewma[v]
		}
	}

	// isChosen slower than median???
	if (r.N-1)%2 != 0 {
		return slowestLat > ewma[(len(ewma)-1)/2]
	} else {
		return slowestLat > (ewma[len(ewma)/2]+ewma[(len(ewma)/2)-1])/2
	}
}

func (r *Replica) beginTracking(instID stats.InstanceID, ballot stdpaxosproto.Ballot, sentTo []int, trackingName string, proposalTrackingName string) string {
	if len(sentTo) == r.N || r.isSlowestSlowerThanMedian(sentTo) {
		r.InstanceStats.RecordComplexStatStart(instID, trackingName, "Slow Quorum")
		r.ProposalStats.RecordOccurence(instID, lwcproto.ConfigBal{Config: -1, Ballot: ballot}, proposalTrackingName+" Slow Quorum", 1)
		return "slow"
	} else {
		r.InstanceStats.RecordComplexStatStart(instID, trackingName, "Fast Quorum")
		r.ProposalStats.RecordOccurence(instID, lwcproto.ConfigBal{Config: -1, Ballot: ballot}, proposalTrackingName+" Fast Quorum", 1)
		return "fast"
	}
}

func (r *Replica) bcastPrepare(instance int32) {
	args := &stdpaxosproto.Prepare{r.Id, instance, r.instanceSpace[instance].PropCurBal.Ballot}

	//if args.Ballot.Number > 20000 {
	//	panic("aslkdjf;laksdjfals;kdfjasl;dkfja")
	//}
	pbk := r.instanceSpace[instance]
	//dlog.Println("sending prepare for instance", instance, "and ballot", pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
	var sentTo []int
	if r.sendPreparesToAllAcceptors {
		sentTo = make([]int, 0, r.N)
		for i := 0; i < r.N; i++ {
			if i == int(r.Id) {
				if r.writeAheadAcceptor && !r.IsInQrm(instance, r.Id) {
					continue
				}
				prep := &stdpaxosproto.Prepare{r.Id, instance, r.instanceSpace[instance].PropCurBal.Ballot}
				go func() {
					r.prepareChan <- prep
				}()
				sentTo = append(sentTo, i)
				continue
			}
			sentTo = append(sentTo, i)
			r.Replica.SendMsg(int32(i), r.prepareRPC, args)
		}
	} else {
		sentTo = pbk.Qrms[pbk.PropCurBal].Broadcast(r.prepareRPC, args)
	}

	instID := stats.InstanceID{
		Log: 0,
		Seq: instance,
	}
	if r.doStats {
		r.InstanceStats.RecordOccurrence(instID, "My Phase 1 Proposals", 1)
	}
	speed := r.beginTracking(instID, args.Ballot, sentTo, "Phase 1", "Phase 1")
	dlog.AgentPrintfN(r.Id, "Sending prepare for instance %d at ballot %d.%d to replicas %v (a %s quorum)", args.Instance, args.Number, args.PropID, sentTo, speed)
	//r.beginTimeout(args.Instance, args.Ballot, proposalmanager.PREPARING, r.timeout, r.prepareRPC, args)
}

var pa stdpaxosproto.Accept

func (r *Replica) bcastAccept(instance int32) {
	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = r.instanceSpace[instance].PropCurBal.Ballot
	pa.Command = r.instanceSpace[instance].Cmds
	pa.WhoseCmd = r.instanceSpace[instance].WhoseCmds
	args := &pa
	//pbk := r.instanceSpace[instance]
	var sentTo []int

	// fixme lazy way
	if r.bcastAcceptance {
		sentTo = make([]int, 0, r.N)
		pa.LeaderId = -1
		sendC := r.F + 1
		if !r.Thrifty {
			sendC = r.F*2 + 1
		}

		peerList := r.Replica.GetAliveRandomPeerOrder()
		if r.sendFastestQrm {
			peerList = r.Replica.GetPeerOrderLatency()
		}

		for _, peer := range peerList {
			if len(sentTo) >= sendC {
				pa.LeaderId = -2
				//dlog.AgentPrintfN(r.Id, "instance %d acpt passive r is %d", instance, peer)
			}
			if peer == r.Id {
				if !r.AcceptorQrmInfo.IsInQrm(instance, r.Id) {
					continue
				}
				acc := &stdpaxosproto.Accept{
					LeaderId: pa.LeaderId,
					Instance: pa.Instance,
					Ballot:   pa.Ballot,
					WhoseCmd: pa.WhoseCmd,
					Command:  pa.Command,
				}
				go func() {
					r.acceptChan <- acc
				}()
				sentTo = append(sentTo, int(peer))
				continue
			}
			r.Replica.SendMsg(peer, r.acceptRPC, args)
			sentTo = append(sentTo, int(peer))
		}
	} else {
		//sentTo = pbk.Qrms[pbk.PropCurBal].Broadcast(r.acceptRPC, args)
		sentTo = make([]int, 0, r.N)
		pa.LeaderId = -1
		sendC := r.F + 1
		if !r.Thrifty {
			sendC = r.F*2 + 1
		}

		peerList := r.Replica.GetAliveRandomPeerOrder()
		if r.sendFastestQrm {
			peerList = r.Replica.GetPeerOrderLatency()
		}

		for _, peer := range peerList {
			if len(sentTo) >= sendC {
				break
			}
			if peer == r.Id {
				if !r.AcceptorQrmInfo.IsInQrm(instance, r.Id) {
					continue
				}
				acc := &stdpaxosproto.Accept{
					LeaderId: pa.LeaderId,
					Instance: pa.Instance,
					Ballot:   pa.Ballot,
					WhoseCmd: pa.WhoseCmd,
					Command:  pa.Command,
				}
				go func() {
					r.acceptChan <- acc
				}()
				sentTo = append(sentTo, int(peer))
				continue
			}
			r.Replica.SendMsg(peer, r.acceptRPC, args)
			sentTo = append(sentTo, int(peer))
		}
	}

	instID := stats.InstanceID{
		Log: 0,
		Seq: instance,
	}
	if r.doStats {
		r.InstanceStats.RecordOccurrence(instID, "My Phase 2 Proposals", 1)
	}
	speed := r.beginTracking(instID, args.Ballot, sentTo, "Phase 2", "Phase 2")
	dlog.AgentPrintfN(r.Id, "Sending accept for instance %d with whose commands %d, at ballot %d.%d to Replicas %v (a %s quorum)", pa.Instance, pa.WhoseCmd, pa.Number, pa.PropID, sentTo, speed)
	//r.beginTimeout(args.Instance, args.Ballot, proposalmanager.PROPOSING, r.timeout, r.acceptRPC, args)
}

var pc stdpaxosproto.Commit
var pcs stdpaxosproto.CommitShort

func (r *Replica) bcastCommitToAll(instance int32, Ballot stdpaxosproto.Ballot, command []*state.Command) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("commit bcast preempted:", err)
		}
	}()
	pc.LeaderId = r.Id // fixme should be ballot prop id?
	pc.Instance = instance
	pc.Ballot = Ballot
	pc.WhoseCmd = r.instanceSpace[instance].WhoseCmds
	pc.MoreToCome = 0
	pc.Command = command

	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = Ballot
	pcs.WhoseCmd = r.instanceSpace[instance].WhoseCmds
	pcs.Count = int32(len(command))
	argsShort := pcs
	dlog.AgentPrintfN(r.Id, "Sending commit for instance %d with whose commands %d, at ballot %d.%d", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID)
	r.CalculateAlive()
	if r.bcastAcceptance {
		//return
		if !r.bcastCommit {
			return
		}
		for q := int32(0); q < int32(r.N); q++ {
			if q == r.Id {
				continue
			}
			r.SendMsg(q, r.commitShortRPC, &argsShort)
		}
		return
	}

	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id {
			continue
		}
		inQrm := r.instanceSpace[instance].Qrms[lwcproto.ConfigBal{Config: -1, Ballot: Ballot}].HasAcknowledged(int(q))
		if inQrm {
			r.SendMsg(q, r.commitShortRPC, &argsShort)
		} else {
			r.SendMsg(q, r.commitRPC, &pc)
		}
	}
}

func min(x, y int) int {
	if x <= y {
		return x
	} else {
		return y
	}
}

type ProposerAccValHandler int

const (
	IGNORED ProposerAccValHandler = iota
	NEW_VAL
	ACKED
	CHOSEN
)

func (r *Replica) beginNextInstance() {
	do := func(inst int32) {
		if inst <= r.executedUpTo {
			panic("alsdkfjal;kdfjal;skdfj")
		}
		curInst := r.instanceSpace[inst]
		r.GlobalInstanceManager.StartNextProposal(curInst, inst)

		if r.doStats {
			r.InstanceStats.RecordOpened(stats.InstanceID{0, inst}, time.Now())
			r.TimeseriesStats.Update("Instances Opened", 1)
			r.ProposalStats.Open(stats.InstanceID{0, inst}, curInst.PropCurBal)
		}

		prepMsg := getPrepareMessage(r.Id, inst, curInst)
		dlog.AgentPrintfN(r.Id, "Opened new instance %d, with ballot %d.%d \n", inst, prepMsg.Number, prepMsg.PropID)
		//acceptorHandlePrepareLocal(r.Id, r.Acceptor, prepMsg, r.PrepareResponsesRPC, r.prepareReplyChan)
		r.bcastPrepare(inst)
		if r.doPatientProposals {
			r.patientProposals.startedProposal(inst, curInst.PropCurBal.Ballot)
		}
	}
	r.GlobalInstanceManager.StartNextInstance(&r.instanceSpace, do)
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
	if !r.GlobalInstanceManager.DecideRetry(inst, next) {
		return
	}

	dlog.AgentPrintfN(r.Id, "Retry needed as backoff expired for instance %d", next.Inst)
	r.GlobalInstanceManager.StartNextProposal(inst, next.Inst)
	nextBallot := inst.PropCurBal

	if r.doStats {
		r.ProposalStats.Open(stats.InstanceID{0, next.Inst}, nextBallot)
		r.InstanceStats.RecordOccurrence(stats.InstanceID{0, next.Inst}, "My Phase 1 Proposals", 1)
	}
	r.bcastPrepare(next.Inst)
	if r.doPatientProposals {
		r.patientProposals.startedProposal(next.Inst, inst.PropCurBal.Ballot)
	}
}

func (r *Replica) recordStatsPreempted(inst int32, pbk *proposalmanager.PBK) {
	if pbk.Status != proposalmanager.BACKING_OFF && r.doStats {
		id := stats.InstanceID{Log: 0, Seq: inst}
		if pbk.Status == proposalmanager.PREPARING || pbk.Status == proposalmanager.READY_TO_PROPOSE {
			r.InstanceStats.RecordOccurrence(id, "My Phase 1 Preempted", 1)
			r.TimeseriesStats.Update("My Phase 1 Preempted", 1)
			r.ProposalStats.CloseAndOutput(id, pbk.PropCurBal, stats.HIGHERPROPOSALONGOING)
		} else if pbk.Status == proposalmanager.PROPOSING {
			r.InstanceStats.RecordOccurrence(id, "My Phase 2 Preempted", 1)
			r.TimeseriesStats.Update("My Phase 2 Preempted", 1)
			r.ProposalStats.CloseAndOutput(id, pbk.PropCurBal, stats.HIGHERPROPOSALONGOING)
		}
	}
}

func (r *Replica) handlePrepare(prepare *stdpaxosproto.Prepare) {
	dlog.AgentPrintfN(r.Id, "Replica received a Prepare from Replica %d in instance %d at ballot %d.%d", prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID)
	if int32(prepare.PropID) == r.Id {
		dlog.AgentPrintfN(r.Id, "Giving Prepare from Replica %d in instance %d at ballot %d.%d to acceptor as it is needed for safety or can form a quorum", prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID)
		acceptorHandlePrepareLocal(r.Id, r.Acceptor, r.Replica, prepare, r.PrepareResponsesRPC, r.prepareReplyChan)
		return
	}

	if r.AcceptorQrmInfo.IsInQrm(prepare.Instance, r.Id) {
		dlog.AgentPrintfN(r.Id, "Giving Prepare from Replica %d in instance %d at ballot %d.%d to acceptor as it can form a quorum", prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID)
		acceptorHandlePrepare(r.Id, r.Acceptor, prepare, r.PrepareResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.sendPreparesToAllAcceptors)
	}

	if r.doPatientProposals {
		r.patientProposals.learnOfProposal(prepare.Instance, prepare.Ballot)
	}

	if r.GlobalInstanceManager.LearnOfBallot(&r.instanceSpace, prepare.Instance, lwcproto.ConfigBal{Config: -1, Ballot: prepare.Ballot}, stdpaxosproto.PROMISE) {
		pCurBal := r.instanceSpace[prepare.Instance].PropCurBal
		if !pCurBal.IsZero() {
			dlog.AgentPrintfN(r.Id, "Prepare Received from Replica %d in instance %d at ballot %d.%d preempted our ballot %d.%d",
				prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID, pCurBal.Number, pCurBal.PropID)
		}
	}

	r.ProposedClientValuesManager.learnOfBallot(r.instanceSpace[prepare.Instance], prepare.Instance, lwcproto.ConfigBal{Config: -1, Ballot: prepare.Ballot})
}

//func (r *Replica) learn
func (r *Replica) proposerWittnessAcceptedValue(inst int32, aid int32, accepted stdpaxosproto.Ballot, val []*state.Command, whoseCmds int32) bool {
	if accepted.IsZero() {
		return false
	}
	r.GlobalInstanceManager.LearnOfBallotAccepted(&r.instanceSpace, inst, lwcproto.ConfigBal{Config: -1, Ballot: accepted}, whoseCmds)
	pbk := r.instanceSpace[inst]
	if pbk.Status == proposalmanager.CLOSED {
		return false
	}
	r.ProposedClientValuesManager.learnOfAcceptedBallot(pbk, inst, lwcproto.ConfigBal{Config: -1, Ballot: accepted}, whoseCmds)
	newVal := false
	if accepted.GreaterThan(pbk.ProposeValueBal.Ballot) {
		setProposingValue(pbk, whoseCmds, accepted, val)
		newVal = true
	}
	return newVal
}

func addToAcceptanceQuorum(inst int32, aid int32, accepted stdpaxosproto.Ballot, pbk *proposalmanager.PBK, lnrQrm proposalmanager.LearnerQuorumaliser, accQrm proposalmanager.AcceptorQrmInfo) {
	if !accQrm.IsInQrm(inst, aid) {
		return
	}

	_, exists := pbk.Qrms[lwcproto.ConfigBal{Config: -1, Ballot: accepted}]
	if !exists {
		lnrQrm.TrackProposalAcceptance(pbk, inst, lwcproto.ConfigBal{Config: -1, Ballot: accepted})
	}

	pbk.Qrms[lwcproto.ConfigBal{Config: -1, Ballot: accepted}].AddToQuorum(int(aid))
}

func (r *Replica) learnerHandleAcceptedValue(inst int32, aid int32, accepted stdpaxosproto.Ballot, val []*state.Command, whoseCmds int32) ProposerAccValHandler {
	if accepted.IsZero() {
		return IGNORED
	}
	pbk := r.instanceSpace[inst]
	if pbk.Status == proposalmanager.CLOSED {
		return IGNORED
	}

	addToAcceptanceQuorum(inst, aid, accepted, pbk, r.LearnerQuorumaliser, r.AcceptorQrmInfo)
	if pbk.Qrms[lwcproto.ConfigBal{Config: -1, Ballot: accepted}].QuorumReached() {

		dlog.AgentPrintfN(r.Id, "Acceptance Quorum reached in instance %d at ballot %d.%d with whose commands %d", inst, accepted.Number, accepted.PropID, whoseCmds)
		if !r.bcastAcceptance {
			r.bcastCommitToAll(inst, accepted, val)
		}
		if !pbk.Qrms[lwcproto.ConfigBal{Config: -1, Ballot: accepted}].HasAcknowledged(int(r.Id)) {
			cmt := &stdpaxosproto.Commit{
				LeaderId:   int32(accepted.PropID),
				Instance:   inst,
				Ballot:     accepted,
				WhoseCmd:   whoseCmds,
				MoreToCome: 0,
				Command:    val,
			}
			r.Acceptor.RecvCommitRemote(cmt)
		} else {
			cmt := &stdpaxosproto.CommitShort{
				LeaderId: int32(accepted.PropID),
				Instance: inst,
				Ballot:   accepted,
				WhoseCmd: whoseCmds,
			}
			r.Acceptor.RecvCommitShortRemote(cmt)
		}
		r.proposerCloseCommit(inst, accepted, pbk.Cmds, whoseCmds)
		return CHOSEN
	} else {
		return ACKED
	}
}

func setProposingValue(pbk *proposalmanager.PBK, whoseCmds int32, bal stdpaxosproto.Ballot, val []*state.Command) {
	pbk.WhoseCmds = whoseCmds
	pbk.ProposeValueBal = lwcproto.ConfigBal{Config: -1, Ballot: bal}
	pbk.Cmds = val
}

func (r *Replica) handlePrepareReply(preply *stdpaxosproto.PrepareReply) {
	pbk := r.instanceSpace[preply.Instance]
	dlog.AgentPrintfN(r.Id, "Replica received a Prepare Reply from Replica %d in instance %d at requested ballot %d.%d and current ballot %d.%d", preply.AcceptorId, preply.Instance, preply.Req.Number, preply.Req.PropID, preply.Cur.Number, preply.Cur.PropID)

	if r.bcastAcceptance && !preply.VBal.IsZero() {
		r.bcastAcceptCheckVBalPReply(preply, pbk)
	}

	if pbk.Status == proposalmanager.CLOSED {
		dlog.AgentPrintfN(r.Id, "Discarding Prepare Reply from Replica %d in instance %d at requested ballot %d.%d because it's already chosen", preply.AcceptorId, preply.Instance, preply.Req.Number, preply.Req.PropID)
		return
	}

	// check if there isChosen a value and track it
	// IS IN LATE?
	//if !r.bcastAcceptance {
	r.proposerWittnessAcceptedValue(preply.Instance, preply.AcceptorId, preply.VBal, preply.Command, preply.WhoseCmd)
	valWhatDone := r.learnerHandleAcceptedValue(preply.Instance, preply.AcceptorId, preply.VBal, preply.Command, preply.WhoseCmd)
	if valWhatDone == CHOSEN {
		return
	}
	//}

	if pbk.PropCurBal.GreaterThan(lwcproto.ConfigBal{Config: -1, Ballot: preply.Req}) || pbk.Status != proposalmanager.PREPARING {

		// even if late check if cur proposal preempts our current proposal
		r.GlobalInstanceManager.LearnOfBallot(&r.instanceSpace, preply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: preply.Cur}, preply.CurPhase)
		dlog.AgentPrintfN(r.Id, "Prepare Reply for instance %d with current ballot %d.%d and requested ballot %d.%d in late, either because we are now at %d.%d or aren't preparing any more",
			preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.Req.Number, preply.Req.PropID, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
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
		isNewPreempted := r.GlobalInstanceManager.LearnOfBallot(&r.instanceSpace, preply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: preply.Cur}, stdpaxosproto.PROMISE)
		if isNewPreempted {
			r.ProposedClientValuesManager.learnOfBallot(r.instanceSpace[preply.Instance], preply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: preply.Cur})
			if r.doPatientProposals {
				r.patientProposals.learnOfProposal(preply.Instance, preply.Cur)
			}

			pCurBal := r.instanceSpace[preply.Instance].PropCurBal
			dlog.AgentPrintfN(r.Id, "Prepare Reply Received from Replica %d in instance %d at with current ballot %d.%d preempted our ballot %d.%d",
				preply.AcceptorId, preply.Instance, preply.Cur.Number, preply.Cur.PropID, pCurBal.Number, pCurBal.PropID)
		}

		// Proactively try promise
		if r.AcceptorQrmInfo.IsInQrm(preply.Instance, r.Id) && r.proactivelyPrepareOnPreempt && isNewPreempted && int32(preply.Req.PropID) != r.Id {
			newPrep := &stdpaxosproto.Prepare{
				LeaderId: int32(preply.Cur.PropID),
				Instance: preply.Instance,
				Ballot:   preply.Cur,
			}
			acceptorHandlePrepare(r.Id, r.Acceptor, newPrep, r.PrepareResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.sendPreparesToAllAcceptors)
		}
		return
	}

	// IS PROMISE.
	dlog.AgentPrintfN(r.Id, "Promise recorded on instance %d at ballot %d.%d from Replica %d with value ballot %d.%d and whose commands %d",
		preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.AcceptorId, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd)
	if r.doPatientProposals {
		r.patientProposals.gotPromise(preply.Instance, preply.Req, preply.AcceptorId)
	}

	qrm := pbk.Qrms[pbk.PropCurBal]
	qrm.AddToQuorum(int(preply.AcceptorId))

	if !qrm.QuorumReached() {
		return
	}

	dlog.AgentPrintfN(r.Id, "Promise Quorum reached in instance %d at ballot %d.%d",
		preply.Instance, preply.Cur.Number, preply.Cur.PropID)
	r.tryInitaliseForPropose(preply.Instance, preply.Req)
}

func (r *Replica) bcastAcceptCheckVBalPReply(preply *stdpaxosproto.PrepareReply, pbk *proposalmanager.PBK) bool {
	if r.bcastAcceptLearning.IsLearnt(preply.Instance) {
		dlog.AgentPrintfN(r.Id, "Discarding Prepare Reply from Replica %d in instance %d at requested ballot %d.%d because it's already chosen", preply.AcceptorId, preply.Instance, preply.Req.Number, preply.Req.PropID)
		return true
	}

	valueChosen := r.bcastAcceptLearning.GotValue(preply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: preply.VBal}, preply.Command)
	addToAcceptanceQuorum(preply.Instance, preply.AcceptorId, preply.VBal, pbk, r.LearnerQuorumaliser, r.AcceptorQrmInfo)
	if !pbk.Qrms[lwcproto.ConfigBal{Config: -1, Ballot: preply.VBal}].QuorumReached() {
		return false
	}
	valueChosen = r.bcastAcceptLearning.GotChosenBallot(preply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: preply.VBal})
	if !valueChosen {
		panic("If we have a qrm and a value then there should be no problem")
		//dlog.AgentPrintfN(r.Id, "Cannot learn instance %d with ballot %d.%d with whose commands %d as we do not have an acceptance quorum", accept.Instance, accept.Number, accept.PropID, accept.WhoseCmd)
		//return false
	}
	//we've learnt a lower ballot to be chosen, so all proposals in higher ballots propose same value
	dlog.AgentPrintfN(r.Id, "Can learn instance %d with ballot %d.%d with whose commands %d as we have an acceptance quorum", preply.Instance, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd)
	r.bcastAcceptLearnInstance(preply.Instance, preply.VBal, preply.Command, preply.WhoseCmd)
	return true
}

func (r *Replica) tryInitaliseForPropose(inst int32, ballot stdpaxosproto.Ballot) {
	pbk := r.instanceSpace[inst]
	if !pbk.PropCurBal.Ballot.Equal(ballot) || pbk.Status != proposalmanager.PREPARING {
		return
	}

	qrm := pbk.Qrms[pbk.PropCurBal]
	if (!qrm.HasAcknowledged(int(r.Id)) && !r.writeAheadAcceptor) || (r.writeAheadAcceptor && !r.isLeased(inst, pbk.PropCurBal.Ballot)) {
		dlog.AgentPrintfN(r.Id, "Not safe to send accept requests for instance %d, need to wait until a lease or promise from our acceptor is received", inst)
		go func() {
			time.Sleep(5 * time.Millisecond)
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
	timeDelay := time.Duration(0)
	if r.doPatientProposals {
		timeDelay = r.patientProposals.getTimeToDelayProposal(inst, pbk.PropCurBal.Ballot)
	}
	if timeDelay <= 0 { //* time.Millisecond {
		dlog.AgentPrintfN(r.Id, "Decided not to sleep instance %d as quorum acquired after last expected response", inst) // or not doing patient
		r.tryPropose(inst, 0)
		return
	}
	dlog.AgentPrintfN(r.Id, "Sleeping instance %d at ballot %d.%d for %d microseconds before trying to propose value", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID, timeDelay.Microseconds())
	go func(bal stdpaxosproto.Ballot) {
		//r.nudge <- struct{}{}
		timer := time.NewTimer(timeDelay)
		timesUp := false
		var bat batching.ProposalBatch = nil
		for stopSelect := false; !stopSelect; {
			select {
			case b := <-r.Queueing.GetHead():
				r.Queueing.Dequeued(b, func() {
					bat = b
					dlog.AgentPrintfN(r.Id, "Received batch with UID %d to attempt to propose in instance %d which is currently sleeping", b.GetUID(), inst)
					stopSelect = true
				})
				break
			case <-timer.C:
				timesUp = true
				stopSelect = true
				break
				//case r.nudge <- struct{}{}:
				//	break
			}
		}

		if !timesUp {
			<-timer.C
		}

		r.proposableInstances <- ProposalInfo{
			inst:          inst,
			proposingBal:  bal,
			ProposalBatch: bat,
			PrevSleeps:    0,
		}
	}(pbk.PropCurBal.Ballot)
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
			r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Previous Value Proposed", 1)
			r.TimeseriesStats.Update("Times Previous Value Proposed", 1)
			r.ProposalStats.RecordPreviousValueProposed(stats.InstanceID{0, inst}, pbk.PropCurBal, len(pbk.Cmds))
		}
		dlog.AgentPrintfN(r.Id, "Proposing previous value from ballot %d.%d with whose command %d in instance %d at ballot %d.%d",
			pbk.ProposeValueBal.Number, pbk.ProposeValueBal.PropID, pbk.WhoseCmds, inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
	}

	if pbk.ProposeValueBal.IsZero() {
		if pbk.ClientProposals == nil {
			for foundValue := false; !foundValue; {
				select {
				case b := <-r.Queueing.GetHead():
					r.Queueing.Dequeued(b, func() {
						if !r.ProposeBatchOracle.ShouldPropose(b) {
							return
						}
						r.ProposedClientValuesManager.intendingToProposeBatch(pbk, inst, b)
						foundValue = true
					})
					break
				default:
					if !(r.shouldNoop(inst) && (priorAttempts > 0 || r.noopWait <= 0)) {
						r.BeginWaitingForClientProposals(inst, pbk)
						return
					}
					if r.doStats {
						r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Noop Proposed", 1)
						r.TimeseriesStats.Update("Times Noops Proposed", 1)
						r.ProposalStats.RecordNoopProposed(stats.InstanceID{0, inst}, pbk.PropCurBal)
					}
					setProposingValue(pbk, -1, pbk.PropCurBal.Ballot, state.NOOPP())
					dlog.AgentPrintfN(r.Id, "Proposing noop in instance %d at ballot %d.%d", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
					foundValue = true
					break
				}
			}
		}

		if pbk.ClientProposals != nil {
			if !r.ProposeBatchOracle.ShouldPropose(pbk.ClientProposals) {
				pbk.ClientProposals = nil
				r.tryPropose(inst, priorAttempts)
				return
			}
			setProposingValue(pbk, r.Id, pbk.PropCurBal.Ballot, pbk.ClientProposals.GetCmds())
			if r.doStats {
				r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Client Value Proposed", 1)
				r.ProposalStats.RecordClientValuesProposed(stats.InstanceID{0, inst}, pbk.PropCurBal, len(pbk.Cmds))
				r.TimeseriesStats.Update("Times Client Values Proposed", 1)
			}
			for _, obs := range r.batchProposedObservers {
				obs.ObserveProposed(pbk.ClientProposals)
			}
			dlog.AgentPrintfN(r.Id, "Proposing %d client value(s) from batch with UID %d in instance %d at ballot %d.%d \n", len(pbk.ClientProposals.GetCmds()), pbk.ClientProposals.GetUID(), inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
		}
	}

	if pbk.Cmds == nil {
		panic("there must be something to propose")
	}
	pbk.SetNowProposing()
	if r.bcastAcceptance {
		r.bcastAcceptLearning.GotValue(inst, pbk.PropCurBal, pbk.Cmds)
	}
	r.bcastAccept(inst)
}

func (r *Replica) BeginWaitingForClientProposals(inst int32, pbk *proposalmanager.PBK) {
	t := time.NewTimer(r.noopWait)
	go func(curBal stdpaxosproto.Ballot) {
		var bat batching.ProposalBatch = nil
		stopWaiting := false
		nudged := false
		for !stopWaiting {
			select {
			case b := <-r.Queueing.GetHead():
				r.Queueing.Dequeued(b, func() {
					bat = b
					dlog.AgentPrintfN(r.Id, "Received batch with UID %d to attempt to propose in instance %d", b.GetUID(), inst)
					stopWaiting = true
				})
				break
			case <-t.C:
				dlog.AgentPrintfN(r.Id, "Noop wait expired for instance %d", inst)
				stopWaiting = true
				break
			default:
				if !nudged {
					r.nudge <- struct{}{}
					nudged = true
				}
				break
			}
		}
		r.proposableInstances <- ProposalInfo{
			inst:          inst,
			proposingBal:  curBal,
			ProposalBatch: bat,
			PrevSleeps:    1,
		}
	}(pbk.PropCurBal.Ballot)
	dlog.AgentPrintfN(r.Id, "Decided there no need to propose a value in instance %d at ballot %d.%d, waiting %d ms before checking again", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID, r.noopWait.Milliseconds())
}

func (r *Replica) setProposal(inst int32, priorAttempts int, b batching.ProposalBatch, pbk *proposalmanager.PBK) bool {
	proposeF := func() {
		r.ProposedClientValuesManager.intendingToProposeBatch(pbk, inst, b)
		setProposingValue(pbk, r.Id, pbk.PropCurBal.Ballot, pbk.ClientProposals.GetCmds())

		if r.doStats {
			r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Client Value Proposed", 1)
			r.ProposalStats.RecordClientValuesProposed(stats.InstanceID{0, inst}, pbk.PropCurBal, len(pbk.Cmds))
			r.TimeseriesStats.Update("Times Client Values Proposed", 1)
		}
		dlog.AgentPrintfN(r.Id, "%d client value(s) from batch with UID %d received and proposed in recovered instance %d at ballot %d.%d \n", len(pbk.ClientProposals.GetCmds()), pbk.ClientProposals.GetUID(), inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
	}
	if err := r.Queueing.Dequeued(b, proposeF); err != nil {
		r.tryPropose(inst, priorAttempts)
		return false
	}
	return true
}

func (r *Replica) recheckInstanceToPropose(retry ProposalInfo) {
	pbk := r.instanceSpace[retry.inst]
	if pbk == nil {
		panic("????")
	}

	dlog.AgentPrintfN(r.Id, "Rechecking whether to propose in instance %d", retry.inst)
	if pbk.PropCurBal.Ballot.GreaterThan(retry.proposingBal) || pbk.Status != proposalmanager.READY_TO_PROPOSE {
		dlog.AgentPrintfN(r.Id, "Decided to not propose in instance %d as we are no longer on ballot %d.%d", retry.inst, retry.proposingBal.Number, retry.proposingBal.PropID)
		if retry.ProposalBatch != nil { // requeue value that cannot be proposed
			bat := retry.popBatch()
			r.Queueing.Dequeued(bat, func() { r.Queueing.Requeue(bat) })
		}
		return
	}

	if retry.ProposalBatch != nil {
		bat := retry.popBatch()
		r.Queueing.Dequeued(bat, func() {
			if !pbk.ProposeValueBal.IsZero() {
				r.Queueing.Requeue(bat)
				return
			}
			r.ProposedClientValuesManager.intendingToProposeBatch(pbk, retry.inst, bat)
		})
	}

	r.tryPropose(retry.inst, int(retry.PrevSleeps))
}

func (r *Replica) shouldNoop(inst int32) bool {
	if r.alwaysNoop {
		return true
	}
	for i := inst + 1; i < r.CrtInstanceOracle.GetCrtInstance(); i++ {
		if r.instanceSpace[i] == nil {
			continue
		}
		if r.instanceSpace[i].Status == proposalmanager.CLOSED {
			return true
		}
	}
	return false
}

func (r *Replica) checkAndHandleNewlyReceivedInstance(instance int32) {
	if instance < 0 {
		return
	}
	r.GlobalInstanceManager.LearnOfBallot(&r.instanceSpace, instance, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
}

func (r *Replica) handleAccept(accept *stdpaxosproto.Accept) {
	dlog.AgentPrintfN(r.Id, "Replica received Accept from Replica %d in instance %d at ballot %d.%d", accept.PropID, accept.Instance, accept.Number, accept.PropID)
	r.checkAndHandleNewlyReceivedInstance(accept.Instance)

	r.proposerWittnessAcceptedValue(accept.Instance, r.Id, accept.Ballot, accept.Command, accept.WhoseCmd)
	if r.GlobalInstanceManager.LearnOfBallot(&r.instanceSpace, accept.Instance, lwcproto.ConfigBal{Config: -1, Ballot: accept.Ballot}, stdpaxosproto.ACCEPTANCE) {
		pCurBal := r.instanceSpace[accept.Instance].PropCurBal
		if !pCurBal.IsZero() {
			dlog.AgentPrintfN(r.Id, "Accept Received from Replica %d in instance %d at ballot %d.%d preempted our ballot %d.%d",
				accept.PropID, accept.Instance, accept.Number, accept.PropID, pCurBal.Number, pCurBal.PropID)
		}
	}

	if r.bcastAcceptance {
		//if r.bcastAcceptLearning.IsLearnt(accept.Instance) {
		//}
		if r.checkAcceptLateForBcastAcceptLearner(accept) {
			//return
		}
		if accept.LeaderId == -2 {
			dlog.AgentPrintfN(r.Id, "Not passing Accept from Replica %d in instance %d at ballot %d.%d to acceptor as we are passive observers", accept.PropID, accept.Instance, accept.Number, accept.PropID)
			return // fixme very lazy
		}
	}

	if !r.AcceptorQrmInfo.IsInQrm(accept.Instance, r.Id) {
		return
	}

	if int32(accept.PropID) == r.Id {
		acceptorHandleAcceptLocal(r.Id, r.Acceptor, accept, r.AcceptResponsesRPC, r.acceptReplyChan, r.Replica, r.bcastAcceptance)
		return
	}
	acceptorHandleAccept(r.Id, r.Acceptor, accept, r.AcceptResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.bcastAcceptance, r.acceptReplyChan, r.sendPreparesToAllAcceptors)
}

func (r *Replica) checkAcceptLateForBcastAcceptLearner(accept *stdpaxosproto.Accept) bool {
	if r.bcastAcceptLearning.IsLearnt(accept.Instance) {
		return true
	}
	valueChosen := r.bcastAcceptLearning.GotValue(accept.Instance, lwcproto.ConfigBal{Config: -1, Ballot: accept.Ballot}, accept.Command)
	if !valueChosen {
		dlog.AgentPrintfN(r.Id, "Cannot learn instance %d with ballot %d.%d with whose commands %d as we do not have an acceptance quorum", accept.Instance, accept.Number, accept.PropID, accept.WhoseCmd)
		return false
	}
	//we've learnt a lower ballot to be chosen, so all proposals in higher ballots propose same value
	dlog.AgentPrintfN(r.Id, "Can learn instance %d with ballot %d.%d with whose commands %d as we have an acceptance quorum", accept.Instance, accept.Number, accept.PropID, accept.WhoseCmd)
	r.bcastAcceptLearnInstance(accept.Instance, accept.Ballot, accept.Command, accept.WhoseCmd)
	return true
}

func (r *Replica) handleAcceptReply(areply *stdpaxosproto.AcceptReply) {
	r.checkAndHandleNewlyReceivedInstance(areply.Instance)
	dlog.AgentPrintfN(r.Id, "Replica received Accept Reply from Replica %d in instance %d at requested ballot %d.%d and current ballot %d.%d", areply.AcceptorId, areply.Instance, areply.Req.Number, areply.Req.PropID, areply.Cur.Number, areply.Cur.PropID)
	pbk := r.instanceSpace[areply.Instance]
	if r.bcastAcceptance {
		if r.bcastAcceptLearning.IsLearnt(areply.Instance) {
			dlog.AgentPrintfN(r.Id, "Ignoring accept reply as instance %d already closed", areply.Instance)
			return
		}

		r.LearnOfBallot(&r.instanceSpace, areply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur}, stdpaxosproto.ACCEPTANCE)

		if areply.Req.Equal(areply.Cur) {
			r.bcastAcceptCheckIfChosenValue(areply, pbk)
			return
		}
	}
	// must be before closed check as we might have learnt an instance is closed before finding out about value
	if pbk.Status == proposalmanager.CLOSED {
		dlog.AgentPrintfN(r.Id, "Discarding Accept Reply from Replica %d in instance %d at requested ballot %d.%d because it's already chosen", areply.AcceptorId, areply.Instance, areply.Req.Number, areply.Req.PropID)
		//dlog.Printf("Already committed ")
		return
	}

	if pbk.PropCurBal.Ballot.GreaterThan(areply.Req) {
		dlog.AgentPrintfN(r.Id, "Accept Reply for instance %d with current ballot %d.%d and requested ballot %d.%d in late, because we are now at ballot %d.%d",
			areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.Req.Number, areply.Req.PropID, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
		return
	}

	if areply.Cur.GreaterThan(areply.Req) {
		pCurBal := r.instanceSpace[areply.Instance].PropCurBal
		dlog.AgentPrintfN(r.Id, "Accept Reply received from Replica %d in instance %d with current ballot %d.%d preempted our ballot %d.%d",
			areply.AcceptorId, areply.Instance, areply.Cur.Number, areply.Cur.PropID, pCurBal.Number, pCurBal.PropID)
		r.GlobalInstanceManager.LearnOfBallot(&r.instanceSpace, areply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur}, areply.CurPhase)
		r.ProposedClientValuesManager.learnOfBallot(pbk, areply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur})
		return
	}

	if areply.Req.GreaterThan(pbk.PropCurBal.Ballot) {
		panic("got a future acceptance??")
	}

	if areply.Req.GreaterThan(areply.Cur) {
		panic("Acceptor didn't accept request")
	}

	dlog.AgentPrintfN(r.Id, "Acceptance recorded on our proposal instance %d at ballot %d.%d from Replica %d with whose commands %d",
		areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId, areply.WhoseCmd)
	r.learnerHandleAcceptedValue(areply.Instance, areply.AcceptorId, areply.Cur, pbk.Cmds, areply.WhoseCmd)
}

func (r *Replica) bcastAcceptCheckIfChosenValue(areply *stdpaxosproto.AcceptReply, pbk *proposalmanager.PBK) {
	dlog.AgentPrintfN(r.Id, "Acceptance received for proposal in instance %d with ballot %d.%d", areply.Instance, areply.Cur.Number, areply.Cur.PropID)
	if r.bcastAcceptLearning.IsLearnt(areply.Instance) {
		dlog.AgentPrintfN(r.Id, "Ignoring acceptance in instance %d at ballot %d.%d as it is already learnt", areply.Instance, areply.Cur.Number, areply.Cur.PropID)
		return
	}
	addToAcceptanceQuorum(areply.Instance, areply.AcceptorId, areply.Cur, pbk, r.LearnerQuorumaliser, r.AcceptorQrmInfo)
	r.GlobalInstanceManager.LearnOfBallotAccepted(&r.instanceSpace, areply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur}, areply.WhoseCmd)
	if !pbk.Qrms[lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur}].QuorumReached() {
		return
	}
	gotChosenValue := r.bcastAcceptLearning.GotChosenBallot(areply.Instance, lwcproto.ConfigBal{Config: -1, Ballot: areply.Cur})
	dlog.AgentPrintfN(r.Id, "Acceptance quorum reached on proposal in instance %d with ballot %d.%d with whose commands %d", areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.WhoseCmd)
	if !gotChosenValue {
		dlog.AgentPrintfN(r.Id, "Cannot learn instance %d with ballot %d.%d with whose commands %d as we do not have a value", areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.WhoseCmd)
		return
	}
	dlog.AgentPrintfN(r.Id, "Can learn instance %d with ballot %d.%d with whose commands %d as we have a value", areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.WhoseCmd)
	r.bcastAcceptLearning.InstanceClosed(areply.Instance)
	val := r.bcastAcceptLearning.GetChosenValue(areply.Instance)
	r.Acceptor.RecvCommitRemote(&stdpaxosproto.Commit{
		LeaderId:   int32(areply.Cur.PropID),
		Instance:   areply.Instance,
		Ballot:     areply.Cur,
		WhoseCmd:   areply.WhoseCmd,
		MoreToCome: 0,
		Command:    val,
	})
	r.proposerCloseCommit(areply.Instance, areply.Cur, val, areply.WhoseCmd)
	r.bcastCommitToAll(areply.Instance, areply.Cur, val)
	return
}

func (r *Replica) proposerCloseCommit(inst int32, chosenAt stdpaxosproto.Ballot, chosenVal []*state.Command, whoseCmd int32) {
	if r.bcastAcceptance && !r.bcastAcceptLearning.IsLearnt(inst) {
		panic("should be closed by now")
	}
	r.GlobalInstanceManager.LearnBallotChosen(&r.instanceSpace, inst, lwcproto.ConfigBal{Config: -1, Ballot: chosenAt}) // todo add client value chosen log
	if r.bcastAcceptance {
		r.bcastAcceptLearning.InstanceClosed(inst)
	}
	pbk := r.instanceSpace[inst]

	if !pbk.PropCurBal.IsZero() && r.doPatientProposals {
		r.patientProposals.stoppingProposals(inst, pbk.PropCurBal.Ballot)
	}

	r.ProposedClientValuesManager.valueChosen(pbk, inst, whoseCmd, chosenVal)

	setProposingValue(pbk, whoseCmd, chosenAt, chosenVal)

	if whoseCmd == r.Id && pbk.ClientProposals != nil {
		dlog.AgentPrintfN(r.Id, "Learn instance %d with whose commands %d (UID %d)", inst, whoseCmd, pbk.ClientProposals.GetUID())
		for _, l := range r.batchLearners {
			l.Learn(pbk.ClientProposals)
		}
	}

	if whoseCmd == -1 {
		for _, l := range r.noopLearners {
			l.LearnNoop(inst, int32(chosenAt.PropID))
		}
	}

	if pbk.ClientProposals != nil && !r.Dreply {
		r.replyToNondurablyClients(pbk)
	}

	if r.Exec {
		r.executeCmds()
	}
}

func (r *Replica) replyToNondurablyClients(pbk *proposalmanager.PBK) {
	for i := 0; i < len(pbk.Cmds); i++ {
		r.replyToClientOfCmd(pbk, i, state.NIL())
	}
}

func (r *Replica) replyToClientOfCmd(pbk *proposalmanager.PBK, i int, value state.Value) {
	proposals := pbk.ClientProposals.GetProposals()
	propreply := &genericsmrproto.ProposeReplyTS{
		1,
		proposals[i].CommandId,
		value,
		proposals[i].Timestamp}
	r.ReplyProposeTS(propreply, proposals[i].Reply, proposals[i].Mutex)
}

func (r *Replica) executeCmds() {
	oldExecutedUpTo := r.executedUpTo
	for i := r.executedUpTo + 1; i <= r.CrtInstanceOracle.GetCrtInstance(); i++ {
		returnInst := r.instanceSpace[i]
		if returnInst != nil && (returnInst.Status == proposalmanager.CLOSED && !r.bcastAcceptance) || (r.bcastAcceptance && r.bcastAcceptLearning.IsLearnt(i)) { //&& returnInst.abk.Cmds != nil {
			if returnInst.ClientProposals == nil {
				dlog.AgentPrintfN(r.Id, "Executing instance %d with whose commands %d", i, returnInst.WhoseCmds)
			} else {
				dlog.AgentPrintfN(r.Id, "Executing instance %d with whose commands %d (UID %d)", i, returnInst.WhoseCmds, returnInst.ClientProposals.GetUID())
			}
			if r.doStats {
				r.InstanceStats.RecordExecuted(stats.InstanceID{0, i}, time.Now())
				r.TimeseriesStats.Update("Instances Executed", 1)
				r.InstanceStats.OutputRecord(stats.InstanceID{0, i})
			}
			length := len(returnInst.Cmds)
			for j := 0; j < length; j++ {
				dlog.Printf("Executing " + returnInst.Cmds[j].String())
				if r.Dreply && returnInst != nil && returnInst.ClientProposals != nil {
					val := returnInst.Cmds[j].Execute(r.State)
					r.replyToClientOfCmd(returnInst, j, val)
					dlog.Printf("Returning executed client value")
				} else if returnInst.Cmds[j].Op == state.PUT {
					returnInst.Cmds[j].Execute(r.State)
				}
			}
			r.executedUpTo += 1
		} else {
			if r.executedUpTo > oldExecutedUpTo {
				r.recordExecutedUpTo()
			}
			break
		}
	}
}

func (r *Replica) handleCommit(commit *stdpaxosproto.Commit) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	if r.bcastAcceptance {
		if r.bcastAcceptLearning.IsLearnt(commit.Instance) {
			dlog.AgentPrintfN(r.Id, "Ignoring commit for instance %d as it is already learnt", commit.Instance)
			return
		}
		r.bcastAcceptLearnInstance(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
		return
	}
	r.Acceptor.RecvCommitRemote(commit)
	r.proposerCloseCommit(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
}

// if commited at one ballot, only need one ack from a higher ballot to be chosen
func (r *Replica) handleCommitShort(commit *stdpaxosproto.CommitShort) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	dlog.AgentPrintfN(r.Id, "Replica received Commit Short from Replica %d in instance %d at ballot %d.%d with whose commands %d", commit.PropID, commit.Instance, commit.Ballot.Number, commit.Ballot.PropID, commit.WhoseCmd)
	pbk := r.instanceSpace[commit.Instance]
	if r.bcastAcceptance {
		if r.bcastAcceptLearning.IsLearnt(commit.Instance) {
			dlog.AgentPrintfN(r.Id, "Ignoring commit short for instance %d as it is already learnt", commit.Instance)
			return
		}
		chosenValGot := r.bcastAcceptLearning.GotChosenBallot(commit.Instance, lwcproto.ConfigBal{Config: -1, Ballot: commit.Ballot})
		if !chosenValGot {
			dlog.AgentPrintfN(r.Id, "Cannot learn instance %d at ballot %d.%d with whose commands %d as we do not have a value", commit.Instance, commit.Ballot.Number, commit.Ballot.PropID, commit.WhoseCmd)
			r.GlobalInstanceManager.LearnBallotChosen(&r.instanceSpace, commit.Instance, lwcproto.ConfigBal{Config: -1, Ballot: commit.Ballot}) // todo add client value chosen log
			if !pbk.PropCurBal.IsZero() && r.doPatientProposals {
				r.patientProposals.stoppingProposals(commit.Instance, pbk.PropCurBal.Ballot)
			}
			r.ProposedClientValuesManager.valueChosen(pbk, commit.Instance, commit.WhoseCmd, nil)
			return
		}
		dlog.AgentPrintfN(r.Id, "Can learn instance %d at ballot %d.%d with whose commands %d as we have a value", commit.Instance, commit.Ballot.Number, commit.Ballot.PropID, commit.WhoseCmd)
		val := r.bcastAcceptLearning.GetChosenValue(commit.Instance)
		r.bcastAcceptLearnInstance(commit.Instance, commit.Ballot, val, commit.WhoseCmd)
		return
	}

	r.Acceptor.RecvCommitShortRemote(commit)
	if pbk.Cmds == nil {
		panic("We don't have any record of the value to be committed")
	}
	r.proposerCloseCommit(commit.Instance, commit.Ballot, pbk.Cmds, commit.WhoseCmd)
}

func (r *Replica) bcastAcceptLearnInstance(inst int32, ballot stdpaxosproto.Ballot, val []*state.Command, whosecmd int32) {
	r.bcastAcceptLearning.InstanceClosed(inst)
	msg := &stdpaxosproto.Commit{
		LeaderId:   whosecmd,
		Instance:   inst,
		Ballot:     ballot,
		WhoseCmd:   whosecmd,
		MoreToCome: 0,
		Command:    val,
	}
	r.Acceptor.RecvCommitRemote(msg)
	r.proposerCloseCommit(inst, ballot, val, whosecmd)
}

func (r *Replica) handleState(state *proposerstate.State) {
	r.GlobalInstanceManager.LearnOfBallot(&r.instanceSpace, state.CurrentInstance, lwcproto.ConfigBal{-2, stdpaxosproto.Ballot{-2, int16(state.ProposerID)}}, stdpaxosproto.PROMISE)
	//r.checkAndHandleNewlyReceivedInstance(state.CurrentInstance)
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

// find out that a ballot is chosen, find out the value of a ballot equal to or greater than
type bcastAcceptLearning struct {
	vals           map[int32]map[lwcproto.ConfigBal][]*state.Command
	chosen         map[int32]map[lwcproto.ConfigBal]struct{}
	chosenVal      map[int32][]*state.Command
	instanceClosed map[int32]struct{}
}

func (b *bcastAcceptLearning) IsLearnt(inst int32) bool {
	_, e := b.instanceClosed[inst]
	return e
}

// returns true when value is chosen value
func (b *bcastAcceptLearning) GotValue(inst int32, ballot lwcproto.ConfigBal, vals []*state.Command) bool {
	if _, e := b.vals[inst]; !e {
		b.vals[inst] = make(map[lwcproto.ConfigBal][]*state.Command)
	}
	b.vals[inst][ballot] = vals

	if _, e := b.chosen[inst]; !e {
		return false
	}

	chosenBals := b.chosen[inst]
	for cB, _ := range chosenBals {
		if ballot.GreaterThan(cB) || ballot.Equal(cB) {
			b.chosenVal[inst] = vals
			return true
		}
	}

	return false
}

func (b *bcastAcceptLearning) GotChosenBallot(inst int32, ballot lwcproto.ConfigBal) bool {
	if _, e := b.chosen[inst]; !e {
		b.chosen[inst] = make(map[lwcproto.ConfigBal]struct{})
	}
	b.chosen[inst][ballot] = struct{}{}

	if _, e := b.vals[inst]; !e {
		return false
	}

	vBals := b.vals[inst]
	for vBal, v := range vBals {
		if vBal.GreaterThan(ballot) || vBal.Equal(ballot) {
			b.chosenVal[inst] = v
			return true
		}
	}
	return false
}

func (b *bcastAcceptLearning) GetChosenValue(inst int32) []*state.Command {
	if _, e := b.chosenVal[inst]; !e {
		panic("cannot return chosen value as it is unknown right now")
	}
	return b.chosenVal[inst]
}

func (b *bcastAcceptLearning) InstanceClosed(instance int32) {
	b.instanceClosed[instance] = struct{}{}
}
