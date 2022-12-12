package twophase

import (
	"epaxos/acceptor"
	"epaxos/batching"
	"epaxos/fastrpc"
	"epaxos/genericsmr"
	"epaxos/instanceagentmapper"
	"epaxos/proposerstate"
	"epaxos/quorumsystem"
	"epaxos/stats"
	"epaxos/stdpaxosproto"
	"epaxos/twophase/aceptormessagefilter"
	balloter2 "epaxos/twophase/balloter"
	_const "epaxos/twophase/const"
	"epaxos/twophase/exec"
	"epaxos/twophase/learner"
	"epaxos/twophase/proposalmanager"
	"fmt"
	"sync"
	"time"
)

func NewBaselineTwoPhaseReplica(id int, replica *genericsmr.Replica, durable bool, batchWait int, storageLoc string,
	maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool,
	factor float64, whoCrash int32, whenCrash time.Duration, howlongCrash time.Duration, emulatedSS bool,
	emulatedWriteTime time.Duration, catchupBatchSize int32, timeout time.Duration, group1Size int, flushCommit bool,
	softFac bool, doStats bool, statsParentLoc string, commitCatchup bool, deadTime int32, batchSize int,
	constBackoff bool, requeueOnPreempt bool, tsStatsFilename string, instStatsFilename string,
	propsStatsFilename string, sendProposerState bool, proactivePreemptOnNewB bool, batchingAcceptor bool,
	maxAccBatchWait time.Duration, sendPreparesToAllAcceptors bool, minimalProposers bool, timeBasedBallots bool,
	mappedProposers bool, dynamicMappedProposers bool, bcastAcceptance bool, mappedProposersNum int32,
	instsToOpenPerBatch int32, doEager bool, sendFastestQrm bool, useGridQrms bool, minimalAcceptors bool,
	minimalAcceptorNegatives bool, prewriteAcceptor bool, doPatientProposals bool, sendFastestAccQrm bool, forwardInduction bool,
	q1 bool, bcastCommit bool, nopreempt bool, pam bool, pamloc string, syncaceptor bool, disklessNOOP bool, forceDisklessNOOP bool, eagerByExec bool, bcastAcceptDisklessNoop bool, eagerByExecFac float32) *Replica {

	r := &Replica{
		bcastAcceptDisklessNOOP:      bcastAcceptDisklessNoop,
		disklessNOOPPromises:         make(map[int32]map[stdpaxosproto.Ballot]map[int32]struct{}),
		disklessNOOPPromisesAwaiting: make(map[int32]chan struct{}),
		forceDisklessNOOP:            forceDisklessNOOP,
		disklessNOOP:                 disklessNOOP,
		syncAcceptor:                 syncaceptor,
		nopreempt:                    nopreempt,
		bcastCommit:                  bcastCommit,
		nudge:                        make(chan chan batching.ProposalBatch, maxOpenInstances*int32(replica.N)),
		sendFastestQrm:               sendFastestAccQrm,
		Replica:                      replica,
		proposedBatcheNumber:         make(map[int32]int32),
		bcastAcceptance:              bcastAcceptance,
		stateChan:                    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		configChan:                   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareChan:                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptChan:                   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitChan:                   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitShortChan:              make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareReplyChan:             make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptReplyChan:              make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		tryInitPropose:               make(chan proposalmanager.RetryInfo, 100),
		prepareRPC:                   0,
		acceptRPC:                    0,
		commitRPC:                    0,
		commitShortRPC:               0,
		prepareReplyRPC:              0,
		acceptReplyRPC:               0,
		instanceSpace:                make([]*proposalmanager.PBK, _const.ISpaceLen),
		Shutdown:                     false,
		counter:                      0,
		flush:                        true,
		maxBatchWait:                 batchWait,
		crtOpenedInstances:           make([]int32, maxOpenInstances),
		proposableInstances:          make(chan struct{}, MAXPROPOSABLEINST*replica.N),
		noopWaitUs:                   noopwait,
		retryInstance:                make(chan proposalmanager.RetryInfo, maxOpenInstances*10000),
		alwaysNoop:                   alwaysNoop,
		fastLearn:                    false,
		whoCrash:                     whoCrash,
		whenCrash:                    whenCrash,
		howLongCrash:                 howlongCrash,
		timeoutMsgs:                  make(chan TimeoutInfo, 5000),
		timeout:                      timeout,
		catchupBatchSize:             catchupBatchSize,
		lastSettleBatchInst:          -1,
		catchingUp:                   false,
		flushCommit:                  flushCommit,
		commitCatchUp:                commitCatchup,
		maxBatchSize:                 batchSize,
		doStats:                      doStats,
		sendProposerState:            sendProposerState,
		noopWait:                     time.Duration(noopwait) * time.Microsecond,
		proactivelyPrepareOnPreempt:  proactivePreemptOnNewB,
		expectedBatchedRequests:      200,
		sendPreparesToAllAcceptors:   sendPreparesToAllAcceptors,
		startInstanceSig:             make(chan struct{}, 100),
		doEager:                      doEager,
		instanceProposeValueTimeout: &InstanceProposeValueTimeout{
			ProposedClientValuesManager: ProposedClientValuesManagerNew(int32(id), nil, false),
			nextBatch: curBatch{
				maxLength:  0,
				cmds:       nil,
				clientVals: nil,
				uid:        0,
			},
			sleepingInsts:              make(map[int32]time.Time),
			constructedAwaitingBatches: make([]batching.ProposalBatch, 100),
			chosenBatches:              make(map[int32]struct{}),
		},
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
			Acceptors: pids,
			F:         int32(r.F),
			N:         int32(r.N),
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

	if r.UDP {
		r.prepareRPC = r.RegisterUDPRPC("prepare request", new(stdpaxosproto.Prepare), r.prepareChan)
		r.acceptRPC = r.RegisterUDPRPC("accept request", new(stdpaxosproto.Accept), r.acceptChan)
		r.commitRPC = r.RegisterUDPRPC("Commit", new(stdpaxosproto.Commit), r.commitChan)
		r.commitShortRPC = r.RegisterUDPRPC("Commit short", new(stdpaxosproto.CommitShort), r.commitShortChan)
		r.prepareReplyRPC = r.RegisterUDPRPC("prepare reply", new(stdpaxosproto.PrepareReply), r.prepareReplyChan)
		r.acceptReplyRPC = r.RegisterUDPRPC("accept reply", new(stdpaxosproto.AcceptReply), r.acceptReplyChan)
		//r.stateChanRPC = r.RegisterUDPRPC(new(proposerstate.State), r.stateChan)
	} else {
		r.prepareRPC = r.RegisterRPC(new(stdpaxosproto.Prepare), r.prepareChan)
		r.acceptRPC = r.RegisterRPC(new(stdpaxosproto.Accept), r.acceptChan)
		r.commitRPC = r.RegisterRPC(new(stdpaxosproto.Commit), r.commitChan)
		r.commitShortRPC = r.RegisterRPC(new(stdpaxosproto.CommitShort), r.commitShortChan)
		r.prepareReplyRPC = r.RegisterRPC(new(stdpaxosproto.PrepareReply), r.prepareReplyChan)
		r.acceptReplyRPC = r.RegisterRPC(new(stdpaxosproto.AcceptReply), r.acceptReplyChan)
		r.stateChanRPC = r.RegisterRPC(new(proposerstate.State), r.stateChan)
	}

	r.PrepareResponsesRPC = PrepareResponsesRPC{
		PrepareReply: r.prepareReplyRPC,
		Commit:       r.commitRPC,
	}
	r.AcceptResponsesRPC = AcceptResponsesRPC{
		AcceptReply: r.acceptReplyRPC,
		Commit:      r.commitRPC,
	}

	//if leaderbased {
	//
	//}

	if prewriteAcceptor {
		r.iWriteAhead = 10000000
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
				r.prepareReplyRPC, r.acceptReplyRPC, r.commitRPC, r.commitShortRPC, commitCatchup, r.promiseLeases, r.iWriteAhead, proactivePreemptOnNewB, r.disklessNOOP)
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

	r.clientBatcher = GetBatcher(int32(id), 500)

	// Quorum system
	var qrm quorumsystem.SynodQuorumSystemConstructor
	qrm = &quorumsystem.SynodCountingQuorumSystemConstructor{
		F:       r.F,
		Thrifty: r.Thrifty,
		Replica: r.Replica,
		//BroadcastFastest: sendFastestQrm,
		AllAids: pids,
		//SendAllAcceptors: false,
	}
	if useGridQrms {
		qrm = &quorumsystem.SynodGridQuorumSystemConstructor{
			F:       r.F,
			Replica: r.Replica,
			Thrifty: r.Thrifty,
			//BroadcastFastest: sendFastestQrm,
		}
	}

	var instancequormaliser proposalmanager.InstanceQuormaliser
	instancequormaliser = &proposalmanager.Standard{
		SynodQuorumSystemConstructor: qrm,
		Aids:                         pids,
		MyID:                         r.Id,
	}

	// LEARNER GROUPS SET UP
	var aqc learner.AQConstructor
	stdaqc := learner.GetStandardGroupAQConstructorr(pids, qrm.(quorumsystem.SynodQuorumSystemConstructor), r.Id)
	aqc = &stdaqc

	// Random but determininstic proposer acceptor mapping -- 2f+1
	if minimalAcceptors {
		laqc := learner.GetMinimalGroupAQConstructorr(int32(r.N), int32(r.F), pids, qrm.(quorumsystem.AcceptanceQuorumsConstructor), r.Id)
		aqc = &laqc
		var mapper instanceagentmapper.InstanceAgentMapper
		if useGridQrms {
			mapper = &instanceagentmapper.InstanceAcceptorGridMapper{
				Acceptors: pids,
				F:         int32(r.F),
				N:         int32(r.N),
			}
		} else {
			mapper = &instanceagentmapper.InstanceAcceptorSetMapper{
				Acceptors: pids,
				F:         int32(r.F),
				N:         int32(r.N),
			}
		}

		instancequormaliser = &proposalmanager.Minimal{
			AcceptorMapper:               mapper,
			SynodQuorumSystemConstructor: qrm,
			MapperCache:                  make(map[int32][]int32),
			MyID:                         r.Id,
		}
	}

	if pam {
		pamapping := instanceagentmapper.ReadFromFile(pamloc)
		amapping := instanceagentmapper.GetAMap(pamapping)
		//pmapping := instanceagentmapper.GetPMap(pamapping)
		learner.GetStaticDefinedAQConstructor(amapping, qrm.(quorumsystem.SynodQuorumSystemConstructor))
		instancequormaliser = &proposalmanager.StaticMapped{
			AcceptorMapper:               instanceagentmapper.FixedInstanceAgentMapping{Groups: amapping},
			SynodQuorumSystemConstructor: qrm,
			MyID:                         r.Id,
		}

		if doPatientProposals {
			panic("option not implemented")
		}
	}

	l := learner.GetBcastAcceptLearner(aqc)
	r.Learner = &l
	r.Executor = exec.GetNewExecutor(int32(id), replica, r.StableStore, r.Dreply)

	r.ProposerInstanceQuorumaliser = instancequormaliser
	r.AcceptorQrmInfo = instancequormaliser
	r.LearnerQuorumaliser = instancequormaliser

	if group1Size <= r.N-r.F {
		r.group1Size = r.N - r.F
	} else {
		r.group1Size = group1Size
	}

	r.instanceProposeValueTimeout.ProposedClientValuesManager = ProposedClientValuesManagerNew(r.Id, r.TimeseriesStats, r.doStats)

	balloter := &balloter2.Balloter{r.Id, int32(r.N), 10000, time.Time{}, timeBasedBallots}
	r.balloter = balloter

	backoffManager := proposalmanager.BackoffManagerNew(minBackoff, maxInitBackoff, maxBackoff, r.retryInstance, factor, softFac, constBackoff, r.Id)
	var instanceManager proposalmanager.SingleInstanceManager = proposalmanager.SimpleInstanceManagerNew(r.Id, backoffManager, balloter, doStats,
		r.ProposerInstanceQuorumaliser, r.TimeseriesStats, r.ProposalStats, r.InstanceStats)

	var awaitingGroup ProposerGroupGetter = SimpleProposersAwaitingGroupGetterNew(pids)
	var minimalGroupGetter *MinimalProposersAwaitingGroup
	if minimalProposers {
		minimalShouldMaker := proposalmanager.MinimalProposersShouldMakerNew(int16(r.Id), r.F)
		instanceManager = proposalmanager.MinimalProposersInstanceManagerNew(instanceManager.(*proposalmanager.SimpleInstanceManager), minimalShouldMaker)
		minimalGroupGetter = MinimalProposersAwaitingGroupNew(awaitingGroup.(*SimpleProposersAwaitingGroup), minimalShouldMaker, int32(r.F))
		awaitingGroup = minimalGroupGetter
	}

	// SET UP SIGNAL
	sig := proposalmanager.SimpleSigNew(r.startInstanceSig, r.Id)
	var openInstSig proposalmanager.OpenInstSignal = sig
	var ballotInstSig proposalmanager.BallotOpenInstanceSignal = sig
	if doEager || eagerByExec {
		eSig := proposalmanager.EagerSigNew(openInstSig.(*proposalmanager.SimpleSig), maxOpenInstances)
		openInstSig = eSig
		ballotInstSig = eSig
	}

	if eagerByExec {
		eESig := proposalmanager.EagerExecUpToSigNew(openInstSig.(*proposalmanager.EagerSig), float32(r.N), eagerByExecFac)
		openInstSig = eESig
		ballotInstSig = eESig
		r.execSig = eESig
	}

	simpleGlobalManager := proposalmanager.SimpleProposalManagerNew(r.Id, openInstSig, ballotInstSig, instanceManager, backoffManager)
	var globalManager proposalmanager.Proposer = simpleGlobalManager

	if instsToOpenPerBatch < 1 {
		panic("Will not open any instances")
	}
	if instsToOpenPerBatch > 1 && (mappedProposers || dynamicMappedProposers || doEager) {
		panic("incompatible options")
	}

	r.noopLearners = []proposalmanager.NoopLearner{}
	// PROPOSER QUORUMS
	if mappedProposers || dynamicMappedProposers || pam {
		var agentMapper instanceagentmapper.InstanceAgentMapper
		agentMapper = &instanceagentmapper.DetRandInstanceSetMapper{
			Ids: pids,
			G:   mappedProposersNum,
			N:   int32(r.N),
		}
		if pam { // PAM get from file the proposer mappings
			pamap := instanceagentmapper.ReadFromFile(pamloc)
			pmap := instanceagentmapper.GetPMap(pamap)
			agentMapper = &instanceagentmapper.FixedInstanceAgentMapping{Groups: pmap}
		}

		globalManager = proposalmanager.MappedProposersProposalManagerNew(r.startInstanceSig, simpleGlobalManager, instanceManager, agentMapper)
		mappedGroupGetter := MappedProposersAwaitingGroupNew(agentMapper)
		if dynamicMappedProposers {
			dAgentMapper := &proposalmanager.DynamicInstanceSetMapper{
				DetRandInstanceSetMapper: *agentMapper.(*instanceagentmapper.DetRandInstanceSetMapper),
			}
			globalManager = proposalmanager.DynamicMappedProposerManagerNew(r.startInstanceSig, simpleGlobalManager, instanceManager, dAgentMapper, int32(r.N), int32(r.F))
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
	r.Proposer = globalManager
	r.Durable = durable

	//var acceptSelfSender AcceptSelfSender = &AsyncAcceptSender{
	//	acceptChan: r.acceptChan,
	//}
	//var prepareSelfSender PrepareSelfSender =  &AsyncPrepareSender{
	//	prepareChan: r.prepareChan,
	//}
	//
	//if syncaceptor {
	//	prepareSelfSender = &SyncPrepareSender{
	//		id: r.Id,
	//		r:  r,
	//	}
	//}

	//var sendQrmSize SendQrmSize = &NonThrifty{
	//	f: r.F,
	//}
	//if r.Thrifty {
	//	sendQrmSize = &Thrifty{r.F}
	//}
	//if r.UDP {
	//	if !r.sendPreparesToAllAcceptors {
	//		panic("Not implemented yet")
	//	}
	//	reliableSender := &UnreliableUDPListSender{Replica: r.Replica}
	//	r.PrepareBroadcaster = NewPrepareAllFromWriteAheadReplica(int32(r.N), r.Id, r.prepareRPC, r.AcceptorQrmInfo, prepareSelfSender, reliableSender)
	//	unreliableSender := &ReliableUDPListSender{Replica: r.Replica}
	//	r.ValueBroadcaster = NewBcastSlowLearning(r.acceptRPC, r.commitShortRPC, r.commitRPC, r.Id, int32(r.N), pids, sendQrmSize, unreliableSender, acceptSelfSender, r)
	//}
	//r.doPatientProposals = doPatientProposals
	//if r.doPatientProposals {
	//	r.patientProposals = patientProposals{
	//		myId:                r.Id,
	//		promisesRequestedAt: make(map[int32]map[stdpaxosproto.Ballot]time.Time),
	//		pidsPropRecv:        make(map[int32]map[int32]struct{}),
	//		doPatient:           true,
	//		Ewma:                make([]float64, r.N),
	//ProposerGroupGetter: awaitingGroup,
	//closed:              make(map[int32]struct{}),
	//}
	//}
	go r.run()
	return r
}
