package twophase

import (
	"epaxos/acceptor"
	"epaxos/batching"
	"epaxos/fastrpc"
	"epaxos/genericsmr"
	"epaxos/instanceagentmapper"
	"epaxos/proposerstate"
	"epaxos/quorumsystem"
	"epaxos/stdpaxosproto"
	"epaxos/twophase/balloter"
	"epaxos/twophase/learner"
	"epaxos/twophase/proposer"
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
	minimalAcceptorNegatives bool, prewriteAcceptor bool, doPatientProposals bool, sendFastestAccQrm bool,
	forwardInduction bool, doChosenFWI bool, doValueFWI bool, doLatePropsFWI bool,
	forwardingInstances int32, q1 bool, bcastCommit bool, nopreempt bool, pam bool, pamloc string, syncaceptor bool,
	disklessNOOP bool, forceDisklessNOOP bool, eagerByExec bool, bcastAcceptDisklessNoop bool, eagerByExecFac float32,
	inductiveConfs bool, proposeToCatchUp bool, openInstToCatchUp bool, signalIfNoInstStarted bool,
	limPipelineOnPreempt bool, eagerMaxOutstanding bool, asyncResp bool, asyncBcast bool) *Replica {

	r := &Replica{
		asyncResp:                    asyncResp,
		asyncBcast:                   asyncBcast,
		bcastAcceptDisklessNOOP:      bcastAcceptDisklessNoop,
		disklessNOOPPromises:         make(map[int32]map[stdpaxosproto.Ballot]map[int32]struct{}),
		disklessNOOPPromisesAwaiting: make(map[int32]chan struct{}),
		forceDisklessNOOP:            forceDisklessNOOP,
		disklessNOOP:                 disklessNOOP,
		syncAcceptor:                 syncaceptor,
		nopreempt:                    nopreempt,
		//bcastCommit:                  bcastCommit,
		Replica:          replica,
		bcastAcceptance:  bcastAcceptance,
		stateChan:        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		configChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitShortChan:  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptReplyChan:  make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		tryInitPropose:   make(chan proposer.RetryInfo, 100),
		prepareRPC:       0,
		acceptRPC:        0,
		commitRPC:        0,
		commitShortRPC:   0,
		prepareReplyRPC:  0,
		acceptReplyRPC:   0,
		//instanceSpace:                make([]*proposer.PBK, _const.ISpaceLen),
		Shutdown:                    false,
		counter:                     0,
		flush:                       true,
		maxBatchWait:                batchWait,
		proposableInstances:         make(chan struct{}, 1000*replica.N),
		noopWaitUs:                  noopwait,
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
		noopWait:                    time.Duration(noopwait) * time.Microsecond,
		proactivelyPrepareOnPreempt: proactivePreemptOnNewB,
		instanceProposeValueTimeout: &InstanceProposeValueTimeout{
			//ProposedClientValuesManager: proposer.ProposedClientValuesManagerNew(int32(id)), //todo move to proposer
			sleepingInsts:              make(map[int32]time.Time),
			constructedAwaitingBatches: make([]batching.ProposalBatch, 100),
		},
	}

	pids := make([]int32, r.N)
	ids := make([]int, r.N)
	for i := range pids {
		pids[i] = int32(i)
		ids[i] = i
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

	//r.ClientBatcher = proposer.GetBatcher(int32(id), batchSize)

	// Quorum system
	var qrm quorumsystem.SynodQuorumSystemConstructor
	qrm = &quorumsystem.SynodCountingQuorumSystemConstructor{
		F:       r.F,
		Thrifty: r.Thrifty,
		Replica: r.Replica,
		AllAids: pids,
	}
	if useGridQrms {
		qrm = &quorumsystem.SynodGridQuorumSystemConstructor{
			F:       r.F,
			Replica: r.Replica,
			Thrifty: r.Thrifty,
		}
	}

	var instancequormaliser proposer.InstanceQuormaliser
	instancequormaliser = &proposer.Standard{
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

		instancequormaliser = &proposer.Minimal{
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
		instancequormaliser = &proposer.StaticMapped{
			AcceptorMapper:               &instanceagentmapper.FixedInstanceAgentMapping{Groups: amapping},
			SynodQuorumSystemConstructor: qrm,
			MyID:                         r.Id,
		}
		if doPatientProposals {
			panic("option not implemented")
		}
	}

	l := learner.GetBcastAcceptLearner(aqc)
	r.Learner = &l

	r.AcceptorQrmInfo = instancequormaliser
	r.LearnerQuorumaliser = instancequormaliser

	if group1Size <= r.N-r.F {
		r.group1Size = r.N - r.F
	} else {
		r.group1Size = group1Size
	}

	ReplicaProposerSetup(r.Id, int32(r.F), int32(r.N), instancequormaliser, maxOpenInstances, minBackoff,
		maxInitBackoff, maxBackoff, factor, softFac, constBackoff, minimalProposers, timeBasedBallots, mappedProposers,
		dynamicMappedProposers, mappedProposersNum, pam, pamloc, doEager, forwardInduction,
		doChosenFWI, doValueFWI, doLatePropsFWI, forwardingInstances,
		eagerByExec, eagerByExecFac, r, batchSize, inductiveConfs, proposeToCatchUp, openInstToCatchUp, signalIfNoInstStarted,
		limPipelineOnPreempt, eagerMaxOutstanding)

	r.Durable = durable

	var acceptSelfSender AcceptSelfSender = &AsyncAcceptSender{
		acceptChan: r.acceptChan,
	}
	var prepareSelfSender PrepareSelfSender = &AsyncPrepareSender{
		prepareChan: r.prepareChan,
	}

	if syncaceptor {
		prepareSelfSender = &SyncPrepareSender{
			id: r.Id,
			r:  r,
		}
	}

	var sendQrmSize SendQrmSize = &NonThrifty{
		f: r.F,
	}
	if r.Thrifty {
		sendQrmSize = &Thrifty{r.F}
	}
	tcpSender := TCPListSender{
		Replica: r.Replica,
	}
	var prepareSender ListSender = &tcpSender
	var valueSender ListSender = &tcpSender
	if r.UDP {
		if !sendPreparesToAllAcceptors {
			panic("Not implemented yet")
		}
		prepareSender = &UnreliableUDPListSender{Replica: r.Replica}
		valueSender = &ReliableUDPListSender{Replica: r.Replica}
	}
	r.PrepareBroadcaster = NewPrepareAllFromWriteAheadReplica(int32(r.N), r.Id, r.prepareRPC, int32(r.F), r.Thrifty, r.AcceptorQrmInfo, prepareSelfSender, prepareSender)
	r.ValueBroadcaster = NewBcastSlowLearning(r.acceptRPC, r.commitShortRPC, r.commitRPC, r.Id, int32(r.N), pids, sendQrmSize, valueSender, acceptSelfSender, r)
	if bcastAcceptance {
		r.ValueBroadcaster = NewBcastFastLearning(r.acceptRPC, r.commitShortRPC, r.Id, int32(r.N), pids, r.AcceptorQrmInfo, sendQrmSize, valueSender, acceptSelfSender)
	}

	go r.run()
	return r
}

func ReplicaProposerSetup(id int32, f int32, n int32, proposerInstanceQuorumaliser proposer.ProposerInstanceQuorumaliser,
	maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, factor float64, softFac bool,
	constBackoff bool, minimalProposers bool, timeBasedBallots bool, mappedProposers bool, dynamicMappedProposers bool,
	mappedProposersNum int32, pam bool, pamloc string, doEager bool, doEagerFI bool, doChosenFWI bool, doValueFWI bool,
	doLatePropFWI bool, forwardingInstances int32, eagerByExec bool, eagerByExecFac float32, replica *Replica,
	maxBatchSize int, inductiveConfs bool, proposeToCatchUp bool, OpenInstToCatchUp bool, signalIfNoInstStarted bool,
	limPipelineOnPreempt bool, eagerMaxOutstanding bool) {

	//replica.signalIfNoInstStarted = signalIfNoInstStarted
	replica.ProposerInstanceQuorumaliser = proposerInstanceQuorumaliser
	pids := make([]int32, n)
	for i := range pids {
		pids[i] = int32(i)
	}

	replica.proposeToCatchUp = proposeToCatchUp

	retrySig := make(chan proposer.RetryInfo, maxOpenInstances*n)
	replica.RetryInstance = retrySig
	balloter := &balloter.Balloter{
		PropID:            id,
		N:                 n,
		MaxInc:            10000,
		DoTimeBasedBallot: timeBasedBallots,
	}
	backoffManager := proposer.BackoffManagerNew(minBackoff, maxInitBackoff, maxBackoff, retrySig, factor, softFac, constBackoff, id)

	// single instance manager
	var instanceManager proposer.SingleInstanceManager = proposer.SimpleInstanceManagerNew(id, backoffManager, balloter, proposerInstanceQuorumaliser)
	if minimalProposers {
		minimalShouldMaker := proposer.MinimalProposersShouldMakerNew(int16(id), int(f))
		instanceManager = proposer.MinimalProposersInstanceManagerNew(instanceManager.(*proposer.SimpleInstanceManager), minimalShouldMaker)
	}

	openInstanceSig := make(chan struct{}, maxOpenInstances)
	sig := proposer.SimpleSigNew(openInstanceSig, id, signalIfNoInstStarted)

	//simpleBatcher := proposer.GetBatcher(id, maxBatchSize)

	baselineProposer := proposer.BaselineProposerNew(id, sig, sig, sig, instanceManager, backoffManager, balloter, maxBatchSize)

	simpleExecutor := proposer.GetNewExecutor(id, replica.Replica, replica.StableStore, replica.Dreply)
	if !doEager && !doEagerFI && pam {
		panic("pam requires some form of eagerness")
	}

	// setup baseline
	if !doEager && !eagerByExec && !doEagerFI {
		replica.Executor = &simpleExecutor
		if inductiveConfs {
			inductiveGlobalManager := proposer.InductiveConflictsManager{baselineProposer}
			replica.Proposer = &inductiveGlobalManager
			return
		}
		replica.Proposer = baselineProposer
		return
	}

	// setup eager sig
	eSig := proposer.EagerSigNew(sig, maxOpenInstances)
	var os proposer.OpenInstSignal
	var s proposer.BallotOpenInstanceSignal
	var es proposer.ExecOpenInstanceSignal
	var rs proposer.RequestRecivedSignaller
	if eagerByExec {
		eESig := proposer.EagerExecUpToSigNew(eSig, float32(n), eagerByExecFac, limPipelineOnPreempt)
		os = eESig
		s = eESig
		es = eESig
		rs = eESig
	} else if eagerMaxOutstanding {
		eESig := proposer.EagerMaxOutstandingSigNew(eSig, int(eagerByExecFac))
		os = eESig
		s = eESig
		es = eESig
		rs = eESig
	}
	baselineProposer = proposer.BaselineProposerNew(id, os, s, rs, instanceManager, backoffManager, balloter, maxBatchSize)

	// decide between eager and eager fi
	var eagerProposer proposer.EagerByExecProposer = nil
	if doEagerFI {
		eagerProposer = &proposer.EagerFI{
			BaselineManager:        baselineProposer,
			InducedUpTo:            -1,
			Induced:                make(map[int32]map[int32]stdpaxosproto.Ballot),
			DoChosenFWI:            doChosenFWI,
			DoValueFWI:             doValueFWI,
			DoLateProposalFWI:      doLatePropFWI,
			Id:                     id,
			N:                      n,
			Windy:                  make(map[int32][]int32),
			ExecOpenInstanceSignal: es,
			Forwarding:             forwardingInstances,
			MaxStarted:             -1,
			MaxAt:                  make(map[int32]int32),
		}

		for _, pid := range pids {
			eagerProposer.(*proposer.EagerFI).Windy[pid] = make([]int32, forwardingInstances+1)
		}

	} else {
		inductiveGlobalManager := proposer.InductiveConflictsManager{BaselineManager: baselineProposer}
		eagerProposer = &proposer.Eager{
			InductiveConflictsManager: inductiveGlobalManager,
			ExecOpenInstanceSignal:    es,
		}

	}
	replica.Executor = &proposer.EagerByExecExecutor{
		SimpleExecutor:         simpleExecutor,
		ExecOpenInstanceSignal: eagerProposer.GetExecSignaller(),
	}

	if !pam && !mappedProposers && !dynamicMappedProposers {
		replica.Proposer = eagerProposer
		return
	}

	if doEagerFI {
		panic("eager fi and pam not yet implemented")
	}

	//PROPOSER QUORUMS
	if !pam && !mappedProposers {
		panic("Invalid options")
	}

	var agentMapper instanceagentmapper.InstanceAgentMapper
	if mappedProposers {
		agentMapper = &instanceagentmapper.LoadBalancingSetMapper{
			Ids: pids,
			G:   mappedProposersNum,
		}
	}
	if pam {
		pamap := instanceagentmapper.ReadFromFile(pamloc)
		proposerMappings := instanceagentmapper.GetPMap(pamap)
		if mappedProposers {
			agentMapper = instanceagentmapper.NewFixedButLoadBalacingSetMapper(proposerMappings, mappedProposersNum)
		} else {
			agentMapper = &instanceagentmapper.FixedInstanceAgentMapping{
				Groups: proposerMappings,
			}
		}
	}

	pamProposer := proposer.MappedProposersProposalManagerNew(eagerProposer.(*proposer.Eager), agentMapper, OpenInstToCatchUp)
	replica.Proposer = pamProposer

	// todo add eager fi pam proposer

	//mappedGroupGetter := MappedProposersAwaitingGroupNew(agentMapper)

	//if dynamicMappedProposers {
	//	dAgentMapper := &DynamicInstanceSetMapper{
	//		DetRandInstanceSetMapper: *agentMapper.(*instanceagentmapper.DetRandInstanceSetMapper),
	//	}
	//	globalManager = DynamicMappedProposerManagerNew(r.startInstanceSig, baselineProposer.BaselineManager, instanceManager, dAgentMapper, int32(r.N), int32(r.F))
	//	mappedGroupGetter = MappedProposersAwaitingGroupNew(dAgentMapper)
	//	r.noopLearners = []NoopLearner{globalManager.(*DynamicMappedGlobalManager)}
	//}
	//if minimalProposers {
	//	awaitingGroup = MinimalMappedProposersAwaitingGroupNew(*minimalGroupGetter, *mappedGroupGetter)
	//}
}
