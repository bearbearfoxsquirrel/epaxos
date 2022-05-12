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
	"os"
	"proposerstate"
	"stablestore"
	"state"
	"stats"
	"stdpaxosproto"
	"sync"
	"time"
	"twophase/aceptormessagefilter"
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

type LWPReplica struct {
	//ProposerQuorumaliser
	CrtInstanceOracle

	InstanceManager
	ProposedClientValuesManager

	ProposerQuorumaliser
	LearnerQuorumaliser
	AcceptorQrmInfo

	Queueing

	batchLearners []MyBatchLearner
	noopLearners  []NoopLearner

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
	instanceSpace                 []*ProposingBookkeeping // the space of all instances (used and not yet used)
	Shutdown                      bool
	counter                       int
	flush                         bool
	executedUpTo                  int32
	maxBatchWait                  int
	maxOpenInstances              int32
	crtOpenedInstances            []int32
	proposableInstances           chan ProposalInfo
	noopWaitUs                    int32
	retryInstance                 chan RetryInfo
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
	openInst                      chan struct{}
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
	bcastAcceptance        bool
	instsToOpenPerBatch    int32
	batchProposedObservers []ProposedObserver
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

func NewBaselineTwoPhaseReplica(quoralP ProposerQuorumaliser, quoralL LearnerQuorumaliser, quoralA AcceptorQrmInfo, id int, replica *genericsmr.Replica, durable bool, batchWait int, storageLoc string, maxOpenInstances int32,
	minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool, factor float64,
	whoCrash int32, whenCrash time.Duration, howlongCrash time.Duration, emulatedSS bool, emulatedWriteTime time.Duration,
	catchupBatchSize int32, timeout time.Duration, group1Size int, flushCommit bool, softFac bool, doStats bool,
	statsParentLoc string, commitCatchup bool, deadTime int32, batchSize int, constBackoff bool, requeueOnPreempt bool,
	tsStatsFilename string, instStatsFilename string, propsStatsFilename string, sendProposerState bool,
	proactivePrepareOnPreempt bool, batchingAcceptor bool, maxAccBatchWait time.Duration, filter aceptormessagefilter.AcceptorMessageFilter,
	sendPreparesToAllAcceptors bool, q Queueing,
	minimalProposers bool, timeBasedBallots bool, cliPropLearners []MyBatchLearner, mappedProposers bool, dynamicMappedProposers bool, bcastAcceptance bool, mappedProposersNum int, instsToOpenPerBatch int32, batchProposedObservers []ProposedObserver) *LWPReplica {
	retryInstances := make(chan RetryInfo, maxOpenInstances*10000)

	r := &LWPReplica{
		Replica:              replica,
		ProposerQuorumaliser: quoralP,
		LearnerQuorumaliser:  quoralL,
		AcceptorQrmInfo:      quoralA,
		batchLearners:        cliPropLearners,
		Queueing:             q,

		bcastAcceptance: bcastAcceptance,

		stateChan:        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		configChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitShortChan:  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptReplyChan:  make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		prepareRPC:       0,
		acceptRPC:        0,
		commitRPC:        0,
		commitShortRPC:   0,
		prepareReplyRPC:  0,
		acceptReplyRPC:   0,
		instanceSpace:    make([]*ProposingBookkeeping, 15*1024*1024),
		Shutdown:         false,
		counter:          0,
		flush:            true,
		executedUpTo:     -1, //get from storage
		maxBatchWait:     batchWait,
		//maxBalInc:                   10000,
		maxOpenInstances:            maxOpenInstances,
		crtOpenedInstances:          make([]int32, maxOpenInstances),
		proposableInstances:         make(chan ProposalInfo, MAXPROPOSABLEINST),
		noopWaitUs:                  noopwait,
		retryInstance:               retryInstances,
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
		openInst:                    make(chan struct{}, maxOpenInstances),
		sendProposerState:           sendProposerState,
		noopWait:                    time.Duration(noopwait) * time.Microsecond,
		proactivelyPrepareOnPreempt: proactivePrepareOnPreempt,
		isAccMsgFilter:              filter != nil,
		expectedBatchedRequests:     200,
		sendPreparesToAllAcceptors:  sendPreparesToAllAcceptors,
		instsToOpenPerBatch:         instsToOpenPerBatch,
		batchProposedObservers:      batchProposedObservers,
	}

	if filter != nil {
		messageFilter := messageFilterRoutine{
			AcceptorMessageFilter: filter,
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

	pids := make([]int32, r.N)
	ids := make([]int, r.N)
	for i := range pids {
		pids[i] = int32(i)
		ids[i] = i
	}

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

	if group1Size <= r.N-r.F {
		r.group1Size = r.N - r.F
	} else {
		r.group1Size = group1Size
	}

	r.ProposedClientValuesManager = ProposedClientValuesManagerNew(r.Id, r.TimeseriesStats, r.doStats, q)

	var propManager = SimplePropsalManagerNew(r.Id, int32(r.N), quoralP, minBackoff, maxInitBackoff, maxBackoff, r.retryInstance, factor, softFac, constBackoff, timeBasedBallots, doStats, r.TimeseriesStats, r.ProposalStats, r.InstanceStats)
	if minimalProposers {
		propManager = MinProposerProposalManagerNew(r.F, r.Id, int32(r.N), quoralP, minBackoff, maxInitBackoff, maxBackoff, r.retryInstance, factor, softFac, constBackoff, timeBasedBallots, doStats, r.TimeseriesStats, r.ProposalStats, r.InstanceStats)
	}
	if mappedProposers {
		propManager = MappedProposersProposalManagerNew(r.Id, int32(r.N), quoralP, minBackoff, maxInitBackoff, maxBackoff, r.retryInstance, factor, softFac, constBackoff, timeBasedBallots, doStats, r.TimeseriesStats, r.ProposalStats, r.InstanceStats, ids, mappedProposersNum)
	}
	if dynamicMappedProposers {
		propManager = DynamicMappedProposerManagerNew(r.Id, int32(r.N), quoralP, minBackoff, maxInitBackoff, maxBackoff, r.retryInstance, factor, softFac, constBackoff, timeBasedBallots, doStats, r.TimeseriesStats, r.ProposalStats, r.InstanceStats, ids, r.F)
		r.noopLearners = []NoopLearner{propManager.(*DynamicMappedProposalManager)}
	} else {
		r.noopLearners = []NoopLearner{}
	}
	r.CrtInstanceOracle = propManager
	r.InstanceManager = propManager

	r.Durable = durable

	r.crtOpenedInstances = make([]int32, r.maxOpenInstances)
	for i := 0; i < len(r.crtOpenedInstances); i++ {
		r.crtOpenedInstances[i] = -1
		r.openInst <- struct{}{}
	}

	go r.run()
	return r
}

func (r *LWPReplica) CloseUp() {
	if r.doStats {
		r.TimeseriesStats.Close()
		r.InstanceStats.Close()
		r.ProposalStats.CloseOutput()
	}
}

func (r *LWPReplica) recordExecutedUpTo() {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(r.executedUpTo))
	r.StableStore.WriteAt(b[:], 4)
}

/* Clock goroutine */
func (r *LWPReplica) fastClock() {
	for !r.Shutdown {
		time.Sleep(time.Duration(r.maxBatchWait) * time.Millisecond) // ms
		dlog.Println("sending fast clock")
		fastClockChan <- true
	}
}

func (r *LWPReplica) BatchingEnabled() bool {
	return r.maxBatchWait > 0
}

func (r *LWPReplica) run() {
	r.ConnectToPeers()
	r.RandomisePeerOrder()

	fastClockChan = make(chan bool, 1)
	if r.BatchingEnabled() {
		go batching.StartBatching(r.Id, r.ProposeChan, r.Queueing.GetTail(), r.expectedBatchedRequests, r.maxBatchSize, time.Duration(r.maxBatchWait)*time.Millisecond)
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

	var stateGo *time.Timer
	var stateGoC <-chan time.Time
	if r.sendProposerState {
		stateGo = time.NewTimer(200 * time.Millisecond)
		stateGoC = stateGo.C
	}

	for !r.Shutdown {
		select {
		case <-stateGoC:
			for i := int32(0); i < int32(r.N); i++ {
				if i == r.Id {
					continue
				}
				msg := proposerstate.State{
					ProposerID:      r.Id,
					CurrentInstance: r.CrtInstanceOracle.GetCrtInstance(),
				}
				//dlog.AgentPrintfN(r.id, "Sending current state %d to all other proposers", r.crtInstance)
				r.SendMsg(i, r.stateChanRPC, &msg)
			}
			stateGo.Reset(time.Duration(200) * time.Millisecond)
			break
		case stateS := <-r.stateChan:
			recvState := stateS.(*proposerstate.State)
			r.handleState(recvState)
			break
		case props := <-r.Queueing.GetHead(): //<-startNewInstanceChan:
			if err := r.Queueing.Dequeued(props, func() { r.beginNextInstance(props) }); err != nil {
				dlog.AgentPrintfN(r.Id, "Batch with UID %d received to start instance with has been chosen so now throwing out", props.GetUID())
			}
			break
		case <-c:
			r.TimeseriesStats.PrintAndReset()
			break
		case maybeTimedout := <-r.timeoutMsgs:
			r.retryBallot(maybeTimedout)
			break
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

func (r *LWPReplica) retryBallot(maybeTimedout TimeoutInfo) {
	pbk := r.instanceSpace[maybeTimedout.inst]
	if pbk.propCurBal.Equal(maybeTimedout.ballot) && pbk.status == maybeTimedout.phase {
		if r.doStats {
			r.TimeseriesStats.Update("Message Timeouts", 1)
			id := stats.InstanceID{
				Log: 0,
				Seq: maybeTimedout.inst,
			}
			if maybeTimedout.phase == PREPARING {
				r.InstanceStats.RecordOccurrence(id, "Phase 1 Timeout", 1)
			} else if maybeTimedout.phase == PROPOSING {
				r.InstanceStats.RecordOccurrence(id, "Phase 2 Timeout", 1)
			}
		}
		group := pbk.qrms[pbk.propCurBal].Broadcast(maybeTimedout.msgCode, maybeTimedout.msg)
		phase := "prepare"
		if maybeTimedout.phase == PROPOSING {
			phase = "accept request"
		}
		dlog.AgentPrintfN(r.Id, "Phase %s message in instance %d at ballot %d.%d timedout. Broadcasting to %v", phase, maybeTimedout.inst, pbk.propCurBal.Number, pbk.propCurBal.PropID, group)
		r.beginTimeout(maybeTimedout.inst, maybeTimedout.ballot, maybeTimedout.phase, r.timeout, maybeTimedout.msgCode, maybeTimedout.msg)
	}
}

func (r *LWPReplica) sendSinglePrepare(instance int32, to int32) {
	// cheats - DecideRetry really be a special recovery message but lazzzzzyyyy
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()
	args := &stdpaxosproto.Prepare{r.Id, instance, r.instanceSpace[instance].propCurBal}
	dlog.Printf("send prepare to %d\n", to)
	r.SendMsg(to, r.prepareRPC, args)
	r.beginTimeout(args.Instance, args.Ballot, PREPARING, r.timeout*5, r.prepareRPC, args)
}

func (r *LWPReplica) beginTimeout(inst int32, attempted stdpaxosproto.Ballot, onWhatPhase ProposerStatus, timeout time.Duration, msgcode uint8, msg fastrpc.Serializable) {
	time.AfterFunc(timeout, func() {
		r.timeoutMsgs <- TimeoutInfo{
			inst:    inst,
			ballot:  attempted,
			phase:   onWhatPhase,
			msgCode: msgcode,
			msg:     msg,
		}
	})
}

func (r *LWPReplica) isSlowestSlowerThanMedian(sent []int) bool {
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

func (r *LWPReplica) beginTracking(instID stats.InstanceID, ballot stdpaxosproto.Ballot, sentTo []int, trackingName string, proposalTrackingName string) {
	if len(sentTo) == r.N || r.isSlowestSlowerThanMedian(sentTo) {
		dlog.AgentPrintfN(r.Id, "Broadcasted instance %d to a slow quorum in %s", instID.Seq, trackingName)
		if r.doStats {
			r.InstanceStats.RecordComplexStatStart(instID, trackingName, "Slow Quorum")
			r.ProposalStats.RecordOccurence(instID, ballot, proposalTrackingName+" Slow Quorum", 1)
		}
	} else {
		dlog.AgentPrintfN(r.Id, "Broadcasted instance %d to a fast quorum in %s", instID.Seq, trackingName)
		if r.doStats {
			r.InstanceStats.RecordComplexStatStart(instID, trackingName, "Fast Quorum")
			r.ProposalStats.RecordOccurence(instID, ballot, proposalTrackingName+" Fast Quorum", 1)
		}
	}
}

func (r *LWPReplica) bcastPrepare(instance int32) {
	args := &stdpaxosproto.Prepare{r.Id, instance, r.instanceSpace[instance].propCurBal}
	pbk := r.instanceSpace[instance]
	//dlog.Println("sending prepare for instance", instance, "and ballot", pbk.propCurBal.Number, pbk.propCurBal.PropID)
	var sentTo []int
	if r.sendPreparesToAllAcceptors {
		sentTo = make([]int, 0, r.N)
		for i := 0; i < r.N; i++ {
			if i == int(r.Id) {
				sentTo = append(sentTo, i)
				continue
			}
			r.Replica.SendMsg(int32(i), r.prepareRPC, args)
			sentTo = append(sentTo, i)
		}
	} else {
		sentTo = pbk.qrms[pbk.propCurBal].Broadcast(r.prepareRPC, args)
	}
	dlog.AgentPrintfN(r.Id, "Broadcasted prepare for instance %d at ballot %d.%d to replicas %v", args.Instance, args.Number, args.PropID, sentTo)

	instID := stats.InstanceID{
		Log: 0,
		Seq: instance,
	}
	if r.doStats {
		r.InstanceStats.RecordOccurrence(instID, "My Phase 1 Proposals", 1)
	}
	r.beginTracking(instID, args.Ballot, sentTo, "Phase 1", "Phase 1")
	r.beginTimeout(args.Instance, args.Ballot, PREPARING, r.timeout, r.prepareRPC, args)
}

func (r *LWPReplica) bcastAccept(instance int32) {
	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = r.instanceSpace[instance].propCurBal
	pa.Command = r.instanceSpace[instance].cmds
	pa.WhoseCmd = r.instanceSpace[instance].whoseCmds
	args := &pa
	pbk := r.instanceSpace[instance]
	var sentTo []int
	if r.bcastAcceptance {
		sentTo = make([]int, 0, r.N)
		for i := 0; i < r.N; i++ {
			if i == int(r.Id) {
				sentTo = append(sentTo, i)
				continue
			}
			r.Replica.SendMsg(int32(i), r.acceptRPC, args)
			sentTo = append(sentTo, i)
		}
	} else {
		sentTo = pbk.qrms[pbk.propCurBal].Broadcast(r.acceptRPC, args)
	}

	dlog.AgentPrintfN(r.Id, "Broadcasting accept for instance %d with whose commands %d, at ballot %d.%d to Replicas %v", pa.Instance, pa.WhoseCmd, pa.Number, pa.PropID, sentTo)
	instID := stats.InstanceID{
		Log: 0,
		Seq: instance,
	}
	if r.doStats {
		r.InstanceStats.RecordOccurrence(instID, "My Phase 2 Proposals", 1)
	}
	r.beginTracking(instID, args.Ballot, sentTo, "Phase 2", "Phase 2")
	r.beginTimeout(args.Instance, args.Ballot, PROPOSING, r.timeout, r.acceptRPC, args)
}

func (r *LWPReplica) bcastCommitToAll(instance int32, Ballot stdpaxosproto.Ballot, command []*state.Command) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id // fixme should be ballot prop id?
	pc.Instance = instance
	pc.Ballot = Ballot
	pc.WhoseCmd = r.instanceSpace[instance].whoseCmds
	pc.MoreToCome = 0
	pc.Command = command

	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = Ballot
	pcs.WhoseCmd = r.instanceSpace[instance].whoseCmds
	pcs.Count = int32(len(command))
	argsShort := pcs
	dlog.AgentPrintfN(r.Id, "Broadcasting commit for instance %d with whose commands %d, at ballot %d.%d", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID)
	r.CalculateAlive()
	sent := 0
	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id {
			continue
		}
		inQrm := r.instanceSpace[instance].qrms[Ballot].HasAcknowledged(int(q))
		if inQrm {
			r.SendMsg(q, r.commitShortRPC, &argsShort)
		} else {

			r.SendMsg(q, r.commitRPC, &pc)
		}
		sent++
	}
}

func (r *LWPReplica) beginNextInstance(valsToPropose batching.ProposalBatch) {
	for i := int32(0); i < r.instsToOpenPerBatch; i++ {
		inst := r.InstanceManager.startNextInstance(&r.instanceSpace)
		curInst := r.instanceSpace[inst]
		r.InstanceManager.startNextProposal(curInst, inst)

		if r.doStats {
			r.InstanceStats.RecordOpened(stats.InstanceID{0, inst}, time.Now())
			r.TimeseriesStats.Update("Instances Opened", 1)
			r.ProposalStats.Open(stats.InstanceID{0, inst}, curInst.propCurBal)
		}

		r.instanceSpace[inst].putBatch(valsToPropose)
		prepMsg := getPrepareMessage(r.Id, inst, curInst)
		dlog.AgentPrintfN(r.Id, "Opened new instance %d, with ballot %d.%d \n", inst, prepMsg.Number, prepMsg.PropID)
		acceptorHandlePrepareLocal(r.Id, r.Acceptor, prepMsg, r.PrepareResponsesRPC, r.prepareReplyChan)
	}
}

func getPrepareMessage(id int32, inst int32, curInst *ProposingBookkeeping) *stdpaxosproto.Prepare {
	prepMsg := &stdpaxosproto.Prepare{
		LeaderId: id,
		Instance: inst,
		Ballot:   curInst.propCurBal,
	}
	return prepMsg
}

func (r *LWPReplica) tryNextAttempt(next RetryInfo) {
	inst := r.instanceSpace[next.inst]
	if !r.InstanceManager.DecideRetry(inst, next) {
		return
	}

	r.InstanceManager.startNextProposal(inst, next.inst)
	nextBallot := inst.propCurBal
	prepare := getPrepareMessage(r.Id, next.inst, inst)
	acceptorHandlePrepareLocal(r.Id, r.Acceptor, prepare, r.PrepareResponsesRPC, r.prepareReplyChan)

	if r.doStats {
		r.ProposalStats.Open(stats.InstanceID{0, next.inst}, nextBallot)
		r.InstanceStats.RecordOccurrence(stats.InstanceID{0, next.inst}, "My Phase 1 Proposals", 1)
	}
}

func (r *LWPReplica) recordStatsPreempted(inst int32, pbk *ProposingBookkeeping) {
	if pbk.status != BACKING_OFF && r.doStats {
		id := stats.InstanceID{Log: 0, Seq: inst}
		if pbk.status == PREPARING || pbk.status == READY_TO_PROPOSE {
			r.InstanceStats.RecordOccurrence(id, "My Phase 1 Preempted", 1)
			r.TimeseriesStats.Update("My Phase 1 Preempted", 1)
			r.ProposalStats.CloseAndOutput(id, pbk.propCurBal, stats.HIGHERPROPOSALONGOING)
		} else if pbk.status == PROPOSING {
			r.InstanceStats.RecordOccurrence(id, "My Phase 2 Preempted", 1)
			r.TimeseriesStats.Update("My Phase 2 Preempted", 1)
			r.ProposalStats.CloseAndOutput(id, pbk.propCurBal, stats.HIGHERPROPOSALONGOING)
		}
	}
}

func (r *LWPReplica) handlePrepare(prepare *stdpaxosproto.Prepare) {
	dlog.AgentPrintfN(r.Id, "Replica received a Prepare from Replica %d in instance %d at ballot %d.%d", prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID)

	if r.AcceptorQrmInfo.IsInQrm(prepare.Instance, r.Id) {
		dlog.AgentPrintfN(r.Id, "Giving Prepare from Replica %d in instance %d at ballot %d.%d to acceptor as it can form a quorum", prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID)
		acceptorHandlePrepare(r.Id, r.Acceptor, prepare, r.PrepareResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica)
	}

	if r.InstanceManager.LearnOfBallot(&r.instanceSpace, prepare.Instance, prepare.Ballot, stdpaxosproto.PROMISE) {
		pCurBal := r.instanceSpace[prepare.Instance].propCurBal
		dlog.AgentPrintfN(r.Id, "Prepare Received from Replica %d in instance %d at ballot %d.%d Preempted Previous Ballot we had at ballot %d.%d",
			prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID, pCurBal.Number, pCurBal.PropID)
	}
	r.ProposedClientValuesManager.learnOfBallot(r.instanceSpace[prepare.Instance], prepare.Instance, prepare.Ballot)
}

//func (r *LWPReplica) learn
func (r *LWPReplica) proposerWittnessValue(inst int32, aid int32, accepted stdpaxosproto.Ballot, val []*state.Command, whoseCmds int32) bool {
	if accepted.IsZero() {
		return false
	}
	r.InstanceManager.LearnOfBallot(&r.instanceSpace, inst, accepted, stdpaxosproto.ACCEPTANCE)
	pbk := r.instanceSpace[inst]
	if pbk.status == CLOSED {
		return false
	}

	//proposer
	r.ProposedClientValuesManager.learnOfAcceptedBallot(pbk, inst, accepted, whoseCmds)
	newVal := false
	if accepted.GreaterThan(pbk.proposeValueBal) {
		setValue(pbk, whoseCmds, accepted, val)
		newVal = true
	}

	return newVal

}

func (r *LWPReplica) learnerHandleAcceptedValue(inst int32, aid int32, accepted stdpaxosproto.Ballot, val []*state.Command, whoseCmds int32) ProposerAccValHandler {
	if accepted.IsZero() {
		return IGNORED
	}
	pbk := r.instanceSpace[inst]
	if pbk.status == CLOSED {
		return IGNORED
	}

	addToAcceptanceQuorum(inst, aid, accepted, pbk, r.LearnerQuorumaliser, r.AcceptorQrmInfo)
	if pbk.qrms[accepted].QuorumReached() {
		dlog.AgentPrintfN(r.Id, "Quorum reached on Acceptance on instance %d at round %d.%d with whose commands %d", inst, accepted.Number, accepted.PropID, whoseCmds)
		if !r.bcastAcceptance {
			r.bcastCommitToAll(inst, accepted, val)
		}
		if !pbk.qrms[accepted].HasAcknowledged(int(r.Id)) {
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
		r.proposerCloseCommit(inst, accepted, pbk.cmds, whoseCmds)
		return CHOSEN
	} else {
		return ACKED
	}
}

func setValue(pbk *ProposingBookkeeping, whoseCmds int32, bal stdpaxosproto.Ballot, val []*state.Command) {
	pbk.whoseCmds = whoseCmds
	pbk.proposeValueBal = bal
	pbk.cmds = val
}

func (r *LWPReplica) handlePrepareReply(preply *stdpaxosproto.PrepareReply) {
	pbk := r.instanceSpace[preply.Instance]
	dlog.AgentPrintfN(r.Id, "Replica received a Prepare Reply from Replica %d in instance %d at requested ballot %d.%d and current ballot %d.%d", preply.AcceptorId, preply.Instance, preply.Req.Number, preply.Req.PropID, preply.Cur.Number, preply.Cur.PropID)

	if pbk.status == CLOSED {
		dlog.AgentPrintfN(r.Id, "Discarding Prepare Reply from Replica %d in instance %d at requested ballot %d.%d because it's already chosen", preply.AcceptorId, preply.Instance, preply.Req.Number, preply.Req.PropID)
		return
	}

	// check if there isChosen a value and track it
	r.proposerWittnessValue(preply.Instance, preply.AcceptorId, preply.VBal, preply.Command, preply.WhoseCmd)
	valWhatDone := r.learnerHandleAcceptedValue(preply.Instance, preply.AcceptorId, preply.VBal, preply.Command, preply.WhoseCmd)
	if valWhatDone == CHOSEN {
		return
	}

	if pbk.propCurBal.GreaterThan(preply.Req) || pbk.status != PREPARING {
		// even if late check if cur proposal preempts our current proposal
		r.InstanceManager.LearnOfBallot(&r.instanceSpace, preply.Instance, preply.Cur, preply.CurPhase)
		dlog.AgentPrintfN(r.Id, "Prepare Reply for instance %d with current ballot %d.%d and requested ballot %d.%d in late, either because we are now at %d.%d or aren't preparing any more",
			preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.Req.Number, preply.Req.PropID, pbk.propCurBal.Number, pbk.propCurBal.PropID)
		return
	}

	if preply.Req.GreaterThan(pbk.propCurBal) {
		panic("Some how got a promise on a future proposal")
	}

	if preply.Req.GreaterThan(preply.Cur) {
		panic("somehow acceptor did not promise us")
	}

	r.ProposedClientValuesManager.learnOfBallot(r.instanceSpace[preply.Instance], preply.Instance, preply.Cur)
	if preply.Cur.GreaterThan(preply.Req) {
		isNewPreempted := r.InstanceManager.LearnOfBallot(&r.instanceSpace, preply.Instance, preply.Cur, stdpaxosproto.PROMISE)
		if isNewPreempted {
			pCurBal := r.instanceSpace[preply.Instance].propCurBal
			dlog.AgentPrintfN(r.Id, "Prepare Reply Received from Replica %d in instance %d at with current ballot %d.%d Preempted Previous Ballot we had at ballot %d.%d",
				preply.AcceptorId, preply.Instance, preply.Cur.Number, preply.Cur.PropID, pCurBal.Number, pCurBal.PropID)
		}

		if r.AcceptorQrmInfo.IsInQrm(preply.Instance, r.Id) && r.proactivelyPrepareOnPreempt && isNewPreempted && int32(preply.Req.PropID) != r.Id {
			newPrep := &stdpaxosproto.Prepare{
				LeaderId: int32(preply.Cur.PropID),
				Instance: preply.Instance,
				Ballot:   preply.Cur,
			}
			acceptorHandlePrepare(r.Id, r.Acceptor, newPrep, r.PrepareResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica)
		}
		return
	}

	dlog.AgentPrintfN(r.Id, "Promise recorded on instance %d at ballot %d.%d from Replica %d with value ballot %d.%d and whose commands %d",
		preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.AcceptorId, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd)
	qrm := pbk.qrms[pbk.propCurBal]
	qrm.AddToQuorum(int(preply.AcceptorId))

	if int32(preply.Req.PropID) == r.Id && preply.AcceptorId == r.Id { // my proposal
		r.bcastPrepare(preply.Instance)
		return
	}

	if qrm.QuorumReached() {
		dlog.AgentPrintfN(r.Id, "Promise Quorum reached on instance %d at ballot %d.%d",
			preply.Instance, preply.Cur.Number, preply.Cur.PropID)
		id := stats.InstanceID{
			Log: 0,
			Seq: preply.Instance,
		}
		if r.doStats {
			r.InstanceStats.RecordComplexStatEnd(id, "Phase 1", "Success")
		}

		if !pbk.proposeValueBal.IsZero() && pbk.whoseCmds != r.Id && pbk.clientProposals != nil {
			pbk.clientProposals = nil //at this point, our client proposal will not be chosen
		}
		r.tryPropose(preply.Instance, 0)
	}
}

func (r *LWPReplica) tryPropose(inst int32, priorAttempts int) {
	pbk := r.instanceSpace[inst]
	pbk.status = READY_TO_PROPOSE
	dlog.AgentPrintfN(r.Id, "Attempting to propose value in instance %d", inst)
	qrm := pbk.qrms[pbk.propCurBal]
	qrm.StartAcceptanceQuorum()

	if pbk.proposeValueBal.IsZero() {
		if pbk.cmds != nil {
			panic("there must be a previously chosen value")
		}

		if pbk.clientProposals != nil {
			setValue(pbk, r.Id, pbk.propCurBal, pbk.clientProposals.GetCmds())

			dlog.AgentPrintfN(r.Id, "%d client value(s) from batch with UID %d proposed in instance %d at ballot %d.%d", len(pbk.clientProposals.GetCmds()), pbk.clientProposals.GetUID(), inst, pbk.propCurBal.Number, pbk.propCurBal.PropID)
			if r.doStats {
				r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Client Value Proposed", 1)
				r.ProposalStats.RecordClientValuesProposed(stats.InstanceID{0, inst}, pbk.propCurBal, len(pbk.cmds))
				r.TimeseriesStats.Update("Times Client Values Proposed", 1)
			}
		} else {
			select {
			case b := <-r.Queueing.GetHead():
				if !r.TrySetProposalsOrTryAgain(inst, priorAttempts, b, pbk) {
					return
				}
				break
			default:
				if r.shouldNoop(inst) && (priorAttempts > 0 || r.noopWait <= 0) {
					if r.doStats {
						r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Noop Proposed", 1)
						r.TimeseriesStats.Update("Times Noops Proposed", 1)
						r.ProposalStats.RecordNoopProposed(stats.InstanceID{0, inst}, pbk.propCurBal)
					}

					setValue(pbk, -1, pbk.propCurBal, state.NOOPP())

					dlog.AgentPrintfN(r.Id, "Proposing noop in recovered instance %d at ballot %d.%d", inst, pbk.propCurBal.Number, pbk.propCurBal.PropID)
					break
				} else {
					r.BeginWaitingForClientProposals(inst, pbk)
					return
				}
			}
		}
	} else {
		if r.doStats {
			r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Previous Value Proposed", 1)
			r.TimeseriesStats.Update("Times Previous Value Proposed", 1)
			r.ProposalStats.RecordPreviousValueProposed(stats.InstanceID{0, inst}, pbk.propCurBal, len(pbk.cmds))
		}
		dlog.AgentPrintfN(r.Id, "Proposing previous value from ballot %d.%d with whose command %d in instance %d at ballot %d.%d",
			pbk.proposeValueBal.Number, pbk.proposeValueBal.PropID, pbk.whoseCmds, inst, pbk.propCurBal.Number, pbk.propCurBal.PropID)
	}

	if pbk.whoseCmds != r.Id && pbk.clientProposals != nil {
		panic("alsdkfjal")
		//pbk.clientProposals = nil
	}
	//pbk.status = PROPOSING
	//pbk.proposeValueBal = pbk.propCurBal
	if pbk.clientProposals != nil {
		for _, obs := range r.batchProposedObservers {
			obs.ObserveProposed(pbk.clientProposals)
		}
	}

	pbk.setNowProposing()
	acptMsg := getAcceptRequestMsg(r.Id, inst, pbk)
	if r.AcceptorQrmInfo.IsInQrm(inst, r.Id) {
		acceptorHandleAcceptLocal(r.Id, r.Acceptor, acptMsg, r.AcceptResponsesRPC, r.acceptReplyChan, r.Replica, r.bcastAcceptance)
	}

	r.bcastAccept(inst)
}

func getAcceptRequestMsg(id int32, inst int32, pbk *ProposingBookkeeping) *stdpaxosproto.Accept {
	acptMsg := &stdpaxosproto.Accept{
		LeaderId: id,
		Instance: inst,
		Ballot:   pbk.propCurBal,
		WhoseCmd: pbk.whoseCmds,
		Command:  pbk.cmds,
	}
	return acptMsg
}

func (r *LWPReplica) BeginWaitingForClientProposals(inst int32, pbk *ProposingBookkeeping) {
	t := time.NewTimer(r.noopWait)
	go func(curBal stdpaxosproto.Ballot) {
		var bat batching.ProposalBatch = nil
		q := r.Queueing.GetHead()

		valToPropose := false
		for !valToPropose {
			select {
			case b := <-q:
				err := r.Queueing.Dequeued(b, func() {
					bat = b
					dlog.AgentPrintfN(r.Id, "Received batch with UID %d to attempt to propose in instance %d", b.GetUID(), inst)
					valToPropose = true
				})

				if err != nil {
					//dlog.AgentPrintfN(r.Id, "Received batch with UID %d to propose in instance %d should not be proposed so tossing", b.GetUID(), inst)
					break
				}
				break
			case <-t.C:
				dlog.AgentPrintfN(r.Id, "Noop wait expired for instance %d", inst)
				valToPropose = true
				break
			}
		}
		r.proposableInstances <- ProposalInfo{
			inst:          inst,
			proposingBal:  curBal,
			ProposalBatch: bat,
		}
	}(pbk.propCurBal)
	dlog.AgentPrintfN(r.Id, "Decided there no need to propose a value in instance %d at ballot %d.%d, waiting %d ms before checking again", inst, pbk.propCurBal.Number, pbk.propCurBal.PropID, r.noopWait.Milliseconds())
}

func (r *LWPReplica) TrySetProposalsOrTryAgain(inst int32, priorAttempts int, b batching.ProposalBatch, pbk *ProposingBookkeeping) bool {
	proposeF := func() {
		pbk.clientProposals = b
		setValue(pbk, r.Id, pbk.propCurBal, pbk.clientProposals.GetCmds())

		if r.doStats {
			r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Client Value Proposed", 1)
			r.ProposalStats.RecordClientValuesProposed(stats.InstanceID{0, inst}, pbk.propCurBal, len(pbk.cmds))
			r.TimeseriesStats.Update("Times Client Values Proposed", 1)
		}
		dlog.AgentPrintfN(r.Id, "%d client value(s) from batch with UID %d received and proposed in recovered instance %d at ballot %d.%d \n", len(pbk.clientProposals.GetCmds()), pbk.clientProposals.GetUID(), inst, pbk.propCurBal.Number, pbk.propCurBal.PropID)
	}
	if err := r.Queueing.Dequeued(b, proposeF); err != nil {
		r.tryPropose(inst, priorAttempts)
		return false
	}
	return true
}

func (r *LWPReplica) recheckInstanceToPropose(retry ProposalInfo) {
	pbk := r.instanceSpace[retry.inst]
	if pbk == nil {
		panic("????")
	}

	dlog.AgentPrintfN(r.Id, "Rechecking whether to propose in instance %d", retry.inst)
	if pbk.propCurBal.GreaterThan(retry.proposingBal) || pbk.status != READY_TO_PROPOSE {
		dlog.AgentPrintfN(r.Id, "Decided to not propose in instance %d as we are no longer on ballot %d.%d and are now on %d.%d or ballot isChosen no longer in proposable state", retry.inst, retry.proposingBal.Number, retry.proposingBal.PropID, pbk.propCurBal.Number, pbk.propCurBal.PropID)
		if retry.ProposalBatch != nil { // requeue value that cannot be proposed
			bat := retry.popBatch()
			r.Queueing.Dequeued(bat, func() { r.Queueing.Requeue(bat) })
		}
		return
	}

	if retry.ProposalBatch != nil {
		bat := retry.popBatch()
		r.Queueing.Dequeued(bat, func() { pbk.putBatch(bat) })
	}

	if !pbk.proposeValueBal.IsZero() && pbk.clientProposals != nil {
		r.Queueing.Requeue(pbk.popBatch())
	}

	r.tryPropose(retry.inst, 1)
}

func (r *LWPReplica) shouldNoop(inst int32) bool {
	if r.alwaysNoop {
		return true
	}

	for i := inst + 1; i < r.CrtInstanceOracle.GetCrtInstance(); i++ {
		if r.instanceSpace[i] == nil {
			continue
		}
		if r.instanceSpace[i].status == CLOSED {
			return true
		}
	}
	return false
}

func (r *LWPReplica) checkAndHandleNewlyReceivedInstance(instance int32) {
	if instance < 0 {
		return
	}
	r.InstanceManager.LearnOfBallot(&r.instanceSpace, instance, stdpaxosproto.Ballot{Number: -1, PropID: -1}, stdpaxosproto.PROMISE)
}

func (r *LWPReplica) handleAccept(accept *stdpaxosproto.Accept) {
	dlog.AgentPrintfN(r.Id, "Replica received Accept from Replica %d in instance %d at ballot %d.%d", accept.PropID, accept.Instance, accept.Number, accept.PropID)
	r.checkAndHandleNewlyReceivedInstance(accept.Instance)
	//if r.instanceSpace[accept.Instance].status == CLOSED {
	//	return
	//}

	r.proposerWittnessValue(accept.Instance, r.Id, accept.Ballot, accept.Command, accept.WhoseCmd)

	if r.bcastAcceptance {
		if r.checkAcceptLateForBcastAcceptLearner(accept) {
			return
		}
	}

	if r.AcceptorQrmInfo.IsInQrm(accept.Instance, r.Id) {
		acceptorHandleAccept(r.Id, r.Acceptor, accept, r.AcceptResponsesRPC, r.isAccMsgFilter, r.messageFilterIn, r.Replica, r.bcastAcceptance, r.acceptReplyChan)
	}

	if r.InstanceManager.LearnOfBallot(&r.instanceSpace, accept.Instance, accept.Ballot, stdpaxosproto.PROMISE) {
		pCurBal := r.instanceSpace[accept.Instance].propCurBal
		dlog.AgentPrintfN(r.Id, "Accept Received from Replica %d in instance %d at ballot %d.%d Preempted Previous Ballot we had at ballot %d.%d",
			accept.PropID, accept.Instance, accept.Number, accept.PropID, pCurBal.Number, pCurBal.PropID)
	}
}

func (r *LWPReplica) checkAcceptLateForBcastAcceptLearner(accept *stdpaxosproto.Accept) bool {
	pbk := r.instanceSpace[accept.Instance]
	if pbk.qrms[accept.Ballot] == nil {
		return false
	}
	if !pbk.qrms[accept.Ballot].QuorumReached() {
		return false
	}

	dlog.AgentPrintfN(r.Id, "Value learnt and so chosen ballot (%d.%d) in instance %d can be committed", accept.Number, accept.PropID, accept.Instance)
	r.handleCommit(&stdpaxosproto.Commit{
		LeaderId:   int32(accept.PropID),
		Instance:   accept.Instance,
		Ballot:     accept.Ballot,
		WhoseCmd:   accept.WhoseCmd,
		MoreToCome: 0,
		Command:    accept.Command,
	})
	return true
}

func (r *LWPReplica) handleAcceptReply(areply *stdpaxosproto.AcceptReply) {
	if r.bcastAcceptance {
		r.LearnOfBallot(&r.instanceSpace, areply.Instance, areply.Cur, stdpaxosproto.ACCEPTANCE)
	}

	dlog.AgentPrintfN(r.Id, "Replica received Accept Reply from Replica %d in instance %d at requested ballot %d.%d and current ballot %d.%d", areply.AcceptorId, areply.Instance, areply.Req.Number, areply.Req.PropID, areply.Cur.Number, areply.Cur.PropID)
	pbk := r.instanceSpace[areply.Instance]
	if pbk.status == CLOSED {
		dlog.AgentPrintfN(r.Id, "Discarding Accept Reply from Replica %d in instance %d at requested ballot %d.%d because it's already chosen", areply.AcceptorId, areply.Instance, areply.Req.Number, areply.Req.PropID)
		dlog.Printf("Already committed ")
		return
	}

	if areply.Req.Equal(areply.Cur) && int32(areply.Req.PropID) != r.Id && r.bcastAcceptance {
		dlog.AgentPrintfN(r.Id, "Acceptance received for other's proposal in instance %d with ballot %d.%d", areply.Instance, areply.Cur.Number, areply.Cur.PropID)
		addToAcceptanceQuorum(areply.Instance, areply.AcceptorId, areply.Cur, pbk, r.LearnerQuorumaliser, r.AcceptorQrmInfo)
		if pbk.qrms[areply.Cur].QuorumReached() && (pbk.proposeValueBal.GreaterThan(areply.Cur) || pbk.proposeValueBal.Equal(areply.Cur)) {
			r.handleCommit(&stdpaxosproto.Commit{
				LeaderId:   int32(areply.Cur.PropID),
				Instance:   areply.Instance,
				Ballot:     areply.Cur,
				WhoseCmd:   areply.WhoseCmd,
				MoreToCome: 0,
				Command:    pbk.cmds,
			})
		}
		return
	}

	if pbk.propCurBal.GreaterThan(areply.Req) {
		dlog.AgentPrintfN(r.Id, "Accept Reply for instance %d with current ballot %d.%d and requested ballot %d.%d in late, because we are now at ballot %d.%d",
			areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.Req.Number, areply.Req.PropID, pbk.propCurBal.Number, pbk.propCurBal.PropID)
		return
	}

	if areply.Req.GreaterThan(pbk.propCurBal) {
		panic("got a future acceptance??")
	}

	if areply.Req.GreaterThan(areply.Cur) {
		panic("Acceptor didn't accept request")
	}

	preempted := areply.Cur.GreaterThan(areply.Req)
	if preempted {
		pCurBal := r.instanceSpace[areply.Instance].propCurBal
		dlog.AgentPrintfN(r.Id, "Accept Reply received from Replica %d in instance %d with current ballot %d.%d preempting previous ballot we had at ballot %d.%d",
			areply.AcceptorId, areply.Instance, areply.Cur.Number, areply.Cur.PropID, pCurBal.Number, pCurBal.PropID)
		r.InstanceManager.LearnOfBallot(&r.instanceSpace, areply.Instance, areply.Cur, areply.CurPhase)
		r.ProposedClientValuesManager.learnOfBallot(pbk, areply.Instance, areply.Cur)
		return
	}

	if !areply.Cur.Equal(areply.Req) {
		panic("????????/")
	}
	dlog.AgentPrintfN(r.Id, "Acceptance recorded on our proposal instance %d at ballot %d.%d from Replica %d with whose commands %d",
		areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId, areply.WhoseCmd)
	r.learnerHandleAcceptedValue(areply.Instance, areply.AcceptorId, areply.Cur, pbk.cmds, areply.WhoseCmd)
}

func (r *LWPReplica) proposerCloseCommit(inst int32, chosenAt stdpaxosproto.Ballot, chosenVal []*state.Command, whoseCmd int32) {
	r.InstanceManager.LearnBallotChosen(&r.instanceSpace, inst, chosenAt)
	pbk := r.instanceSpace[inst]
	r.ProposedClientValuesManager.valueChosen(pbk, inst, whoseCmd, chosenVal)

	setValue(pbk, whoseCmd, chosenAt, chosenVal)

	if whoseCmd == r.Id && pbk.clientProposals != nil {
		for _, l := range r.batchLearners {
			l.Learn(pbk.clientProposals)
		}
	}

	if whoseCmd == -1 {
		for _, l := range r.noopLearners {
			l.LearnNoop(inst, int32(chosenAt.PropID))
		}
	}

	if pbk.clientProposals != nil && !r.Dreply {
		// give client the all clear
		r.replyToNondurablyClients(pbk)
	}

	if r.Exec {
		r.executeCmds()
	}
}

func (r *LWPReplica) replyToNondurablyClients(pbk *ProposingBookkeeping) {
	for i := 0; i < len(pbk.cmds); i++ {
		r.replyToClientOfCmd(pbk, i, state.NIL())
	}
}

func (r *LWPReplica) replyToClientOfCmd(pbk *ProposingBookkeeping, i int, value state.Value) {
	proposals := pbk.clientProposals.GetProposals()
	propreply := &genericsmrproto.ProposeReplyTS{
		TRUE,
		proposals[i].CommandId,
		value,
		proposals[i].Timestamp}
	r.ReplyProposeTS(propreply, proposals[i].Reply, proposals[i].Mutex)
}

func (r *LWPReplica) executeCmds() {
	oldExecutedUpTo := r.executedUpTo
	for i := r.executedUpTo + 1; i <= r.CrtInstanceOracle.GetCrtInstance(); i++ {
		returnInst := r.instanceSpace[i]
		if returnInst != nil && returnInst.status == CLOSED { //&& returnInst.abk.cmds != nil {
			dlog.AgentPrintfN(r.Id, "Executing pbk %d with whose commands %d", i, returnInst.whoseCmds)

			if r.doStats {
				r.InstanceStats.RecordExecuted(stats.InstanceID{0, i}, time.Now())
				r.TimeseriesStats.Update("Instances Executed", 1)
				r.InstanceStats.OutputRecord(stats.InstanceID{0, i})
			}
			length := len(returnInst.cmds)
			for j := 0; j < length; j++ {
				dlog.Printf("Executing " + returnInst.cmds[j].String())
				if r.Dreply && returnInst != nil && returnInst.clientProposals != nil {
					val := returnInst.cmds[j].Execute(r.State)
					r.replyToClientOfCmd(returnInst, j, val)
					dlog.Printf("Returning executed client value")
				} else if returnInst.cmds[j].Op == state.PUT {
					returnInst.cmds[j].Execute(r.State)
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

func (r *LWPReplica) handleCommit(commit *stdpaxosproto.Commit) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	r.Acceptor.RecvCommitRemote(commit)
	r.proposerCloseCommit(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
}

func (r *LWPReplica) handleCommitShort(commit *stdpaxosproto.CommitShort) {
	inst := r.instanceSpace[commit.Instance]
	r.Acceptor.RecvCommitShortRemote(commit)
	if inst.cmds == nil {
		panic("We don't have any record of the value to be committed")
	}
	r.proposerCloseCommit(commit.Instance, commit.Ballot, inst.cmds, commit.WhoseCmd)
}

func (r *LWPReplica) handleState(state *proposerstate.State) {
	r.checkAndHandleNewlyReceivedInstance(state.CurrentInstance)
}
