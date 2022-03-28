package twophase

import (
	"acceptor"
	"clientproposalqueue"
	"dlog"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
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
	ProposalManager
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
	instanceSpace                 []*Instance // the space of all instances (used and not yet used)
	crtInstance                   int32       // highest active instance number that this replica knows about
	Shutdown                      bool
	counter                       int
	flush                         bool
	executedUpTo                  int32
	batchWait                     int
	maxBalInc                     int32
	maxOpenInstances              int32
	crtOpenedInstances            []int32
	proposableInstances           chan ProposalInfo
	clientValueQueue              clientproposalqueue.ClientProposalQueue
	noopWaitUs                    int32
	retryInstance                 chan RetryInfo
	BackoffManager                BackoffManager
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
	batchSize                     int
	requeueOnPreempt              bool
	doStats                       bool
	TimeseriesStats               *stats.TimeseriesStats
	InstanceStats                 *stats.InstanceStats
	ProposalStats                 *stats.ProposalStats
	batchedProps                  chan proposalBatch
	openInst                      chan struct{}
	sendProposerState             bool
	proposerState                 proposerstate.State
	acceptor.Acceptor
	stablestore.StableStore
	proactivelyPrepareOnPreempt bool
	noopWait                    time.Duration
	messageFilterIn             chan *messageFilterComm
	isAccMsgFilter              bool
}

type messageFilterComm struct {
	inst int32
	ret  chan bool
	//	from []int32
}

type messageFilterRoutine struct {
	aceptormessagefilter.AcceptorMessageFilter
	aid             int32
	messageFilterIn chan *messageFilterComm
}

func (m *messageFilterRoutine) startFilter() {
	for {
		//select {
		req := <-m.messageFilterIn
		req.ret <- m.AcceptorMessageFilter.ShouldFilterMessage(m.aid, req.inst)
		//	}
	}
}

func NewBaselineTwoPhaseReplica(propMan ProposalManager, id int, replica *genericsmr.Replica, durable bool, batchWait int, storageLoc string, maxOpenInstances int32,
	minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool, factor float64,
	whoCrash int32, whenCrash time.Duration, howlongCrash time.Duration, emulatedSS bool, emulatedWriteTime time.Duration,
	catchupBatchSize int32, timeout time.Duration, group1Size int, flushCommit bool, softFac bool, doStats bool,
	statsParentLoc string, commitCatchup bool, deadTime int32, batchSize int, constBackoff bool, requeueOnPreempt bool,
	tsStatsFilename string, instStatsFilename string, propsStatsFilename string, sendProposerState bool,
	proactivePrepareOnPreempt bool, batchingAcceptor bool, maxAccBatchWait time.Duration, filter aceptormessagefilter.AcceptorMessageFilter) *LWPReplica {
	retryInstances := make(chan RetryInfo, maxOpenInstances*10000)
	r := &LWPReplica{
		ProposalManager:             propMan,
		Replica:                     replica,
		stateChan:                   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		configChan:                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareChan:                 make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptChan:                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitChan:                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitShortChan:             make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareReplyChan:            make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptReplyChan:             make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		prepareRPC:                  0,
		acceptRPC:                   0,
		commitRPC:                   0,
		commitShortRPC:              0,
		prepareReplyRPC:             0,
		acceptReplyRPC:              0,
		instanceSpace:               make([]*Instance, 15*1024*1024),
		crtInstance:                 -1, //get from storage
		Shutdown:                    false,
		counter:                     0,
		flush:                       true,
		executedUpTo:                -1, //get from storage
		batchWait:                   batchWait,
		maxBalInc:                   10000,
		maxOpenInstances:            maxOpenInstances,
		crtOpenedInstances:          make([]int32, maxOpenInstances),
		proposableInstances:         make(chan ProposalInfo, MAXPROPOSABLEINST),
		noopWaitUs:                  noopwait,
		retryInstance:               retryInstances,
		BackoffManager:              NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, &retryInstances, factor, softFac, constBackoff),
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
		batchSize:                   batchSize,
		requeueOnPreempt:            requeueOnPreempt,
		doStats:                     doStats,
		batchedProps:                make(chan proposalBatch, 500),
		openInst:                    make(chan struct{}, maxOpenInstances),
		sendProposerState:           sendProposerState,
		noopWait:                    time.Duration(noopwait) * time.Microsecond,
		proactivelyPrepareOnPreempt: proactivePrepareOnPreempt,
		isAccMsgFilter:              filter != nil,
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

	if batchingAcceptor {
		r.StableStore = &ConcurrentFile{
			File:  r.StableStorage,
			Mutex: sync.Mutex{},
		}

		pids := make([]int32, r.N)
		for i, _ := range pids {
			pids[i] = int32(i)
		}
		r.Acceptor = acceptor.BetterBatchingAcceptorNew(r.StableStore, durable, emulatedSS,
			emulatedWriteTime, int32(id), maxAccBatchWait, pids, r.prepareReplyRPC, r.acceptReplyRPC, r.commitRPC, r.commitShortRPC)
	} else {
		r.StableStore = r.StableStorage
		r.Acceptor = acceptor.StandardAcceptorNew(r.StableStore, durable, emulatedSS, emulatedWriteTime, int32(id),
			r.prepareReplyRPC, r.acceptReplyRPC, r.commitRPC, r.commitShortRPC)
	}

	if r.doStats {
		r.TimeseriesStats = stats.TimeseriesStatsNew(stats.DefaultTSMetrics{}.Get(), statsParentLoc+fmt.Sprintf("/%s", tsStatsFilename), time.Second)

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
	}

	if group1Size <= r.N-r.F {
		r.group1Size = r.N - r.F
	} else {
		r.group1Size = group1Size
	}

	r.Durable = durable
	r.clientValueQueue = clientproposalqueue.ClientProposalQueueInit(r.ProposeChan)

	r.crtOpenedInstances = make([]int32, r.maxOpenInstances)

	for i := 0; i < len(r.crtOpenedInstances); i++ {
		r.crtOpenedInstances[i] = -1
		r.openInst <- struct{}{}
	}

	go r.run()
	return r
}

func (r *LWPReplica) CloseUp() {
	r.TimeseriesStats.Close()
	r.InstanceStats.Close()
	r.ProposalStats.CloseOutput()
}

func (r *LWPReplica) GetPBK(inst int32) *ProposingBookkeeping {
	return r.instanceSpace[inst].pbk
}

func (r *LWPReplica) recordNewConfig(config int32) {
	if !r.Durable {
		return
	}

	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(config))
	r.StableStore.WriteAt(b[:], 0)
}

func (r *LWPReplica) recordExecutedUpTo() {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(r.executedUpTo))
	r.StableStore.WriteAt(b[:], 4)
}

func (r *LWPReplica) replyPrepare(replicaId int32, reply *stdpaxosproto.PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *LWPReplica) replyAccept(replicaId int32, reply *stdpaxosproto.AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

/* Clock goroutine */
func (r *LWPReplica) fastClock() {
	for !r.Shutdown {
		time.Sleep(time.Duration(r.batchWait) * time.Millisecond) // ms
		dlog.Println("sending fast clock")
		fastClockChan <- true
	}
}

type proposalBatch interface {
	getProposals() []*genericsmr.Propose
	getCmds() []*state.Command
}

type batch struct {
	proposals []*genericsmr.Propose
	cmds      []*state.Command
	//getproposals func() []*genericsmr.Propose
	//getcmds func() []*state.Command
}

func (p *batch) getProposals() []*genericsmr.Propose {
	return p.proposals
}

func (p *batch) getCmds() []*state.Command {
	return p.cmds
}

func (r *LWPReplica) startBatching() {
	for !r.Shutdown {
		q := r.clientValueQueue.GetQueueChan()
		select {
		case v := <-q:
			time.Sleep(time.Duration(r.batchWait) * time.Millisecond)
			batchC := r.getProposalBatch(q, v)
			<-r.openInst
			r.batchedProps <- batchC
			dlog.Println("Client value(s) received beginning new instance")
		}
	}
}

func (r *LWPReplica) getProposalBatch(q chan *genericsmr.Propose, first *genericsmr.Propose) *batch {
	batchSize := min(r.batchSize, len(q)+1)
	clientProposals := make([]*genericsmr.Propose, batchSize) // todo change to interface of batch - get the command and the clent proposal
	clientCmds := make([]*state.Command, batchSize)
	clientProposals[0] = first
	clientCmds[0] = &first.Command
	for i := 1; i < batchSize; i++ {
		first = <-q
		clientProposals[i] = first
		clientCmds[i] = &first.Command
	}
	batchC := &batch{
		proposals: clientProposals,
		cmds:      clientCmds,
	}
	return batchC
}

func (r *LWPReplica) BatchingEnabled() bool {
	return r.batchWait > 0
}

/* ============= */
//
///* Main event processing loop */
//func (r *LWPReplica) restart() {
//	for cont := true; cont; {
//		select {
//		case <-r.prepareChan:
//		case <-r.stateChan:
//		case <-r.ProposeChan:
//		case <-r.retryInstance:
//		case <-r.acceptChan:
//		case <-r.acceptReplyChan:
//		case <-r.commitChan:
//		case <-r.commitShortChan:
//		case <-r.prepareReplyChan:
//		case <-r.configChan:
//		case <-r.proposableInstances:
//		case <-r.timeoutMsgs:
//			break
//		default:
//			cont = false
//			break
//		}
//	}
//
//	if r.doStats {
//		r.TimeseriesStats.Reset()
//	}
//
//	r.BackoffManager = NewBackoffManager(r.BackoffManager.minBackoff, r.BackoffManager.maxInitBackoff, r.BackoffManager.maxBackoff, &r.retryInstance, r.BackoffManager.factor, r.BackoffManager.softFac, r.BackoffManager.constBackoff)
//	r.catchingUp = true
//
//	r.recoveringFrom = r.executedUpTo + 1
//	r.nextRecoveryBatchPoint = r.recoveringFrom
//	r.sendNextRecoveryRequestBatch()
//}

//func (r *LWPReplica) sendNextRecoveryRequestBatch() {
//	// assumes r.nextRecoveryBatchPoint is initialised correctly
//	for i := int32(0); i < int32(r.N); i++ {
//		if i == r.Id {
//			continue
//		}
//		dlog.Printf("Sending next batch for recovery from %d to %d to acceptor %d", r.nextRecoveryBatchPoint, r.nextRecoveryBatchPoint+r.catchupBatchSize, i)
//		r.sendRecoveryRequest(r.nextRecoveryBatchPoint, i)
//		r.nextRecoveryBatchPoint += r.catchupBatchSize
//	}
//}
//
//func (r *LWPReplica) makeCatchupInstance(inst int32) {
//	r.instanceSpace[inst] = r.proposerMakeEmptyInstance()
//}
//
//func (r *LWPReplica) sendRecoveryRequest(fromInst int32, toAccptor int32) {
//	//for i := fromInst; i < fromInst + r.nextRecoveryBatchPoint; i++ {
//	r.makeCatchupInstance(fromInst)
//	r.sendSinglePrepare(fromInst, toAccptor)
//	//}
//}
//
//func (r *LWPReplica) checkAndHandlecatchupRequest(prepare *stdpaxosproto.Prepare) bool {
//	//config is ignored here and not acknowledged until new proposals are actually made
//	if prepare.IsZero() {
//		dlog.Printf("received catch up request from to instance %d to %d", prepare.Instance, prepare.Instance+r.catchupBatchSize)
//		panic("Not implemented")
//		//	r.checkAndHandleCommit(prepare.Instance, prepare.LeaderId, r.catchupBatchSize)
//		return true
//	} else {
//		return false
//	}
//}
//
////func (r *Replica) setNextcatchupPoint()
//func (r *LWPReplica) checkAndHandlecatchupResponse(commit *stdpaxosproto.Commit) {
//	if r.catchingUp {
//		//dlog.Printf("got catch up for %d", commit.Instance)
//		if r.crtInstance-r.executedUpTo <= r.maxOpenInstances && int32(r.instanceSpace[r.executedUpTo].pbk.maxKnownBal.PropID) == r.Id && r.executedUpTo > r.recoveringFrom { //r.crtInstance - r.executedUpTo <= r.maxOpenInstances {
//			r.catchingUp = false
//			dlog.Printf("Caught up with consensus group")
//			//reset client connections so that we can begin benchmarking again
//			r.Mutex.Lock()
//			for i := 0; i < len(r.Clients); i++ {
//				_ = r.Clients[i].Close()
//			}
//			r.Clients = make([]net.Conn, 0)
//			r.Mutex.Unlock()
//		} else {
//			//if commit.Instance == r.nextRecoveryBatchPoint {
//			if commit.Instance >= r.nextRecoveryBatchPoint-(r.catchupBatchSize/4) {
//				r.sendNextRecoveryRequestBatch()
//			}
//		}
//	}
//}

func (r *LWPReplica) run() {
	r.ConnectToPeers()
	r.RandomisePeerOrder()

	fastClockChan = make(chan bool, 1)
	if r.BatchingEnabled() {
		go r.startBatching()
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

	var stateGo time.Timer
	if r.sendProposerState {
		stateGo = *time.NewTimer(50 * time.Millisecond)
	}

	if r.BatchingEnabled() {
		for !r.Shutdown {
			select {
			case <-stateGo.C:
				for i := int32(0); i < int32(r.N); i++ {
					if i == r.Id {
						continue
					}
					msg := proposerstate.State{
						ProposerID:      r.Id,
						CurrentInstance: r.crtInstance,
					}
					r.SendMsg(i, r.stateChanRPC, &msg)
				}
				break
			case stateS := <-r.stateChan:
				recvState := stateS.(*proposerstate.State)
				r.handleState(recvState)
				break
			case props := <-r.batchedProps:
				r.beginNextInstance(props)
				break
			case <-c:
				r.TimeseriesStats.PrintAndReset()
				break
			case maybeTimedout := <-r.timeoutMsgs:
				r.retryBallot(maybeTimedout)
				break
			//case <-doner:
			//	dlog.Println("Crahsing")
			//	time.Sleep(r.howLongCrash)
			//	r.restart()
			//	dlog.Println("Done crashing")
			//	break
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
	} else {
		//	for !r.Shutdown {
		//		select {
		//		case <-c:
		//			r.TimeseriesStats.PrintAndReset()
		//			break
		//		case maybeTimedout := <-r.timeoutMsgs:
		//			r.retryBallot(maybeTimedout)
		//			break
		//		//case <-doner:
		//		//	dlog.Println("Crahsing")
		//		//	time.Sleep(r.howLongCrash)
		//		//	//r.restart()
		//		//	dlog.Println("Done crashing")
		//		//	break
		//		case next := <-r.retryInstance:
		//			dlog.Println("Checking whether to retry a proposal")
		//			r.tryNextAttempt(next)
		//			break
		//		case prepareS := <-r.prepareChan:
		//			prepare := prepareS.(*stdpaxosproto.Prepare)
		//			//got a Prepare message
		//			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
		//			//if !r.checkAndHandlecatchupRequest(prepare) {
		//			r.handlePrepare(prepare)
		//			//}
		//			break
		//		case acceptS := <-r.acceptChan:
		//			accept := acceptS.(*stdpaxosproto.Accept)
		//			//got an Accept message
		//			dlog.Printf("Received Accept Request from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
		//			r.handleAccept(accept)
		//			break
		//		case commitS := <-r.commitChan:
		//			commit := commitS.(*stdpaxosproto.Commit)
		//			//got a Commit message
		//			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
		//			r.handleCommit(commit)
		//			//r.checkAndHandlecatchupResponse(commit)
		//			break
		//		case commitS := <-r.commitShortChan:
		//			commit := commitS.(*stdpaxosproto.CommitShort)
		//			//got a Commit message
		//			dlog.Printf("Received short Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
		//			r.handleCommitShort(commit)
		//			break
		//		case prepareReplyS := <-r.prepareReplyChan:
		//			prepareReply := prepareReplyS.(*stdpaxosproto.PrepareReply)
		//			//got a Prepare reply
		//			dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
		//			r.handlePrepareReply(prepareReply)
		//			break
		//		case acceptReplyS := <-r.acceptReplyChan:
		//			acceptReply := acceptReplyS.(*stdpaxosproto.AcceptReply)
		//			//got an Accept reply
		//			dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
		//			r.handleAcceptReply(acceptReply)
		//			break
		//		default:
		//			break
		//		}
		//
		//		switch cliProp := r.clientValueQueue.TryDequeue(); {
		//		case cliProp != nil:
		//			numEnqueued := r.clientValueQueue.Len() + 1
		//			batchSize := min(numEnqueued, r.batchSize)
		//			clientProposals := make([]*genericsmr.Propose, batchSize)
		//			clientProposals[0] = cliProp
		//
		//			for i := 1; i < batchSize; i++ {
		//				cliProp = r.clientValueQueue.TryDequeue()
		//				if cliProp == nil {
		//					clientProposals = clientProposals[:i]
		//					break
		//				}
		//				clientProposals[i] = cliProp
		//			}
		//			dlog.Println("Client value(s) received beginning new instance")
		//			r.beginNextInstance(clientProposals)
		//		}
		//	}
		//}
	}
}

func (r *LWPReplica) retryBallot(maybeTimedout TimeoutInfo) {
	inst := r.instanceSpace[maybeTimedout.inst]
	if inst.pbk.propCurBal.Equal(maybeTimedout.proposingBal) && inst.pbk.status == maybeTimedout.phase {
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
		//log.Println("resending")
		inst.pbk.proposalInfos[inst.pbk.propCurBal].Broadcast(maybeTimedout.msgCode, maybeTimedout.msg)
		r.beginTimeout(maybeTimedout.inst, maybeTimedout.proposingBal, maybeTimedout.phase, r.timeout, maybeTimedout.msgCode, maybeTimedout.msg)
	}
}

func (r *LWPReplica) tryNextAttempt(next RetryInfo) {
	inst := r.instanceSpace[next.InstToPrep]
	if !next.backedoff {
		if inst == nil {
			r.instanceSpace[next.InstToPrep] = r.proposerMakeEmptyInstance()
			inst = r.instanceSpace[next.InstToPrep]
		}
	}

	if (r.BackoffManager.NoHigherBackoff(next) && inst.pbk.status == BACKING_OFF) || !next.backedoff {
		r.ProposalManager.StartNewProposal(r, next.InstToPrep)
		nextBallot := r.instanceSpace[next.InstToPrep].pbk.propCurBal
		done := r.Acceptor.RecvPrepareRemote(&stdpaxosproto.Prepare{
			LeaderId: r.Id,
			Instance: next.InstToPrep,
			Ballot:   nextBallot,
		})
		go func(instance int32, req stdpaxosproto.Ballot, c <-chan acceptor.Response) {
			msg, msgokie := <-c
			if !msgokie {
				return
			}

			if msg.GetType() != r.prepareReplyRPC {
				return
			}

			ser := msg.GetSerialisable()
			respPrep := ser.(*stdpaxosproto.PrepareReply)
			if respPrep.Cur.GreaterThan(req) {
				return
			}

			r.prepareReplyChan <- respPrep

		}(next.InstToPrep, nextBallot, done)
		dlog.Printf("Proposing next conf-bal %d.%d to instance %d\n", nextBallot.Number, nextBallot.PropID, next.InstToPrep)
		if r.doStats {
			r.ProposalStats.Open(stats.InstanceID{0, r.crtInstance}, nextBallot)
		}
	} else {
		dlog.Printf("Skipping retry of instance %d due to preempted again or closed\n", next.InstToPrep)
	}
}

func (r *LWPReplica) sendSinglePrepare(instance int32, to int32) {
	// cheats - should really be a special recovery message but lazzzzzyyyy
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()
	args := &stdpaxosproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurBal}
	dlog.Printf("send prepare to %d\n", to)
	r.SendMsg(to, r.prepareRPC, args)
	r.beginTimeout(args.Instance, args.Ballot, PREPARING, r.timeout*5, r.prepareRPC, args)
}

func (r *LWPReplica) beginTimeout(inst int32, attempted stdpaxosproto.Ballot, onWhatPhase ProposerStatus, timeout time.Duration, msgcode uint8, msg fastrpc.Serializable) {
	time.AfterFunc(timeout, func() {
		r.timeoutMsgs <- TimeoutInfo{
			ProposalInfo: ProposalInfo{inst, attempted},
			phase:        onWhatPhase,
			msgCode:      msgcode,
			msg:          msg,
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

	// is slower than median???
	if (r.N-1)%2 != 0 {
		return slowestLat > ewma[(len(ewma)-1)/2]
	} else {
		return slowestLat > (ewma[len(ewma)/2]+ewma[(len(ewma)/2)-1])/2
	}
}

func (r *LWPReplica) beginTracking(instID stats.InstanceID, ballot stdpaxosproto.Ballot, sentTo []int, trackingName string, proposalTrackingName string) {
	if len(sentTo) == r.N || r.isSlowestSlowerThanMedian(sentTo) {
		r.InstanceStats.RecordComplexStatStart(instID, trackingName, "Slow Quorum")
		r.ProposalStats.RecordOccurence(instID, ballot, proposalTrackingName+" Slow Quorum", 1)
	} else {
		r.InstanceStats.RecordComplexStatStart(instID, trackingName, "Fast Quorum")
		r.ProposalStats.RecordOccurence(instID, ballot, proposalTrackingName+" Fast Quorum", 1)
	}
}

func (r *LWPReplica) bcastPrepare(instance int32) {
	args := &stdpaxosproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurBal}
	pbk := r.instanceSpace[instance].pbk
	dlog.Println("sending prepare")

	sentTo := pbk.proposalInfos[pbk.propCurBal].Broadcast(r.prepareRPC, args)

	if r.doStats {
		instID := stats.InstanceID{
			Log: 0,
			Seq: instance,
		}
		r.InstanceStats.RecordOccurrence(instID, "My Phase 1 Proposals", 1)
		r.beginTracking(instID, args.Ballot, sentTo, "Phase 1", "Phase 1")
	}
	r.beginTimeout(args.Instance, args.Ballot, PREPARING, r.timeout, r.prepareRPC, args)
}

func (r *LWPReplica) bcastPrepareMsg(prepare *stdpaxosproto.Prepare) {
	pbk := r.instanceSpace[prepare.Instance].pbk
	dlog.Println("sending prepare")

	sentTo := pbk.proposalInfos[pbk.propCurBal].Broadcast(r.prepareRPC, prepare)

	if r.doStats {
		instID := stats.InstanceID{
			Log: 0,
			Seq: prepare.Instance,
		}
		r.InstanceStats.RecordOccurrence(instID, "My Phase 1 Proposals", 1)
		r.beginTracking(instID, prepare.Ballot, sentTo, "Phase 1", "Phase 1")
	}
	r.beginTimeout(prepare.Instance, prepare.Ballot, PREPARING, r.timeout, r.prepareRPC, prepare)
}

func (r *LWPReplica) bcastAcceptMsg(accept *stdpaxosproto.Accept) {
	pbk := r.instanceSpace[accept.Instance].pbk
	sentTo := pbk.proposalInfos[pbk.propCurBal].Broadcast(r.acceptRPC, accept)

	if r.doStats {
		instID := stats.InstanceID{
			Log: 0,
			Seq: accept.Instance,
		}
		r.InstanceStats.RecordOccurrence(instID, "My Phase 2 Proposals", 1)
		r.beginTracking(instID, accept.Ballot, sentTo, "Phase 2", "Phase 2")
	}

	r.beginTimeout(accept.Instance, accept.Ballot, PROPOSING, r.timeout, r.acceptRPC, accept)
}

func (r *LWPReplica) bcastAccept(instance int32) {
	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = r.instanceSpace[instance].pbk.propCurBal
	pa.Command = r.instanceSpace[instance].pbk.cmds
	pa.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
	args := &pa

	pbk := r.instanceSpace[instance].pbk
	sentTo := pbk.proposalInfos[pbk.propCurBal].Broadcast(r.acceptRPC, args)

	if r.doStats {
		instID := stats.InstanceID{
			Log: 0,
			Seq: instance,
		}
		r.InstanceStats.RecordOccurrence(instID, "My Phase 2 Proposals", 1)
		r.beginTracking(instID, args.Ballot, sentTo, "Phase 2", "Phase 2")
	}

	r.beginTimeout(args.Instance, args.Ballot, PROPOSING, r.timeout, r.acceptRPC, args)
}

func (r *LWPReplica) bcastCommitToAll(instance int32, Ballot stdpaxosproto.Ballot, command []*state.Command) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = Ballot
	pc.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
	pc.MoreToCome = 0
	pc.Command = command

	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = Ballot
	pcs.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
	pcs.Count = int32(len(command))
	argsShort := pcs

	r.CalculateAlive()
	sent := 0
	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id { //todo update others with this
			continue
		}
		inQrm := r.instanceSpace[instance].pbk.proposalInfos[Ballot].HasAcknowledged(int(q))
		if inQrm {
			r.SendMsg(q, r.commitShortRPC, &argsShort)
		} else {
			r.SendMsg(q, r.commitRPC, &pc)
		}
		sent++
	}
}

func (r *LWPReplica) incToNextOpenInstance() {
	r.crtInstance++
}

func (r *LWPReplica) proposerMakeEmptyInstance() *Instance {
	return &Instance{
		pbk: &ProposingBookkeeping{
			status:        NOT_BEGUN,
			proposalInfos: make(map[stdpaxosproto.Ballot]quorumsystem.SynodQuorumSystem),

			maxKnownBal:     stdpaxosproto.Ballot{-1, -1},
			proposeValueBal: stdpaxosproto.Ballot{-1, -1},
			whoseCmds:       -1,
			cmds:            nil,
			propCurBal:      stdpaxosproto.Ballot{-1, -1},
			clientProposals: nil,
		},
	}
}

func (r *LWPReplica) beginNextInstance(valsToPropose proposalBatch) {
	r.incToNextOpenInstance()
	r.instanceSpace[r.crtInstance] = r.proposerMakeEmptyInstance()
	curInst := r.instanceSpace[r.crtInstance]
	r.ProposalManager.StartNewProposal(r, r.crtInstance)

	for i := 0; i < len(r.crtOpenedInstances); i++ {
		if r.crtOpenedInstances[i] == -1 {
			r.crtOpenedInstances[i] = r.crtInstance
			break
		}
		if i == len(r.crtOpenedInstances) {
			panic("aslkfjlkefj")
		}
	}

	if r.doStats {
		r.InstanceStats.RecordOpened(stats.InstanceID{0, r.crtInstance}, time.Now())
		r.TimeseriesStats.Update("Instances Opened", 1)
		r.ProposalStats.Open(stats.InstanceID{0, r.crtInstance}, curInst.pbk.propCurBal)
	}

	r.instanceSpace[r.crtInstance].pbk.clientProposals = valsToPropose
	prepMsg := &stdpaxosproto.Prepare{
		LeaderId: r.Id,
		Instance: r.crtInstance,
		Ballot:   curInst.pbk.propCurBal,
	}
	resp := r.Acceptor.RecvPrepareRemote(prepMsg)

	go func(prepMsg *stdpaxosproto.Prepare, c <-chan acceptor.Response) {
		msg, msgokie := <-c
		if !msgokie {
			return
		}

		if msg.GetType() != r.prepareReplyRPC {
			return
		}

		prepResp := msg.GetSerialisable().(*stdpaxosproto.PrepareReply)

		if prepResp.Cur.GreaterThan(prepMsg.Ballot) {
			return
		}

		r.prepareReplyChan <- prepResp
	}(prepMsg, resp)

	dlog.Printf("Opened new instance %d\n", r.crtInstance)
}

func (r *LWPReplica) handlePropose(propose *genericsmr.Propose) {
	dlog.Printf("Received new client value\n")
	r.clientValueQueue.TryEnqueue(propose)
	//check if any open instances
}

func (r *LWPReplica) proposerCheckAndHandlePreempt(inst int32, preemptingBallot stdpaxosproto.Ballot, preemptingPhase stdpaxosproto.Phase) bool {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if pbk.status == CLOSED {
		return false
	}

	if pbk.propCurBal.GreaterThan(preemptingBallot) || pbk.propCurBal.Equal(preemptingBallot) {
		return false
	}

	if pbk.status != BACKING_OFF && r.doStats {
		id := stats.InstanceID{Log: 0, Seq: inst}
		if pbk.status == PREPARING || pbk.status == READY_TO_PROPOSE {
			r.InstanceStats.RecordOccurrence(id, "My Phase 1 Preempted", 1)
			//r.InstanceStats.RecordComplexStatEnd(id, "Phase 1", "Failure")
			r.TimeseriesStats.Update("My Phase 1 Preempted", 1)
			r.ProposalStats.CloseAndOutput(id, pbk.propCurBal, stats.HIGHERPROPOSALONGOING)
		} else if pbk.status == PROPOSING {
			r.InstanceStats.RecordOccurrence(id, "My Phase 2 Preempted", 1)
			//r.InstanceStats.RecordComplexStatEnd(id, "Phase 2", "Failure")
			r.TimeseriesStats.Update("My Phase 2 Preempted", 1)
			r.ProposalStats.CloseAndOutput(id, pbk.propCurBal, stats.HIGHERPROPOSALONGOING)
		}
	}
	//if preemptingBallot.GreaterThan(pbk.propCurBal) && pbk.status != BACKING_OFF {
	//	r.ProposalStats.CloseAndOutput(stats.InstanceID{0, r.crtInstance}, pbk.propCurBal, stats.HIGHERPROPOSALONGOING)
	//}
	pbk.status = BACKING_OFF
	if preemptingBallot.GreaterThan(pbk.maxKnownBal) {
		pbk.maxKnownBal = preemptingBallot
	}

	r.BackoffManager.CheckAndHandleBackoff(inst, pbk.propCurBal, preemptingBallot, preemptingPhase)

	if pbk.status == PREPARING && pbk.clientProposals != nil {
		r.requeueClientProposals(inst)
		pbk.clientProposals = nil
	}
	return true
}

func (r *LWPReplica) handlePrepare(prepare *stdpaxosproto.Prepare) {
	r.checkAndHandleNewlyReceivedInstance(prepare.Instance)
	if r.ProposalManager.IsInQrm(prepare.Instance, r.Id) {
		response := r.Acceptor.RecvPrepareRemote(prepare)
		go func(response <-chan acceptor.Response) {
			for msg := range response {
				if r.isAccMsgFilter {
					if msg.IsNegative() {
						c := make(chan bool, 1)
						r.messageFilterIn <- &messageFilterComm{
							inst: prepare.Instance,
							ret:  c,
						}
						if yes := <-c; yes {
							r.SendMsg(msg.ToWhom(), msg.GetType(), msg)
							return
						}
					}
				}
				r.SendMsg(msg.ToWhom(), msg.GetType(), msg)
			}
		}(response)
	}
	r.proposerCheckAndHandlePreempt(prepare.Instance, prepare.Ballot, stdpaxosproto.PROMISE)

}

func (r *LWPReplica) proposerCheckAndHandleAcceptedValue(inst int32, aid int32, accepted stdpaxosproto.Ballot, val []*state.Command, whoseCmds int32) ProposerAccValHandler {
	if accepted.IsZero() {
		return IGNORED
	}
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	newVal := false

	if pbk.status == CLOSED {
		return CHOSEN
	}

	_, exists := pbk.proposalInfos[accepted]
	if !exists {
		r.ProposalManager.trackProposalAcceptance(r, inst, accepted)
	}
	pbk.proposalInfos[accepted].AddToQuorum(int(aid))
	dlog.Printf("Acceptance on instance %d at conf-round %d.%d by acceptor %d", inst, accepted.Number, accepted.PropID, aid)

	if accepted.GreaterThan(pbk.maxKnownBal) {
		pbk.maxKnownBal = accepted
	}
	if accepted.GreaterThan(pbk.proposeValueBal) {
		newVal = true
		pbk.whoseCmds = whoseCmds
		pbk.proposeValueBal = accepted
		pbk.cmds = val

		// todo should we clear previous proposal?????????????

		if r.whatHappenedToClientProposals(inst) == ProposedButNotChosen {
			//r.requeueClientProposals(inst)
			//pbk.clientProposals = nil
		}
	}

	// not assumed local acceptor has accepted it
	if pbk.proposalInfos[accepted].QuorumReached() {
		r.bcastCommitToAll(inst, accepted, val)
		if int32(accepted.PropID) != r.Id {
			//	panic("commit short not our stuff")
			cmt := &stdpaxosproto.Commit{
				LeaderId:   int32(accepted.PropID),
				Instance:   inst,
				Ballot:     accepted,
				WhoseCmd:   whoseCmds,
				MoreToCome: 0,
				Command:    val,
			}
			done := r.Acceptor.RecvCommitRemote(cmt)
			<-done
		} else {
			cmt := &stdpaxosproto.CommitShort{
				LeaderId: int32(accepted.PropID),
				Instance: inst,
				Ballot:   accepted,
				WhoseCmd: whoseCmds,
			}
			done := r.Acceptor.RecvCommitShortRemote(cmt)
			<-done
		}
		r.proposerCloseCommit(inst, accepted, pbk.cmds, whoseCmds)
		return CHOSEN
	} else if newVal {
		return NEW_VAL
	} else {
		return ACKED
	}
}

func (r *LWPReplica) handlePrepareReply(preply *stdpaxosproto.PrepareReply) {
	inst := r.instanceSpace[preply.Instance]
	pbk := inst.pbk
	if pbk.status == CLOSED {
		return
	}

	// check if there is a value and track it
	//	r.proposerCheckAndHandleAcceptedValue(preply.Instance, preply.AcceptorId, preply.VBal, preply.Command, preply.WhoseCmd)
	valWhatDone := r.proposerCheckAndHandleAcceptedValue(preply.Instance, int32(preply.VBal.PropID), preply.VBal, preply.Command, preply.WhoseCmd)

	if valWhatDone == NEW_VAL {
		dlog.Printf("Promise from %d in instance %d has new value at Config-Ballot %d.%d", preply.AcceptorId,
			preply.Instance, preply.VBal.Number, preply.VBal.PropID)
	} else if valWhatDone == CHOSEN {
		dlog.Printf("Preparing instance recognised as chosen (instance %d), returning commit \n", preply.Instance)
		return
	}

	if pbk.propCurBal.GreaterThan(preply.Req) || pbk.status != PREPARING {
		dlog.Printf("Message in late \n")
		return
	}

	if preply.Req.GreaterThan(pbk.propCurBal) {
		panic("Some how got a promise on a future proposal")
	}

	if preply.Req.GreaterThan(preply.Cur) {
		panic("somehow acceptor did not promise us")
	}

	if preply.Cur.GreaterThan(preply.Req) {
		isNewPreempted := r.proposerCheckAndHandlePreempt(preply.Instance, preply.Cur, stdpaxosproto.PROMISE)
		if r.proactivelyPrepareOnPreempt && isNewPreempted && int32(preply.Req.PropID) != r.Id {
			newPrep := &stdpaxosproto.Prepare{
				LeaderId: int32(preply.Cur.PropID),
				Instance: preply.Instance,
				Ballot:   preply.Cur,
			}
			responseC := r.Acceptor.RecvPrepareRemote(newPrep)
			dlog.Printf("Another active proposer using config-ballot %d.%d.%d greater than mine\n", preply.Cur)
			go func(responseC <-chan acceptor.Response) {
				for msgResp := range responseC {
					if r.isAccMsgFilter {
						if msgResp.IsNegative() {
							c := make(chan bool, 1)
							r.messageFilterIn <- &messageFilterComm{
								inst: newPrep.Instance,
								ret:  c,
							}
							if yes := <-c; yes {
								r.SendMsg(msgResp.ToWhom(), msgResp.GetType(), msgResp)
								return
							}
						}
					}
					r.SendMsg(msgResp.ToWhom(), msgResp.GetType(), msgResp)
				}
			}(responseC)
		}
		return
	}

	qrm := pbk.proposalInfos[pbk.propCurBal]
	qrm.AddToQuorum(int(preply.AcceptorId))

	if int32(preply.Req.PropID) == r.Id && preply.AcceptorId == r.Id { // my proposal
		r.bcastPrepare(preply.Instance)
		return
	}

	dlog.Printf("Added replica's %d promise to qrm", preply.AcceptorId)
	if qrm.QuorumReached() {
		id := stats.InstanceID{
			Log: 0,
			Seq: preply.Instance,
		}
		if r.doStats {
			r.InstanceStats.RecordComplexStatEnd(id, "Phase 1", "Success")
		}
		r.tryPropose(preply.Instance)
	}
}

func (r *LWPReplica) tryPropose(inst int32) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	pbk.status = READY_TO_PROPOSE
	dlog.Println("Can now tryPropose in instance", inst)
	qrm := pbk.proposalInfos[pbk.propCurBal]
	qrm.StartAcceptanceQuorum()

	if pbk.proposeValueBal.IsZero() {
		if pbk.cmds != nil {
			panic("there must be a previously chosen value")
		}

		if pbk.clientProposals != nil {
			pbk.whoseCmds = r.Id
			pbk.cmds = pbk.clientProposals.getCmds()
			dlog.Printf("%d client value(s) proposed in instance %d\n", len(pbk.clientProposals.getCmds()), inst)
		} else {
			q := r.clientValueQueue.GetQueueChan()
			select {
			case v := <-q:
				batchToPropose := r.getProposalBatch(q, v)
				pbk.cmds = batchToPropose.getCmds()
				pbk.whoseCmds = r.Id

				if r.doStats {
					r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Client Value Proposed", 1)
					r.ProposalStats.RecordClientValuesProposed(stats.InstanceID{0, inst}, pbk.propCurBal, len(pbk.cmds))
					r.TimeseriesStats.Update("Times Client Values Proposed", 1)
				}

				dlog.Printf("%d client value(s) received and proposed in instance %d which was recovered \n", len(pbk.clientProposals.getCmds()), inst)
				break
			default:
				if r.shouldNoop(inst) {
					if r.doStats {
						r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Noop Proposed", 1)
						r.TimeseriesStats.Update("Times Noops Proposed", 1)
						r.ProposalStats.RecordNoopProposed(stats.InstanceID{0, inst}, pbk.propCurBal)
					}
					pbk.cmds = state.NOOPP()
					dlog.Println("Proposing noop in recovered instance", inst)
				} else {
					time.AfterFunc(r.noopWait, func() {
						r.proposableInstances <- ProposalInfo{
							inst:         inst,
							proposingBal: pbk.propCurBal,
						}
					})
					return
				}
			}
		}
	} else {
		if r.doStats {
			r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Previous Value Proposed", 1)
			r.TimeseriesStats.Update("Times Previous Value Proposed", 1)
			r.ProposalStats.RecordPreviousValueProposed(stats.InstanceID{0, inst}, pbk.propCurBal)
		}
	}

	//r.proposerCheckAndHandleAcceptedValue(inst, r.Id, pbk.propCurBal, pbk.cmds, pbk.whoseCmds)
	// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
	pbk.status = PROPOSING
	pbk.proposeValueBal = pbk.propCurBal

	acptMsg := &stdpaxosproto.Accept{
		LeaderId: r.Id,
		Instance: inst,
		Ballot:   pbk.propCurBal,
		WhoseCmd: pbk.whoseCmds,
		Command:  pbk.cmds,
	}

	c := r.Acceptor.RecvAcceptRemote(acptMsg)
	go func(c <-chan acceptor.Response, acptMsg *stdpaxosproto.Accept) {
		if msg, msgokie := <-c; msgokie {
			if msg.GetType() != r.acceptReplyRPC {
				return
			}
			acc := msg.GetSerialisable().(*stdpaxosproto.AcceptReply)
			if acc.Cur.GreaterThan(acc.Req) {
				return
			}
			r.acceptReplyChan <- acc
		}
	}(c, acptMsg)
}

func (r *LWPReplica) recheckInstanceToPropose(proposalInfo ProposalInfo) {
	inst := r.instanceSpace[proposalInfo.inst]
	pbk := inst.pbk
	if pbk == nil {
		return
	}

	if pbk.propCurBal.GreaterThan(proposalInfo.proposingBal) || pbk.status == READY_TO_PROPOSE {
		return
	}

	r.tryPropose(proposalInfo.inst)
}

func (r *LWPReplica) shouldNoop(inst int32) bool {
	if r.alwaysNoop {
		return true
	}

	for i := inst + 1; i < r.crtInstance; i++ {
		if r.instanceSpace[i] == nil {
			continue
		}
		if r.instanceSpace[i].pbk.status == CLOSED {
			return true
		}
	}
	return false
}

func (r *LWPReplica) checkAndHandleNewlyReceivedInstance(instance int32) {
	if instance < 0 {
		dlog.Printf("Got instance %d from proposer who has not begun an instance yet", instance)
		return
	}
	inst := r.instanceSpace[instance]
	if inst == nil {
		if instance > r.crtInstance {
			r.crtInstance = instance
		}
		r.instanceSpace[instance] = r.proposerMakeEmptyInstance()
	}
}

func (r *LWPReplica) handleAccept(accept *stdpaxosproto.Accept) {
	//start := time.Now()
	if r.ProposalManager.IsInQrm(accept.Instance, r.Id) {
		r.checkAndHandleNewlyReceivedInstance(accept.Instance)
		responseC := r.Acceptor.RecvAcceptRemote(accept)

		go func(responseC <-chan acceptor.Response) {
			for resp := range responseC {
				if resp.GetType() != r.acceptReplyRPC && resp.GetType() != r.commitRPC {
					panic("what message have we got here????")
				}
				if r.isAccMsgFilter {
					if resp.IsNegative() {
						c := make(chan bool, 1)
						r.messageFilterIn <- &messageFilterComm{
							inst: accept.Instance,
							ret:  c,
						}
						if yes := <-c; yes {
							r.SendMsg(resp.ToWhom(), resp.GetType(), resp)
							return
						}
					}
				}
				r.SendMsg(resp.ToWhom(), resp.GetType(), resp)
			}
		}(responseC)
	}

	//	r.proposerCheckAndHandleAcceptedValue(accept.Instance, int32(accept.PropID), accept.Ballot, accept.Command, accept.WhoseCmd) // must go first as acceptor might have already learnt of value
	accValState := r.proposerCheckAndHandleAcceptedValue(accept.Instance, r.Id, accept.Ballot, accept.Command, accept.WhoseCmd)
	if accValState == CHOSEN {
		return
	}
	r.proposerCheckAndHandlePreempt(accept.Instance, accept.Ballot, stdpaxosproto.ACCEPTANCE)

}

func (r *LWPReplica) handleAcceptReply(areply *stdpaxosproto.AcceptReply) {
	//start := time.Now()

	// could modify to have record of all ballots
	inst := r.instanceSpace[areply.Instance]
	pbk := inst.pbk
	if pbk.status == CLOSED {
		dlog.Printf("Already committed ")
		return
	}

	if pbk.propCurBal.GreaterThan(areply.Req) {
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
		r.proposerCheckAndHandlePreempt(areply.Instance, areply.Cur, areply.CurPhase)
		return
	}

	dlog.Printf("Acceptance of instance %d at %d.%d by Acceptor %d received\n", areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId)
	r.proposerCheckAndHandleAcceptedValue(areply.Instance, areply.AcceptorId, areply.Cur, pbk.cmds, areply.WhoseCmd)

	// my acceptor has accepted it so now should forward on to rest of acceptors
	if int32(areply.Req.PropID) == r.Id && areply.AcceptorId == r.Id {
		r.bcastAccept(areply.Instance)
	}
	//elapsed := time.Since(start)
	//log.Println("Handling accept reply took ", elapsed)
}

func (r *LWPReplica) requeueClientProposals(instance int32) {
	inst := r.instanceSpace[instance]
	dlog.Printf("Requeing client values in instance %d", instance)

	if r.doStats && inst.pbk.clientProposals != nil {
		r.TimeseriesStats.Update("Requeued Client Values", 1)
	}

	if r.batchWait > 0 {
		go func(propose proposalBatch) {
			<-r.openInst
			r.batchedProps <- propose
		}(inst.pbk.clientProposals)
	} else {
		//for i := 0; i < len(inst.pbk.clientProposals); i++ {
		//	r.clientValueQueue.TryRequeue(inst.pbk.clientProposals[i])
		//}
	}

}

func (r *LWPReplica) whatHappenedToClientProposals(instance int32) ClientProposalStory {
	inst := r.instanceSpace[instance]
	pbk := inst.pbk
	if pbk.whoseCmds != r.Id && pbk.clientProposals != nil {
		return ProposedButNotChosen
	} else if pbk.whoseCmds == r.Id {
		return ProposedAndChosen
	} else {
		return NotProposed
	}
}

func (r *LWPReplica) howManyAttemptsToChoose(inst int32) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	attempts := pbk.proposeValueBal.Number / r.maxBalInc
	dlog.Printf("Attempts to chose instance %d: %d", inst, attempts)
}

func (r *LWPReplica) proposerCloseCommit(inst int32, chosenAt stdpaxosproto.Ballot, chosenVal []*state.Command, whoseCmd int32) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if instance.pbk.status == CLOSED {
		panic("asldkfjalksdfj")
	}

	pbk.status = CLOSED
	dlog.Printf("Instance %d chosen now\n", inst)
	if r.doStats && pbk.status != BACKING_OFF && !pbk.propCurBal.IsZero() {
		if pbk.propCurBal.GreaterThan(chosenAt) {
			r.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.propCurBal, stats.LOWERPROPOSALCHOSEN)
		} else if chosenAt.GreaterThan(pbk.propCurBal) {
			r.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.propCurBal, stats.HIGHERPROPOSALONGOING)
		} else {
			r.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.propCurBal, stats.ITWASCHOSEN)
		}
	}
	r.BackoffManager.ClearBackoff(inst)

	if int32(chosenAt.PropID) == r.Id {
		r.timeSinceLastProposedInstance = time.Now()
	}

	if chosenAt.Equal(instance.pbk.propCurBal) && r.doStats {
		r.InstanceStats.RecordComplexStatEnd(stats.InstanceID{0, inst}, "Phase 2", "Success")
	}

	pbk.proposeValueBal = chosenAt
	pbk.cmds = chosenVal
	pbk.whoseCmds = whoseCmd

	switch r.whatHappenedToClientProposals(inst) {
	case NotProposed:
		break
	case ProposedButNotChosen:
		dlog.Printf("%d client value(s) proposed in instance %d\n not chosen", len(pbk.clientProposals.getCmds()), inst)
		r.requeueClientProposals(inst)
		pbk.clientProposals = nil
		break
	case ProposedAndChosen:
		dlog.Printf("%d client value(s) chosen in instance %d\n", len(pbk.clientProposals.getCmds()), inst)
		break
	}

	for i := 0; i < len(r.crtOpenedInstances); i++ {
		if r.crtOpenedInstances[i] == inst {
			r.crtOpenedInstances[i] = -1
			r.openInst <- struct{}{}
			break
		}
		if i == len(r.crtOpenedInstances) {
			panic("aslkfjlkefj")
		}
	}

	if r.doStats {
		balloter := r.ProposalManager.getBalloter()
		atmts := balloter.GetAttemptNumber(chosenAt.Number)
		r.InstanceStats.RecordCommitted(stats.InstanceID{0, inst}, atmts, time.Now())
		r.TimeseriesStats.Update("Instances Learnt", 1)
		if int32(chosenAt.PropID) == r.Id {
			r.TimeseriesStats.Update("Instances I Choose", 1)
			r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "I Chose", 1)
		}
		if !r.Exec {
			r.InstanceStats.OutputRecord(stats.InstanceID{0, inst})
		}
	}

	if pbk.clientProposals != nil && !r.Dreply {
		// give client the all clear
		for i := 0; i < len(pbk.cmds); i++ {
			proposals := pbk.clientProposals.getProposals()
			propreply := &genericsmrproto.ProposeReplyTS{
				TRUE,
				proposals[i].CommandId,
				state.NIL(),
				proposals[i].Timestamp}
			r.ReplyProposeTS(propreply, proposals[i].Reply, proposals[i].Mutex)
		}
	}
	//r.howManyAttemptsToChoose(inst)

	if r.Exec {
		oldExecutedUpTo := r.executedUpTo
		for i := r.executedUpTo + 1; i <= r.crtInstance; i++ {
			returnInst := r.instanceSpace[i]
			if returnInst != nil && returnInst.pbk.status == CLOSED { //&& returnInst.abk.cmds != nil {
				dlog.Printf("Executing instance %d\n", i)

				if r.doStats {
					r.InstanceStats.RecordExecuted(stats.InstanceID{0, i}, time.Now())
					r.TimeseriesStats.Update("Instances Executed", 1)
					r.InstanceStats.OutputRecord(stats.InstanceID{0, i})
				}
				length := len(returnInst.pbk.cmds)
				for j := 0; j < length; j++ {
					dlog.Printf("Executing " + returnInst.pbk.cmds[j].String())
					if r.Dreply && returnInst.pbk != nil && returnInst.pbk.clientProposals != nil {
						val := returnInst.pbk.cmds[j].Execute(r.State)
						proposals := returnInst.pbk.clientProposals.getProposals()
						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							proposals[j].CommandId,
							val,
							proposals[j].Timestamp}
						r.ReplyProposeTS(propreply, proposals[j].Reply, proposals[j].Mutex)
						dlog.Printf("Returning executed client value")
					} else if returnInst.pbk.cmds[j].Op == state.PUT {
						returnInst.pbk.cmds[j].Execute(r.State)
					}
				}
				r.executedUpTo += 1
				dlog.Printf("Executed up to %d (crtInstance=%d)", r.executedUpTo, r.crtInstance)
			} else {
				if r.executedUpTo > oldExecutedUpTo {
					r.recordExecutedUpTo()
				}
				break
			}
		}
	}
}

func (r *LWPReplica) handleCommit(commit *stdpaxosproto.Commit) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	inst := r.instanceSpace[commit.Instance]

	if inst.pbk.status == CLOSED {
		dlog.Printf("Already committed \n")
		return
	}

	if inst.pbk.status == CLOSED {
		panic("asdfjsladfkjal;kejf")
	}

	//	r.acceptorCommit(commit.Instance, commit.Ballot, commit.Command)
	//	r.
	done := r.Acceptor.RecvCommitRemote(commit)
	<-done
	r.proposerCloseCommit(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
}

func (r *LWPReplica) handleCommitShort(commit *stdpaxosproto.CommitShort) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	inst := r.instanceSpace[commit.Instance]

	if inst.pbk.status == CLOSED {
		dlog.Printf("Already committed \n")
		return
	}

	done := r.Acceptor.RecvCommitShortRemote(commit)
	<-done

	if inst.pbk.cmds == nil {
		panic("We don't have any record of the value to be committed")
	}
	r.proposerCloseCommit(commit.Instance, commit.Ballot, inst.pbk.cmds, commit.WhoseCmd)
}

func (r *LWPReplica) handleState(state *proposerstate.State) {
	r.checkAndHandleNewlyReceivedInstance(state.CurrentInstance)
}
