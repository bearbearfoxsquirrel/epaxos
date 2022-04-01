package twophase

//
//import (
//	"clientproposalqueue"
//	"dlog"
//	"encoding/binary"
//	"fastrpc"
//	"fmt"
//	"genericsmr"
//	"genericsmrproto"
//	"io"
//	"log"
//	"net"
//	"proposerstate"
//	"quorumsystem"
//	"state"
//	"stats"
//	"stdpaxosproto"
//	"time"
//)
//
//package twophase
//
//import (
//"clientproposalqueue"
//"dlog"
//"encoding/binary"
//"fastrpc"
//"fmt"
//"genericsmr"
//"genericsmrproto"
//"io"
//"log"
//"net"
////	"proposalssatiated"
//"proposerstate"
//"quorumsystem"
//"state"
//"stats"
//"stdpaxosproto"
//"time"
//)
//
//type LWPReplica struct {
//	ProposalManager
//	*genericsmr.Replica// extends a generic Paxos replica
//	configChan          chan fastrpc.Serializable
//	prepareChan         chan fastrpc.Serializable
//	acceptChan          chan fastrpc.Serializable
//	commitChan          chan fastrpc.Serializable
//	commitShortChan     chan fastrpc.Serializable
//	prepareReplyChan    chan fastrpc.Serializable
//	acceptReplyChan     chan fastrpc.Serializable
//	stateChan           chan fastrpc.Serializable
//	//instancesToRecover  chan int32
//	stateChanRPC                  uint8
//	prepareRPC                    uint8
//	acceptRPC                     uint8
//	commitRPC                     uint8
//	commitShortRPC                uint8
//	prepareReplyRPC               uint8
//	acceptReplyRPC                uint8
//	instanceSpace                 []*Instance // the space of all instances (used and not yet used)
//	crtInstance                   int32       // highest active instance number that this replica knows about
//	Shutdown                      bool
//	counter                       int
//	flush                         bool
//	executedUpTo                  int32
//	maxBatchWait                     int
//	maxBalInc                     int32
//	maxOpenInstances              int32
//	crtOpenedInstances            []int32
//	proposableInstances           chan ProposalInfo
//	clientValueQueue              clientproposalqueue.ClientProposalQueue
//	noopWaitUs                    int32
//	retryInstance                 chan RetryInfo
//	BackoffManager                BackoffManager
//	alwaysNoop                    bool
//	lastTimeClientChosen          time.Time
//	lastOpenProposalTime          time.Time
//	timeSinceLastProposedInstance time.Time
//	fastLearn                     bool
//	whenCrash                     time.Duration
//	howLongCrash                  time.Duration
//	whoCrash                      int32
//	emulatedSS                    bool
//	emulatedWriteTime             time.Duration
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
//	maxBatchSize                     int
//	requeueOnPreempt              bool
//	doStats                  bool
//	TimeseriesStats          *stats.TimeseriesStats
//	InstanceStats            *stats.InstanceStats
//	ProposalStats            *stats.ProposalStats
//	batchedProps             chan []*genericsmr.Propose
//	openInst                 chan struct{}
//	sendProposerState        bool
//	sendProposalsSatiated    bool
//}
//
//
//func NewBaselineTwoPhaseReplica(propMan ProposalManager, id int, replica *genericsmr.Replica, durable bool, maxBatchWait int, storageLoc string, maxOpenInstances int32,
//	minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool, factor float64,
//	whoCrash int32, whenCrash time.Duration, howlongCrash time.Duration, emulatedSS bool, emulatedWriteTime time.Duration,
//	catchupBatchSize int32, timeout time.Duration, group1Size int, flushCommit bool, softFac bool, doStats bool,
//	statsParentLoc string, commitCatchup bool, deadTime int32, maxBatchSize int, constBackoff bool, requeueOnPreempt bool,
//	tsStatsFilename string, instStatsFilename string, propsStatsFilename string, sendProposerState bool,
//	minimalProposerNumbers bool, bcastOnProposalsSatiated bool) *LWPReplica {
//	retryInstances := make(chan RetryInfo, maxOpenInstances*10000)
//	r := &LWPReplica{
//		ProposalManager:       propMan,
//		Replica:               replica,
//		stateChan:             make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		configChan:            make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		prepareChan:           make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		acceptChan:            make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		commitChan:            make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		commitShortChan:     make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		prepareReplyChan:    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
//		acceptReplyChan:     make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
//		prepareRPC:          0,
//		acceptRPC:           0,
//		commitRPC:           0,
//		commitShortRPC:      0,
//		prepareReplyRPC:     0,
//		acceptReplyRPC:      0,
//		instanceSpace:       make([]*Instance, 15*1024*1024),
//		crtInstance:         -1, //get from storage
//		Shutdown:            false,
//		counter:             0,
//		flush:               true,
//		executedUpTo:        -1, //get from storage
//		maxBatchWait:           maxBatchWait,
//		maxBalInc:           10000,
//		maxOpenInstances:    maxOpenInstances,
//		crtOpenedInstances:  make([]int32, maxOpenInstances),
//		proposableInstances: make(chan ProposalInfo, MAXPROPOSABLEINST),
//		noopWaitUs:          noopwait,
//		retryInstance:       retryInstances,
//		BackoffManager:      NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, &retryInstances, factor, softFac, constBackoff),
//		alwaysNoop:          alwaysNoop,
//		fastLearn:           false,
//		whoCrash:            whoCrash,
//		whenCrash:           whenCrash,
//		howLongCrash:        howlongCrash,
//		emulatedSS:          emulatedSS,
//		emulatedWriteTime:   emulatedWriteTime,
//		timeoutMsgs:         make(chan TimeoutInfo, 5000),
//		timeout:             timeout,
//		catchupBatchSize:    catchupBatchSize,
//		lastSettleBatchInst:   -1,
//		catchingUp:               false,
//		flushCommit:              flushCommit,
//		commitCatchUp:            commitCatchup,
//		maxBatchSize:                maxBatchSize,
//		requeueOnPreempt:         requeueOnPreempt,
//		doStats:                  doStats,
//		batchedProps:             make(chan []*genericsmr.Propose, 500),
//		openInst:                 make(chan struct{}, maxOpenInstances),
//		sendProposerState:        sendProposerState,
//		sendProposalsSatiated:    minimalProposerNumbers,
//	}
//
//	if r.doStats {
//		r.TimeseriesStats = stats.TimeseriesStatsNew(stats.DefaultTSMetrics{}.Get(), statsParentLoc+fmt.Sprintf("/%s", tsStatsFilename), time.Second)
//
//		phaseStarts := []string{"Fast Quorum", "Slow Quorum"}
//		phaseEnds := []string{"Success", "Failure"}
//		phaseRes := map[string][]string{
//			"Fast Quorum": phaseEnds,
//			"Slow Quorum": phaseEnds,
//		}
//		phaseNegs := map[string]string{
//			"Fast Quorum": "Failure",
//			"Slow Quorum": "Failure",
//		}
//		phaseCStats := []stats.MultiStartMultiOutStatConstructor{
//			{"Phase 1", phaseStarts, phaseRes, phaseNegs, true},
//			{"Phase 2", phaseStarts, phaseRes, phaseNegs, true},
//			//stats.MultiStartMultiOutStatNew(),
//			//stats.MultiStartMultiOutStatNew(),
//		}
//
//		r.InstanceStats = stats.InstanceStatsNew(statsParentLoc+fmt.Sprintf("/%s", instStatsFilename), stats.DefaultIMetrics{}.Get(), phaseCStats)
//		r.ProposalStats = stats.ProposalStatsNew(make([]string, 0, 10), statsParentLoc + fmt.Sprintf("/%s", propsStatsFilename))
//	}
//
//	if group1Size <= r.N-r.F {
//		r.group1Size = r.N - r.F
//	} else {
//		r.group1Size = group1Size
//	}
//
//	r.Durable = durable
//	r.clientValueQueue = clientproposalqueue.ClientProposalQueueInit(r.ProposeChan)
//
//	r.prepareRPC = r.RegisterRPC(new(stdpaxosproto.Prepare), r.prepareChan)
//	r.acceptRPC = r.RegisterRPC(new(stdpaxosproto.Accept), r.acceptChan)
//	r.commitRPC = r.RegisterRPC(new(stdpaxosproto.Commit), r.commitChan)
//	r.commitShortRPC = r.RegisterRPC(new(stdpaxosproto.CommitShort), r.commitShortChan)
//	r.prepareReplyRPC = r.RegisterRPC(new(stdpaxosproto.PrepareReply), r.prepareReplyChan)
//	r.acceptReplyRPC = r.RegisterRPC(new(stdpaxosproto.AcceptReply), r.acceptReplyChan)
//	//	if r.sendProposerState {
//	r.stateChanRPC = r.RegisterRPC(new(proposerstate.State), r.stateChan)
//	//}
//
//	r.crtOpenedInstances = make([]int32, r.maxOpenInstances)
//
//	for i := 0; i < len(r.crtOpenedInstances); i++ {
//		r.crtOpenedInstances[i] = -1
//		r.openInst <- struct{}{}
//	}
//
//	go r.run()
//	return r
//}
//
//func (r *LWPReplica) CloseUp() {
//	r.TimeseriesStats.Close()
//	r.InstanceStats.Close()
//	r.ProposalStats.CloseOutput()
//}
//
//func (r *LWPReplica) GetPBK(inst int32) *ProposingBookkeeping {
//	return r.instanceSpace[inst].pbk
//}
//
//func (r *LWPReplica) recordNewConfig(config int32) {
//	if !r.Durable {
//		return
//	}
//
//	var b [4]byte
//	binary.LittleEndian.PutUint32(b[0:4], uint32(config))
//	r.StableStorage.WriteAt(b[:], 0)
//}
//
//func (r *LWPReplica) recordExecutedUpTo() {
//	var b [4]byte
//	binary.LittleEndian.PutUint32(b[0:4], uint32(r.executedUpTo))
//	r.StableStorage.WriteAt(b[:], 4)
//}
//
////append a log entry to stable storage
//func (r *LWPReplica) recordInstanceMetadata(inst *Instance) {
//	if !r.Durable {
//		return
//	}
//
//	var b [12]byte
//	binary.LittleEndian.PutUint32(b[4:8], uint32(inst.abk.curBal.Number))
//	binary.LittleEndian.PutUint32(b[8:12], uint32(inst.abk.curBal.PropID))
//	_, _ = r.StableStorage.Write(b[:])
//}
//
////write a sequence of commands to stable storage
//func (r *LWPReplica) recordCommands(cmds []state.Command) {
//	if !r.Durable {
//		return
//	}
//
//	if cmds == nil {
//		return
//	}
//	for i := 0; i < len(cmds); i++ {
//		cmds[i].Marshal(io.Writer(r.StableStorage))
//	}
//}
//
////sync with the stable store
//func (r *LWPReplica) sync() {
//	if !r.Durable {
//		return
//	}
//	//dlog.Println("synced")
//	if r.emulatedSS {
//		time.Sleep(r.emulatedWriteTime)
//	} else {
//		_ = r.StableStorage.Sync()
//	}
//}
//
//func (r *LWPReplica) replyPrepare(replicaId int32, reply *stdpaxosproto.PrepareReply) {
//	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
//}
//
//func (r *LWPReplica) replyAccept(replicaId int32, reply *stdpaxosproto.AcceptReply) {
//	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
//}
//
///* Clock goroutine */
//
//func (r *LWPReplica) fastClock() {
//	for !r.Shutdown {
//		time.Sleep(time.Duration(r.maxBatchWait) * time.Millisecond) // ms
//		dlog.Println("sending fast clock")
//		fastClockChan <- true
//	}
//}
//
//func (r *LWPReplica) startBatching() {
//	for !r.Shutdown {
//		q := r.clientValueQueue.GetQueueChan()
//		select {
//		case c := <-q:
//			time.Sleep(time.Duration(r.maxBatchWait) * time.Millisecond)
//
//			maxBatchSize := min(r.maxBatchSize, len(q)+1)
//			clientProposals := make([]*genericsmr.Propose, maxBatchSize)
//			clientProposals[0] = c
//			for i := 1; i < maxBatchSize; i++ {
//				c = <-q
//				clientProposals[i] = c
//			}
//
//			<-r.openInst // await an open instance
//			r.batchedProps <- clientProposals
//			dlog.Println("Client value(s) received beginning new instance")
//		}
//	}
//}
//
//func (r *LWPReplica) BatchingEnabled() bool {
//	return r.maxBatchWait > 0
//}
//
///* ============= */
//
///* Main event processing loop */
//func (r *LWPReplica) restart() {
//	for cont := true; cont; {
//		select {
//		case <-r.prepareChan:
//		case<-r.stateChan:
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
//
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
//		r.checkAndHandleCommit(prepare.Instance, prepare.LeaderId, r.catchupBatchSize)
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
//		if r.crtInstance-r.executedUpTo <= r.maxOpenInstances && int32(r.instanceSpace[r.executedUpTo].abk.curBal.PropID) == r.Id && r.executedUpTo > r.recoveringFrom { //r.crtInstance - r.executedUpTo <= r.maxOpenInstances {
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
//
////func (r *LWPReplica) doHeartbeat() {
////	heartBeat := stdpaxosproto.Prepare{
////		LeaderId: r.Id,
////		Instance: -2,
////		Ballot:   stdpaxosproto.Ballot{-1, -1},
////	}
////	for i := 0; i < r.N-1; i++ {
////		if r.PreferredPeerOrder[i] == r.Id {
////			continue
////		}
////		r.SendMsg(r.PreferredPeerOrder[i], r.prepareRPC, &heartBeat)
////	}
////
////	go func() {
////		timer := time.NewTimer(100 * time.Millisecond)
////		<-timer.C
////		r.goHeartbeat <- struct{}{}
////	}()
////}
//
//func (r *LWPReplica) run() {
//	r.ConnectToPeers()
//	r.RandomisePeerOrder()
//
//	fastClockChan = make(chan bool, 1)
//	//Enabled fast clock when startBatching
//	if r.BatchingEnabled() {
//		//	go r.fastClock()
//		go r.startBatching()
//	}
//	//	onOffProposeChan := r.ProposeChan
//
//	go r.WaitForClientConnections()
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
//	//	r.doHeartbeat()
//	var c chan struct{}
//	if r.doStats {
//		r.TimeseriesStats.GoClock()
//		c = r.TimeseriesStats.C
//	} else {
//		c = make(chan struct{})
//	}
//
//	var stateGo time.Timer
//	if r.sendProposerState {
//		stateGo = *time.NewTimer(50 * time.Millisecond)
//	}
//
//	if r.BatchingEnabled() {
//		for !r.Shutdown {
//			select {
//			case <-stateGo.C:
//				for i := int32(0); i<int32(r.N); i++ {
//					if i == r.Id {
//						continue
//					}
//					msg := proposerstate.State{
//						ProposerID:       r.Id,
//						CurrentInstance: r.crtInstance,
//					}
//					r.SendMsg(i, r.stateChanRPC, &msg)
//				}
//				break
//			case stateS := <-r.stateChan:
//				recvState := stateS.(*proposerstate.State)
//				r.handleState(recvState)
//				break
//			case props := <-r.batchedProps:
//				r.beginNextInstance(props)
//				break
//			case <-c:
//				r.TimeseriesStats.PrintAndReset()
//				break
//				//	case <-r.goHeartbeat:
//			//	r.doHeartbeat()
//			//	break
//			case maybeTimedout := <-r.timeoutMsgs:
//				r.retryBallot(maybeTimedout)
//				break
//			case <-doner:
//				dlog.Println("Crahsing")
//				time.Sleep(r.howLongCrash)
//				r.restart()
//				dlog.Println("Done crashing")
//				break
//			case next := <-r.retryInstance:
//				dlog.Println("Checking whether to retry a proposal")
//				r.tryNextAttempt(next)
//				break
//			case prepareS := <-r.prepareChan:
//				prepare := prepareS.(*stdpaxosproto.Prepare)
//				//got a Prepare message
//				dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
//				if !r.checkAndHandlecatchupRequest(prepare) {
//					r.handlePrepare(prepare)
//				}
//				break
//			case acceptS := <-r.acceptChan:
//				accept := acceptS.(*stdpaxosproto.Accept)
//				//got an Accept message
//				dlog.Printf("Received Accept Request from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
//				r.handleAccept(accept)
//				break
//			case commitS := <-r.commitChan:
//				commit := commitS.(*stdpaxosproto.Commit)
//				//got a Commit message
//				dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
//				r.handleCommit(commit)
//				r.checkAndHandlecatchupResponse(commit)
//				break
//			case commitS := <-r.commitShortChan:
//				commit := commitS.(*stdpaxosproto.CommitShort)
//				//got a Commit message
//				dlog.Printf("Received short Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
//				r.handleCommitShort(commit)
//				break
//			case prepareReplyS := <-r.prepareReplyChan:
//				prepareReply := prepareReplyS.(*stdpaxosproto.PrepareReply)
//				//got a Prepare reply
//				dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
//				r.handlePrepareReply(prepareReply)
//				break
//			case acceptReplyS := <-r.acceptReplyChan:
//				acceptReply := acceptReplyS.(*stdpaxosproto.AcceptReply)
//				//got an Accept reply
//				dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
//				r.handleAcceptReply(acceptReply)
//				break
//			}
//		}
//	} else {
//		for !r.Shutdown {
//			select {
//			case <-c:
//				r.TimeseriesStats.PrintAndReset()
//				break
//			case maybeTimedout := <-r.timeoutMsgs:
//				r.retryBallot(maybeTimedout)
//				break
//			case <-doner:
//				dlog.Println("Crahsing")
//				time.Sleep(r.howLongCrash)
//				r.restart()
//				dlog.Println("Done crashing")
//				break
//			case next := <-r.retryInstance:
//				dlog.Println("Checking whether to retry a proposal")
//				r.tryNextAttempt(next)
//				break
//			case prepareS := <-r.prepareChan:
//				prepare := prepareS.(*stdpaxosproto.Prepare)
//				//got a Prepare message
//				dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
//				if !r.checkAndHandlecatchupRequest(prepare) {
//					r.handlePrepare(prepare)
//				}
//				break
//			case acceptS := <-r.acceptChan:
//				accept := acceptS.(*stdpaxosproto.Accept)
//				//got an Accept message
//				dlog.Printf("Received Accept Request from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
//				r.handleAccept(accept)
//				break
//			case commitS := <-r.commitChan:
//				commit := commitS.(*stdpaxosproto.Commit)
//				//got a Commit message
//				dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
//				r.handleCommit(commit)
//				r.checkAndHandlecatchupResponse(commit)
//				break
//			case commitS := <-r.commitShortChan:
//				commit := commitS.(*stdpaxosproto.CommitShort)
//				//got a Commit message
//				dlog.Printf("Received short Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
//				r.handleCommitShort(commit)
//				break
//			case prepareReplyS := <-r.prepareReplyChan:
//				prepareReply := prepareReplyS.(*stdpaxosproto.PrepareReply)
//				//got a Prepare reply
//				dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
//				r.handlePrepareReply(prepareReply)
//				break
//			case acceptReplyS := <-r.acceptReplyChan:
//				acceptReply := acceptReplyS.(*stdpaxosproto.AcceptReply)
//				//got an Accept reply
//				dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
//				r.handleAcceptReply(acceptReply)
//				break
//			default:
//				break
//			}
//
//			switch cliProp := r.clientValueQueue.TryDequeue(); {
//			case cliProp != nil:
//				numEnqueued := r.clientValueQueue.Len() + 1
//				maxBatchSize := min(numEnqueued, r.maxBatchSize)
//				clientProposals := make([]*genericsmr.Propose, maxBatchSize)
//				clientProposals[0] = cliProp
//
//				for i := 1; i < maxBatchSize; i++ {
//					cliProp = r.clientValueQueue.TryDequeue()
//					if cliProp == nil {
//						clientProposals = clientProposals[:i]
//						break
//					}
//					clientProposals[i] = cliProp
//				}
//				dlog.Println("Client value(s) received beginning new instance")
//				r.beginNextInstance(clientProposals)
//			}
//		}
//	}
//}
//
//func (r *LWPReplica) retryBallot(maybeTimedout TimeoutInfo) {
//	inst := r.instanceSpace[maybeTimedout.inst]
//	if inst.pbk.propCurBal.Equal(maybeTimedout.proposingBal) && inst.pbk.status == maybeTimedout.phase {
//		if r.doStats {
//			r.TimeseriesStats.Update("Message Timeouts", 1)
//			id := stats.InstanceID{
//				Log: 0,
//				Seq: maybeTimedout.inst,
//			}
//			if maybeTimedout.phase == PREPARING {
//				r.InstanceStats.RecordOccurrence(id, "Phase 1 Timeout", 1)
//			} else if maybeTimedout.phase == PROPOSING {
//				r.InstanceStats.RecordOccurrence(id, "Phase 2 Timeout", 1)
//			}
//		}
//		//log.Println("resending")
//		inst.pbk.proposalInfos[inst.pbk.propCurBal].Broadcast(maybeTimedout.msgCode, maybeTimedout.msg)
//		r.beginTimeout(maybeTimedout.inst, maybeTimedout.proposingBal, maybeTimedout.phase, r.timeout, maybeTimedout.msgCode, maybeTimedout.msg)
//	}
//}
//
//func (r *LWPReplica) tryNextAttempt(next RetryInfo) {
//	inst := r.instanceSpace[next.InstToPrep]
//	if !next.backedoff {
//		if inst == nil {
//			r.instanceSpace[next.InstToPrep] = r.proposerMakeEmptyInstance()
//			inst = r.instanceSpace[next.InstToPrep]
//		}
//	}
//
//	if (r.BackoffManager.StillRelevant(next) || !next.backedoff) && inst.pbk.status == BACKING_OFF {
//		r.ProposalManager.StartNewProposal(r, next.InstToPrep)
//		nextBallot := r.instanceSpace[next.InstToPrep].pbk.propCurBal
//		r.acceptorPrepareOnBallot(next.InstToPrep, nextBallot)
//		inst.pbk.proposalInfos[nextBallot].AddToQuorum(int(r.Id))
//		r.bcastPrepare(next.InstToPrep)
//		dlog.Printf("Proposing next conf-bal %d.%d to instance %d\n", nextBallot.Number, nextBallot.PropID, next.InstToPrep)
//		if r.doStats {
//			r.ProposalStats.Open(stats.InstanceID{0, r.crtInstance}, nextBallot)
//		}
//	} else {
//		dlog.Printf("Skipping retry of instance %d due to preempted again or closed\n", next.InstToPrep)
//	}
//}
//
//func (r *LWPReplica) sendSinglePrepare(instance int32, to int32) {
//	// cheats - should really be a special recovery message but lazzzzzyyyy
//	defer func() {
//		if err := recover(); err != nil {
//			dlog.Println("Prepare bcast failed:", err)
//		}
//	}()
//	args := &stdpaxosproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurBal}
//	dlog.Printf("send prepare to %d\n", to)
//	r.SendMsg(to, r.prepareRPC, args)
//	r.beginTimeout(args.Instance, args.Ballot, PREPARING, r.timeout*5, r.prepareRPC, args)
//}
//
//func (r *LWPReplica) beginTimeout(inst int32, attempted stdpaxosproto.Ballot, onWhatPhase ProposerStatus, timeout time.Duration, msgcode uint8, msg fastrpc.Serializable) {
//	go func(instance int32, tried stdpaxosproto.Ballot, phase ProposerStatus, timeoutWait time.Duration) {
//		timer := time.NewTimer(timeout)
//		<-timer.C
//		if r.instanceSpace[inst].pbk.propCurBal.Equal(tried) && r.instanceSpace[inst].pbk.status == phase {
//			// not atomic and might change when message received but that's okay (only to limit number of channel messages sent)
//			r.timeoutMsgs <- TimeoutInfo{
//				ProposalInfo: ProposalInfo{instance, tried},
//				phase:        phase,
//				msgCode:      msgcode,
//				msg:          msg,
//			}
//		}
//	}(inst, attempted, onWhatPhase, timeout)
//}
//
//func (r *LWPReplica) isSlowestSlowerThanMedian(sent []int) bool {
//	slowestLat := float64(-1)
//	ewma := r.CopyEWMA()
//	for _, c := range sent {
//		if ewma[c] > slowestLat {
//			slowestLat = ewma[c]
//		}
//	}
//
//	// is slower than median???
//	if (r.N-1)%2 != 0 {
//		return slowestLat > ewma[len(ewma)-1/2]
//	} else {
//		return slowestLat > (ewma[len(ewma)/2]+ewma[(len(ewma)/2)-1])/2
//	}
//}
//
//func (r *LWPReplica) beginTracking(instID stats.InstanceID, sentTo []int, trackingName string) {
//	if len(sentTo) == r.N-1 || r.isSlowestSlowerThanMedian(sentTo) {
//		r.InstanceStats.RecordComplexStatStart(instID, trackingName, "Slow Quorum")
//	} else {
//		r.InstanceStats.RecordComplexStatStart(instID, trackingName, "Fast Quorum")
//	}
//}
//
//func (r *LWPReplica) bcastPrepare(instance int32) {
//	args := &stdpaxosproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurBal}
//	pbk := r.instanceSpace[instance].pbk
//	dlog.Println("sending prepare")
//
//	sentTo := pbk.proposalInfos[pbk.propCurBal].Broadcast(r.prepareRPC, args)
//
//	if r.doStats {
//		instID := stats.InstanceID{
//			Log: 0,
//			Seq: instance,
//		}
//		r.InstanceStats.RecordOccurrence(instID, "My Phase 1 Proposals", 1)
//		r.beginTracking(instID, sentTo, "Phase 1")
//	}
//	r.beginTimeout(args.Instance, args.Ballot, PREPARING, r.timeout, r.prepareRPC, args)
//}
//
//func (r *LWPReplica) bcastAccept(instance int32) {
//	pa.LeaderId = r.Id
//	pa.Instance = instance
//	pa.Ballot = r.instanceSpace[instance].pbk.propCurBal
//	pa.Command = r.instanceSpace[instance].pbk.cmds
//	pa.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
//	args := &pa
//
//	pbk := r.instanceSpace[instance].pbk
//	sentTo := pbk.proposalInfos[pbk.propCurBal].Broadcast(r.acceptRPC, args)
//
//	if r.doStats {
//		instID := stats.InstanceID{
//			Log: 0,
//			Seq: instance,
//		}
//		r.InstanceStats.RecordOccurrence(instID, "My Phase 2 Proposals", 1)
//		r.beginTracking(instID, sentTo, "Phase 2")
//	}
//
//	r.beginTimeout(args.Instance, args.Ballot, PROPOSING, r.timeout, r.acceptRPC, args)
//}
//
//func (r *LWPReplica) bcastCommitToAll(instance int32, Ballot stdpaxosproto.Ballot, command []state.Command) {
//	defer func() {
//		if err := recover(); err != nil {
//			dlog.Println("commit bcast failed:", err)
//		}
//	}()
//	pc.LeaderId = r.Id
//	pc.Instance = instance
//	pc.Ballot = Ballot
//	pc.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
//	pc.MoreToCome = 0
//	pc.Command = command
//
//	pcs.LeaderId = r.Id
//	pcs.Instance = instance
//	pcs.Ballot = Ballot
//	pcs.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
//	pcs.Count = int32(len(command))
//	argsShort := pcs
//
//	r.CalculateAlive()
//	sent := 0
//	for q := int32(0); q < int32(r.N); q++ {
//		if q == r.Id { //todo update others with this
//			continue
//		}
//		inQrm := r.instanceSpace[instance].pbk.proposalInfos[Ballot].HasAcknowledged(int(q)) //pbk.proposalInfos[Ballot].Broadcast//.aids[r.PreferredPeerOrder[q]]
//		if inQrm {
//			r.SendMsg(q, r.commitShortRPC, &argsShort)
//		} else {
//			r.SendMsg(q, r.commitRPC, &pc)
//		}
//		sent++
//	}
//}
//
//func (r *LWPReplica) incToNextOpenInstance() {
//	r.crtInstance++
//}
//
//func (r *LWPReplica) proposerMakeEmptyInstance() *Instance {
//	return &Instance{
//		abk: &AcceptorBookkeeping{
//			status: NOT_STARTED,
//			cmds:   nil,
//			curBal: stdpaxosproto.Ballot{-1, -1},
//			vBal:   stdpaxosproto.Ballot{-1, -1},
//		},
//		pbk: &ProposingBookkeeping{
//			status:        NOT_BEGUN,
//			proposalInfos: make(map[stdpaxosproto.Ballot]quorumsystem.SynodQuorumSystem),
//
//			maxKnownBal:     stdpaxosproto.Ballot{-1, -1},
//			proposeValueBal:  stdpaxosproto.Ballot{-1, -1},
//			whoseCmds:       -1,
//			cmds:            nil,
//			propCurBal:      stdpaxosproto.Ballot{-1, -1},
//			clientProposals: nil,
//		},
//	}
//}
//
//func (r *LWPReplica) beginNextInstance(valsToPropose []*genericsmr.Propose) {
//	r.incToNextOpenInstance()
//	r.instanceSpace[r.crtInstance] = r.proposerMakeEmptyInstance()
//	curInst := r.instanceSpace[r.crtInstance]
//	r.ProposalManager.StartNewProposal(r, r.crtInstance)
//
//	for i := 0; i < len(r.crtOpenedInstances); i++ {
//		if r.crtOpenedInstances[i] == -1 {
//			r.crtOpenedInstances[i] = r.crtInstance
//			break
//		}
//		if i == len(r.crtOpenedInstances) {
//			panic("aslkfjlkefj")
//		}
//	}
//
//	if r.doStats {
//		r.InstanceStats.RecordOpened(stats.InstanceID{0, r.crtInstance}, time.Now())
//		r.TimeseriesStats.Update("Instances Opened", 1)
//	}
//
//	r.instanceSpace[r.crtInstance].pbk.clientProposals = valsToPropose
//	r.acceptorPrepareOnBallot(r.crtInstance, curInst.pbk.propCurBal)
//	curInst.pbk.proposalInfos[curInst.pbk.propCurBal].AddToQuorum(int(r.Id))
//	if r.doStats {
//		r.ProposalStats.Open(stats.InstanceID{0, r.crtInstance}, curInst.pbk.propCurBal)
//	}
//	r.bcastPrepare(r.crtInstance)
//	dlog.Printf("Opened new instance %d\n", r.crtInstance)
//}
//
//func (r *LWPReplica) handlePropose(tryPropose *genericsmr.Propose) {
//	dlog.Printf("Received new client value\n")
//	r.clientValueQueue.TryEnqueue(tryPropose)
//	//check if any open instances
//}
//
//func (r *LWPReplica) acceptorPrepareOnBallot(inst int32, bal stdpaxosproto.Ballot) {
//	r.instanceSpace[inst].abk.status = PREPARED
//	dlog.Printf("Acceptor Preparing Ballot %d.%d ", bal.Number, bal.PropID)
//	r.instanceSpace[inst].abk.curBal = bal
//	r.recordInstanceMetadata(r.instanceSpace[inst])
//	r.sync()
//}
//
//func (r *LWPReplica) acceptorAcceptOnBallot(inst int32, bal stdpaxosproto.Ballot, cmds []state.Command) {
//	abk := r.instanceSpace[inst].abk
//	if abk.status == COMMITTED {
//		panic("why over write commit????")
//	}
//	abk.status = ACCEPTED
//
//	dlog.Printf("Acceptor Accepting Ballot %d.%d ", bal.Number, bal.PropID)
//	abk.curBal = bal
//	abk.vBal = bal
//	abk.cmds = cmds
//
//	r.recordInstanceMetadata(r.instanceSpace[inst])
//	r.recordCommands(cmds)
//	dlog.Printf("accept sync instance %d", inst)
//	r.sync()
//}
//
//func (r *LWPReplica) proposerCheckAndHandlePreempt(inst int32, preemptingBallot stdpaxosproto.Ballot, preemterPhase Phase) bool {
//	instance := r.instanceSpace[inst]
//	pbk := instance.pbk
//
//	if pbk.status != CLOSED && preemptingBallot.GreaterThan(pbk.propCurBal) {
//		//if pbk.status != BACKING_OFF { // option for add multiple preempts if backing off already?
//		r.BackoffManager.CheckAndHandleBackoff(inst, pbk.propCurBal, preemptingBallot, preemterPhase)
//
//		if pbk.status != BACKING_OFF && r.doStats {
//			id := stats.InstanceID{0, inst}
//			if pbk.status == PREPARING || pbk.status == READY_TO_PROPOSE {
//				r.InstanceStats.RecordOccurrence(id, "My Phase 1 Preempted", 1)
//				//r.InstanceStats.RecordComplexStatEnd(id, "Phase 1", "Failure")
//				r.TimeseriesStats.Update("My Phase 1 Preempted", 1)
//				r.ProposalStats.CloseAndOutput(id, pbk.propCurBal, stats.HIGHERPROPOSALONGOING)
//			} else if pbk.status == PROPOSING {
//				r.InstanceStats.RecordOccurrence(id, "My Phase 2 Preempted", 1)
//				//r.InstanceStats.RecordComplexStatEnd(id, "Phase 2", "Failure")
//				r.TimeseriesStats.Update("My Phase 2 Preempted", 1)
//				r.ProposalStats.CloseAndOutput(id, pbk.propCurBal, stats.HIGHERPROPOSALONGOING)
//			}
//		}
//
//		// if we are preparing for a new instance to tryPropose in but are preempted
//		if pbk.status == PREPARING && pbk.clientProposals != nil || pbk.status == PROPOSING && pbk.clientProposals != nil && preemterPhase == ACCEPTANCE {
//			r.requeueClientProposals(inst)
//			pbk.clientProposals = nil
//		}
//
//		//		if r.requeueOnPreempt {
//		//			r.requeueClientProposals(inst)
//		//		}
//
//		if preemptingBallot.GreaterThan(pbk.maxKnownBal) {
//			pbk.maxKnownBal = preemptingBallot
//		}
//
//		pbk.status = BACKING_OFF
//		return true
//	} else {
//		return false
//	}
//}
//
//func (r *LWPReplica) isMoreCommitsToComeAfter(inst int32) bool {
//	for i := inst + 1; i <= r.crtInstance; i++ {
//		instance := r.instanceSpace[i]
//		if instance != nil {
//			if instance.abk.status == COMMITTED {
//				return true
//			}
//		}
//	}
//	return false
//}
//
//func (r *LWPReplica) howManyExtraCommitsToSend(inst int32) int32 {
//	if r.commitCatchUp {
//		return r.crtInstance - inst
//	} else {
//		return 0
//	}
//}
//
//func (r *LWPReplica) checkAndHandleCommit(instance int32, whoRespondTo int32, maxExtraInstances int32) bool {
//	inst := r.instanceSpace[instance]
//	if inst == nil {
//		return false
//	}
//	if inst.abk.status == COMMITTED {
//		//	if instance+(int32(r.N)*r.maxOpenInstances) < r.crtInstance {
//		count := int32(0)
//		for i := instance; i < r.crtInstance; i++ {
//			returingInst := r.instanceSpace[i]
//			if returingInst != nil {
//				if returingInst.abk.status == COMMITTED {
//					dlog.Printf("Already committed instance %d, returning commit to %d \n", instance, whoRespondTo)
//					pc.LeaderId = int32(returingInst.abk.vBal.PropID) //prepare.LeaderId
//					pc.Instance = i
//					pc.Ballot = returingInst.abk.vBal
//					pc.Command = returingInst.abk.cmds
//					pc.WhoseCmd = returingInst.pbk.whoseCmds
//					if r.isMoreCommitsToComeAfter(i) && count < maxExtraInstances {
//						pc.MoreToCome = 1
//						r.SendMsgNoFlush(whoRespondTo, r.commitRPC, &pc)
//						count++
//					} else {
//						pc.MoreToCome = 0
//						r.SendMsgNoFlush(whoRespondTo, r.commitRPC, &pc)
//						break
//					}
//				}
//			}
//		}
//		r.Replica.FlushBuffer(whoRespondTo)
//		return true
//	} else {
//		return false
//	}
//}
//
//func (r *LWPReplica) handlePrepare(prepare *stdpaxosproto.Prepare) {
//	r.checkAndHandleNewlyReceivedInstance(prepare.Instance)
//
//	if r.checkAndHandleCommit(prepare.Instance, prepare.LeaderId, r.howManyExtraCommitsToSend(prepare.Instance)) {
//		return
//	}
//
//	inst := r.instanceSpace[prepare.Instance]
//	minSafe := inst.abk.curBal
//
//	if minSafe.GreaterThan(prepare.Ballot) {
//		dlog.Printf("Already prepared on higher Config-Ballot %d.%d.%d < %d.%d.%d", prepare.Number, prepare.PropID, minSafe.Number, minSafe.PropID)
//	} else if prepare.Ballot.GreaterThan(minSafe) {
//		dlog.Printf("Preparing on prepared on new Config-Ballot %d.%d.%d", prepare.Number, prepare.PropID)
//		r.acceptorPrepareOnBallot(prepare.Instance, prepare.Ballot)
//		r.proposerCheckAndHandlePreempt(prepare.Instance, prepare.Ballot, PROMISE)
//
//		r.checkAndHandleOldPreempted(prepare.Ballot, minSafe, inst.abk.vBal, inst.abk.cmds, prepare.Instance)
//
//	} else {
//		dlog.Printf("Config-Ballot %d.%d already joined, returning same promise", prepare.Number, prepare.PropID)
//	}
//
//	newBallot := inst.abk.curBal
//
//	var preply = &stdpaxosproto.PrepareReply{
//		Instance:   prepare.Instance,
//		Bal:        newBallot,
//		VBal:       inst.abk.vBal,
//		AcceptorId: r.Id,
//		WhoseCmd:   inst.pbk.whoseCmds,
//		Command:    inst.abk.cmds,
//	}
//
//	r.replyPrepare(prepare.LeaderId, preply)
//}
//
//func (r *LWPReplica) checkAndHandleOldPreempted(new stdpaxosproto.Ballot, old stdpaxosproto.Ballot, accepted stdpaxosproto.Ballot, acceptedVal []state.Command, inst int32) {
//	if new.PropID != old.PropID && int32(new.PropID) != r.Id && old.PropID != -1 && new.GreaterThan(old) {
//		preemptOldPropMsg := &stdpaxosproto.PrepareReply{
//			Instance:   inst,
//			Bal:        new,
//			VBal:       accepted,
//			WhoseCmd:   r.instanceSpace[inst].pbk.whoseCmds,
//			AcceptorId: r.Id,
//			Command:    acceptedVal,
//		}
//		r.replyPrepare(int32(new.PropID), preemptOldPropMsg)
//	}
//}
//
//func (r *LWPReplica) proposerCheckAndHandleAcceptedValue(inst int32, aid int32, accepted stdpaxosproto.Ballot, val []state.Command, whoseCmds int32) ProposerAccValHandler {
//	if accepted.IsZero() {
//		return IGNORED
//	}
//	instance := r.instanceSpace[inst]
//	pbk := instance.pbk
//	newVal := false
//
//	if pbk.status == CLOSED {
//		return CHOSEN
//	}
//	if accepted.GreaterThan(pbk.maxKnownBal) {
//		pbk.maxKnownBal = accepted
//	}
//	if accepted.GreaterThan(pbk.proposeValueBal) {
//		newVal = true
//		pbk.whoseCmds = whoseCmds
//		pbk.proposeValueBal = accepted
//		pbk.cmds = val
//
//		if r.whatHappenedToClientProposals(inst) == ProposedButNotChosen {
//			//f	r.requeueClientProposals(inst)
//			//		pbk.clientProposals = nil
//		}
//	}
//
//	//todo fix requeueing
//	//todo add timeout to reties
//	// todo log messages
//
//	_, exists := pbk.proposalInfos[accepted]
//	if !exists {
//		r.ProposalManager.trackProposalAcceptance(r, inst, accepted)
//	}
//
//	pbk.proposalInfos[accepted].AddToQuorum(int(aid))
//	dlog.Printf("Acceptance on instance %d at conf-round %d.%d by acceptor %d", inst, accepted.Number, accepted.PropID, aid)
//	// not assumed local acceptor has accepted it
//	if pbk.proposalInfos[accepted].QuorumReached() {
//		//	if pbk.proposeValueBal.GreaterThan(accepted) && pbk.whoseCmds != whoseCmds && pbk.proposalInfos[pbk.propCurBal].qrmType == ACCEPTANCE {
//		//		panic("break in safety!!!")
//		//	}
//		r.bcastCommitToAll(inst, accepted, val)
//		r.acceptorCommit(inst, accepted, val)
//		r.proposerCloseCommit(inst, accepted, pbk.cmds, whoseCmds)
//		//		if pbk.status != CLOSED || instance.abk.status != COMMITTED {
//		//		panic("not set agents to closed")
//		//	}
//		return CHOSEN
//	} else if newVal {
//		return NEW_VAL
//	} else {
//		return ACKED
//	}
//}
//
//func (r *LWPReplica) handlePrepareReply(preply *stdpaxosproto.PrepareReply) {
//	inst := r.instanceSpace[preply.Instance]
//	pbk := inst.pbk
//	// todo should do check and handle commit instead???
//	if inst.abk.status == COMMITTED {
//		dlog.Println("Inst already known to be chosen")
//		return
//	}
//
//	valWhatDone := r.proposerCheckAndHandleAcceptedValue(preply.Instance, preply.AcceptorId, preply.VBal, preply.Command, preply.WhoseCmd)
//	if r.fastLearn {
//		valWhatDone = r.proposerCheckAndHandleAcceptedValue(preply.Instance, int32(preply.VBal.PropID), preply.VBal, preply.Command, preply.WhoseCmd)
//	}
//	if valWhatDone == NEW_VAL {
//		dlog.Printf("Promise from %d in instance %d has new value at Config-Ballot %d.%d", preply.AcceptorId,
//			preply.Instance, preply.VBal.Number, preply.VBal.PropID)
//	} else if valWhatDone == CHOSEN {
//		dlog.Printf("Preparing instance recognised as chosen (instance %d), returning commit \n", preply.Instance)
//
//		return
//	}
//
//	if pbk.propCurBal.GreaterThan(preply.Bal) || pbk.status != PREPARING {
//		dlog.Printf("Message in late \n")
//		return
//	}
//
//	if r.proposerCheckAndHandlePreempt(preply.Instance, preply.Bal, PROMISE) {
//		dlog.Printf("Another active proposer using config-ballot %d.%d.%d greater than mine\n", preply.Bal)
//		r.acceptorPrepareOnBallot(preply.Instance, preply.Bal)
//		myReply := stdpaxosproto.PrepareReply{
//			Instance:   preply.Instance,
//			Bal:        preply.Bal,
//			VBal:       inst.abk.vBal,
//			AcceptorId: r.Id,
//			WhoseCmd:   pbk.whoseCmds,
//			Command:    inst.abk.cmds,
//		}
//		r.SendMsg(int32(preply.Bal.PropID), r.prepareReplyRPC, &myReply)
//		return
//	}
//
//	qrm := pbk.proposalInfos[pbk.propCurBal]
//	qrm.AddToQuorum(int(preply.AcceptorId))
//	dlog.Printf("Added replica's %d promise to qrm", preply.AcceptorId)
//	if qrm.QuorumReached() { //int(qrm.quorumCount()+1) >= r.ELPReplica.ReadQuorumSize() {
//		id := stats.InstanceID{
//			Log: 0,
//			Seq: preply.Instance,
//		}
//		r.InstanceStats.RecordComplexStatEnd(id, "Phase 1", "Success")
//
//		r.tryPropose(preply.Instance)
//	}
//}
//
//func (r *LWPReplica) tryPropose(inst int32) {
//	instance := r.instanceSpace[inst]
//	pbk := instance.pbk
//
//	pbk.status = READY_TO_PROPOSE
//	dlog.Println("Can now tryPropose in instance", inst)
//	qrm := pbk.proposalInfos[pbk.propCurBal]
//	qrm.StartAcceptanceQuorum()
//
//	whoseCmds := int32(-1)
//	if pbk.proposeValueBal.IsZero() {
//		whoseCmds = r.Id
//		if pbk.clientProposals != nil {
//			pbk.cmds = make([]state.Command, len(pbk.clientProposals))
//			for i, prop := range pbk.clientProposals {
//				pbk.cmds[i] = prop.Command
//			}
//			log.Printf("%d client value(s) proposed in instance %d\n", len(pbk.clientProposals), inst)
//		} else {
//
//			q := r.clientValueQueue.GetQueueChan()
//			select {
//			case c := <-q:
//				maxBatchSize := min(r.maxBatchSize, len(q)+1)
//				clientProposals := make([]*genericsmr.Propose, maxBatchSize)
//				cmds := make([]state.Command, maxBatchSize)
//				clientProposals[0] = c
//				cmds[0] = c.Command
//
//				for i := 1; i < maxBatchSize; i++ {
//					c = <-q
//					clientProposals[i] = c
//					cmds[i] = c.Command
//				}
//				dlog.Println("Client value(s) received beginning new instance")
//				pbk.clientProposals = clientProposals
//				pbk.cmds = cmds
//
//				if r.doStats {
//					r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Client Value Proposed", 1)
//					r.ProposalStats.RecordClientValuesProposed(stats.InstanceID{0, inst}, pbk.propCurBal, len(pbk.cmds))
//					r.TimeseriesStats.Update("Times Client Values Proposed", 1)
//				}
//
//				log.Printf("%d client value(s) received and proposed in instance %d which was recovered \n", len(pbk.clientProposals), inst)
//				break
//			default:
//				if r.shouldNoop(inst) {
//					if r.doStats {
//						r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Noop Proposed", 1)
//						r.TimeseriesStats.Update("Times Noops Proposed", 1)
//						r.ProposalStats.RecordNoopProposed(stats.InstanceID{0, inst}, pbk.propCurBal)
//					}
//					pbk.cmds = state.NOOP()
//					log.Println("Proposing noop in recovered instance", inst)
//				} else {
//					return
//				}
//			}
//		}
//	} else {
//		if r.doStats {
//			r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Previous Value Proposed", 1)
//			r.TimeseriesStats.Update("Times Previous Value Proposed", 1)
//		}
//		whoseCmds = pbk.whoseCmds
//	}
//
//	pbk.status = PROPOSING
//	r.proposerCheckAndHandleAcceptedValue(inst, r.Id, pbk.propCurBal, pbk.cmds, whoseCmds)
//	// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
//	if r.fastLearn {
//		r.acceptorAcceptOnBallot(inst, pbk.propCurBal, pbk.cmds)
//		r.bcastAccept(inst)
//	} else {
//		r.bcastAccept(inst)
//		r.acceptorAcceptOnBallot(inst, pbk.propCurBal, pbk.cmds)
//	}
//}
//
//func (r *LWPReplica) shouldNoop(inst int32) bool {
//	if r.alwaysNoop {
//		return true
//	}
//
//	for i := inst + 1; i < r.crtInstance; i++ {
//		if r.instanceSpace[i] == nil {
//			continue
//		}
//		if r.instanceSpace[i].abk.status == COMMITTED {
//			return true
//		}
//
//	}
//	return false
//}
//
//func (r *LWPReplica) checkAndHandleNewlyReceivedInstance(instance int32) {
//	inst := r.instanceSpace[instance]
//	if inst == nil {
//		if instance > r.crtInstance {
//			r.crtInstance = instance
//		}
//		r.instanceSpace[instance] = r.proposerMakeEmptyInstance()
//	}
//}
//
//func (r *LWPReplica) handleAccept(accept *stdpaxosproto.Accept) {
//	r.checkAndHandleNewlyReceivedInstance(accept.Instance)
//
//	if r.checkAndHandleCommit(accept.Instance, accept.LeaderId, accept.Instance) {
//		return
//	}
//
//	inst := r.instanceSpace[accept.Instance]
//	minAcceptableBallot := inst.abk.curBal
//	// should always be unless there is a restart
//	if accept.Ballot.GreaterThan(minAcceptableBallot) || accept.Ballot.Equal(minAcceptableBallot) {
//		dlog.Printf("Accepted instance %d on conf-ball %d.%d", accept.Instance, accept.Number, accept.PropID)
//		r.acceptorAcceptOnBallot(accept.Instance, accept.Ballot, accept.Command)
//
//		//check proposer qrm
//		// here is where we can add fast learning bit - also add acceptance by config-bal's owner
//		if r.fastLearn {
//			r.proposerCheckAndHandleAcceptedValue(accept.Instance, int32(accept.PropID), accept.Ballot, accept.Command, accept.WhoseCmd) // must go first as acceptor might have already learnt of value
//		}
//		accValState := r.proposerCheckAndHandleAcceptedValue(accept.Instance, r.Id, accept.Ballot, accept.Command, accept.WhoseCmd)
//		if accValState == CHOSEN {
//			return
//		}
//		r.proposerCheckAndHandlePreempt(accept.Instance, accept.Ballot, ACCEPTANCE)
//	} else if minAcceptableBallot.GreaterThan(accept.Ballot) {
//		dlog.Printf("Returning preempt for config-ballot %d.%d < %d.%d in Instance %d\n", accept.Number, accept.PropID, minAcceptableBallot.Number, minAcceptableBallot.PropID, accept.Instance)
//	} else {
//		dlog.Printf("Already acknowledged accept request but will return again", accept.Number, accept.PropID, minAcceptableBallot.Number, minAcceptableBallot.PropID, accept.Instance)
//	}
//
//	replyBallot := inst.abk.curBal
//
//	areply := &stdpaxosproto.AcceptReply{accept.Instance, r.Id, replyBallot, accept.Ballot, inst.pbk.whoseCmds}
//	r.replyAccept(accept.LeaderId, areply)
//}
//
//func (r *LWPReplica) handleAcceptReply(areply *stdpaxosproto.AcceptReply) {
//	// could modify to have record of all ballots
//	inst := r.instanceSpace[areply.Instance]
//	pbk := inst.pbk
//	if inst.abk.status == COMMITTED {
//		dlog.Printf("Already committed ")
//		return
//	}
//
//	accepted := areply.Cur.Equal(areply.Req)
//	preempted := areply.Cur.GreaterThan(areply.Req)
//	if accepted {
//		dlog.Printf("Acceptance of instance %d at %d.%d by Acceptor %d received\n", areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId)
//		r.proposerCheckAndHandleAcceptedValue(areply.Instance, areply.AcceptorId, areply.Cur, pbk.cmds, areply.WhoseCmd)
//		// we can count proposer of value too because they durably accept before sending accept request - only if fast learn is on
//		////if res == CHOSEN {
//		////	id := stats.InstanceID{
//		////		Log: 0,
//		////		Seq: areply.Instance,
//		////	}
//		//}
//	} else if preempted {
//		r.proposerCheckAndHandlePreempt(areply.Instance, areply.Cur, ACCEPTANCE) // todo fix
//	} else {
//		msg := fmt.Sprintf("Somehow cur Conf-Bal of %d is %d.%d when we requested %d.%d for acceptance",
//			areply.AcceptorId, areply.Cur.Number, areply.Cur.PropID,
//			areply.Req.Number, areply.Req.PropID)
//		panic(msg)
//	}
//}
//
//func (r *LWPReplica) requeueClientProposals(instance int32) {
//	inst := r.instanceSpace[instance]
//	dlog.Printf("Requeing client values in instance %d", instance)
//
//	if r.doStats && len(inst.pbk.clientProposals) > 0 {
//		r.TimeseriesStats.Update("Requeued Client Values", 1)
//	}
//
//	if r.maxBatchWait > 0 {
//		prop := make([]*genericsmr.Propose, len(inst.pbk.clientProposals))
//		copy(prop, inst.pbk.clientProposals)
//		go func(tryPropose []*genericsmr.Propose) {
//			<-r.openInst
//			r.batchedProps <- tryPropose
//		}(prop)
//	} else {
//		for i := 0; i < len(inst.pbk.clientProposals); i++ {
//			r.clientValueQueue.TryRequeue(inst.pbk.clientProposals[i])
//		}
//	}
//
//}
//
//func (r *LWPReplica) whatHappenedToClientProposals(instance int32) ClientProposalStory {
//	inst := r.instanceSpace[instance]
//	pbk := inst.pbk
//	if pbk.whoseCmds != r.Id && pbk.clientProposals != nil {
//		//dlog.Printf("not chosen but proposed")
//		return ProposedButNotChosen
//	} else if pbk.whoseCmds == r.Id {
//		//dlog.Printf("chosen proposal")
//		return ProposedAndChosen
//	} else {
//		//dlog.Printf("not proposed")
//		return NotProposed
//	}
//}
//
//func (r *LWPReplica) howManyAttemptsToChoose(inst int32) {
//	instance := r.instanceSpace[inst]
//	pbk := instance.pbk
//
//	attempts := pbk.proposeValueBal.Number / r.maxBalInc
//	dlog.Printf("Attempts to chose instance %d: %d", inst, attempts)
//}
//
//func (r *LWPReplica) proposerCloseCommit(inst int32, chosenAt stdpaxosproto.Ballot, chosenVal []state.Command, whoseCmd int32) {
//	instance := r.instanceSpace[inst]
//	pbk := instance.pbk
//
//	if instance.abk.status != COMMITTED || instance.pbk.status == CLOSED {
//		panic("asldkfjalksdfj")
//	}
//
//	pbk.status = CLOSED
//	dlog.Printf("Instance %d chosen now\n", inst)
//	if r.doStats {
//		if pbk.propCurBal.GreaterThan(chosenAt) {
//			r.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.propCurBal, stats.LOWERPROPOSALCHOSEN)
//		} else if chosenAt.GreaterThan(pbk.propCurBal) {
//			r.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.propCurBal, stats.HIGHERPROPOSALONGOING)
//		} else {
//			r.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.propCurBal, stats.ITWASCHOSEN)
//		}
//	}
//	r.BackoffManager.ClearBackoff(inst)
//
//	if int32(chosenAt.PropID) == r.Id {
//		r.timeSinceLastProposedInstance = time.Now()
//	}
//
//	if chosenAt.Equal(instance.pbk.propCurBal) {
//		r.InstanceStats.RecordComplexStatEnd(stats.InstanceID{0, inst}, "Phase 2", "Success")
//	}
//
//	pbk.proposeValueBal = chosenAt
//	pbk.cmds = chosenVal
//	pbk.whoseCmds = whoseCmd
//
//	switch r.whatHappenedToClientProposals(inst) {
//	case NotProposed:
//		break
//	case ProposedButNotChosen:
//		dlog.Printf("%d client value(s) proposed in instance %d\n not chosen", len(pbk.clientProposals), inst)
//		r.requeueClientProposals(inst)
//		pbk.clientProposals = nil
//		break
//	case ProposedAndChosen:
//		dlog.Printf("%d client value(s) chosen in instance %d\n", len(pbk.clientProposals), inst)
//		break
//	}
//
//	for i := 0; i < len(r.crtOpenedInstances); i++ {
//		if r.crtOpenedInstances[i] == inst {
//			r.crtOpenedInstances[i] = -1
//			r.openInst <- struct{}{}
//			break
//		}
//		if i == len(r.crtOpenedInstances) {
//			panic("aslkfjlkefj")
//		}
//	}
//
//	if r.doStats {
//		balloter := r.ProposalManager.getBalloter()
//		atmts := balloter.GetAttemptNumber(chosenAt.Number)
//		r.InstanceStats.RecordCommitted(stats.InstanceID{0, inst}, atmts, time.Now())
//		r.TimeseriesStats.Update("Instances Learnt", 1)
//		if int32(chosenAt.PropID) == r.Id {
//			r.TimeseriesStats.Update("Instances I Choose", 1)
//			r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "I Chose", 1)
//		}
//		if !r.Exec {
//			r.InstanceStats.OutputRecord(stats.InstanceID{0, inst})
//		}
//	}
//
//	if pbk.clientProposals != nil && !r.Dreply {
//		// give client the all clear
//		for i := 0; i < len(pbk.cmds); i++ {
//			propreply := &genericsmrproto.ProposeReplyTS{
//				TRUE,
//				pbk.clientProposals[i].CommandId,
//				state.NIL(),
//				pbk.clientProposals[i].Timestamp}
//			r.ReplyProposeTS(propreply, pbk.clientProposals[i].Reply, pbk.clientProposals[i].Mutex)
//		}
//	}
//	//r.howManyAttemptsToChoose(inst)
//
//	if r.Exec {
//		oldExecutedUpTo := r.executedUpTo
//		for i := r.executedUpTo + 1; i <= r.crtInstance; i++ {
//			returnInst := r.instanceSpace[i]
//			if returnInst != nil && returnInst.abk.status == COMMITTED { //&& returnInst.abk.cmds != nil {
//				dlog.Printf("Executing instance %d\n", i)
//
//				if r.doStats {
//					r.InstanceStats.RecordExecuted(stats.InstanceID{0, i}, time.Now())
//					r.TimeseriesStats.Update("Instances Executed", 1)
//					r.InstanceStats.OutputRecord(stats.InstanceID{0, i})
//				}
//
//				for j := 0; j < len(returnInst.abk.cmds); j++ {
//					dlog.Printf("Executing " + returnInst.abk.cmds[j].String())
//					if r.Dreply && returnInst.pbk != nil && returnInst.pbk.clientProposals != nil {
//						val := returnInst.abk.cmds[j].Execute(r.State)
//
//						propreply := &genericsmrproto.ProposeReplyTS{
//							TRUE,
//							returnInst.pbk.clientProposals[j].CommandId,
//							val,
//							returnInst.pbk.clientProposals[j].Timestamp}
//						r.ReplyProposeTS(propreply, returnInst.pbk.clientProposals[j].Reply, returnInst.pbk.clientProposals[j].Mutex)
//						dlog.Printf("Returning executed client value")
//					} else if returnInst.abk.cmds[j].Op == state.PUT {
//						returnInst.abk.cmds[j].Execute(r.State)
//					}
//
//				}
//				//	returnInst.pbk = nil
//				r.executedUpTo += 1
//				dlog.Printf("Executed up to %d (crtInstance=%d)", r.executedUpTo, r.crtInstance)
//			} else {
//				if r.executedUpTo > oldExecutedUpTo {
//					r.recordExecutedUpTo()
//				}
//				break
//			}
//		}
//	}
//}
//
//func (r *LWPReplica) acceptorCommit(instance int32, chosenAt stdpaxosproto.Ballot, cmds []state.Command) {
//	inst := r.instanceSpace[instance]
//	abk := inst.abk
//	dlog.Printf("Committing (crtInstance=%d)\n", instance)
//
//	inst.abk.status = COMMITTED
//	knowsVal := abk.vBal.Equal(chosenAt)
//	shouldSync := false
//
//	abk.curBal = chosenAt
//	abk.vBal = chosenAt
//	abk.cmds = cmds
//
//	if !knowsVal {
//		r.recordInstanceMetadata(inst)
//		r.recordCommands(cmds)
//		if shouldSync {
//			r.sync()
//		}
//	}
//}
//
//func (r *LWPReplica) handleCommit(commit *stdpaxosproto.Commit) {
//	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
//	inst := r.instanceSpace[commit.Instance]
//
//	if inst.abk.status == COMMITTED {
//		dlog.Printf("Already committed \n")
//		return
//	}
//
//	if inst.pbk.status == CLOSED {
//		panic("asdfjsladfkjal;kejf")
//	}
//
//
//	r.acceptorCommit(commit.Instance, commit.Ballot, commit.Command)
//	r.proposerCloseCommit(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
//}
//
//func (r *LWPReplica) handleCommitShort(commit *stdpaxosproto.CommitShort) {
//	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
//	inst := r.instanceSpace[commit.Instance]
//
//	if inst.abk.status == COMMITTED {
//		dlog.Printf("Already committed \n")
//		return
//	}
//
//	if inst.pbk.status == CLOSED {
//		panic("asdfjsladfkjal;kejf")
//	}
//
//	r.acceptorCommit(commit.Instance, commit.Ballot, inst.abk.cmds)
//	r.proposerCloseCommit(commit.Instance, commit.Ballot, inst.abk.cmds, commit.WhoseCmd)
//}
//
//func (r *LWPReplica) handleState(state *proposerstate.State) {
//	r.checkAndHandleNewlyReceivedInstance(state.CurrentInstance)
//}
//