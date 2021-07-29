package lwcpatient

import (
	"clientproposalqueue"
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
	"net"
	"state"
	"sync"
	"time"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

const COMMIT_GRACE_PERIOD = 10 * 1e9 // 10 second(s) //int64(500 * time.Millisecond)//
const SLEEP_TIME_NS = 1e6

type ProposalTuples struct {
	cmd      []state.Command
	proposal []*genericsmr.Propose
}

type ProposalInfo struct {
	inst             int32
	proposingConfBal lwcproto.ConfigBal
}

type TimeoutInfo struct {
	ProposalInfo
	phase    ProposerStatus
	whoTried []int32
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
	prepareRPC                    uint8
	acceptRPC                     uint8
	commitRPC                     uint8
	commitShortRPC                uint8
	prepareReplyRPC               uint8
	acceptReplyRPC                uint8
	instanceSpace                 []*Instance // the space of all instances (used and not yet used)
	crtInstance                   int32       // highest active instance number that this replica knows about
	crtConfig                     int32
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
	emulatedSS                    bool
	emulatedWriteTime             time.Duration
	goHeartbeat                   chan struct{}
	timeoutRetry                  chan TimeoutInfo
	timeout                       time.Duration
	catchupBatchSize              int32
	catchingUp                    bool
	lastSettleBatchInst           int32
	flushCommit                   bool
	group1Size                    int
	nextRecoveryBatchPoint        int32
	recoveringFrom                int32
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
		nacks:   0,
		aids:    make(map[int32]struct{}),
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
	nacks   int
	aids    map[int32]struct{}
}

type ProposingBookkeeping struct {
	status             ProposerStatus
	proposalInfos      map[lwcproto.ConfigBal]*QuorumInfo
	maxAcceptedConfBal lwcproto.ConfigBal // highest maxAcceptedConfBal at which a command was accepted
	whoseCmds          int32
	cmds               []state.Command    // the accepted command
	propCurConfBal     lwcproto.ConfigBal // highest this replica maxAcceptedConfBal tried so far
	clientProposals    []*genericsmr.Propose
	maxKnownBal        lwcproto.Ballot
}

type RetryInfo struct {
	backedoff        bool
	InstToPrep       int32
	attemptedConfBal lwcproto.ConfigBal
	preempterConfBal lwcproto.ConfigBal
	preempterAt      QuorumType
	prevSoftExp      float64
	timesPreempted   int32
}

type BackoffInfo struct {
	minBackoff     int32
	maxInitBackoff int32
	maxBackoff     int32
}

type BackoffManager struct {
	currentBackoffs map[int32]RetryInfo
	BackoffInfo
	sig      *chan RetryInfo
	factor   float64
	mapMutex sync.RWMutex
	softFac  bool
}

func NewBackoffManager(minBO, maxInitBO, maxBO int32, signalChan *chan RetryInfo, factor float64, softFac bool) BackoffManager {
	if minBO > maxInitBO {
		panic(fmt.Sprintf("minbackoff %d, maxinitbackoff %d, incorrectly set up", minBO, maxInitBO))
	}

	return BackoffManager{
		currentBackoffs: make(map[int32]RetryInfo),
		BackoffInfo: BackoffInfo{
			minBackoff:     minBO,
			maxInitBackoff: maxInitBO,
			maxBackoff:     maxBO,
		},
		sig:     signalChan,
		factor:  factor,
		softFac: softFac,
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

func (bm *BackoffManager) ShouldBackoff(inst int32, preempter lwcproto.ConfigBal, preempterPhase QuorumType) bool {
	curBackoffInfo, exists := bm.currentBackoffs[inst]
	if !exists {
		return true
	} else if preempter.GreaterThan(curBackoffInfo.preempterConfBal) || (preempter.Equal(curBackoffInfo.preempterConfBal) && preempterPhase > curBackoffInfo.preempterAt) {
		return true
	} else {
		return false
	}
}
func (bm *BackoffManager) CheckAndHandleBackoff(inst int32, attemptedConfBal lwcproto.ConfigBal, preempter lwcproto.ConfigBal, prempterPhase QuorumType) {
	// if we give this a pointer to the timer we could stop the previous backoff before it gets pinged
	curBackoffInfo, exists := bm.currentBackoffs[inst]

	if !bm.ShouldBackoff(inst, preempter, prempterPhase) {
		dlog.Println("Ignoring backoff request as already backing off instance for this conf-bal or a greater one")
		return
	}

	var preemptNum int32
	var prev float64
	if exists {
		preemptNum = curBackoffInfo.timesPreempted + 1
		prev = curBackoffInfo.prevSoftExp
	} else {
		preemptNum = 0
		//prev = 0.
		prev = 0 //rand.Int31n(bm.maxInitBackoff - bm.minBackoff + 1) + bm.minBackoff//random//float64((rand.Int31() % bm.maxInitBackoff) + bm.minBackoff)
	}

	t := float64(preemptNum) + rand.Float64()
	tmp := math.Pow(2, t) * math.Tanh(math.Sqrt(bm.factor*t))
	tmp = tmp - prev
	if !bm.softFac {
		tmp = math.Max(tmp, 1)
	}
	next := int32(tmp * float64(rand.Int31n(bm.maxInitBackoff-bm.minBackoff+1)+bm.minBackoff)) //+ rand.Int31n(bm.maxInitBackoff - bm.minBackoff + 1) + bm.minBackoff// bm.minBackoff
	//if next > bm.maxBackoff {
	//	next = bm.maxBackoff - prev
	//}
	//if next < bm.minBackoff {
	//	next += bm.minBackoff
	//}
	if next < 0 {
		panic("can't have negative backoff")
	}
	dlog.Printf("Beginning backoff of %d us for instance %d on conf-bal %d.%d (attempt %d)", next, inst, attemptedConfBal.Number, attemptedConfBal.PropID, preemptNum)
	bm.currentBackoffs[inst] = RetryInfo{
		backedoff:        true,
		InstToPrep:       inst,
		attemptedConfBal: attemptedConfBal,
		preempterConfBal: preempter,
		preempterAt:      prempterPhase,
		prevSoftExp:      tmp - prev,
		timesPreempted:   preemptNum,
	}

	go func(instance int32, attempted lwcproto.ConfigBal, preempterConfBal lwcproto.ConfigBal, preempterP QuorumType, backoff int32, numTimesBackedOff int32) {

		timer := time.NewTimer(time.Duration(next) * time.Microsecond)
		<-timer.C
		*bm.sig <- RetryInfo{
			backedoff:        true,
			InstToPrep:       instance,
			attemptedConfBal: attempted,
			preempterConfBal: preempterConfBal,
			preempterAt:      preempterP,
			prevSoftExp:      tmp - prev,
			timesPreempted:   numTimesBackedOff,
		}
	}(inst, attemptedConfBal, preempter, prempterPhase, next, preemptNum)
}

func (bm *BackoffManager) NoHigherBackoff(backoff RetryInfo) bool {
	curBackoff, exists := bm.currentBackoffs[backoff.InstToPrep]

	if !exists {
		dlog.Printf("backoff has no record")
		return false
	} else {
		stillRelevant := backoff == curBackoff //should update so that inst also has bal backed off
		dlog.Println("Backoff of instance ", backoff.InstToPrep, "is relevant? ", stillRelevant)
		return stillRelevant
	}
}

func (bm *BackoffManager) ClearBackoff(inst int32) {
	delete(bm.currentBackoffs, inst)
}

func (r *Replica) noopStillRelevant(inst int32) bool {
	return r.instanceSpace[inst].pbk.cmds == nil
}

const MAXPROPOSABLEINST = 1000

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, lread bool, dreply bool, durable bool, batchWait int, f int, crtConfig int32, storageLoc string, maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool, factor float64, whoCrash int32, whenCrash time.Duration, howlongCrash time.Duration, emulatedSS bool, emulatedWriteTime time.Duration, catchupBatchSize int32, timeout time.Duration, group1Size int, flushCommit bool, softFac bool) *Replica {
	retryInstances := make(chan RetryInfo, maxOpenInstances*10000)
	r := &Replica{
		Replica:             genericsmr.NewReplica(id, peerAddrList, thrifty, exec, lread, dreply, f, storageLoc),
		configChan:          make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareChan:         make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptChan:          make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitChan:          make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitShortChan:     make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareReplyChan:    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptReplyChan:     make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		prepareRPC:          0,
		acceptRPC:           0,
		commitRPC:           0,
		commitShortRPC:      0,
		prepareReplyRPC:     0,
		acceptReplyRPC:      0,
		instanceSpace:       make([]*Instance, 15*1024*1024),
		crtInstance:         -1, //get from storage
		crtConfig:           -1,
		Shutdown:            false,
		counter:             0,
		flush:               true,
		executedUpTo:        -1, //get from storage
		batchWait:           batchWait,
		maxBalInc:           10000,
		maxOpenInstances:    maxOpenInstances,
		crtOpenedInstances:  make([]int32, maxOpenInstances),
		proposableInstances: make(chan ProposalInfo, MAXPROPOSABLEINST),
		noopWaitUs:          noopwait,
		retryInstance:       retryInstances,
		BackoffManager:      NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, &retryInstances, factor, softFac),
		alwaysNoop:          alwaysNoop,
		fastLearn:           false,
		whoCrash:            whoCrash,
		whenCrash:           whenCrash,
		howLongCrash:        howlongCrash,
		emulatedSS:          emulatedSS,
		emulatedWriteTime:   emulatedWriteTime,
		goHeartbeat:         make(chan struct{}),
		timeoutRetry:        make(chan TimeoutInfo, 1000),
		timeout:             timeout,
		catchupBatchSize:    catchupBatchSize,
		lastSettleBatchInst: -1,
		catchingUp:          false,
		flushCommit:         flushCommit,
	}

	if group1Size <= r.N-r.F {
		r.group1Size = r.N - r.F
	} else {
		r.group1Size = group1Size
	}

	r.Durable = durable
	r.clientValueQueue = clientproposalqueue.ClientProposalQueueInit(r.ProposeChan)

	r.prepareRPC = r.RegisterRPC(new(lwcproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(lwcproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(lwcproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(lwcproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(lwcproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(lwcproto.AcceptReply), r.acceptReplyChan)

	go r.run()
	return r
}

func (r *Replica) recordNewConfig(config int32) {
	if !r.Durable {
		return
	}

	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(config))
	r.StableStore.WriteAt(b[:], 0)
}

func (r *Replica) recordExecutedUpTo() {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(r.executedUpTo))
	r.StableStore.WriteAt(b[:], 4)
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
	_, _ = r.StableStore.Write(b[:])
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
	//dlog.Println("synced")
	if r.emulatedSS {
		time.Sleep(r.emulatedWriteTime)
	} else {
		_ = r.StableStore.Sync()
	}
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
		dlog.Println("sending fast clock")
		fastClockChan <- true
	}
}

func (r *Replica) BatchingEnabled() bool {
	return r.batchWait > 0
}

/* ============= */

/* Main event processing loop */
func (r *Replica) restart() {
	for cont := true; cont; {
		select {
		case <-r.prepareChan:
		case <-r.ProposeChan:
		case <-r.retryInstance:
		case <-r.acceptChan:
		case <-r.acceptReplyChan:
		case <-r.commitChan:
		case <-r.commitShortChan:
		case <-r.prepareReplyChan:
		case <-r.configChan:
		case <-r.proposableInstances:
		case <-r.timeoutRetry:
			break
		default:
			cont = false
			break
		}
	}

	r.BackoffManager = NewBackoffManager(r.BackoffManager.minBackoff, r.BackoffManager.maxInitBackoff, r.BackoffManager.maxBackoff, &r.retryInstance, r.BackoffManager.factor, r.BackoffManager.softFac)
	r.crtConfig++
	r.recordNewConfig(r.crtConfig)
	r.catchingUp = true

	r.recoveringFrom = r.executedUpTo + 1
	r.nextRecoveryBatchPoint = r.recoveringFrom
	r.sendNextRecoveryRequestBatch()
}

func (r *Replica) sendNextRecoveryRequestBatch() {
	// assumes r.nextRecoveryBatchPoint is initialised correctly
	for i := int32(0); i < int32(r.N); i++ {
		if i == r.Id {
			continue
		}
		dlog.Printf("Sending next batch for recovery from %d to %d to acceptor %d", r.nextRecoveryBatchPoint, r.nextRecoveryBatchPoint+r.catchupBatchSize, i)
		r.sendRecoveryRequest(r.nextRecoveryBatchPoint, i)
		r.nextRecoveryBatchPoint += r.catchupBatchSize
	}
}

func (r *Replica) makeCatchupInstance(inst int32) {
	r.instanceSpace[inst] = r.makeEmptyInstance()
}

func (r *Replica) sendRecoveryRequest(fromInst int32, toAccptor int32) {
	//for i := fromInst; i < fromInst + r.nextRecoveryBatchPoint; i++ {
	r.makeCatchupInstance(fromInst)
	r.sendSinglePrepare(fromInst, toAccptor)
	//}
}

func (r *Replica) checkAndHandlecatchupRequest(prepare *lwcproto.Prepare) bool {
	//config is ignored here and not acknowledged until new proposals are actually made
	if prepare.IsZero() {
		dlog.Printf("received catch up request from to instance %d to %d", prepare.Instance, prepare.Instance+r.catchupBatchSize)
		r.checkAndHandleCommit(prepare.Instance, prepare.LeaderId, r.catchupBatchSize)
		return true
	} else {
		return false
	}
}

//func (r *Replica) setNextcatchupPoint()
func (r *Replica) checkAndHandlecatchupResponse(commit *lwcproto.Commit) {
	if r.catchingUp {
		//dlog.Printf("got catch up for %d", commit.Instance)
		if r.crtInstance-r.executedUpTo <= r.maxOpenInstances && int32(r.instanceSpace[r.executedUpTo].abk.curBal.PropID) == r.Id && r.executedUpTo > r.recoveringFrom { //r.crtInstance - r.executedUpTo <= r.maxOpenInstances {
			r.catchingUp = false
			dlog.Printf("Caught up with consensus group")
			//reset client connections so that we can begin benchmarking again
			r.Mutex.Lock()
			for i := 0; i < len(r.Clients); i++ {
				_ = r.Clients[i].Close()
			}
			r.Clients = make([]net.Conn, 0)
			r.Mutex.Unlock()
		} else {
			//if commit.Instance == r.nextRecoveryBatchPoint {
			if commit.Instance >= r.nextRecoveryBatchPoint-(r.catchupBatchSize/4) {
				r.sendNextRecoveryRequestBatch()
			}
		}
	}
}

func (r *Replica) doHeartbeat() {
	heartBeat := lwcproto.Prepare{
		LeaderId: r.Id,
		Instance: -1,
		ConfigBal: lwcproto.ConfigBal{
			Config: -1,
			Ballot: lwcproto.Ballot{-1, -1},
		},
	}
	for i := 0; i < r.N-1; i++ {
		if r.PreferredPeerOrder[i] == r.Id {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[i], r.prepareRPC, &heartBeat)
	}

	go func() {
		timer := time.NewTimer(100 * time.Millisecond)
		<-timer.C
		r.goHeartbeat <- struct{}{}
	}()
}

func (r *Replica) run() {
	r.ConnectToPeers()
	r.RandomisePeerOrder()

	fastClockChan = make(chan bool, 1)
	//Enabled fast clock when batching
	if r.BatchingEnabled() {
		go r.fastClock()
	}
	//	onOffProposeChan := r.ProposeChan

	go r.WaitForClientConnections()

	doner := make(chan struct{})
	if r.Id == r.whoCrash {
		go func() {
			t := time.NewTimer(r.whenCrash)
			<-t.C
			doner <- struct{}{}
		}()
	}

	r.doHeartbeat()

	for !r.Shutdown {

		select {
		case <-r.goHeartbeat:
			r.doHeartbeat()
			break
		case maybeTimedout := <-r.timeoutRetry:
			r.retryConfigBal(maybeTimedout)
			break
		case <-doner:
			dlog.Println("Crahsing")
			time.Sleep(r.howLongCrash)
			r.restart()
			dlog.Println("Done crashing")
			break
		case next := <-r.retryInstance:
			dlog.Println("Checking whether to retry a proposal")
			r.tryNextAttempt(next)
			break
		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*lwcproto.Prepare)
			//got a Prepare message
			if prepare.Instance == -1 {
				break
			}
			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)

			if !r.checkAndHandlecatchupRequest(prepare) {
				r.handlePrepare(prepare)
			}
			break
		case acceptS := <-r.acceptChan:
			accept := acceptS.(*lwcproto.Accept)
			//got an Accept message
			dlog.Printf("Received Accept Request from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
			r.handleAccept(accept)
			break
		case commitS := <-r.commitChan:
			commit := commitS.(*lwcproto.Commit)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommit(commit)
			r.checkAndHandlecatchupResponse(commit)
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
		default:
			break
		}

		switch cliProp := r.clientValueQueue.TryDequeue(); {
		case cliProp != nil:
			numEnqueued := r.clientValueQueue.Len() + 1
			batchSize := numEnqueued
			clientProposals := make([]*genericsmr.Propose, batchSize)
			clientProposals[0] = cliProp

			for i := 1; i < batchSize; i++ {
				cliProp = r.clientValueQueue.TryDequeue()
				if cliProp == nil {
					clientProposals = clientProposals[:i]
					break
				}
				clientProposals[i] = cliProp
			}
			dlog.Println("Client value(s) received beginning new instance")
			r.beginNextInstance(clientProposals)
		}
	}
}

func (r *Replica) retryConfigBal(maybeTimedout TimeoutInfo) {
	inst := r.instanceSpace[maybeTimedout.inst]
	if inst.pbk.propCurConfBal.Equal(maybeTimedout.proposingConfBal) && inst.pbk.status == maybeTimedout.phase {
		if maybeTimedout.phase == PREPARING {
			dlog.Printf("retrying instance in phase 1")
			r.bcastPrepare(maybeTimedout.inst)
		} else if maybeTimedout.phase == PROPOSING {
			dlog.Printf("retrying instance in phase 2")
			r.bcastAccept(maybeTimedout.inst)
		}
	}
}

func (r *Replica) tryNextAttempt(next RetryInfo) {
	inst := r.instanceSpace[next.InstToPrep]
	if !next.backedoff {
		if inst == nil {
			r.instanceSpace[next.InstToPrep] = r.makeEmptyInstance()
			inst = r.instanceSpace[next.InstToPrep]
		}
	}

	if (r.BackoffManager.NoHigherBackoff(next) || !next.backedoff) && inst.pbk.status == BACKING_OFF {
		r.proposerBeginNextConfBal(next.InstToPrep)
		nextConfBal := r.instanceSpace[next.InstToPrep].pbk.propCurConfBal

		r.acceptorPrepareOnConfBal(next.InstToPrep, nextConfBal)
		r.bcastPrepare(next.InstToPrep)
		dlog.Printf("Proposing next conf-bal %d.%d.%d to instance %d\n", nextConfBal.Config, nextConfBal.Number, nextConfBal.PropID, next.InstToPrep)
	} else {
		dlog.Printf("Skipping retry of instance %d due to preempted again or closed\n", next.InstToPrep)
	}
}

func (r *Replica) sendSinglePrepare(instance int32, to int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()
	args := &lwcproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurConfBal}
	dlog.Printf("send prepare to %d\n", to)
	r.SendMsg(to, r.prepareRPC, args)
	whoSent := []int32{0}
	r.beginTimeout(args.Instance, args.ConfigBal, whoSent, PREPARING, r.timeout*10)
}

func (r *Replica) bcastPrepareToAll(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()

	args := &lwcproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurConfBal}
	n := r.N - 1
	r.RandomisePeerOrder()
	sent := 0
	whoSent := make([]int32, n)
	for q := 0; q < r.N-1; q++ {
		dlog.Printf("send prepare to %d\n", r.PreferredPeerOrder[q])
		r.SendMsg(r.PreferredPeerOrder[q], r.prepareRPC, args)
		whoSent[sent] = r.PreferredPeerOrder[q]
		sent++
		if sent >= n {
			break
		}
	}
	r.beginTimeout(args.Instance, args.ConfigBal, whoSent, PREPARING, r.timeout)
}

func (r *Replica) bcastPrepare(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()

	args := &lwcproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurConfBal}

	n := r.N - 1

	if r.Thrifty {
		n = int(r.group1Size)
	}
	r.CalculateAlive()
	r.RandomisePeerOrder()
	sent := 0
	whoSent := make([]int32, n)
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		dlog.Printf("send prepare to %d\n", r.PreferredPeerOrder[q])
		r.SendMsg(r.PreferredPeerOrder[q], r.prepareRPC, args)
		whoSent[sent] = r.PreferredPeerOrder[q]
		sent++
		if sent >= n {
			break
		}
	}
	r.beginTimeout(args.Instance, args.ConfigBal, whoSent, PREPARING, r.timeout)
}

func (r *Replica) beginTimeout(inst int32, attempted lwcproto.ConfigBal, whoSent []int32, onWhatPhase ProposerStatus, timeout time.Duration) {
	go func(instance int32, tried lwcproto.ConfigBal, whoWeSentTo []int32, phase ProposerStatus, timeoutWait time.Duration) {
		timer := time.NewTimer(timeout)
		<-timer.C
		if r.instanceSpace[inst].pbk.propCurConfBal.Equal(tried) && r.instanceSpace[inst].pbk.status == phase {
			// not atomic and might change when message received but that's okay (only to limit number of channel messages sent)
			r.timeoutRetry <- TimeoutInfo{
				ProposalInfo: ProposalInfo{instance, tried},
				phase:        phase,
				whoTried:     whoWeSentTo,
			}
		}
	}(inst, attempted, whoSent, onWhatPhase, timeout)
}

var pa lwcproto.Accept

func (r *Replica) bcastAcceptToAll(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Accept bcast failed:", err)
		}
	}()

	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.ConfigBal = r.instanceSpace[instance].pbk.propCurConfBal
	pa.Command = r.instanceSpace[instance].pbk.cmds
	pa.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
	args := &pa

	n := r.N - 1
	//if r.Thrifty {
	//	n = r.WriteQuorumSize() - 1
	//}

	r.CalculateAlive()
	r.RandomisePeerOrder()
	whoSent := make([]int32, n)
	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		dlog.Printf("send accept to %d\n", r.PreferredPeerOrder[q])
		r.SendMsg(r.PreferredPeerOrder[q], r.acceptRPC, args)
		whoSent[sent] = r.PreferredPeerOrder[q]
		sent++
		if sent >= n {
			break
		}
	}
	r.beginTimeout(args.Instance, args.ConfigBal, whoSent, PROPOSING, r.timeout)
}

func (r *Replica) bcastAccept(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Accept bcast failed:", err)
		}
	}()

	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.ConfigBal = r.instanceSpace[instance].pbk.propCurConfBal
	pa.Command = r.instanceSpace[instance].pbk.cmds
	pa.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
	args := &pa

	n := r.N - 1
	if r.Thrifty {
		n = r.WriteQuorumSize() - 1
	}

	r.CalculateAlive()
	r.RandomisePeerOrder()
	whoSent := make([]int32, n)
	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		dlog.Printf("send accept to %d\n", r.PreferredPeerOrder[q])
		r.SendMsg(r.PreferredPeerOrder[q], r.acceptRPC, args)
		whoSent[sent] = r.PreferredPeerOrder[q]
		sent++
		if sent >= n {
			break
		}
	}
	r.beginTimeout(args.Instance, args.ConfigBal, whoSent, PROPOSING, r.timeout)
}

var pc lwcproto.Commit
var pcs lwcproto.CommitShort

func (r *Replica) bcastCommitToAll(instance int32, confBal lwcproto.ConfigBal, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.ConfigBal = confBal
	pc.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
	pc.Command = command

	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.ConfigBal = confBal
	pcs.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
	pcs.Count = int32(len(command))
	argsShort := &pcs

	r.CalculateAlive()
	sent := 0
	for q := 0; q < r.N-1; q++ {
		_, inQrm := r.instanceSpace[instance].pbk.proposalInfos[confBal].aids[r.PreferredPeerOrder[q]]
		if inQrm {
			if r.flushCommit {
				r.SendMsg(r.PreferredPeerOrder[q], r.commitShortRPC, argsShort)
			} else {
				r.SendMsgNoFlush(r.PreferredPeerOrder[q], r.commitShortRPC, argsShort)
			}
		} else {
			if r.flushCommit {
				r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, &pc)
			} else {
				r.SendMsgNoFlush(r.PreferredPeerOrder[q], r.commitRPC, &pc)
			}
		}
		sent++
	}
}

func (r *Replica) getNextProposingConfigBal(instance int32) lwcproto.ConfigBal {
	pbk := r.instanceSpace[instance].pbk
	//abk := r.instanceSpace[instance].abk
	min := ((pbk.maxKnownBal.Number/r.maxBalInc)+1)*r.maxBalInc + int32(r.N)
	//zero := time.Time{}
	var max int32
	//	if r.timeSinceLastProposedInstance != zero {
	//	timeDif := time.Now().Sub(r.timeSinceLastProposedInstance)
	//	max = min + (r.maxBalInc) + int32(timeDif.Milliseconds()*10)
	//	} else {
	max = min + (r.maxBalInc)
	//	}

	next := int32(math.Floor(rand.Float64()*float64(max-min+1) + float64(min)))

	//if (instance % int32(r.N)) == r.Id {
	//		next += max
	//	}

	proposingConfBal := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: lwcproto.Ballot{next - r.Id, int16(r.Id)}, //- int16(r.Id / 2), int16(r.Id)},
	}

	dlog.Printf("For instance", instance, "now incrementing to new conf-bal", proposingConfBal)
	return proposingConfBal
}

func (r *Replica) incToNextOpenInstance() {
	r.crtInstance++

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

			maxKnownBal: lwcproto.Ballot{-1, -1},
			maxAcceptedConfBal: lwcproto.ConfigBal{
				Config: -1,
				Ballot: lwcproto.Ballot{-1, -1},
			},
			whoseCmds: -1,
			cmds:      nil,
			propCurConfBal: lwcproto.ConfigBal{
				Config: -1,
				Ballot: lwcproto.Ballot{-1, -1},
			},
			clientProposals: nil,
		},
	}
}

func (r *Replica) proposerBeginNextConfBal(inst int32) {
	pbk := r.instanceSpace[inst].pbk

	pbk.status = PREPARING
	nextConfBal := r.getNextProposingConfigBal(inst)
	pbk.proposalInfos[nextConfBal] = new(QuorumInfo)

	pbk.propCurConfBal = nextConfBal
	pbk.maxKnownBal = nextConfBal.Ballot

	pbk.proposalInfos[nextConfBal] = NewQuorumInfo(PROMISE)
}

func (r *Replica) beginNextInstance(valsToPropose []*genericsmr.Propose) {
	r.incToNextOpenInstance()
	r.instanceSpace[r.crtInstance] = r.makeEmptyInstance()
	curInst := r.instanceSpace[r.crtInstance]
	r.proposerBeginNextConfBal(r.crtInstance)
	r.instanceSpace[r.crtInstance].pbk.clientProposals = valsToPropose
	r.acceptorPrepareOnConfBal(r.crtInstance, curInst.pbk.propCurConfBal)
	r.bcastPrepare(r.crtInstance)
	dlog.Printf("Opened new instance %d\n", r.crtInstance)
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	dlog.Printf("Received new client value\n")
	r.clientValueQueue.TryEnqueue(propose)
	//check if any open instances
}

func (r *Replica) acceptorPrepareOnConfBal(inst int32, confBal lwcproto.ConfigBal) {

	instance := r.instanceSpace[inst]
	abk := instance.abk
	//	if r.instanceSpace[inst].pbk.status == CLOSED || r.instanceSpace[inst].abk.status == COMMITTED {
	//		panic("oh nnooooooooo")
	//	}
	//	cur := lwcproto.ConfigBal{
	//		Config: r.crtConfig,
	//		Ballot: abk.curBal,
	//	}
	//if cur.GreaterThan(confBal) && !cur.Equal(confBal) {
	//		panic("preparing on out of date conf bal")
	//	}
	abk.status = PREPARED
	dlog.Printf("Acceptor Preparing Config-Ballot %d.%d.%d ", confBal.Config, confBal.Number, confBal.PropID)
	abk.curBal = confBal.Ballot
}

func (r *Replica) acceptorAcceptOnConfBal(inst int32, confBal lwcproto.ConfigBal, cmds []state.Command) {

	abk := r.instanceSpace[inst].abk

	abk.status = ACCEPTED

	//cur := lwcproto.ConfigBal{
	//	Config: r.crtConfig,
	//	Ballot: abk.curBal,
	//}
	//if cur.GreaterThan(confBal) {
	//	panic("accepted outof date conf bal")
	//}

	dlog.Printf("Acceptor Accepting Config-Ballot %d.%d.%d ", confBal.Config, confBal.Number, confBal.PropID)
	abk.curBal = confBal.Ballot
	abk.vConfBal = confBal
	abk.cmds = cmds

	r.recordInstanceMetadata(r.instanceSpace[inst])
	r.recordCommands(cmds)
	r.sync()
}

func (r *Replica) proposerCheckAndHandlePreempt(inst int32, preemptingConfigBal lwcproto.ConfigBal, preemterPhase QuorumType) bool {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if pbk.status != CLOSED && preemptingConfigBal.GreaterThan(pbk.propCurConfBal) {
		//if pbk.status != BACKING_OFF { // option for add multiple preempts if backing off already?
		r.BackoffManager.CheckAndHandleBackoff(inst, pbk.propCurConfBal, preemptingConfigBal, preemterPhase)

		// if we are preparing for a new instance to propose in but are preempted
		if pbk.status == PREPARING && pbk.clientProposals != nil {
			r.requeueClientProposals(inst)
			pbk.clientProposals = nil
		}

		if preemptingConfigBal.Ballot.GreaterThan(pbk.maxKnownBal) {
			pbk.maxKnownBal = preemptingConfigBal.Ballot
		}

		pbk.status = BACKING_OFF
		return true
	} else {
		return false
	}
}

func (r *Replica) checkAndHandleConfigPreempt(inst int32, preemptingConfigBal lwcproto.ConfigBal, preemterPhase QuorumType) bool {
	// in this function there is an error where if we start up a process and other processes are at a higher config, we will go into this function
	// if
	if r.crtConfig < preemptingConfigBal.Config {
		dlog.Printf("Current config %d preempted by config %d (instance %d)\n", r.crtConfig, preemptingConfigBal.Config, inst)
		r.recordNewConfig(preemptingConfigBal.Config)
		r.sync()
		r.crtConfig = preemptingConfigBal.Config

		for i := r.executedUpTo; i <= r.crtInstance; i++ {
			pbk := r.instanceSpace[i].pbk
			if r.instanceSpace[i] != nil {
				if r.instanceSpace[i].abk.status != COMMITTED {
					if i != inst {
						pbk.status = PREPARING
						r.proposerBeginNextConfBal(i)
						nextConfBal := r.instanceSpace[i].pbk.propCurConfBal
						r.acceptorPrepareOnConfBal(i, nextConfBal)
						r.bcastPrepare(i)
					} else {
						pbk.status = BACKING_OFF
						if preemptingConfigBal.Ballot.GreaterThan(pbk.maxKnownBal) {
							pbk.maxKnownBal = preemptingConfigBal.Ballot
						}
						r.BackoffManager.CheckAndHandleBackoff(i, pbk.propCurConfBal, preemptingConfigBal, preemterPhase)
					}
				}
			}
		}
		return true
	} else {
		return false
	}
}

func (r *Replica) checkAndHandleCommit(instance int32, whoRespondTo int32, maxExtraInstances int32) bool {
	inst := r.instanceSpace[instance]
	if inst.abk.status == COMMITTED {
		count := int32(0)
		for i := instance; i < r.crtInstance; i++ {
			returingInst := r.instanceSpace[i]
			if returingInst != nil {
				if returingInst.abk.status == COMMITTED {
					dlog.Printf("Already committed instance %d, returning commit to %d \n", instance, whoRespondTo)
					pc.LeaderId = int32(returingInst.abk.vConfBal.PropID) //prepare.LeaderId
					pc.Instance = i
					pc.ConfigBal = returingInst.abk.vConfBal
					pc.Command = returingInst.abk.cmds
					pc.WhoseCmd = returingInst.pbk.whoseCmds
					args := &pc
					if count < maxExtraInstances {
						count++
						pc.MoreToCome = 0
						r.SendMsgNoFlush(whoRespondTo, r.commitRPC, args)
					} else {
						break
					}
				}
			}
		}
		_ = r.PeerWriters[whoRespondTo].Flush()
		return true
	} else {
		return false
	}
}

func (r *Replica) handlePrepare(prepare *lwcproto.Prepare) {
	r.checkAndHandleNewlyReceivedInstance(prepare.Instance)
	configPreempted := r.checkAndHandleConfigPreempt(prepare.Instance, prepare.ConfigBal, PROMISE)

	if r.checkAndHandleCommit(prepare.Instance, prepare.LeaderId, r.crtInstance-prepare.Instance) {
		return
	}

	if configPreempted {
		return
	}

	inst := r.instanceSpace[prepare.Instance]
	minSafe := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: inst.abk.curBal,
	}

	if minSafe.GreaterThan(prepare.ConfigBal) {
		dlog.Printf("Already prepared on higher Config-Ballot %d.%d.%d < %d.%d.%d", prepare.Config, prepare.Number, prepare.PropID, minSafe.Config, minSafe.Number, minSafe.PropID)
	} else if prepare.ConfigBal.GreaterThan(minSafe) {
		dlog.Printf("Preparing on prepared on new Config-Ballot %d.%d.%d", prepare.Config, prepare.Number, prepare.PropID)
		r.acceptorPrepareOnConfBal(prepare.Instance, prepare.ConfigBal)
		r.proposerCheckAndHandlePreempt(prepare.Instance, prepare.ConfigBal, PROMISE)
		r.checkAndHandleOldPreempted(prepare.ConfigBal, minSafe, inst.abk.vConfBal, inst.abk.cmds, prepare.Instance)

	} else {
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
		WhoseCmd:   inst.pbk.whoseCmds,
		Command:    inst.abk.cmds,
	}

	r.replyPrepare(prepare.LeaderId, preply)
}

func (r *Replica) checkAndHandleOldPreempted(new lwcproto.ConfigBal, old lwcproto.ConfigBal, accepted lwcproto.ConfigBal, acceptedVal []state.Command, inst int32) {
	if new.PropID != old.PropID && int32(new.PropID) != r.Id && old.PropID != -1 && new.GreaterThan(old) {
		preemptOldPropMsg := &lwcproto.PrepareReply{
			Instance:   inst,
			ConfigBal:  new,
			VConfigBal: accepted,
			WhoseCmd:   r.instanceSpace[inst].pbk.whoseCmds,
			AcceptorId: r.Id,
			Command:    acceptedVal,
		}
		r.replyPrepare(int32(new.PropID), preemptOldPropMsg)
	}
}

type ProposerAccValHandler int

const (
	IGNORED ProposerAccValHandler = iota
	NEW_VAL
	ACKED
	CHOSEN
)

func (pbk *ProposingBookkeeping) isProposingClientValue() bool {
	return pbk.status == PROPOSING && pbk.clientProposals != nil
}

func (r *Replica) proposerCheckAndHandleAcceptedValue(inst int32, aid int32, accepted lwcproto.ConfigBal, val []state.Command, whoseCmds int32) ProposerAccValHandler {
	if accepted.IsZero() {
		return IGNORED
	}
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	newVal := false

	if pbk.status == CLOSED {
		return CHOSEN
	}
	if accepted.Ballot.GreaterThan(pbk.maxKnownBal) {
		pbk.maxKnownBal = accepted.Ballot
	}
	if accepted.GreaterThan(pbk.maxAcceptedConfBal) {
		newVal = true
		pbk.whoseCmds = whoseCmds
		pbk.maxAcceptedConfBal = accepted
		pbk.cmds = val

		if r.whatHappenedToClientProposals(inst) == ProposedButNotChosen {
			r.requeueClientProposals(inst)
			pbk.clientProposals = nil
		}
	}

	_, exists := pbk.proposalInfos[accepted]
	if !exists {
		pbk.proposalInfos[accepted] = NewQuorumInfo(ACCEPTANCE)
	}
	pbk.proposalInfos[accepted].quorumAdd(aid)
	// not assumed local acceptor has accepted it
	if int(pbk.proposalInfos[accepted].quorumCount()) >= r.WriteQuorumSize() {
		//	if pbk.maxAcceptedConfBal.GreaterThan(accepted) && pbk.whoseCmds != whoseCmds && pbk.proposalInfos[pbk.propCurConfBal].qrmType == ACCEPTANCE {
		//		panic("break in safety!!!")
		//	}
		r.bcastCommitToAll(inst, accepted, val)
		r.acceptorCommit(inst, accepted, val)
		r.proposerCloseCommit(inst, accepted, pbk.cmds, whoseCmds)
		//		if pbk.status != CLOSED || instance.abk.status != COMMITTED {
		//		panic("not set agents to closed")
		//	}
		return CHOSEN
	} else if newVal {
		return NEW_VAL
	} else {
		return ACKED
	}
}

func (r *Replica) handlePrepareReply(preply *lwcproto.PrepareReply) {
	inst := r.instanceSpace[preply.Instance]
	pbk := inst.pbk
	configPreempt := r.checkAndHandleConfigPreempt(preply.Instance, preply.ConfigBal, PROMISE)
	// todo should do check and handle commit instead???
	if inst.abk.status == COMMITTED {
		dlog.Println("Inst already known to be chosen")
		return
	}

	valWhatDone := r.proposerCheckAndHandleAcceptedValue(preply.Instance, preply.AcceptorId, preply.VConfigBal, preply.Command, preply.WhoseCmd)
	if r.fastLearn {
		valWhatDone = r.proposerCheckAndHandleAcceptedValue(preply.Instance, int32(preply.VConfigBal.PropID), preply.VConfigBal, preply.Command, preply.WhoseCmd)
	}
	if valWhatDone == NEW_VAL {
		dlog.Printf("Promise from %d in instance %d has new value at Config-Ballot %d.%d.%d", preply.AcceptorId,
			preply.Instance, preply.VConfigBal.Config, preply.VConfigBal.Number, preply.VConfigBal.PropID)
	} else if valWhatDone == CHOSEN {
		dlog.Printf("Preparing instance recognised as chosen (instance %d), returning commit \n", preply.Instance)

		return
	}

	if configPreempt {
		return
	}

	if pbk.propCurConfBal.GreaterThan(preply.ConfigBal) || pbk.status != PREPARING {
		dlog.Printf("Message in late \n")
		return
	}

	if r.proposerCheckAndHandlePreempt(preply.Instance, preply.ConfigBal, PROMISE) {
		dlog.Printf("Another active proposer using config-ballot %d.%d.%d greater than mine\n", preply.ConfigBal)
		r.acceptorPrepareOnConfBal(preply.Instance, preply.ConfigBal)
		myReply := lwcproto.PrepareReply{
			Instance:   preply.Instance,
			ConfigBal:  preply.ConfigBal,
			VConfigBal: inst.abk.vConfBal,
			AcceptorId: r.Id,
			WhoseCmd:   pbk.whoseCmds,
			Command:    inst.abk.cmds,
		}
		r.SendMsg(int32(preply.ConfigBal.PropID), r.prepareReplyRPC, &myReply)
		return
	}

	qrm := pbk.proposalInfos[pbk.propCurConfBal]
	qrm.quorumAdd(preply.AcceptorId)
	dlog.Printf("Added replica's %d promise to qrm", preply.AcceptorId)

	if int(qrm.quorumCount()+1) >= r.Replica.ReadQuorumSize() {
		r.propose(preply.Instance)
	}
}

func (r *Replica) propose(inst int32) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	pbk.status = READY_TO_PROPOSE
	dlog.Println("Can now propose in instance", inst)
	qrm := pbk.proposalInfos[pbk.propCurConfBal]
	qrm.quorumClear()
	qrm.qrmType = ACCEPTANCE

	whoseCmds := int32(-1)
	if pbk.maxAcceptedConfBal.IsZero() {
		whoseCmds = r.Id
		if pbk.clientProposals != nil {
			pbk.cmds = make([]state.Command, len(pbk.clientProposals))
			for i, prop := range pbk.clientProposals {
				pbk.cmds[i] = prop.Command
			}
		} else {
			switch cliProp := r.clientValueQueue.TryDequeue(); {
			case cliProp != nil:
				numEnqueued := r.clientValueQueue.Len() + 1
				batchSize := numEnqueued
				pbk.clientProposals = make([]*genericsmr.Propose, batchSize)
				pbk.cmds = make([]state.Command, batchSize)
				pbk.clientProposals[0] = cliProp
				pbk.cmds[0] = cliProp.Command

				for i := 1; i < batchSize; i++ {
					//	cliProp = <- r.ProposeChan
					cliProp = r.clientValueQueue.TryDequeue()
					if cliProp == nil {
						pbk.clientProposals = pbk.clientProposals[:i]
						pbk.cmds = pbk.cmds[:i]
						break
					}
					pbk.clientProposals[i] = cliProp
					pbk.cmds[i] = cliProp.Command
				}

				dlog.Printf("%d client value(s) received and proposed in instance %d which was recovered \n", len(pbk.clientProposals), inst)
				break
			default:
				pbk.cmds = state.NOOP()
				dlog.Println("Proposing noop in recovered instance")
			}
		}
	} else {

		whoseCmds = pbk.whoseCmds
	}

	pbk.status = PROPOSING
	r.proposerCheckAndHandleAcceptedValue(inst, r.Id, pbk.propCurConfBal, pbk.cmds, whoseCmds)
	// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
	if r.fastLearn {
		r.acceptorAcceptOnConfBal(inst, pbk.propCurConfBal, pbk.cmds)
		r.bcastAccept(inst)
	} else {
		r.bcastAccept(inst)
		r.acceptorAcceptOnConfBal(inst, pbk.propCurConfBal, pbk.cmds)
	}
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
	configPreempted := r.checkAndHandleConfigPreempt(accept.Instance, accept.ConfigBal, ACCEPTANCE)

	if r.checkAndHandleCommit(accept.Instance, accept.LeaderId, r.crtInstance-accept.Instance) {
		return
	} else if configPreempted {
		return
	}

	inst := r.instanceSpace[accept.Instance]
	minAcceptableConfBal := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: inst.abk.curBal,
	}

	// should always be unless there is a restart
	if accept.ConfigBal.GreaterThan(minAcceptableConfBal) || accept.ConfigBal.Equal(minAcceptableConfBal) {
		dlog.Printf("Accepted instance %d on conf-ball %d.%d.%d", accept.Instance, accept.Config, accept.Number, accept.PropID)
		r.acceptorAcceptOnConfBal(accept.Instance, accept.ConfigBal, accept.Command)

		//check proposer qrm
		// here is where we can add fast learning bit - also add acceptance by config-bal's owner
		if r.fastLearn {
			r.proposerCheckAndHandleAcceptedValue(accept.Instance, int32(accept.PropID), accept.ConfigBal, accept.Command, accept.WhoseCmd) // must go first as acceptor might have already learnt of value
		}
		accValState := r.proposerCheckAndHandleAcceptedValue(accept.Instance, r.Id, accept.ConfigBal, accept.Command, accept.WhoseCmd)
		if accValState == CHOSEN {
			return
		}
		r.proposerCheckAndHandlePreempt(accept.Instance, accept.ConfigBal, ACCEPTANCE)
	} else if minAcceptableConfBal.GreaterThan(accept.ConfigBal) {
		dlog.Printf("Returning preempt for config-ballot %d.%d.%d < %d.%d.%d in Instance %d\n", accept.Config, accept.Number, accept.PropID, minAcceptableConfBal.Config, minAcceptableConfBal.Number, minAcceptableConfBal.PropID, accept.Instance)
	} else {
		dlog.Printf("Already acknowledged accept request but will return again", accept.Config, accept.Number, accept.PropID, minAcceptableConfBal.Config, minAcceptableConfBal.Number, minAcceptableConfBal.PropID, accept.Instance)
	}

	replyConfBal := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: inst.abk.curBal,
	}

	areply := &lwcproto.AcceptReply{accept.Instance, r.Id, replyConfBal, accept.ConfigBal, inst.pbk.whoseCmds}
	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) handleAcceptReply(areply *lwcproto.AcceptReply) {
	// could modify to have record of all ballots
	inst := r.instanceSpace[areply.Instance]
	pbk := inst.pbk
	if r.checkAndHandleConfigPreempt(areply.Instance, areply.Cur, ACCEPTANCE) {
		return
	} else if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed ")
		return
	}

	accepted := areply.Cur.Equal(areply.Req)
	preempted := areply.Cur.GreaterThan(areply.Req)
	if accepted {
		dlog.Printf("Acceptance of instance %d at %d.%d.%d by Acceptor %d received\n", areply.Instance, areply.Cur.Config, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId)
		r.proposerCheckAndHandleAcceptedValue(areply.Instance, areply.AcceptorId, areply.Cur, pbk.cmds, areply.WhoseCmd)
		// we can count proposer of value too because they durably accept before sending accept request - only if fast learn is on
		if r.fastLearn {
			r.proposerCheckAndHandleAcceptedValue(areply.Instance, int32(areply.Req.PropID), areply.Cur, pbk.cmds, areply.WhoseCmd)
		}
	} else if preempted {
		r.proposerCheckAndHandlePreempt(areply.Instance, areply.Cur, ACCEPTANCE)
	} else {
		msg := fmt.Sprintf("Somehow cur Conf-Bal of %d is %d.%d.%d when we requested %d.%d.%d for acceptance",
			areply.AcceptorId, areply.Cur.Config, areply.Cur.Number, areply.Cur.PropID,
			areply.Req.Config, areply.Req.Number, areply.Req.PropID)
		panic(msg)
	}
}

func (r *Replica) requeueClientProposals(instance int32) {
	inst := r.instanceSpace[instance]
	dlog.Printf("Requeing client values in instance %d", instance)
	for i := 0; i < len(inst.pbk.clientProposals); i++ {
		//r.ProposeChan <- inst.pbk.clientProposals[i]
		r.clientValueQueue.TryRequeue(inst.pbk.clientProposals[i])
	}
}

type ClientProposalStory int

const (
	NotProposed ClientProposalStory = iota
	ProposedButNotChosen
	ProposedAndChosen
)

func (r *Replica) whatHappenedToClientProposals(instance int32) ClientProposalStory {
	inst := r.instanceSpace[instance]
	pbk := inst.pbk
	if pbk.whoseCmds != r.Id && pbk.clientProposals != nil {
		//dlog.Printf("not chosen but proposed")
		return ProposedButNotChosen
	} else if pbk.whoseCmds == r.Id {
		//dlog.Printf("chosen proposal")
		return ProposedAndChosen
	} else {
		//dlog.Printf("not proposed")
		return NotProposed
	}
}

func (r *Replica) howManyAttemptsToChoose(inst int32) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	attempts := pbk.maxAcceptedConfBal.Number / r.maxBalInc
	dlog.Printf("Attempts to chose instance %d: %d", inst, attempts)
}

func (r *Replica) proposerCloseCommit(inst int32, chosenAt lwcproto.ConfigBal, chosenVal []state.Command, whoseCmd int32) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	pbk.status = CLOSED
	dlog.Printf("Instance %d chosen now\n", inst)

	r.BackoffManager.ClearBackoff(inst)

	if int32(chosenAt.PropID) == r.Id {
		r.timeSinceLastProposedInstance = time.Now()
	}

	pbk.maxAcceptedConfBal = chosenAt
	pbk.cmds = chosenVal
	pbk.whoseCmds = whoseCmd

	switch r.whatHappenedToClientProposals(inst) {
	case NotProposed:
		break
	case ProposedButNotChosen:
		dlog.Printf("%d client value(s) proposed in instance %d\n not chosen", len(pbk.clientProposals), inst)
		r.requeueClientProposals(inst)
		pbk.clientProposals = nil
		break
	case ProposedAndChosen:
		dlog.Printf("%d client value(s) chosen in instance %d\n", len(pbk.clientProposals), inst)
		//		for i := 0; i < len(pbk.clientProposals); i++ {
		//		r.clientValueQueue.CloseValue(pbk.clientProposals[i])
		//	}
		break
	}

	//if r.instanceSpace[inst].pbk.status != CLOSED && r.instanceSpace[inst].abk.status != COMMITTED {
	//	panic("not commited somehow")
	//}
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
	//r.howManyAttemptsToChoose(inst)

	if r.Exec {
		oldExecutedUpTo := r.executedUpTo
		for i := r.executedUpTo + 1; i <= r.crtInstance; i++ {
			returnInst := r.instanceSpace[i]
			if returnInst != nil && returnInst.abk.status == COMMITTED { //&& returnInst.abk.cmds != nil {
				dlog.Printf("Executing instance %d\n", i)
				for j := 0; j < len(returnInst.abk.cmds); j++ {
					dlog.Printf("Executing " + returnInst.abk.cmds[j].String())
					if r.Dreply && returnInst.pbk != nil && returnInst.pbk.clientProposals != nil {
						val := returnInst.abk.cmds[j].Execute(r.State)

						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							returnInst.pbk.clientProposals[j].CommandId,
							val,
							returnInst.pbk.clientProposals[j].Timestamp}
						r.ReplyProposeTS(propreply, returnInst.pbk.clientProposals[j].Reply, returnInst.pbk.clientProposals[j].Mutex)
						dlog.Printf("Returning executed client value")
					} else if returnInst.abk.cmds[j].Op == state.PUT {
						returnInst.abk.cmds[j].Execute(r.State)
					}

				}
				//	returnInst.pbk = nil
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

func (r *Replica) acceptorCommit(instance int32, chosenAt lwcproto.ConfigBal, cmds []state.Command) {
	inst := r.instanceSpace[instance]
	abk := inst.abk
	dlog.Printf("Committing (crtInstance=%d)\n", instance)

	inst.abk.status = COMMITTED
	knowsVal := abk.vConfBal.Equal(chosenAt)
	shouldSync := false
	if r.crtConfig < chosenAt.Config {
		r.recordNewConfig(chosenAt.Config)
		r.crtConfig = chosenAt.Config
		shouldSync = true
	}

	abk.curBal = chosenAt.Ballot
	abk.vConfBal = chosenAt
	abk.cmds = cmds

	if !knowsVal {
		r.recordInstanceMetadata(inst)
		r.recordCommands(cmds)
		if shouldSync {
			r.sync()
		}
	}
}

func (r *Replica) handleCommit(commit *lwcproto.Commit) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	inst := r.instanceSpace[commit.Instance]

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed \n")
		return
	}

	r.acceptorCommit(commit.Instance, commit.ConfigBal, commit.Command)
	r.proposerCloseCommit(commit.Instance, commit.ConfigBal, commit.Command, commit.WhoseCmd)
}

func (r *Replica) handleCommitShort(commit *lwcproto.CommitShort) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	inst := r.instanceSpace[commit.Instance]

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed \n")
		return
	}

	r.acceptorCommit(commit.Instance, commit.ConfigBal, inst.abk.cmds)
	r.proposerCloseCommit(commit.Instance, commit.ConfigBal, inst.abk.cmds, commit.WhoseCmd)
}
