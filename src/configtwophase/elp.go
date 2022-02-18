package configtwophase

import (
	"CommitExecutionComparator"
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
	"quorumsystem"
	"runnable"
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

type elpReplica struct {
	//	quorumsystem.SynodQuorumSystem
	ProposalManager
	//	quorum.QuorumTally
	//quorumsystem.QuorumSystemConstructor
	*genericsmr.Replica        // extends a generic Paxos replica
	configChan                 chan fastrpc.Serializable
	prepareChan                chan fastrpc.Serializable
	acceptChan                 chan fastrpc.Serializable
	commitChan                 chan fastrpc.Serializable
	commitShortChan            chan fastrpc.Serializable
	prepareReplyChan           chan fastrpc.Serializable
	acceptReplyChan            chan fastrpc.Serializable
	prepareRPC                 uint8
	acceptRPC                  uint8
	commitRPC                  uint8
	commitShortRPC             uint8
	prepareReplyRPC            uint8
	acceptReplyRPC             uint8
	instanceSpace              []*Instance // the space of all instances (used and not yet used)
	crtInstance                int32       // highest active instance number that this replica knows about
	crtConfig                  int32
	Shutdown                   bool
	counter                    int
	flush                      bool
	executedUpTo               int32
	batchWait                  int
	maxBalInc                  int32
	maxOpenInstances           int32
	crtOpenedInstances         []int32
	proposableInstances        chan ProposalInfo
	clientValueQueue           clientproposalqueue.ClientProposalQueue
	noopWaitUs                 int32
	retryInstance              chan RetryInfo
	BackoffManager             BackoffManager
	alwaysNoop                 bool
	lastTimeClientChosen       time.Time
	lastOpenProposalTime       time.Time
	timeSinceValueLastSelected time.Time
	fastLearn                  bool
	whenCrash                  time.Duration
	howLongCrash               time.Duration
	whoCrash                   int32
	initalProposalWait         time.Duration
	emulatedSS                 bool
	emulatedWriteTime          time.Duration
	timeoutMsgs                chan TimeoutInfo // messages to send after a time out
	catchingUp                 bool
	catchUpBatchSize           int32
	lastSettleBatchInst        int32
	timeout                    time.Duration
	group1Size                 int
	flushCommit                bool
	ringCommit                 bool
	nextCatchUpPoint           int32
	recoveringFrom             int32
	commitCatchUp              bool
	commitExecComp             *CommitExecutionComparator.CommitExecutionComparator
	cmpCommitExec              bool
	maxBatchedProposalVals     int
	stats                      ServerStats
	requeueOnPreempt           bool
	reducePropConfs            bool
	bcastAcceptance            bool
	minBatchSize               int32
}

type TimeoutInfo struct {
	ProposalInfo
	phase   ProposerStatus
	msgCode uint8
	msg     fastrpc.Serializable
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

type Phase int

const (
	PROMISE Phase = iota
	ACCEPTANCE
)

type QuorumInfo struct {
	qrmType Phase
	nacks   int
	aids    map[int32]struct{}
}

func (r *elpReplica) GetPBK(inst int32) *ProposingBookkeeping {
	return r.instanceSpace[inst].pbk
}

type RetryInfo struct {
	backedoff        bool
	InstToPrep       int32
	attemptedConfBal lwcproto.ConfigBal
	preempterConfBal lwcproto.ConfigBal
	preempterAt      Phase
	prev             int32
	timesPreempted   int32
}

type BackoffInfo struct {
	minBackoff     int32
	maxInitBackoff int32
	maxBackoff     int32
	constBackoff   bool
}

type BackoffManager struct {
	currentBackoffs map[int32]RetryInfo
	BackoffInfo
	sig      *chan RetryInfo
	factor   float64
	mapMutex sync.RWMutex
	softFac  bool
}

func NewBackoffManager(minBO, maxInitBO, maxBO int32, signalChan *chan RetryInfo, factor float64, softFac bool, constBackoff bool) BackoffManager {
	if minBO > maxInitBO {
		panic(fmt.Sprintf("minbackoff %d, maxinitbackoff %d, incorrectly set up", minBO, maxInitBO))
	}

	return BackoffManager{
		currentBackoffs: make(map[int32]RetryInfo),
		BackoffInfo: BackoffInfo{
			minBackoff:     minBO,
			maxInitBackoff: maxInitBO,
			maxBackoff:     maxBO,
			constBackoff:   constBackoff,
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

func (bm *BackoffManager) ShouldBackoff(inst int32, preempter lwcproto.ConfigBal, preempterPhase Phase) bool {
	curBackoffInfo, exists := bm.currentBackoffs[inst]
	if !exists {
		return true
	} else if preempter.GreaterThan(curBackoffInfo.preempterConfBal) || (preempter.Equal(curBackoffInfo.preempterConfBal) && preempterPhase > curBackoffInfo.preempterAt) {
		return true
	} else {
		return false
	}
}
func (bm *BackoffManager) CheckAndHandleBackoff(inst int32, attemptedConfBal lwcproto.ConfigBal, preempter lwcproto.ConfigBal, prempterPhase Phase) {
	// if we give this a pointer to the timer we could stop the previous backoff before it gets pinged
	curBackoffInfo, exists := bm.currentBackoffs[inst]

	if !bm.ShouldBackoff(inst, preempter, prempterPhase) {
		dlog.Println("Ignoring backoff request as already backing off instance for this conf-bal or a greater one")
		return
	}

	var preemptNum int32
	var prev int32
	if exists {
		preemptNum = curBackoffInfo.timesPreempted + 1
		prev = curBackoffInfo.prev
	} else {
		preemptNum = 0
		prev = 0
	}

	var next int32
	var tmp float64
	if !bm.constBackoff {
		t := float64(preemptNum) + rand.Float64()
		tmp = math.Pow(2, t) //
		if bm.softFac {
			next = int32(tmp * math.Tanh(math.Sqrt(float64(bm.minBackoff)*t)))
		} else {
			next = int32(tmp * float64(bm.minBackoff)) //float64(rand.Int31n(bm.maxInitBackoff-bm.minBackoff+1)+bm.minBackoff))
		}
		next = next - prev //int32(tmp * float64() //+ rand.Int31n(bm.maxInitBackoff - bm.minBackoff + 1) + bm.minBackoff// bm.minBackoff
	}

	if bm.constBackoff {
		next = bm.minBackoff
	}

	if next < 0 {
		panic("can't have negative backoff")
	}
	log.Printf("Beginning backoff of %d us for instance %d on conf-bal %d.%d (attempt %d)", next, inst, attemptedConfBal.Number, attemptedConfBal.PropID, preemptNum)
	bm.currentBackoffs[inst] = RetryInfo{
		backedoff:        true,
		InstToPrep:       inst,
		attemptedConfBal: attemptedConfBal,
		preempterConfBal: preempter,
		preempterAt:      prempterPhase,
		prev:             next,
		timesPreempted:   preemptNum,
	}

	go func(instance int32, attempted lwcproto.ConfigBal, preempterConfBal lwcproto.ConfigBal, preempterP Phase, backoff int32, numTimesBackedOff int32) {

		timer := time.NewTimer(time.Duration(next) * time.Microsecond)
		<-timer.C
		*bm.sig <- RetryInfo{
			backedoff:        true,
			InstToPrep:       instance,
			attemptedConfBal: attempted,
			preempterConfBal: preempterConfBal,
			preempterAt:      preempterP,
			prev:             next,
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

func (r *elpReplica) noopStillRelevant(inst int32) bool {
	return r.instanceSpace[inst].pbk.cmds == nil
}

const MAXPROPOSABLEINST = 1000

func NewElpReplica(smrReplica *genericsmr.Replica, id int, durable bool, batchWait int,
	storageLoc string, maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool,
	factor float64, whoCrash int32, whenCrash time.Duration, howlongCrash time.Duration, initalProposalWait time.Duration, emulatedSS bool,
	emulatedWriteTime time.Duration, catchupBatchSize int32, timeout time.Duration, group1Size int, flushCommit bool, softFac bool, cmpCmtExec bool,
	cmpCmtExecLoc string, commitCatchUp bool, maxProposalVals int, constBackoff bool, requeueOnPreempt bool, reducePropConfs bool, bcastAcceptance bool, minBatchSize int32, initiator ProposalManager) *elpReplica {
	retryInstances := make(chan RetryInfo, maxOpenInstances*10000)

	r := &elpReplica{
		Replica:                smrReplica, //genericsmr.NewElpReplica(id, peerAddrList, thrifty, exec, lread, dreply, f, storageLoc, deadTime),
		ProposalManager:        initiator,
		configChan:             make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareChan:            make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptChan:             make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitChan:             make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitShortChan:        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareReplyChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptReplyChan:        make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		prepareRPC:             0,
		acceptRPC:              0,
		commitRPC:              0,
		commitShortRPC:         0,
		prepareReplyRPC:        0,
		acceptReplyRPC:         0,
		instanceSpace:          make([]*Instance, 15*1024*1024),
		crtInstance:            0, //get from storage
		crtConfig:              1,
		Shutdown:               false,
		counter:                0,
		flush:                  true,
		executedUpTo:           0, //get from storage
		batchWait:              batchWait,
		maxBalInc:              10000,
		maxOpenInstances:       maxOpenInstances,
		crtOpenedInstances:     make([]int32, maxOpenInstances),
		proposableInstances:    make(chan ProposalInfo, MAXPROPOSABLEINST),
		noopWaitUs:             noopwait,
		retryInstance:          retryInstances,
		BackoffManager:         NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, &retryInstances, factor, softFac, constBackoff),
		alwaysNoop:             alwaysNoop,
		fastLearn:              false,
		timeoutMsgs:            make(chan TimeoutInfo, 1000),
		whoCrash:               whoCrash,
		whenCrash:              whenCrash,
		howLongCrash:           howlongCrash,
		initalProposalWait:     initalProposalWait,
		emulatedSS:             emulatedSS,
		emulatedWriteTime:      emulatedWriteTime,
		catchingUp:             false,
		catchUpBatchSize:       catchupBatchSize,
		timeout:                timeout,
		flushCommit:            flushCommit,
		commitCatchUp:          commitCatchUp,
		cmpCommitExec:          cmpCmtExec,
		maxBatchedProposalVals: maxProposalVals,
		stats:                  ServerStatsNew([]string{"Proposed Noops", "Proposed Instances with Values", "Preemptions", "Requeued Proposals"}),
		requeueOnPreempt:       requeueOnPreempt,
		reducePropConfs:        reducePropConfs,
		bcastAcceptance:        bcastAcceptance,
		minBatchSize:           minBatchSize,
	}
	r.ringCommit = false
	// fmt.Sprintf("./server-%d-stats.txt", id)

	if group1Size <= r.N-r.F {
		r.group1Size = r.N - r.F
	} else {
		r.group1Size = group1Size
	}

	if cmpCmtExec {
		r.commitExecComp = CommitExecutionComparator.CommitExecutionComparatorNew(cmpCmtExecLoc)
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

func (r *elpReplica) CloseUp() {
	panic("not implemented")
}

func (r *elpReplica) recordNewConfig(config int32) {
	if !r.Durable {
		return
	}

	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(config))
	r.StableStore.WriteAt(b[:], 0)
}

func (r *elpReplica) recordExecutedUpTo() {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(r.executedUpTo))
	r.StableStore.WriteAt(b[:], 4)
}

//append a log entry to stable storage
func (r *elpReplica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable || r.emulatedSS {
		return
	}

	var b [12]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(r.crtConfig))
	binary.LittleEndian.PutUint32(b[4:8], uint32(inst.abk.curBal.Number))
	binary.LittleEndian.PutUint32(b[8:12], uint32(inst.abk.curBal.PropID))
	_, _ = r.StableStore.Write(b[:])
}

//write a sequence of commands to stable storage
func (r *elpReplica) recordCommands(cmds []state.Command) {
	if !r.Durable || r.emulatedSS {
		return
	}

	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

//sync with the stable store
func (r *elpReplica) sync() {
	if !r.Durable {
		return
	}
	dlog.Println("synced")

	if r.emulatedSS {
		time.Sleep(r.emulatedWriteTime)
	} else {
		_ = r.StableStore.Sync()
	}
}

func (r *elpReplica) replyPrepare(replicaId int32, reply *lwcproto.PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *elpReplica) replyAccept(replicaId int32, reply *lwcproto.AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

/* Clock goroutine */
var fastClockChan chan bool

func (r *elpReplica) fastClock() {
	for !r.Shutdown {
		time.Sleep(time.Duration(r.batchWait) * time.Millisecond) // ms
		dlog.Println("sending fast clock")
		fastClockChan <- true
	}
}

func (r *elpReplica) BatchingEnabled() bool {
	return r.batchWait > 0
}

/* ============= */
func (r *elpReplica) restart() {
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
		case <-r.timeoutMsgs:
			break
		default:
			cont = false
			break
		}
	}

	r.stats.Reset()
	r.BackoffManager = NewBackoffManager(r.BackoffManager.minBackoff, r.BackoffManager.maxInitBackoff, r.BackoffManager.maxBackoff, &r.retryInstance, r.BackoffManager.factor, r.BackoffManager.softFac, r.BackoffManager.constBackoff)
	r.crtConfig++
	r.recordNewConfig(r.crtConfig)
	r.catchingUp = true
	r.crtOpenedInstances = make([]int32, r.maxOpenInstances)

	for i := 0; i < len(r.crtOpenedInstances); i++ {
		r.crtOpenedInstances[i] = -1
	}

	// using crtInstance here as a cheat to find last instance proposed or accepted in so that we can reset the appropriate variables
	for i := r.executedUpTo + 1; i <= r.crtInstance; i++ {
		r.instanceSpace[i].abk.curBal = lwcproto.Ballot{-1, -1}
		r.instanceSpace[i].pbk.maxKnownBal = lwcproto.Ballot{-1, -1}
		r.instanceSpace[i].pbk.propCurConfBal = lwcproto.ConfigBal{Config: -1, Ballot: lwcproto.Ballot{-1, -1}}
		r.instanceSpace[i].pbk.maxAcceptedConfBal = lwcproto.ConfigBal{Config: -1, Ballot: lwcproto.Ballot{-1, -1}}
	}

	r.crtInstance = r.executedUpTo + 1

	r.Mutex.Lock()
	for i := 0; i < len(r.Clients); i++ {
		_ = r.Clients[i].Close()
	}
	r.Clients = make([]net.Conn, 0)
	r.Mutex.Unlock()

	if r.Dreply || r.Exec {
		r.recoveringFrom = r.executedUpTo + 1
		end := r.recoveringFrom + r.catchUpBatchSize
		for i := r.recoveringFrom; i <= end; i++ {
			whoToSendTo := randomExcluded(0, int32(r.N-1), r.Id)
			if r.instanceSpace[i] != nil {
				r.ProposalManager.beginNewProposal(r, i, r.crtConfig)
				r.sendSinglePrepare(i, whoToSendTo) //gonna assume been chosen so only send to one proposer
			} else {
				r.sendRecoveryRequest(i, whoToSendTo)
			}
		}
		r.nextCatchUpPoint = int32(end)
	} else {
		for i := int32(0); i < r.maxOpenInstances; i++ {
			r.beginNextInstance()
		}

		r.timeSinceValueLastSelected = time.Time{}
	}
}

func randomExcluded(min, max, excluded int32) int32 {
	var n = rand.Float64() * float64((max-min)+min)
	if n >= float64(excluded) {
		n++
	}
	return int32(n)
}

func (r *elpReplica) sendNextRecoveryRequestBatch() {
	// assumes r.nextCatchUpPoint is initialised correctly
	var nextPoint int
	//if r.recoveringFrom == r.crtInstance {
	nextPoint = int(r.nextCatchUpPoint + r.catchUpBatchSize)
	if r.crtInstance >= r.maxInstanceChosenBeforeCrash() {
		nextPoint = min(int(r.crtInstance), nextPoint)
	}

	log.Printf("Sending recovery batch from %d to %d", r.nextCatchUpPoint, nextPoint)
	for i := r.nextCatchUpPoint; i <= int32(nextPoint); i++ {
		if r.instanceSpace[i] == nil {
			whoToSendTo := randomExcluded(0, int32(r.N-1), r.Id)
			r.sendRecoveryRequest(i, whoToSendTo)
		}
	}
	r.nextCatchUpPoint = int32(nextPoint)
}

func (r *elpReplica) sendRecoveryRequest(fromInst int32, toAccptor int32) {
	r.makeCatchupInstance(fromInst)
	r.sendSinglePrepare(fromInst, toAccptor)
}

func (r *elpReplica) checkAndHandleCatchUpRequest(prepare *lwcproto.Prepare) bool {
	//config is ignored here and not acknowledged until new proposals are actually made
	if prepare.ConfigBal.IsZero() {
		log.Printf("received catch up request from to instance %d to %d", prepare.Instance, prepare.Instance)
		r.checkAndHandleCommit(prepare.Instance, prepare.LeaderId, 0)
		return true
	} else {
		return false
	}
}

func (r *elpReplica) maxInstanceChosenBeforeCrash() int32 {
	return r.recoveringFrom + r.maxOpenInstances*int32(r.N)
}

func (r *elpReplica) checkAndHandleCatchUpResponse(commit *lwcproto.Commit) {
	if r.catchingUp {
		if commit.Instance > r.maxInstanceChosenBeforeCrash() && int32(commit.PropID) == r.Id && r.crtInstance-r.executedUpTo <= r.maxOpenInstances*int32(r.N) {
			r.catchingUp = false
			dlog.Printf("Caught up with consensus group")
			//reset client connections so that we can begin benchmarking again
			r.Mutex.Lock()
			for i := 0; i < len(r.Clients); i++ {
				_ = r.Clients[i].Close()
			}
			r.Clients = make([]net.Conn, 0)
			r.Mutex.Unlock()
			r.timeSinceValueLastSelected = time.Time{}
		} else {
			if commit.Instance >= r.nextCatchUpPoint-(r.catchUpBatchSize/5) {
				r.sendNextRecoveryRequestBatch()
			}
		}
	}
}

func (r *elpReplica) run() {
	r.ConnectToPeers()
	r.RandomisePeerOrder()
	go r.WaitForClientConnections()

	//if r.reducePropConfs {
	//	for i := 0; i < int(r.maxOpenInstances); i++ {
	//		r.crtOpenedInstances[i] = -1
	//	}
	//	r.beginNextInstance()
	//} else {
	for i := 0; i < int(r.maxOpenInstances); i++ {
		r.crtOpenedInstances[i] = -1
		r.beginNextInstance()
	}
	//}

	doner := make(chan struct{})
	if r.Id == r.whoCrash {
		go func() {
			t := time.NewTimer(r.whenCrash)
			<-t.C
			doner <- struct{}{}
		}()
	}

	r.stats.GoClock()

	for !r.Shutdown {
		select {
		case <-r.stats.C:
			r.stats.Print()
			r.stats.Reset()
			break
		case <-doner:
			dlog.Println("Crahsing")
			time.Sleep(r.howLongCrash)
			r.restart()
			dlog.Println("Done crashing")
			break
		case maybeTimedout := <-r.timeoutMsgs:
			r.retryConfigBal(maybeTimedout)
			break
		case next := <-r.retryInstance:
			dlog.Println("Checking whether to retry a proposal")
			r.tryNextAttempt(next)
			break
		case retry := <-r.proposableInstances:
			r.recheckForValueToPropose(retry)
			break
		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*lwcproto.Prepare)
			//got a Prepare message
			log.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
			if !r.checkAndHandleCatchUpRequest(prepare) {
				r.handlePrepare(prepare)
			}
			break
		case acceptS := <-r.acceptChan:
			accept := acceptS.(*lwcproto.Accept)
			//got an Accept message
			log.Printf("Received Accept Request from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
			r.handleAccept(accept)
			break
		case commitS := <-r.commitChan:
			commit := commitS.(*lwcproto.Commit)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommit(commit)
			r.checkAndHandleCatchUpResponse(commit)
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
			log.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
			r.handlePrepareReply(prepareReply)
			break
		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*lwcproto.AcceptReply)
			//got an Accept reply
			log.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
			r.handleAcceptReply(acceptReply)
			break
			//default:
			//	break
		}
	}
}

func (r *elpReplica) retryConfigBal(maybeTimedout TimeoutInfo) {
	inst := r.instanceSpace[maybeTimedout.inst]
	if inst.pbk.propCurConfBal.Equal(maybeTimedout.proposingConfBal) && inst.pbk.status == maybeTimedout.phase {
		inst.pbk.proposalInfos[inst.pbk.propCurConfBal].Broadcast(maybeTimedout.msgCode, maybeTimedout.msg)
	}
}

//
func (r *elpReplica) recheckForValueToPropose(proposalInfo ProposalInfo) {

	inst := r.instanceSpace[proposalInfo.inst]
	pbk := inst.pbk
	// note that 4 things can happen
	// is chosen or preemped (conf bal no longer relevant
	// 1 recieve value
	// 2 no val received
	// received extra promises and now there is a previously chosen value (don't need to propose again but prolly should (if not a no op) --- could make this optimsation
	if pbk == nil {
		return
	}
	if pbk.propCurConfBal.Equal(proposalInfo.proposingConfBal) && pbk.status == READY_TO_PROPOSE {
		whoseCmds := int32(-1)
		if pbk.maxAcceptedConfBal.IsZero() {
			whoseCmds = r.Id
			switch cliProp := r.clientValueQueue.TryDequeue(); {
			case cliProp != nil:
				numEnqueued := r.clientValueQueue.Len() + 1
				//batchSize := numEnqueued
				batchSize := min(numEnqueued, r.maxBatchedProposalVals)
				pbk.clientProposals = make([]*genericsmr.Propose, batchSize)
				pbk.cmds = make([]state.Command, batchSize)
				pbk.clientProposals[0] = cliProp
				pbk.cmds[0] = cliProp.Command

				for i := 1; i < batchSize; i++ {
					cliProp = r.clientValueQueue.TryDequeue()
					if cliProp == nil {
						pbk.clientProposals = pbk.clientProposals[:i]
						pbk.cmds = pbk.cmds[:i]
						break
					}
					pbk.clientProposals[i] = cliProp
					pbk.cmds[i] = cliProp.Command
				}
				log.Printf("%d client value(s) proposed in instance %d \n", len(pbk.clientProposals), inst)
				r.stats.Update("Proposed Instances with Values", 1)
				break
			default:
				if r.shouldNoop(proposalInfo.inst) {
					pbk.cmds = state.NOOP()
					log.Println("Proposing noop")
					r.stats.Update("Proposed Noops", 1)
					break
				} else {
					go func() {
						timer := time.NewTimer(time.Duration(r.noopWaitUs) * time.Microsecond)
						<-timer.C
						r.proposableInstances <- proposalInfo
					}()
					return
				}
			}
		} else {
			whoseCmds = pbk.whoseCmds
		}

		pbk.status = PROPOSING
		// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
		if r.fastLearn {
			r.acceptorAcceptOnConfBal(proposalInfo.inst, pbk.propCurConfBal, pbk.cmds)
			r.proposerCheckAndHandleAcceptedValue(proposalInfo.inst, r.Id, pbk.propCurConfBal, pbk.cmds, whoseCmds)

			r.bcastAccept(proposalInfo.inst)
		} else {
			r.proposerCheckAndHandleAcceptedValue(proposalInfo.inst, r.Id, pbk.propCurConfBal, pbk.cmds, whoseCmds)

			r.bcastAccept(proposalInfo.inst)
			r.acceptorAcceptOnConfBal(proposalInfo.inst, pbk.propCurConfBal, pbk.cmds)
		}
	}
}

func (r *elpReplica) tryNextAttempt(next RetryInfo) {
	inst := r.instanceSpace[next.InstToPrep]
	if !next.backedoff {
		if inst == nil {
			r.instanceSpace[next.InstToPrep] = r.makeEmptyInstance()
			inst = r.instanceSpace[next.InstToPrep]
		}
	}

	if (r.BackoffManager.NoHigherBackoff(next) && inst.pbk.status == BACKING_OFF) || !next.backedoff {
		//		r.proposerBeginNextConfBal(next.InstToPrep)
		r.ProposalManager.beginNewProposal(r, next.InstToPrep, r.crtConfig)
		nextConfBal := r.instanceSpace[next.InstToPrep].pbk.propCurConfBal
		r.acceptorPrepareOnConfBal(next.InstToPrep, nextConfBal)
		inst.pbk.proposalInfos[nextConfBal].AddToQuorum(int(r.Id))
		r.bcastPrepare(next.InstToPrep)
		log.Printf("Proposing next conf-bal %d.%d.%d to instance %d\n", nextConfBal.Config, nextConfBal.Number, nextConfBal.PropID, next.InstToPrep)
	} else {
		dlog.Printf("Skipping retry of instance %d due to preempted again or closed\n", next.InstToPrep)
	}
}

func (r *elpReplica) sendSinglePrepare(instance int32, to int32) {
	// cheats - should really be a special recovery message but lazzzzzyyyy
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()
	args := &lwcproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurConfBal}
	dlog.Printf("send prepare to %d\n", to)
	r.SendMsg(to, r.prepareRPC, args)
	//whoSent := []int32{to}
	r.beginTimeout(args.Instance, args.ConfigBal, PREPARING, r.timeout*5, r.prepareRPC, args)
}

func (r *elpReplica) bcastPrepare(instance int32) {
	args := &lwcproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurConfBal}
	pbk := r.instanceSpace[instance].pbk
	pbk.proposalInfos[pbk.propCurConfBal].Broadcast(r.prepareRPC, args)
	r.beginTimeout(args.Instance, args.ConfigBal, PREPARING, r.timeout, r.prepareRPC, args)
}

func (r *elpReplica) beginTimeout(inst int32, attempted lwcproto.ConfigBal, onWhatPhase ProposerStatus, timeout time.Duration, msgcode uint8, msg fastrpc.Serializable) {
	go func(instance int32, tried lwcproto.ConfigBal, phase ProposerStatus, timeoutWait time.Duration) {
		timer := time.NewTimer(timeout)
		<-timer.C
		if r.instanceSpace[inst].pbk.propCurConfBal.Equal(tried) && r.instanceSpace[inst].pbk.status == phase {
			// not atomic and might change when message received but that's okay (only to limit number of channel messages sent)
			r.timeoutMsgs <- TimeoutInfo{
				ProposalInfo: ProposalInfo{instance, tried},
				phase:        phase,
				msgCode:      msgcode,
				msg:          msg,
			}
		}
	}(inst, attempted, onWhatPhase, timeout)
}

var pa lwcproto.Accept

func (r *elpReplica) bcastAccept(instance int32) {
	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.ConfigBal = r.instanceSpace[instance].pbk.propCurConfBal
	pa.Command = r.instanceSpace[instance].pbk.cmds
	pa.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
	args := &pa

	pbk := r.instanceSpace[instance].pbk
	pbk.proposalInfos[pbk.propCurConfBal].Broadcast(r.acceptRPC, args)
	r.beginTimeout(args.Instance, args.ConfigBal, PREPARING, r.timeout, r.acceptRPC, args)
}

var pc lwcproto.Commit
var pcs lwcproto.CommitShort

func (r *elpReplica) bcastCommitToAll(instance int32, confBal lwcproto.ConfigBal, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.ConfigBal = confBal
	pc.WhoseCmd = r.instanceSpace[instance].pbk.whoseCmds
	pc.MoreToCome = 0
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
		inQrm := r.instanceSpace[instance].pbk.proposalInfos[confBal].HasAcknowledged(q) //pbk.proposalInfos[confBal].Broadcast//.aids[r.PreferredPeerOrder[q]]
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

func (r *elpReplica) incToNextOpenInstance() {
	r.crtInstance++
}

func (r *elpReplica) makeEmptyInstance() *Instance {
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
			proposalInfos: make(map[lwcproto.ConfigBal]quorumsystem.SynodQuorumSystem),

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

func (r *elpReplica) makeCatchupInstance(inst int32) {
	r.instanceSpace[inst] = r.makeEmptyInstance()
}

func (r *elpReplica) freeInstToOpen() bool {
	for j := 0; j < len(r.crtOpenedInstances); j++ {
		// allow for an openable instance
		if r.crtOpenedInstances[j] == -1 {
			return true
		}
	}
	return false
}

func (r *elpReplica) beginNextInstance() {
	if r.reducePropConfs {
		old := r.crtInstance
		for r.crtInstance < old+int32(r.N/(r.F+1)) {
			r.incToNextOpenInstance()
			idHit := (r.Id + 1) % int32(r.N/(r.F+1))
			instHit := r.crtInstance % int32(r.N/(r.F+1))
			if instHit == idHit {
				for j := 0; j < len(r.crtOpenedInstances); j++ {
					// allow for an openable instance
					if r.crtOpenedInstances[j] == -1 {
						r.crtOpenedInstances[j] = r.crtInstance
						break
					}
				}
				r.instanceSpace[r.crtInstance] = r.makeEmptyInstance()
				curInst := r.instanceSpace[r.crtInstance]

				//	r.proposerBeginNextConfBal(r.crtInstance)
				r.ProposalManager.beginNewProposal(r, r.crtInstance, r.crtConfig)

				r.acceptorPrepareOnConfBal(r.crtInstance, curInst.pbk.propCurConfBal)
				curInst.pbk.proposalInfos[curInst.pbk.propCurConfBal].AddToQuorum(int(r.Id))
				r.bcastPrepare(r.crtInstance)
				dlog.Printf("Opened new instance %d\n", r.crtInstance)
			} else {
				r.instanceSpace[r.crtInstance] = r.makeEmptyInstance() //r.makeCatchupInstance(r.crtInstance)
				r.BackoffManager.CheckAndHandleBackoff(r.crtInstance, lwcproto.ConfigBal{-1, lwcproto.Ballot{-1, -1}},
					lwcproto.ConfigBal{-1, lwcproto.Ballot{-1, -1}}, PROMISE)
				r.instanceSpace[r.crtInstance].pbk.status = BACKING_OFF
				dlog.Printf("Not my instance so backing off %d\n", r.crtInstance)
			}

		}
	} else {
		r.incToNextOpenInstance()
		for i := 0; i < len(r.crtOpenedInstances); i++ {
			// allow for an openable instance
			if r.crtOpenedInstances[i] == -1 {
				r.crtOpenedInstances[i] = r.crtInstance
				break
			}
		}

		r.instanceSpace[r.crtInstance] = r.makeEmptyInstance()
		curInst := r.instanceSpace[r.crtInstance]

		//		r.proposerBeginNextConfBal(r.crtInstance)
		r.ProposalManager.beginNewProposal(r, r.crtInstance, r.crtConfig)
		r.acceptorPrepareOnConfBal(r.crtInstance, curInst.pbk.propCurConfBal)
		curInst.pbk.proposalInfos[curInst.pbk.propCurConfBal].AddToQuorum(int(r.Id))
		r.bcastPrepare(r.crtInstance)
		log.Printf("Opened new instance %d\n", r.crtInstance)
	}

	//	r.instRotId = (r.instRotId + 1) % int32(r.N)

	//

	////	}
}

func (r *elpReplica) acceptorPrepareOnConfBal(inst int32, confBal lwcproto.ConfigBal) {
	r.instanceSpace[inst].abk.status = PREPARED
	dlog.Printf("Acceptor Preparing Config-Ballot %d.%d.%d ", confBal.Config, confBal.Number, confBal.PropID)
	r.instanceSpace[inst].abk.curBal = confBal.Ballot
}

func (r *elpReplica) acceptorAcceptOnConfBal(inst int32, confBal lwcproto.ConfigBal, cmds []state.Command) {
	abk := r.instanceSpace[inst].abk
	abk.status = ACCEPTED

	dlog.Printf("Acceptor Accepting Config-Ballot %d.%d.%d ", confBal.Config, confBal.Number, confBal.PropID)
	abk.curBal = confBal.Ballot
	abk.vConfBal = confBal
	abk.cmds = cmds

	r.recordInstanceMetadata(r.instanceSpace[inst])
	r.recordCommands(cmds)
	dlog.Printf("accept sync instance %d", inst)
	r.sync()
}

func (r *elpReplica) proposerCheckAndHandlePreempt(inst int32, preemptingConfigBal lwcproto.ConfigBal, preemterPhase Phase) bool {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if pbk.status != CLOSED && preemptingConfigBal.GreaterThan(pbk.propCurConfBal) {
		//if pbk.status != BACKING_OFF { // option for add multiple preempts if backing off already?
		r.BackoffManager.CheckAndHandleBackoff(inst, pbk.propCurConfBal, preemptingConfigBal, preemterPhase)
		//	}

		r.checkAndOpenNewInstances(inst)
		pbk.status = BACKING_OFF
		if r.requeueOnPreempt {
			r.requeueClientProposals(inst)
		}
		if preemptingConfigBal.Ballot.GreaterThan(pbk.maxKnownBal) {
			pbk.maxKnownBal = preemptingConfigBal.Ballot
			r.stats.Update("Preemptions", 1)
		}
		return true
	} else {
		return false
	}
}

func (r *elpReplica) checkAndHandleConfigPreempt(inst int32, preemptingConfigBal lwcproto.ConfigBal, preemterPhase Phase) bool {
	if r.crtConfig < preemptingConfigBal.Config {
		dlog.Printf("Current config %d preempted by config %d (instance %d)\n", r.crtConfig, preemptingConfigBal.Config, inst)
		r.recordNewConfig(preemptingConfigBal.Config)
		dlog.Printf("config sync instance %d", inst)
		r.sync()
		r.crtConfig = preemptingConfigBal.Config
		for i := r.executedUpTo; i <= r.crtInstance; i++ {
			if r.instanceSpace[i] != nil {
				pbk := r.instanceSpace[i].pbk
				if r.instanceSpace[i].abk.status != COMMITTED {
					if i != inst {
						pbk.status = PREPARING
						//	r.proposerBeginNextConfBal(i)
						r.ProposalManager.beginNewProposal(r, i, r.crtConfig)
						nextConfBal := r.instanceSpace[i].pbk.propCurConfBal
						r.acceptorPrepareOnConfBal(i, nextConfBal)
						pbk.proposalInfos[nextConfBal].AddToQuorum(int(r.Id))
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

func (r *elpReplica) isMoreCommitsToComeAfter(inst int32) bool {
	for i := inst + 1; i <= r.crtInstance; i++ {
		instance := r.instanceSpace[i]
		if instance != nil {
			if instance.abk.status == COMMITTED {
				return true
			}
		}
	}
	return false
}

func (r *elpReplica) checkAndHandleCommit(instance int32, whoRespondTo int32, maxExtraInstances int32) bool {
	inst := r.instanceSpace[instance]
	if inst == nil {
		return false
	}
	if inst.abk.status == COMMITTED {
		//	if instance+(int32(r.N)*r.maxOpenInstances) < r.crtInstance {
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
					if r.isMoreCommitsToComeAfter(i) && count < maxExtraInstances {
						pc.MoreToCome = 1
						r.SendMsgNoFlush(whoRespondTo, r.commitRPC, &pc)
						count++
					} else {
						pc.MoreToCome = 0
						r.SendMsgNoFlush(whoRespondTo, r.commitRPC, &pc)
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

func (r *elpReplica) howManyExtraCommitsToSend(inst int32) int32 {
	if r.commitCatchUp {
		return r.crtInstance - inst
	} else {
		return 0
	}
}

func (r *elpReplica) handlePrepare(prepare *lwcproto.Prepare) {
	r.checkAndHandleNewlyReceivedInstance(prepare.Instance)
	configPreempted := r.checkAndHandleConfigPreempt(prepare.Instance, prepare.ConfigBal, PROMISE)
	if r.checkAndHandleCommit(prepare.Instance, prepare.LeaderId, r.howManyExtraCommitsToSend(prepare.Instance)) {
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

func (r *elpReplica) checkAndHandleOldPreempted(new lwcproto.ConfigBal, old lwcproto.ConfigBal, accepted lwcproto.ConfigBal, acceptedVal []state.Command, inst int32) {
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

func (r *elpReplica) proposerCheckAndHandleAcceptedValue(inst int32, aid int32, accepted lwcproto.ConfigBal, val []state.Command, whoseCmd int32) ProposerAccValHandler {
	if accepted.IsZero() {
		return IGNORED
	}
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if pbk.status == CLOSED {
		return CHOSEN
	}
	if accepted.Ballot.GreaterThan(pbk.maxKnownBal) {
		pbk.maxKnownBal = accepted.Ballot
	}

	newVal := false
	if accepted.GreaterThan(pbk.maxAcceptedConfBal) {
		newVal = true
		// who proposed original command - used for next proposal
		pbk.whoseCmds = whoseCmd
		pbk.maxAcceptedConfBal = accepted
		pbk.cmds = val

		if int32(accepted.PropID) != r.Id {
			r.checkAndOpenNewInstances(inst)
		}
		if r.whatHappenedToClientProposals(inst) == ProposedButNotChosen {
			r.requeueClientProposals(inst)
			dlog.Printf("requeing")
		}
	}

	_, exists := pbk.proposalInfos[accepted]
	if !exists {
		r.ProposalManager.trackProposalAcceptance(r, inst, accepted)
	}

	pbk.proposalInfos[accepted].AddToQuorum(int(aid))
	log.Printf("Acceptance on instance %d at conf-round %d.%d.%d by acceptor %d", inst, accepted.Config, accepted.Number, accepted.PropID, aid)
	// not assumed local acceptor has accepted it
	if pbk.proposalInfos[accepted].QuorumReached() {

		if !r.bcastAcceptance {
			r.bcastCommitToAll(inst, accepted, val)
		}
		r.acceptorCommit(inst, accepted, val)
		r.proposerCloseCommit(inst, accepted, pbk.cmds, whoseCmd, false)
		return CHOSEN
	} else if newVal {
		return NEW_VAL
	} else {
		return ACKED
	}
}

func (r *elpReplica) handlePrepareReply(preply *lwcproto.PrepareReply) {
	inst := r.instanceSpace[preply.Instance]
	pbk := r.instanceSpace[preply.Instance].pbk

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
	}
	// early learning
	if valWhatDone == CHOSEN {
		dlog.Printf("Preparing instance recognised as chosen (instance %d), returning commit \n", preply.Instance)
		return
	} else if configPreempt {
		return
	}

	if pbk.propCurConfBal.GreaterThan(preply.ConfigBal) || pbk.status != PREPARING {
		dlog.Printf("Message in late \n")
		return
	} else if r.proposerCheckAndHandlePreempt(preply.Instance, preply.ConfigBal, PROMISE) {
		dlog.Printf("Another active proposer using config-ballot %d.%d.%d greater than mine\n", preply.ConfigBal)
		r.acceptorPrepareOnConfBal(preply.Instance, preply.ConfigBal)
		minSafe := lwcproto.ConfigBal{
			Config: r.crtConfig,
			Ballot: inst.abk.curBal,
		}
		r.checkAndHandleOldPreempted(preply.ConfigBal, minSafe, inst.abk.vConfBal, inst.abk.cmds, preply.Instance)
		myReply := lwcproto.PrepareReply{
			Instance:   preply.Instance,
			ConfigBal:  preply.ConfigBal,
			VConfigBal: inst.abk.vConfBal,
			WhoseCmd:   pbk.whoseCmds,
			AcceptorId: r.Id,
			Command:    inst.abk.cmds,
		}
		r.SendMsg(int32(preply.ConfigBal.PropID), r.prepareReplyRPC, &myReply)
		return
	} else {
		qrm := pbk.proposalInfos[pbk.propCurConfBal]
		qrm.AddToQuorum(int(preply.AcceptorId))
		//qrm.quorumAdd(preply.AcceptorId)
		dlog.Printf("Added replica's %d promise to qrm", preply.AcceptorId)
		if qrm.QuorumReached() { //int(qrm.quorumCount()+1) >= r.elpReplica.ReadQuorumSize() {
			r.propose(preply.Instance)
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

//
//
//
//
//abortChan := make(chan(bool), 1)
//go func() {
//	timer := time.NewTimer(time.Microsecond * time.Duration(r.noopWaitUs))
//	for done := true; done {
//		select {
//		case <-timer.C:
//			// if no client values set noop
//			done = false
//			break
//
//		case // abort
//
//		case // client value
//			// if batch filled break out of loop
//		}
//	}
//
//	go func() {
//
//	}()
//	// begin a timeout
//	// begin waiting for a batch
//	// check for preempting configbal
//	//
//}()

func (r *elpReplica) propose(inst int32) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	pbk.status = READY_TO_PROPOSE
	log.Println("Can now propose in instance", inst)
	qrm := pbk.proposalInfos[pbk.propCurConfBal]
	qrm.StartAcceptanceQuorum()

	whoseCmds := int32(-1)
	if pbk.maxAcceptedConfBal.IsZero() {
		whoseCmds = r.Id
		if r.initalProposalWait > 0 {
			go func(inst int32, confBal lwcproto.ConfigBal) {
				timer := time.NewTimer(time.Duration(r.initalProposalWait))
				<-timer.C
				r.proposableInstances <- ProposalInfo{
					inst:             inst,
					proposingConfBal: confBal,
				}
			}(inst, pbk.propCurConfBal)
			return
		} else {
			switch qLen := int32(r.clientValueQueue.Len()); {
			case qLen >= r.minBatchSize:
				numEnqueued := r.clientValueQueue.Len()
				batchSize := min(numEnqueued, r.maxBatchedProposalVals)
				pbk.clientProposals = make([]*genericsmr.Propose, batchSize)
				pbk.cmds = make([]state.Command, batchSize)
				for batched := 0; batched < batchSize; {
					cliProp := r.clientValueQueue.TryDequeue()
					if cliProp == nil {
						continue
					}
					pbk.clientProposals[batched] = cliProp
					pbk.cmds[batched] = cliProp.Command
					batched++
				}
				log.Printf("%d client value(s) proposed in instance %d \n", len(pbk.clientProposals), inst)
				r.stats.Update("Proposed Instances with Values", 1)
				break

			default:
				if r.noopWaitUs > 0 {
					log.Printf("Ready to propose in instance %d but awaiting a full batch of values", inst)
					go func(inst int32, confBal lwcproto.ConfigBal) {
						timer := time.NewTimer(time.Duration(r.noopWaitUs) * time.Microsecond)
						<-timer.C
						r.proposableInstances <- ProposalInfo{
							inst:             inst,
							proposingConfBal: confBal,
						}
					}(inst, pbk.propCurConfBal)
					return
				} else {
					if r.shouldNoop(inst) {
						pbk.cmds = state.NOOP()
						dlog.Println("Proposing noop")
						r.stats.Update("Proposed Noops", 1)
					} else {
						return
					}
				}
			}
		}
	} else {
		whoseCmds = pbk.whoseCmds
	}
	pbk.status = PROPOSING
	// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
	if r.fastLearn {
		r.proposerCheckAndHandleAcceptedValue(inst, r.Id, pbk.propCurConfBal, pbk.cmds, whoseCmds)
		r.acceptorAcceptOnConfBal(inst, pbk.propCurConfBal, pbk.cmds)

		r.bcastAccept(inst)
	} else {
		r.proposerCheckAndHandleAcceptedValue(inst, r.Id, pbk.propCurConfBal, pbk.cmds, whoseCmds)

		r.bcastAccept(inst)
		r.acceptorAcceptOnConfBal(inst, pbk.propCurConfBal, pbk.cmds)
	}
}

func (r *elpReplica) shouldNoop(inst int32) bool {
	if r.alwaysNoop {
		return true
	}

	for i := inst + 1; i < r.crtInstance; i++ {
		if r.instanceSpace[i] == nil {
			continue
		}
		if r.instanceSpace[i].abk.status == COMMITTED {
			return true
		}

	}
	return false
}

func (r *elpReplica) checkAndHandleNewlyReceivedInstance(instance int32) {
	inst := r.instanceSpace[instance]
	if inst == nil {
		if instance > r.crtInstance {
			r.crtInstance = instance
		}
		r.instanceSpace[instance] = r.makeEmptyInstance()
	}
}

func (r *elpReplica) handleAccept(accept *lwcproto.Accept) {
	r.checkAndHandleNewlyReceivedInstance(accept.Instance)
	configPreempted := r.checkAndHandleConfigPreempt(accept.Instance, accept.ConfigBal, ACCEPTANCE)
	if r.checkAndHandleCommit(accept.Instance, accept.LeaderId, r.howManyExtraCommitsToSend(accept.Instance)) {
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
		// here is where we can add fast learning bit - also add acceptance by config-bal's owner
		if r.fastLearn {
			r.proposerCheckAndHandleAcceptedValue(accept.Instance, int32(accept.PropID), accept.ConfigBal, accept.Command, accept.WhoseCmd) // must go first as acceptor might have already learnt of value
		}
		accValState := r.proposerCheckAndHandleAcceptedValue(accept.Instance, r.Id, accept.ConfigBal, accept.Command, accept.WhoseCmd)
		if accValState == CHOSEN {
			return
		}
		r.proposerCheckAndHandlePreempt(accept.Instance, accept.ConfigBal, ACCEPTANCE)
		r.checkAndHandleOldPreempted(accept.ConfigBal, minAcceptableConfBal, inst.abk.vConfBal, inst.abk.cmds, accept.Instance)
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
	if r.bcastAcceptance {
		for i := 0; i < r.N; i++ {
			if int32(i) == r.Id {
				continue
			}
			r.replyAccept(int32(i), areply)
		}
	} else {
		r.replyAccept(accept.LeaderId, areply)
	}
}

func (r *elpReplica) checkAndOpenNewInstances(inst int32) {
	// was instance opened by us
	//	if r.executedUpTo+r.maxOpenInstances*int32(r.N) > r.crtInstance {
	for i := 0; i < len(r.crtOpenedInstances); i++ {
		if r.crtOpenedInstances[i] == -1 {
			r.beginNextInstance()
		} else if r.crtOpenedInstances[i] == inst || r.instanceSpace[r.crtOpenedInstances[i]].abk.status == COMMITTED {
			r.crtOpenedInstances[i] = -1
			r.beginNextInstance()
		}
	}
}

func (r *elpReplica) handleAcceptReply(areply *lwcproto.AcceptReply) {
	if r.checkAndHandleConfigPreempt(areply.Instance, areply.Cur, ACCEPTANCE) {
		return
	}
	inst := r.instanceSpace[areply.Instance]
	pbk := r.instanceSpace[areply.Instance].pbk
	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed ")
		return
	}

	accepted := areply.Cur.Equal(areply.Req)
	preempted := areply.Cur.GreaterThan(areply.Req)
	if accepted {
		dlog.Printf("Acceptance of instance %d at %d.%d.%d (cur %d.%d.%d) by Acceptor %d received\n", areply.Instance, areply.Req.Config, areply.Req.Number, areply.Req.PropID, areply.Cur.Config, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId)
		dlog.Printf("Acceptance of instance %d, whose cmd %d", areply.Instance, areply.WhoseCmd)
		// pbk.cmds should be correct value - proposer only gets to this
		// stage if they learn of any previously chosen value and propose it again themselves
		r.proposerCheckAndHandleAcceptedValue(areply.Instance, areply.AcceptorId, areply.Cur, pbk.cmds, areply.WhoseCmd)
		// we can count proposer of value too because they durably accept before sending accept request
		if r.fastLearn {
			r.proposerCheckAndHandleAcceptedValue(areply.Instance, int32(areply.Req.PropID), areply.Cur, pbk.cmds, areply.WhoseCmd)
		}
	} else if preempted {
		r.proposerCheckAndHandlePreempt(areply.Instance, areply.Cur, ACCEPTANCE)
		minSafe := lwcproto.ConfigBal{
			Config: r.crtConfig,
			Ballot: inst.abk.curBal,
		}
		r.checkAndHandleOldPreempted(areply.Cur, minSafe, inst.abk.vConfBal, inst.abk.cmds, areply.Instance)
	} else {
		msg := fmt.Sprintf("Somehow cur Conf-Bal of %d is %d.%d.%d when we requested %d.%d.%d for acceptance",
			areply.AcceptorId, areply.Cur.Config, areply.Cur.Number, areply.Cur.PropID,
			areply.Req.Config, areply.Req.Number, areply.Req.PropID)
		panic(msg)
	}
}

func (r *elpReplica) requeueClientProposals(instance int32) {
	inst := r.instanceSpace[instance]
	dlog.Println("Requeing client value")
	if len(inst.pbk.clientProposals) > 0 {
		r.stats.Update("Requeued Proposals", 1)
	}
	for i := 0; i < len(inst.pbk.clientProposals); i++ {
		r.clientValueQueue.TryRequeue(inst.pbk.clientProposals[i])
	}
}

type ClientProposalStory int

const (
	NotProposed ClientProposalStory = iota
	ProposedButNotChosen
	ProposedAndChosen
)

func (r *elpReplica) whatHappenedToClientProposals(instance int32) ClientProposalStory {
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

func (r *elpReplica) howManyAttemptsToChoose(inst int32) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	attempts := pbk.maxAcceptedConfBal.Number / r.maxBalInc
	dlog.Printf("Attempts to chose instance %d: %d", inst, attempts)
}

func (r *elpReplica) proposerCloseCommit(inst int32, chosenAt lwcproto.ConfigBal, chosenVal []state.Command, whoseCmd int32, moreToCome bool) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	pbk.status = CLOSED
	dlog.Printf("Instance %d chosen now\n", inst)

	r.BackoffManager.ClearBackoff(inst)

	dlog.Printf("chosen whose cmd %d", whoseCmd)
	pbk.maxAcceptedConfBal = chosenAt
	pbk.whoseCmds = whoseCmd
	pbk.cmds = chosenVal

	switch r.whatHappenedToClientProposals(inst) {
	case NotProposed:
		break
	case ProposedButNotChosen:
		r.requeueClientProposals(inst)
		dlog.Printf("%d client value(s) proposed in instance %d\n not chosen", len(pbk.clientProposals), inst)
		pbk.clientProposals = nil
		break
	case ProposedAndChosen:
		r.timeSinceValueLastSelected = time.Now()
		dlog.Printf("%d client value(s) chosen in instance %d\n", len(pbk.clientProposals), inst)
		break
	}
	if r.cmpCommitExec {
		id := CommitExecutionComparator.InstanceID{Log: 0, Seq: inst}
		r.commitExecComp.RecordCommit(id, time.Now())
	}

	if !moreToCome {
		r.checkAndOpenNewInstances(inst)
	}

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
		for i := r.executedUpTo + 1; i <= r.crtInstance; i++ {
			returnInst := r.instanceSpace[i]
			if returnInst != nil && returnInst.abk.status == COMMITTED {
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

				if r.cmpCommitExec {
					id := CommitExecutionComparator.InstanceID{Log: 0, Seq: inst}
					r.commitExecComp.RecordExecution(id, time.Now())
					//r.commitExecComp.outputCmtExecAndDiffTimes(id)
				}
				//	returnInst.pbk = nil
				r.executedUpTo += 1
				dlog.Printf("Executed up to %d (crtInstance=%d)", r.executedUpTo, r.crtInstance)
				//r.instanceSpace[i] = nil
			} else {
				break
			}
		}
	}
}

func (r *elpReplica) acceptorCommit(instance int32, chosenAt lwcproto.ConfigBal, cmds []state.Command) {
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
			dlog.Printf("commit sync instance %d", instance)
			r.sync()
		}
	}
}

func (r *elpReplica) handleCommit(commit *lwcproto.Commit) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	inst := r.instanceSpace[commit.Instance]

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed \n")
		return
	}

	//if r.ringCommit {
	//	r.SendMsg((r.Id + 1) % int32(r.N), r.commitRPC, commit)
	//}

	r.acceptorCommit(commit.Instance, commit.ConfigBal, commit.Command)
	if commit.MoreToCome > 0 {
		r.proposerCloseCommit(commit.Instance, commit.ConfigBal, commit.Command, commit.WhoseCmd, true)
	} else {
		r.proposerCloseCommit(commit.Instance, commit.ConfigBal, commit.Command, commit.WhoseCmd, false)
	}
}

func (r *elpReplica) handleCommitShort(commit *lwcproto.CommitShort) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	inst := r.instanceSpace[commit.Instance]

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed \n")
		return
	}

	r.acceptorCommit(commit.Instance, commit.ConfigBal, inst.abk.cmds)
	r.proposerCloseCommit(commit.Instance, commit.ConfigBal, inst.abk.cmds, commit.WhoseCmd, false)
}
