package lwc

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
	prepareRPC          uint8
	acceptRPC           uint8
	commitRPC           uint8
	commitShortRPC      uint8
	prepareReplyRPC     uint8
	acceptReplyRPC      uint8
	instanceSpace       []*Instance // the space of all instances (used and not yet used)
	crtInstance         int32       // highest active instance number that this replica knows about
	crtConfig           int32
	Shutdown            bool
	counter             int
	flush               bool
	executedUpTo        int32
	batchWait           int
	maxBalInc           int32
	maxBeganInstances   int32
	beginNew            chan struct{}
	crtOpenedInstances  []int32
	proposableInstances chan ProposalInfo
	clientValueQueue    clientproposalqueue.ClientProposalQueue
	//valuesToPropose     chan ProposalTuples
	noopInstance                  chan ProposalInfo
	noopWaitUs                    int32
	retryInstance                 chan RetryInfo
	BackoffManager                BackoffManager
	alwaysNoop                    bool
	lastTimeClientChosen          time.Time
	lastOpenProposalTime          time.Time
	valuesOutstandingCurrently    int32
	timeout                       int64
	problemInstance               int32
	timeSinceLastProposedInstance time.Time
	fastLearn                     bool
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
		//	oks:     0,
		nacks: 0,
		aids:  make(map[int32]struct{}),
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
	maxKnownBal        lwcproto.ConfigBal
	maxAcceptedConfBal lwcproto.ConfigBal // highest maxAcceptedConfBal at which a command was accepted
	cmds               []state.Command    // the accepted command
	propCurConfBal     lwcproto.ConfigBal // highest this replica maxAcceptedConfBal tried so far
	clientProposals    []*genericsmr.Propose
}

type RetryInfo struct {
	backedoff        bool
	InstToPrep       int32
	attemptedConfBal lwcproto.ConfigBal
	preempterConfBal lwcproto.ConfigBal
	preempterAt      QuorumType
	backoffus        int32
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
}

func NewBackoffManager(minBO, maxInitBO, maxBO int32, signalChan *chan RetryInfo, factor float64) BackoffManager {
	//rand.Seed(time.Now().UTC().UnixNano())
	//	if minBO > maxInitBO || minBO > maxBO {
	//		panic("Incorrect set up times for backoffs")
	//}
	return BackoffManager{
		currentBackoffs: make(map[int32]RetryInfo),
		BackoffInfo: BackoffInfo{
			minBackoff:     minBO,
			maxInitBackoff: maxInitBO,
			maxBackoff:     maxBO,
		},
		sig:    signalChan,
		factor: factor,
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
	var prevBackoff int32
	if exists {
		preemptNum = curBackoffInfo.timesPreempted + 1
		prevBackoff = curBackoffInfo.backoffus
	} else {
		preemptNum = 0
		prevBackoff = (rand.Int31() % bm.maxInitBackoff) + bm.minBackoff
	}

	t := float64(preemptNum) + rand.Float64()
	tmp := math.Pow(2, t) * math.Tanh(math.Sqrt(bm.factor*t))
	tmp *= 1000
	next := int32(tmp) - prevBackoff + bm.minBackoff
	if next > bm.maxBackoff {
		next = bm.maxBackoff - prevBackoff
	}
	if next < bm.minBackoff {
		next += bm.minBackoff
	}
	//next = 100000

	dlog.Printf("Beginning backoff of %dus for instance %d on conf-bal %d.%d.%d", next, inst, attemptedConfBal.Config, attemptedConfBal.Number, attemptedConfBal.PropID)
	bm.currentBackoffs[inst] = RetryInfo{
		backedoff:        true,
		InstToPrep:       inst,
		attemptedConfBal: attemptedConfBal,
		preempterConfBal: preempter,
		preempterAt:      prempterPhase,
		backoffus:        next,
		timesPreempted:   preemptNum,
	}

	go func(instance int32, attempted lwcproto.ConfigBal, preempterConfBal lwcproto.ConfigBal, preempterP QuorumType, backoff int32, numTimesBackedOff int32) {
		timer := time.NewTimer(time.Duration(next) * time.Microsecond)
		<-timer.C
		*bm.sig <- RetryInfo{
			backedoff:        true,
			InstToPrep:       inst,
			attemptedConfBal: attempted,
			preempterConfBal: preempterConfBal,
			preempterAt:      preempterP,
			backoffus:        backoff,
			timesPreempted:   numTimesBackedOff,
		}
	}(inst, attemptedConfBal, preempter, prempterPhase, next, preemptNum)
}

func (bm *BackoffManager) NoHigherBackoff(backoff RetryInfo) bool {
	curBackoff, exists := bm.currentBackoffs[backoff.InstToPrep]

	if !exists {
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

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, lread bool, dreply bool, durable bool, batchWait int, f int, crtConfig int32, storageLoc string, maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool, factor float64) *Replica {
	retryInstances := make(chan RetryInfo, maxOpenInstances*10000)
	r := &Replica{
		Replica:          genericsmr.NewReplica(id, peerAddrList, thrifty, exec, lread, dreply, f, storageLoc),
		configChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitShortChan:  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptReplyChan:  make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		//	instancesToRecover:  nil,
		prepareRPC:          0,
		acceptRPC:           0,
		commitRPC:           0,
		commitShortRPC:      0,
		prepareReplyRPC:     0,
		acceptReplyRPC:      0,
		instanceSpace:       make([]*Instance, 15*1024*1024),
		crtInstance:         -1, //get from storage
		crtConfig:           crtConfig,
		Shutdown:            false,
		counter:             0,
		flush:               true,
		executedUpTo:        -1, //get from storage
		batchWait:           batchWait,
		maxBalInc:           10000,
		maxBeganInstances:   maxOpenInstances,
		beginNew:            make(chan struct{}, maxOpenInstances),
		crtOpenedInstances:  make([]int32, maxOpenInstances),
		proposableInstances: make(chan ProposalInfo, MAXPROPOSABLEINST),
		//valuesToPropose:     make(chan ProposalTuples, 50000),
		clientValueQueue:           clientproposalqueue.ClientProposalQueueInit(),
		noopInstance:               make(chan ProposalInfo, maxOpenInstances*1000),
		noopWaitUs:                 noopwait,
		retryInstance:              retryInstances,
		BackoffManager:             NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, &retryInstances, factor),
		alwaysNoop:                 alwaysNoop,
		valuesOutstandingCurrently: 0,
		fastLearn:                  true,
	}

	r.Durable = durable

	r.prepareRPC = r.RegisterRPC(new(lwcproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(lwcproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(lwcproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(lwcproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(lwcproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(lwcproto.AcceptReply), r.acceptReplyChan)

	//r.submitNewConfig()
	r.recoverExecutedUpTo()
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

func (r *Replica) recoverConfig() {
	var b [4]byte
	r.StableStore.ReadAt(b[:], 0)
	r.crtConfig = int32(binary.LittleEndian.Uint32(b[:]))
}

func (r *Replica) recordExecutedUpTo() {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(r.executedUpTo))
	r.StableStore.WriteAt(b[:], 4)
}

func (r *Replica) recoverExecutedUpTo() {
	var b [4]byte
	r.StableStore.ReadAt(b[:], 4)
	r.executedUpTo = int32(binary.LittleEndian.Uint32(b[:]))
	r.crtInstance = r.executedUpTo
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
	r.StableStore.Write(b[:])
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
	//log.Println("synced")
	r.StableStore.Sync()
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
		log.Println("sending fast clock")
		fastClockChan <- true
	}
}

func (r *Replica) BatchingEnabled() bool {
	return r.batchWait > 0
}

/* ============= */

/* Main event processing loop */
func IntPow(n, m int64) int64 {
	if m == 0 {
		return 1
	}
	result := n
	for i := int64(2); i <= m; i++ {
		result *= n
	}
	return result
}

func (r *Replica) run() {
	r.ConnectToPeers()
	r.RandomisePeerOrder()
	//if r.Exec {
	//	go r.executeCommands()
	//}
	fastClockChan = make(chan bool, 1)
	//Enabled fast clock when batching
	if r.BatchingEnabled() {
		go r.fastClock()
	}
	//	onOffProposeChan := r.ProposeChan
	//if r.crtConfig > 1 {
	// send to all
	//}
	go r.WaitForClientConnections()
	for i := 0; i < int(r.maxBeganInstances); i++ {
		//		log.Println("sending begin new")
		//	r.beginNew <- struct{}{}
		r.crtOpenedInstances[i] = -1
		r.beginNextInstance()
	}
	newPeerOrderTimer := time.NewTimer(1 * time.Second)
	for !r.Shutdown {
		select {
		case <-newPeerOrderTimer.C:
			r.RandomisePeerOrder()
			dlog.Println("Recomputing closest peers")
			newPeerOrderTimer.Reset(10 * time.Millisecond)
			break
		case <-r.beginNew:
			r.beginNextInstance()
			break
		case noopInfo := <-r.noopInstance:
			dlog.Println("Checking whether to noop a proposal")
			r.checkAndNoop(noopInfo)
			break
		case next := <-r.retryInstance:
			dlog.Println("Checking whether to retry a proposal")
			r.checkAndRetry(next)
			break
			//	case <-fastClockChan:
			//activate new proposals channel
		//	onOffProposeChan = r.ProposeChan
		//		break
		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*lwcproto.Prepare)
			//got a Prepare message
			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
			r.handlePrepare(prepare)
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
		}
	}
}

func (r *Replica) checkAndNoop(noopInfo ProposalInfo) {
	inst := r.instanceSpace[noopInfo.inst]
	pbk := inst.pbk
	abk := inst.abk

	minAcceptableConfBal := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: abk.curBal,
	}
	isAcceptable := noopInfo.proposingConfBal.GreaterThan(minAcceptableConfBal) || noopInfo.proposingConfBal.Equal(minAcceptableConfBal)
	if pbk.status == READY_TO_PROPOSE && pbk.propCurConfBal.Equal(noopInfo.proposingConfBal) && isAcceptable {
		pbk.status = PROPOSING
		pbk.cmds = state.NOOP()
		// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
		r.acceptorAcceptOnConfBal(noopInfo.inst, pbk.propCurConfBal, inst.pbk.cmds)
		r.proposerCheckAndHandleAcceptedValue(noopInfo.inst, r.Id, pbk.propCurConfBal, inst.pbk.cmds)
		r.bcastAccept(noopInfo.inst)
	}
}

func (r *Replica) checkAndRetry(next RetryInfo) {
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

func (r *Replica) bcastPrepare(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Prepare bcast failed:", err)
		}
	}()

	args := &lwcproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurConfBal}

	n := r.N - 1

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.prepareRPC, args)
		sent++
		if sent >= n {
			break
		}
	}
}

var pa lwcproto.Accept

func (r *Replica) bcastAccept(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()

	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.ConfigBal = r.instanceSpace[instance].pbk.propCurConfBal
	pa.Command = r.instanceSpace[instance].pbk.cmds
	args := &pa

	n := r.N - 1

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.acceptRPC, args)
		sent++
		if sent >= n {
			break
		}
	}
}

var pc lwcproto.Commit
var pcs lwcproto.CommitShort

func (r *Replica) bcastCommit(instance int32, confBal lwcproto.ConfigBal, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.ConfigBal = confBal
	pc.Command = command

	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.ConfigBal = confBal
	pcs.Count = int32(len(command))

	argsShort := &pcs

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}

		_, inQrm := r.instanceSpace[instance].pbk.proposalInfos[confBal].aids[r.PreferredPeerOrder[q]]
		if inQrm {
			r.SendMsg(r.PreferredPeerOrder[q], r.commitShortRPC, argsShort)
		} else {
			r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, &pc)
		}
		sent++
	}
}

func (r *Replica) getNextProposingConfigBal(instance int32) lwcproto.ConfigBal {
	pbk := r.instanceSpace[instance].pbk
	min := ((pbk.maxKnownBal.Number/r.maxBalInc)+1)*r.maxBalInc + int32(r.N)
	zero := time.Time{}
	var max int32
	if r.lastOpenProposalTime != zero {
		timeDif := time.Now().Sub(r.lastOpenProposalTime)
		max = min + (r.maxBalInc) + int32(timeDif.Milliseconds())
	} else {
		max = min + (r.maxBalInc)
	}
	if max < 0 { // overflow
		max = math.MaxInt32
		panic("too many conf-bals")
	}
	//	rand.Seed(time.Now().UTC().UnixNano())
	//next := min + rand.Int31() * (max - min)
	next := int32(math.Floor(rand.Float64()*float64(max-min+1) + float64(min)))
	if next < pbk.maxKnownBal.Number {
		panic("Trying outdated conf-bal?")
	}

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

	if r.instanceSpace[r.crtInstance] != nil {
		str := fmt.Sprintf("Oh no, we've tried to begin an instance already begun %d", r.crtInstance)
		panic(str)
	}
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
			maxKnownBal: lwcproto.ConfigBal{
				Config: -1,
				Ballot: lwcproto.Ballot{-1, -1},
			},
			maxAcceptedConfBal: lwcproto.ConfigBal{
				Config: -1,
				Ballot: lwcproto.Ballot{-1, -1},
			},
			cmds: nil,
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
	if pbk.status == CLOSED || r.instanceSpace[inst].abk.status == COMMITTED {
		panic("oh nnooooooooo")
	}
	pbk.status = PREPARING
	nextConfBal := r.getNextProposingConfigBal(inst)
	pbk.proposalInfos[nextConfBal] = new(QuorumInfo)

	pbk.propCurConfBal = nextConfBal
	pbk.maxKnownBal = nextConfBal

	pbk.proposalInfos[nextConfBal] = NewQuorumInfo(PROMISE)
}

func (r *Replica) beginNextInstance() {
	r.incToNextOpenInstance()

	foundFree := false
	for i := 0; i < len(r.crtOpenedInstances); i++ {
		// allow for an openable instance
		if r.crtOpenedInstances[i] == -1 {
			r.crtOpenedInstances[i] = r.crtInstance
			foundFree = true
			break
		}
	}

	if !foundFree {
		panic("Opened up too many instances or closed instances not considered closed")
	}

	r.instanceSpace[r.crtInstance] = r.makeEmptyInstance()
	curInst := r.instanceSpace[r.crtInstance]
	//	if r.crtInstance % int32(r.N) == r.Id {
	//	initBal := lwcproto.ConfigBal{r.crtInstance, lwcproto.Ballot{-1, int16(r.Id)}}
	//	curInst.pbk.propCurConfBal = initBal
	//	curInst.pbk.maxKnownBal = initBal.Ballot
	//	curInst.pbk.proposalInfos[initBal] = NewQuorumInfo(PROMISE)
	//	r.propose(r.crtInstance)
	//} else {
	r.proposerBeginNextConfBal(r.crtInstance)
	r.acceptorPrepareOnConfBal(r.crtInstance, curInst.pbk.propCurConfBal)
	r.bcastPrepare(r.crtInstance)
	//	log.Printf("Opened new instance %d\n", r.crtInstance)
	//	}
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	dlog.Printf("Received new client value\n")
	r.clientValueQueue.TryEnqueue(propose)
	//check if any open instances
}

func (r *Replica) acceptorPrepareOnConfBal(inst int32, confBal lwcproto.ConfigBal) {
	abk := r.instanceSpace[inst].abk
	if r.instanceSpace[inst].pbk.status == CLOSED || r.instanceSpace[inst].abk.status == COMMITTED {
		panic("oh nnooooooooo")
	}
	cur := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: abk.curBal,
	}

	if cur.GreaterThan(confBal) && !cur.Equal(confBal) {
		panic("accepted outof date conf bal")
	}

	r.instanceSpace[inst].abk.status = PREPARED
	if confBal.Config > r.crtConfig {
		r.recordNewConfig(confBal.Config)
		r.sync()
		r.crtConfig = confBal.Config
		dlog.Printf("Joined higher Config %d", r.crtConfig)
	}

	dlog.Printf("Acceptor Preparing Config-Ballot %d.%d.%d ", confBal.Config, confBal.Number, confBal.PropID)
	r.instanceSpace[inst].abk.curBal = confBal.Ballot
}

func (r *Replica) acceptorAcceptOnConfBal(inst int32, confBal lwcproto.ConfigBal, cmds []state.Command) {
	abk := r.instanceSpace[inst].abk

	if r.instanceSpace[inst].pbk.status == CLOSED || abk.status == COMMITTED {
		panic("oh nnooooooooo")
	}
	abk.status = ACCEPTED

	cur := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: abk.curBal,
	}

	if cur.GreaterThan(confBal) {
		panic("accepted outof date conf bal")
	}

	if confBal.Config > r.crtConfig {
		r.recordNewConfig(confBal.Config)
		r.crtConfig = confBal.Config
		dlog.Printf("Joined higher Config %d", r.crtConfig)
	}

	dlog.Printf("Acceptor Accepting Config-Ballot %d.%d.%d ", confBal.Config, confBal.Number, confBal.PropID)
	abk.curBal = confBal.Ballot
	abk.vConfBal = confBal
	abk.cmds = cmds

	r.recordInstanceMetadata(r.instanceSpace[inst])
	r.recordCommands(cmds)
	r.sync()
}

func (pbk *ProposingBookkeeping) isOnInitialConfBal(myID int16) bool {
	for k, _ := range pbk.proposalInfos {
		if k.PropID == myID {
			if pbk.propCurConfBal.Equal(k) {
				return true
			} else {
				return false
			}
		}
	}
	return false
}

func (r *Replica) proposerCheckAndHandlePreempt(inst int32, preemptingConfigBal lwcproto.ConfigBal, preemterPhase QuorumType, shouldRequeue bool) bool {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if r.crtInstance < preemptingConfigBal.Config {
		r.recordNewConfig(preemptingConfigBal.Config)
		r.sync()
		r.crtConfig = preemptingConfigBal.Config
	}

	if pbk.status != CLOSED && preemptingConfigBal.GreaterThan(pbk.maxKnownBal) {
		//	if pbk.status == PROPOSING {
		//		r.requeueClientProposals(inst)
		//	}

		if preemptingConfigBal.Config > pbk.maxKnownBal.Config {
			pbk.maxKnownBal.Config = preemptingConfigBal.Config

			//for all instances signal retry
		}

		if preemptingConfigBal.Ballot.GreaterThan(pbk.propCurConfBal.Ballot) {
			pbk.maxKnownBal = preemptingConfigBal
		}

		//	if pbk.isOnInitialConfBal(int16(r.Id)) {
		//		r.checkAndOpenNewInstance(inst)
		//	}
		//	if shouldRequeue {
		//		r.requeueClientProposals(inst)
		//	}
		if pbk.status != BACKING_OFF { // option for add multiple preempts if backing off already?
			r.BackoffManager.CheckAndHandleBackoff(inst, pbk.propCurConfBal, preemptingConfigBal, preemterPhase)
		}
		if instance.abk.status == COMMITTED {
			panic("trying backing off but already commmited")
		}
		pbk.status = BACKING_OFF
		return true
	} else {
		return false
	}
}

func (r *Replica) handlePrepare(prepare *lwcproto.Prepare) {
	r.checkAndHandleNewlyReceivedInstance(prepare.Instance)
	inst := r.instanceSpace[prepare.Instance]

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed instance %d, returning commit \n", prepare.Instance)
		var pc lwcproto.Commit
		pc.LeaderId = prepare.LeaderId
		pc.Instance = prepare.Instance
		pc.ConfigBal = inst.abk.vConfBal
		pc.Command = inst.abk.cmds
		args := &pc
		r.SendMsg(pc.LeaderId, r.commitRPC, args)
		return
	}

	minSafe := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: inst.abk.curBal,
	}

	if minSafe.GreaterThan(prepare.ConfigBal) {
		dlog.Printf("Already prepared on higher Config-Ballot %d.%d.%d < %d.%d.%d", prepare.Config, prepare.Number, prepare.PropID, minSafe.Config, minSafe.Number, minSafe.PropID)
		// return preempt
	} else if prepare.ConfigBal.GreaterThan(minSafe) {
		dlog.Printf("Preparing on prepared on new Config-Ballot %d.%d.%d", prepare.Config, prepare.Number, prepare.PropID)

		r.acceptorPrepareOnConfBal(prepare.Instance, prepare.ConfigBal)
		r.proposerCheckAndHandlePreempt(prepare.Instance, prepare.ConfigBal, PROMISE, false)

		if inst.pbk.status != BACKING_OFF {
			panic("why not backed off????")
		}
	} else {
		// msg reordering
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
		Command:    inst.abk.cmds,
	}

	r.replyPrepare(prepare.LeaderId, preply)
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

func (r *Replica) proposerCheckAndHandleAcceptedValue(inst int32, aid int32, accepted lwcproto.ConfigBal, val []state.Command) ProposerAccValHandler {
	if accepted.IsZeroVal() {
		return IGNORED
	}
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	newVal := false

	if accepted.GreaterThan(pbk.maxAcceptedConfBal) {
		newVal = true
		//	if pbk.isProposingClientValue() {
		//		log.Println("Client proposal superceded, requeuing")
		//		r.requeueClientProposals(inst)
		//	}
		pbk.maxAcceptedConfBal = accepted
		pbk.cmds = val
	}

	_, exists := pbk.proposalInfos[accepted]
	if !exists {
		pbk.proposalInfos[accepted] = NewQuorumInfo(ACCEPTANCE)
	}
	pbk.proposalInfos[accepted].quorumAdd(aid)
	// not assumed local acceptor has accepted it
	if int(pbk.proposalInfos[accepted].quorumCount()) >= r.WriteQuorumSize() {
		r.acceptorCommit(inst, accepted, val)
		r.proposerCloseCommit(inst, accepted, pbk.cmds)
		r.bcastCommit(inst, accepted, val)
		if pbk.status != CLOSED || instance.abk.status != COMMITTED {
			panic("not set agents to closed")
		}
		return CHOSEN
	} else if newVal {
		return NEW_VAL
	} else {
		return ACKED
	}
}

func (r *Replica) handlePrepareReply(preply *lwcproto.PrepareReply) {
	inst := r.instanceSpace[preply.Instance]
	pbk := r.instanceSpace[preply.Instance].pbk

	if inst.abk.status == COMMITTED {
		dlog.Println("Inst already known to be chosen")
		return
	}

	valWhatDone := r.proposerCheckAndHandleAcceptedValue(preply.Instance, preply.AcceptorId, preply.VConfigBal, preply.Command)
	// early learning
	if valWhatDone == CHOSEN {
		dlog.Printf("Preparing instance recognised as chosen (instance %d), returning commit \n", preply.Instance)
		if preply.ConfigBal.IsZeroVal() {
			panic("Why we commit zero ballot???")
		}
		return
	}

	if pbk.propCurConfBal.GreaterThan(preply.ConfigBal) || pbk.status != PREPARING {
		dlog.Printf("Message in late \n")
		return
	}

	if r.proposerCheckAndHandlePreempt(preply.Instance, preply.ConfigBal, PROMISE, false) {
		dlog.Printf("Another active proposer using config-ballot %d.%d.%d greater than mine\n", preply.ConfigBal)
		if preply.ConfigBal.Config > r.crtConfig {
			r.recordNewConfig(preply.ConfigBal.Config)
			r.sync()
		}
		return
		//should open new instance???
	}

	qrm := pbk.proposalInfos[pbk.propCurConfBal]
	qrm.quorumAdd(preply.AcceptorId)
	if inst.abk.curBal.GreaterThan(pbk.propCurConfBal.Ballot) {
		panic("somehow acceptor has moved on but proposer hasn't")
	}
	if int(qrm.quorumCount()+1) >= r.Replica.ReadQuorumSize() {
		r.propose(preply.Instance)
	}
}

func (r *Replica) propose(inst int32) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	//	abk := instance.abk

	if pbk.status == CLOSED || instance.abk.status == COMMITTED {
		panic("oh nnooooooooo")
	}

	pbk.status = READY_TO_PROPOSE

	dlog.Println("Can now propose in instance", inst)
	qrm := pbk.proposalInfos[pbk.propCurConfBal]

	//	if qrm.qrmType != PROMISE && int(qrm.quorumCount()) < r.Replica.ReadQuorumSize() {
	//		panic("Cannot propose without quorum")
	//	}

	qrm.quorumClear()
	qrm.qrmType = ACCEPTANCE

	if pbk.maxAcceptedConfBal.IsZeroVal() {
		//	time.Sleep(time.Millisecond * 5)
		for cont := true; cont; {
			select {
			case prop := <-r.ProposeChan:
				r.handlePropose(prop)
				break
			default:
				cont = false
				break
			}

		}

		switch cliProp := r.clientValueQueue.TryDequeue(); {
		case cliProp != nil:
			numEnqueued := r.clientValueQueue.Len() + 1
			batchSize := numEnqueued
			pbk.clientProposals = make([]*genericsmr.Propose, batchSize)
			pbk.cmds = make([]state.Command, batchSize)
			pbk.clientProposals[0] = cliProp
			pbk.cmds[0] = cliProp.Command
			r.valuesOutstandingCurrently++

			for i := 1; i < batchSize; i++ {
				cliProp = r.clientValueQueue.TryDequeue()
				if cliProp == nil {
					pbk.clientProposals = pbk.clientProposals[:i]
					pbk.cmds = pbk.cmds[:i]
					break
				}
				pbk.clientProposals[i] = cliProp
				pbk.cmds[i] = cliProp.Command
				r.valuesOutstandingCurrently++
			}

			dlog.Printf("%d client value(s) proposed in instance %d \n", len(pbk.clientProposals), inst)
			break
		default:
			pbk.cmds = state.NOOP()
			dlog.Println("Proposing noop")
			break
		}
	} else {
		r.checkAndOpenNewInstance(inst)
	}
	pbk.status = PROPOSING
	// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
	if r.fastLearn {
		r.acceptorAcceptOnConfBal(inst, pbk.propCurConfBal, pbk.cmds)
		r.proposerCheckAndHandleAcceptedValue(inst, r.Id, pbk.propCurConfBal, pbk.cmds)
		r.bcastAccept(inst)
	} else {
		r.bcastAccept(inst)
		r.acceptorAcceptOnConfBal(inst, pbk.propCurConfBal, pbk.cmds)
		r.proposerCheckAndHandleAcceptedValue(inst, r.Id, pbk.propCurConfBal, pbk.cmds)
	}
}

func (r *Replica) shouldProposeNoop(inst int32) bool {
	if r.alwaysNoop {
		return true
	}
	for i := inst + 1; i <= r.crtInstance; i++ {
		if r.instanceSpace[i] != nil {
			if r.instanceSpace[i].abk != nil {
				status := r.instanceSpace[i].abk.status
				if status == ACCEPTED || status == COMMITTED {
					return true
				}
			}
		}
	}
	return false
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
	inst := r.instanceSpace[accept.Instance]

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed \n")
		var pc lwcproto.Commit
		pc.LeaderId = accept.LeaderId
		pc.Instance = accept.Instance
		pc.ConfigBal = inst.abk.vConfBal
		pc.Command = inst.abk.cmds
		args := &pc
		r.SendMsg(pc.LeaderId, r.commitRPC, args)
		return
	}

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
			r.proposerCheckAndHandleAcceptedValue(accept.Instance, int32(accept.PropID), accept.ConfigBal, accept.Command) // must go first as acceptor might have already learnt of value
		}
		accValState := r.proposerCheckAndHandleAcceptedValue(accept.Instance, r.Id, accept.ConfigBal, accept.Command)
		if accValState == CHOSEN {
			return
		}
		r.proposerCheckAndHandlePreempt(accept.Instance, accept.ConfigBal, ACCEPTANCE, true)
	} else if minAcceptableConfBal.GreaterThan(accept.ConfigBal) {
		dlog.Printf("Returning preempt for config-ballot %d.%d.%d < %d.%d.%d in Instance %d\n", accept.Config, accept.Number, accept.PropID, minAcceptableConfBal.Config, minAcceptableConfBal.Number, minAcceptableConfBal.PropID, accept.Instance)
	} else {
		dlog.Printf("Already acknowledged accept request but will return again", accept.Config, accept.Number, accept.PropID, minAcceptableConfBal.Config, minAcceptableConfBal.Number, minAcceptableConfBal.PropID, accept.Instance)
	}

	replyConfBal := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: inst.abk.curBal,
	}

	areply := &lwcproto.AcceptReply{accept.Instance, r.Id, replyConfBal, accept.ConfigBal}
	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) checkAndOpenNewInstance(inst int32) {
	// was instance opened by us
	//	if r.executedUpTo+r.maxBeganInstances*int32(r.N) > r.crtInstance {
	for i := 0; i < len(r.crtOpenedInstances); i++ {
		if r.crtOpenedInstances[i] == inst {
			r.crtOpenedInstances[i] = -1
			r.beginNextInstance()
			break
		}
	}
	//	}
}

func cmdsEqual(a, b []state.Command) bool {
	if len(a) != len(b) {
		return false
	} else {
		for i := 0; i < len(a); i++ {
			if a[i].String() != b[i].String() {
				return false
			}
		}
		return true
	}
}

func (r *Replica) handleAcceptReply(areply *lwcproto.AcceptReply) {
	// could modify to have record of all ballots
	inst := r.instanceSpace[areply.Instance]
	pbk := r.instanceSpace[areply.Instance].pbk

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed ")
		return
	}

	accepted := areply.Cur.Equal(areply.Req)
	preempted := areply.Cur.GreaterThan(areply.Req)
	if accepted {
		dlog.Printf("Acceptance of instance %d at %d.%d.%d by Acceptor %d received\n", areply.Instance, areply.Cur.Config, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId)
		// pbk.cmds should be correct value - proposer only gets to this
		// stage if they learn of any previously chosen value and propose it again themselves
		qrmState := r.proposerCheckAndHandleAcceptedValue(areply.Instance, areply.AcceptorId, areply.Cur, pbk.cmds)
		if qrmState == CHOSEN {
			// set acceptor chosen
			if !cmdsEqual(inst.pbk.cmds, inst.abk.cmds) {
				panic("Commands somehow been overwritten")
			}
		}

		if inst.pbk.clientProposals != nil && !cmdsEqual(inst.pbk.cmds, inst.abk.cmds) {
			panic("WHHHAATT why these different?")
		}
	} else if preempted {
		r.proposerCheckAndHandlePreempt(areply.Instance, areply.Cur, ACCEPTANCE, false)
	} else {
		msg := fmt.Sprintf("Somehow cur Conf-Bal of %d is %d.%d.%d when we requested %d.%d.%d for acceptance",
			areply.AcceptorId, areply.Cur.Config, areply.Cur.Number, areply.Cur.PropID,
			areply.Req.Config, areply.Req.Number, areply.Req.PropID)
		panic(msg)
	}
}

func (r *Replica) requeueClientProposals(instance int32) {
	inst := r.instanceSpace[instance]
	dlog.Println("Requeing client value")
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

func (pbk *ProposingBookkeeping) whatHappenedToClientProposals() ClientProposalStory {
	if pbk.clientProposals == nil {
		return NotProposed
	} else {
		if len(pbk.clientProposals) != len(pbk.cmds) {
			return ProposedButNotChosen
		}
		for i := 0; i < len(pbk.clientProposals); i++ {
			if pbk.clientProposals[i].Command.String() != pbk.cmds[i].String() {
				return ProposedButNotChosen
			}
		}
		return ProposedAndChosen
	}
}

func (r *Replica) proposerCloseCommit(inst int32, chosenAt lwcproto.ConfigBal, chosenVal []state.Command) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	pbk.status = CLOSED
	dlog.Printf("Instance %d chosen now\n", inst)

	r.BackoffManager.ClearBackoff(inst)
	if chosenAt.GreaterThan(pbk.maxKnownBal) {
		pbk.maxKnownBal = chosenAt
	}

	if chosenAt.GreaterThan(pbk.maxAcceptedConfBal) {
		pbk.maxAcceptedConfBal = chosenAt
		pbk.cmds = chosenVal
	}

	if chosenAt.Equal(pbk.propCurConfBal) {
		r.timeSinceLastProposedInstance = time.Now()
	}

	switch pbk.whatHappenedToClientProposals() {
	case NotProposed:
		break
	case ProposedButNotChosen:
		r.requeueClientProposals(inst)
		r.valuesOutstandingCurrently -= int32(len(pbk.clientProposals))
		dlog.Printf("%d client value(s) proposed in instance %d\n not chosen", len(pbk.clientProposals), inst)
		pbk.clientProposals = nil
		break
	case ProposedAndChosen:
		r.valuesOutstandingCurrently -= int32(len(pbk.clientProposals))
		dlog.Printf("%d client value(s) chosen in instance %d\n", len(pbk.clientProposals), inst)
		for i := 0; i < len(pbk.clientProposals); i++ {
			r.clientValueQueue.CloseValue(pbk.clientProposals[i])
		}
		break
	}

	r.checkAndOpenNewInstance(inst)
	if r.instanceSpace[inst].pbk.status != CLOSED && r.instanceSpace[inst].abk.status != COMMITTED {
		panic("not commited somehow")
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

	if r.Exec {
		oldExecutedUpTo := r.executedUpTo
		for i := r.executedUpTo + 1; i <= r.crtInstance; i++ {
			inst := r.instanceSpace[i]
			if inst != nil && inst.abk.status == COMMITTED { //&& inst.abk.cmds != nil {
				dlog.Printf("Executing instance %d\n", i)
				for j := 0; j < len(inst.abk.cmds); j++ {
					dlog.Printf("Executing " + inst.abk.cmds[j].String())
					if r.Dreply && inst.pbk != nil && inst.pbk.clientProposals != nil {
						val := inst.abk.cmds[j].Execute(r.State)
						if !cmdsEqual(inst.pbk.cmds, inst.abk.cmds) {
							panic("client value is wrong")
						}

						if cmdsEqual(inst.pbk.cmds, state.NOOP()) {
							panic("somehow returning noop")
						}

						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							inst.pbk.clientProposals[j].CommandId,
							val,
							inst.pbk.clientProposals[j].Timestamp}
						r.ReplyProposeTS(propreply, inst.pbk.clientProposals[j].Reply, inst.pbk.clientProposals[j].Mutex)
						dlog.Printf("Returning executed client value")
					} else if inst.abk.cmds[j].Op == state.PUT {
						inst.abk.cmds[j].Execute(r.State)
					}

				}
				r.executedUpTo += 1
				dlog.Printf("Executed up to %d (crtInstance=%d)", r.executedUpTo, r.crtInstance)
				//r.instanceSpace[i] = nil
			} else {
				/*	if i == r.problemInstance {
						r.timeout += SLEEP_TIME_NS
						if r.timeout >= COMMIT_GRACE_PERIOD {
							for k := r.problemInstance; k <= r.crtInstance; k++ {
								dlog.Printf("Recovering instance %d \n", k)
								r.retryInstance <- RetryInfo{
									backedoff:        false,
									InstToPrep:       k,
									attemptedConfBal: r.instanceSpace[k].pbk.propCurConfBal,
									preempterConfBal: lwcproto.ConfigBal{-1, lwcproto.Ballot{-1, -1}},
									preempterAt:      0,
									backoffus:        0,
									timesPreempted:   0,
								}
							}
							r.problemInstance = 0
							r.timeout = 0
						}
					} else {
						r.problemInstance = i
						r.timeout = 0
					}
					break*/
				if r.executedUpTo > oldExecutedUpTo {
					r.recordExecutedUpTo()
				}
				break
			}
		}
	}

	if pbk.clientProposals != nil && !cmdsEqual(instance.abk.cmds, instance.pbk.cmds) {
		panic("Somehow we have not requeued client value")
	}
}

func (r *Replica) acceptorCommit(instance int32, chosenAt lwcproto.ConfigBal, cmds []state.Command) {
	inst := r.instanceSpace[instance]
	abk := inst.abk
	dlog.Printf("Committing (crtInstance=%d)\n", instance)

	if inst.pbk.status == CLOSED || inst.abk.status == COMMITTED {
		panic("shouldn't be commiting things multiple times")
	}

	inst.abk.status = COMMITTED
	shouldSync := false
	if r.crtConfig < chosenAt.Config {
		r.recordNewConfig(chosenAt.Config)
		r.crtConfig = chosenAt.Config
		shouldSync = true
	}
	abk.curBal = chosenAt.Ballot
	abk.vConfBal = chosenAt
	abk.cmds = cmds

	r.recordInstanceMetadata(inst)
	r.recordCommands(cmds)
	if shouldSync {
		r.sync()
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
	r.proposerCloseCommit(commit.Instance, commit.ConfigBal, commit.Command)

	if inst.pbk.clientProposals != nil && !cmdsEqual(inst.pbk.cmds, inst.abk.cmds) {
		panic("WHHHAATT why these different?")
	}
}

func (r *Replica) handleCommitShort(commit *lwcproto.CommitShort) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	inst := r.instanceSpace[commit.Instance]

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed \n")
		return
	}

	r.acceptorCommit(commit.Instance, commit.ConfigBal, inst.abk.cmds)
	r.proposerCloseCommit(commit.Instance, commit.ConfigBal, inst.abk.cmds)

	if inst.pbk.clientProposals != nil && !cmdsEqual(inst.pbk.cmds, inst.abk.cmds) {
		panic("WHHHAATT why these different?")
	}
}
