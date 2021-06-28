package stdpaxospatient

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
	"math"
	"math/rand"
	"state"
	"stdpaxosproto"
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
	inst         int32
	proposingBal stdpaxosproto.Ballot
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
	status AcceptorStatus
	cmds   []state.Command
	curBal stdpaxosproto.Ballot
	vBal   stdpaxosproto.Ballot
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
	status          ProposerStatus
	proposalInfos   map[stdpaxosproto.Ballot]*QuorumInfo
	maxAcceptedBal  stdpaxosproto.Ballot // highest maxAcceptedBal at which a command was accepted
	cmds            []state.Command      // the accepted command
	propCurBal      stdpaxosproto.Ballot // highest this replica maxAcceptedBal tried so far
	clientProposals []*genericsmr.Propose
	maxKnownBal     stdpaxosproto.Ballot
}

type RetryInfo struct {
	backedoff      bool
	InstToPrep     int32
	attemptedBal   stdpaxosproto.Ballot
	preempterBal   stdpaxosproto.Ballot
	preempterAt    QuorumType
	backoffus      int32
	timesPreempted int32
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

func (bm *BackoffManager) ShouldBackoff(inst int32, preempter stdpaxosproto.Ballot, preempterPhase QuorumType) bool {
	curBackoffInfo, exists := bm.currentBackoffs[inst]
	if !exists {
		return true
	} else if preempter.GreaterThan(curBackoffInfo.preempterBal) || (preempter.Equal(curBackoffInfo.preempterBal) && preempterPhase > curBackoffInfo.preempterAt) {
		return true
	} else {
		return false
	}
}
func (bm *BackoffManager) CheckAndHandleBackoff(inst int32, attemptedBal stdpaxosproto.Ballot, preempter stdpaxosproto.Ballot, prempterPhase QuorumType) {
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

	dlog.Printf("Beginning backoff of %dus for instance %d on conf-bal %d.%d.%d", next, inst, attemptedBal.Number, attemptedBal.PropID)
	bm.currentBackoffs[inst] = RetryInfo{
		backedoff:      true,
		InstToPrep:     inst,
		attemptedBal:   attemptedBal,
		preempterBal:   preempter,
		preempterAt:    prempterPhase,
		backoffus:      next,
		timesPreempted: preemptNum,
	}

	go func(instance int32, attempted stdpaxosproto.Ballot, preempterBal stdpaxosproto.Ballot, preempterP QuorumType, backoff int32, numTimesBackedOff int32) {
		timer := time.NewTimer(time.Duration(next) * time.Microsecond)
		<-timer.C
		*bm.sig <- RetryInfo{
			backedoff:      true,
			InstToPrep:     inst,
			attemptedBal:   attempted,
			preempterBal:   preempterBal,
			preempterAt:    preempterP,
			backoffus:      backoff,
			timesPreempted: numTimesBackedOff,
		}
	}(inst, attemptedBal, preempter, prempterPhase, next, preemptNum)
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

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, lread bool, dreply bool, durable bool, batchWait int, f int, crtConfig int32, storageLoc string, maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool, factor float64, whoCrash int32, whenCrash time.Duration, howlongCrash time.Duration, emulatedSS bool, emulatedWriteTime time.Duration) *Replica {
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
		BackoffManager:      NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, &retryInstances, factor),
		alwaysNoop:          alwaysNoop,
		fastLearn:           false,
		whoCrash:            whoCrash,
		whenCrash:           whenCrash,
		howLongCrash:        howlongCrash,
		emulatedSS:          emulatedSS,
		emulatedWriteTime:   emulatedWriteTime,
	}

	r.Durable = durable
	r.clientValueQueue = clientproposalqueue.ClientProposalQueueInit(r.ProposeChan)

	r.prepareRPC = r.RegisterRPC(new(stdpaxosproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(stdpaxosproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(stdpaxosproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(stdpaxosproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(stdpaxosproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(stdpaxosproto.AcceptReply), r.acceptReplyChan)

	//r.recoverStateMachineInfo()
	//	r.recoverConfig()
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

func (r *Replica) recoverStateMachineInfo() {
	var b [4]byte
	r.StableStore.ReadAt(b[:], 4)
	r.executedUpTo = int32(binary.LittleEndian.Uint32(b[:]))
	r.crtInstance = r.executedUpTo - 1
}

//append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	var b [12]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.abk.curBal.Number))
	binary.LittleEndian.PutUint32(b[4:8], uint32(inst.abk.curBal.PropID))
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
	//log.Println("synced")
	if r.emulatedSS {
		time.Sleep(r.emulatedWriteTime)
	} else {
		_ = r.StableStore.Sync()
	}
}

func (r *Replica) replyPrepare(replicaId int32, reply *stdpaxosproto.PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *stdpaxosproto.AcceptReply) {
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
			break
		default:
			cont = false
			break
		}
	}
	for i := 0; i < len(r.Clients); i++ {
		_ = r.Clients[i].Close()
		r.ClientsReaders[i] = nil
		r.ClientsWriters[i] = nil
	}
	r.BackoffManager = NewBackoffManager(r.BackoffManager.minBackoff, r.BackoffManager.maxInitBackoff, r.BackoffManager.maxBackoff, &r.retryInstance, r.BackoffManager.factor)

	//	for i := 0; i < r.clientValueQueue.Len(); i ++ {
	//	r.clientValueQueue.TryDequeue()
	//	}

	r.RandomisePeerOrder()

	for i := r.executedUpTo; i < r.crtInstance; i++ {
		inst := r.instanceSpace[i]
		if inst != nil {
			if inst.abk.status != COMMITTED {
				log.Printf("Beginning instance %d", i)
				// prepare on new config
				// send prepare
				r.proposerBeginNextBal(i)
				nextBal := r.instanceSpace[i].pbk.propCurBal
				r.acceptorPrepareOnBal(i, nextBal)
				r.bcastPrepareToAlive(i)
			}
		}

	}
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

	for !r.Shutdown {

		select {
		case <-doner:
			log.Println("Crahsing")
			time.Sleep(r.howLongCrash)
			r.restart()
			log.Println("Done crashing")
			break
		case next := <-r.retryInstance:
			dlog.Println("Checking whether to retry a proposal")
			r.checkAndRetry(next)
			break
		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*stdpaxosproto.Prepare)
			//got a Prepare message
			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
			r.handlePrepare(prepare)
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
			log.Println("Client value(s) received beginning new instance")
			r.beginNextInstance(clientProposals)
		}
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
		r.proposerBeginNextBal(next.InstToPrep)
		nextBal := r.instanceSpace[next.InstToPrep].pbk.propCurBal
		if inst.abk.curBal.GreaterThan(inst.pbk.propCurBal) {
			panic("not moved to highest seen conf bal")
		}
		r.acceptorPrepareOnBal(next.InstToPrep, nextBal)
		r.bcastPrepareToAlive(next.InstToPrep)
		dlog.Printf("Proposing next conf-bal %d.%d.%d to instance %d\n", nextBal.Number, nextBal.PropID, next.InstToPrep)
	} else {
		dlog.Printf("Skipping retry of instance %d due to preempted again or closed\n", next.InstToPrep)
	}
}

func (r *Replica) bcastPrepareToAll(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Prepare bcast failed:", err)
		}
	}()

	args := &stdpaxosproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurBal}

	n := r.N - 1

	//	if r.Thrifty {
	//		n = r.ReadQuorumSize() - 1
	//	}

	r.RandomisePeerOrder()
	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		dlog.Printf("send prepare to %d\n", r.PreferredPeerOrder[q])
		r.SendMsg(r.PreferredPeerOrder[q], r.prepareRPC, args)
		sent++
		if sent >= n {
			break
		}
	}
}
func (r *Replica) bcastPrepareToAlive(instance int32) {

	defer func() {
		if err := recover(); err != nil {
			log.Println("Prepare bcast failed:", err)
		}
	}()

	args := &stdpaxosproto.Prepare{r.Id, instance, r.instanceSpace[instance].pbk.propCurBal}

	n := r.N - 1

	if r.Thrifty {
		n = r.ReadQuorumSize() - 1
	}
	r.CalculateAlive()
	r.RandomisePeerOrder()
	sent := 0
	for q := 0; q < r.N-1; q++ {
		//	if !r.Alive[r.PreferredPeerOrder[q]] {
		//		continue
		//	}
		dlog.Printf("send prepare to %d\n", r.PreferredPeerOrder[q])
		r.SendMsg(r.PreferredPeerOrder[q], r.prepareRPC, args)
		sent++
		if sent >= n {
			break
		}
	}

}

var pa stdpaxosproto.Accept

func (r *Replica) bcastAcceptToAlive(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()

	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = r.instanceSpace[instance].pbk.propCurBal
	pa.Command = r.instanceSpace[instance].pbk.cmds
	args := &pa

	n := r.N - 1

	if r.Thrifty {
		n = r.WriteQuorumSize() - 1
	}

	r.CalculateAlive()
	r.RandomisePeerOrder()
	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		dlog.Printf("send accept to %d\n", r.PreferredPeerOrder[q])
		r.SendMsg(r.PreferredPeerOrder[q], r.acceptRPC, args)
		sent++
		if sent >= n {
			break
		}
	}
}

func (r *Replica) bcastAcceptNoFlush(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()

	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = r.instanceSpace[instance].pbk.propCurBal
	pa.Command = r.instanceSpace[instance].pbk.cmds
	args := &pa

	n := r.N - 1

	if r.Thrifty {
		n = r.WriteQuorumSize() - 1
	}

	r.CalculateAlive()
	r.RandomisePeerOrder()
	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		dlog.Printf("send accept to %d\n", r.PreferredPeerOrder[q])
		r.SendMsgNoFlush(r.PreferredPeerOrder[q], r.acceptRPC, args)
		sent++
		if sent >= n {
			break
		}
	}
}

var pc stdpaxosproto.Commit
var pcs stdpaxosproto.CommitShort

func (r *Replica) bcastCommitToAlive(instance int32, bal stdpaxosproto.Ballot, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = bal
	pc.Command = command

	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = bal
	pcs.Count = int32(len(command))

	argsShort := &pcs

	r.CalculateAlive()
	sent := 0
	for q := 0; q < r.N-1; q++ {
		//	if !r.Alive[r.PreferredPeerOrder[q]] {
		//		continue
		//	}

		_, inQrm := r.instanceSpace[instance].pbk.proposalInfos[bal].aids[r.PreferredPeerOrder[q]]
		if inQrm {
			r.SendMsgNoFlush(r.PreferredPeerOrder[q], r.commitShortRPC, argsShort)
		} else {
			r.SendMsgNoFlush(r.PreferredPeerOrder[q], r.commitRPC, &pc)
		}
		sent++
	}
}

func (r *Replica) getNextProposingBallot(instance int32) stdpaxosproto.Ballot {
	pbk := r.instanceSpace[instance].pbk
	abk := r.instanceSpace[instance].abk
	min := ((pbk.maxKnownBal.Number/r.maxBalInc)+1)*r.maxBalInc + int32(r.N)
	//zero := time.Time{}
	var max int32
	//	if r.timeSinceLastProposedInstance != zero {
	//	timeDif := time.Now().Sub(r.timeSinceLastProposedInstance)
	//	max = min + (r.maxBalInc) + int32(timeDif.Milliseconds()*10)
	//	} else {
	max = min + (r.maxBalInc)
	//	}
	if max < 0 { // overflow
		max = math.MaxInt32
		panic("too many conf-bals")
	}

	next := int32(math.Floor(rand.Float64()*float64(max-min+1) + float64(min)))
	if next < abk.curBal.Number {
		panic("Trying outdated conf-bal?")
	}

	//if (instance % int32(r.N)) == r.Id {
	//		next += max
	//	}
	proposingBal := stdpaxosproto.Ballot{next - r.Id, int16(r.Id)}
	dlog.Printf("For instance", instance, "now incrementing to new conf-bal", proposingBal)
	return proposingBal
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
			curBal: stdpaxosproto.Ballot{-1, -1},
			vBal:   stdpaxosproto.Ballot{-1, -1},
		},
		pbk: &ProposingBookkeeping{
			status:        NOT_BEGUN,
			proposalInfos: make(map[stdpaxosproto.Ballot]*QuorumInfo),

			maxKnownBal:     stdpaxosproto.Ballot{-1, -1},
			maxAcceptedBal:  stdpaxosproto.Ballot{-1, -1},
			cmds:            nil,
			propCurBal:      stdpaxosproto.Ballot{-1, -1},
			clientProposals: nil,
		},
	}
}

func (r *Replica) proposerBeginNextBal(inst int32) {
	pbk := r.instanceSpace[inst].pbk
	if pbk.status == CLOSED || r.instanceSpace[inst].abk.status == COMMITTED {
		panic("oh nnooooooooo")
	}
	pbk.status = PREPARING
	nextBal := r.getNextProposingBallot(inst)
	pbk.proposalInfos[nextBal] = new(QuorumInfo)

	pbk.propCurBal = nextBal
	pbk.maxKnownBal = nextBal

	pbk.proposalInfos[nextBal] = NewQuorumInfo(PROMISE)
}

func (r *Replica) beginNextInstance(valsToPropose []*genericsmr.Propose) {
	r.incToNextOpenInstance()
	r.instanceSpace[r.crtInstance] = r.makeEmptyInstance()
	curInst := r.instanceSpace[r.crtInstance]
	r.proposerBeginNextBal(r.crtInstance)
	r.instanceSpace[r.crtInstance].pbk.clientProposals = valsToPropose
	r.acceptorPrepareOnBal(r.crtInstance, curInst.pbk.propCurBal)
	r.bcastPrepareToAlive(r.crtInstance)
	log.Printf("Opened new instance %d\n", r.crtInstance)
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	dlog.Printf("Received new client value\n")
	r.clientValueQueue.TryEnqueue(propose)
	//check if any open instances
}

func (r *Replica) acceptorPrepareOnBal(inst int32, bal stdpaxosproto.Ballot) {
	instance := r.instanceSpace[inst]
	abk := instance.abk
	if r.instanceSpace[inst].pbk.status == CLOSED || r.instanceSpace[inst].abk.status == COMMITTED {
		panic("oh nnooooooooo")
	}
	cur := abk.curBal

	if cur.GreaterThan(bal) && !cur.Equal(bal) {
		panic("preparing on out of date conf bal")
	}

	r.instanceSpace[inst].abk.status = PREPARED

	dlog.Printf("Acceptor Preparing Config-Ballot %d.%d.%d ", bal.Number, bal.PropID)
	r.instanceSpace[inst].abk.curBal = bal
	r.recordInstanceMetadata(instance)
	r.sync()
}

func (r *Replica) acceptorAcceptOnBal(inst int32, bal stdpaxosproto.Ballot, cmds []state.Command) {
	abk := r.instanceSpace[inst].abk
	if r.instanceSpace[inst].pbk.status == CLOSED || abk.status == COMMITTED {
		panic("oh nnooooooooo")
	}
	abk.status = ACCEPTED

	cur := abk.curBal

	if cur.GreaterThan(bal) {
		panic("accepted outof date conf bal")
	}

	dlog.Printf("Acceptor Accepting Config-Ballot %d.%d.%d ", bal.Number, bal.PropID)
	abk.curBal = bal
	abk.vBal = bal
	abk.cmds = cmds

	r.recordInstanceMetadata(r.instanceSpace[inst])
	r.recordCommands(cmds)
	r.sync()
}

func (pbk *ProposingBookkeeping) isOnInitialBal(myID int16) bool {
	for k, _ := range pbk.proposalInfos {
		if k.PropID == myID {
			if pbk.propCurBal.Equal(k) {
				return true
			} else {
				return false
			}
		}
	}
	return false
}

func (r *Replica) getCurBal(inst int32) stdpaxosproto.Ballot {
	return r.instanceSpace[inst].abk.curBal
}

func (r *Replica) proposerCheckAndHandlePreempt(inst int32, preemptingBallot stdpaxosproto.Ballot, preemterPhase QuorumType, shouldRequeue bool) bool {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if pbk.status != CLOSED && preemptingBallot.GreaterThan(pbk.propCurBal) {
		//if pbk.status != BACKING_OFF { // option for add multiple preempts if backing off already?
		r.BackoffManager.CheckAndHandleBackoff(inst, pbk.propCurBal, preemptingBallot, preemterPhase)

		// if we are preparing for a new instance to propose in but are preempted
		if pbk.status == PREPARING && pbk.clientProposals != nil {
			r.requeueClientProposals(inst)
			pbk.clientProposals = nil
		}

		if preemptingBallot.GreaterThan(pbk.maxKnownBal) {
			pbk.maxKnownBal = preemptingBallot
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

func (r *Replica) checkAndHandleCommit(instance int32, whoRespondTo int32) bool {
	inst := r.instanceSpace[instance]
	if inst.abk.status == COMMITTED {
		if instance < r.crtInstance-(int32(r.N)*r.maxOpenInstances) {
			for i := instance; i <= r.crtInstance; i++ {
				returingInst := r.instanceSpace[i]
				if returingInst != nil {
					if returingInst.abk.status == COMMITTED {
						dlog.Printf("Already committed instance %d, returning commit \n", instance)
						var pc stdpaxosproto.Commit
						pc.LeaderId = int32(returingInst.abk.vBal.PropID) //prepare.LeaderId
						pc.Instance = i
						pc.Ballot = returingInst.abk.vBal
						pc.Command = returingInst.abk.cmds
						args := &pc
						r.SendMsgNoFlush(whoRespondTo, r.commitRPC, args)
					}
				}
			}
			_ = r.PeerWriters[whoRespondTo].Flush()
		} else {
			pc.LeaderId = int32(inst.abk.vBal.PropID) //prepare.LeaderId
			pc.Instance = instance
			pc.Ballot = inst.abk.vBal
			pc.Command = inst.abk.cmds
			args := &pc
			r.SendMsg(whoRespondTo, r.commitRPC, args)
		}
		return true
	} else {
		return false
	}
}

func (r *Replica) handlePrepare(prepare *stdpaxosproto.Prepare) {
	r.checkAndHandleNewlyReceivedInstance(prepare.Instance)

	if r.checkAndHandleCommit(prepare.Instance, prepare.LeaderId) {
		return
	}

	inst := r.instanceSpace[prepare.Instance]
	minSafe := inst.abk.curBal

	if minSafe.GreaterThan(prepare.Ballot) {
		dlog.Printf("Already prepared on higher Config-Ballot %d.%d.%d < %d.%d.%d", prepare.Number, prepare.PropID, minSafe.Number, minSafe.PropID)
	} else if prepare.GreaterThan(minSafe) {
		dlog.Printf("Preparing on prepared on new Config-Ballot %d.%d.%d", prepare.Number, prepare.PropID)
		r.acceptorPrepareOnBal(prepare.Instance, prepare.Ballot)
		r.proposerCheckAndHandlePreempt(prepare.Instance, prepare.Ballot, PROMISE, false)
		if inst.pbk.propCurBal.GreaterThan(prepare.Ballot) {
			panic("why not proposer acknowledging or is greater than????")
		}
		if inst.pbk.status != BACKING_OFF {
			panic("why not backed off????")
		}
	} else {
		dlog.Printf("Config-Ballot %d.%d.%d already joined, returning same promise", prepare.Number, prepare.PropID)
	}

	newBallot := inst.abk.curBal

	var preply = &stdpaxosproto.PrepareReply{
		Instance:   prepare.Instance,
		Bal:        newBallot,
		VBal:       inst.abk.vBal,
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

func (r *Replica) proposerCheckAndHandleAcceptedValue(inst int32, aid int32, accepted stdpaxosproto.Ballot, val []state.Command) ProposerAccValHandler {
	if accepted.IsZero() {
		return IGNORED
	}
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	newVal := false

	if pbk.status == CLOSED {
		return CHOSEN
	}
	if accepted.GreaterThan(pbk.maxKnownBal) {
		pbk.maxKnownBal = accepted
	}
	if accepted.GreaterThan(pbk.maxAcceptedBal) {
		newVal = true
		pbk.maxAcceptedBal = accepted
		pbk.cmds = val

		//	if pbk.whatHappenedToClientProposals() == ProposedButNotChosen {
		//		r.requeueClientProposals(inst)
		//		pbk.clientProposals = nil
		//	}
	}

	_, exists := pbk.proposalInfos[accepted]
	if !exists {
		pbk.proposalInfos[accepted] = NewQuorumInfo(ACCEPTANCE)
	}
	pbk.proposalInfos[accepted].quorumAdd(aid)
	// not assumed local acceptor has accepted it
	if int(pbk.proposalInfos[accepted].quorumCount()) >= r.WriteQuorumSize() {
		r.bcastCommitToAlive(inst, accepted, val)
		r.acceptorCommit(inst, accepted, val)
		r.proposerCloseCommit(inst, accepted, pbk.cmds)
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

func (r *Replica) handlePrepareReply(preply *stdpaxosproto.PrepareReply) {
	inst := r.instanceSpace[preply.Instance]
	pbk := r.instanceSpace[preply.Instance].pbk

	// todo should do check and handle commit instead???
	if inst.abk.status == COMMITTED {
		dlog.Println("Inst already known to be chosen")
		return
	}

	valWhatDone := r.proposerCheckAndHandleAcceptedValue(preply.Instance, preply.AcceptorId, preply.VBal, preply.Command)
	if valWhatDone == NEW_VAL {
		dlog.Printf("Promise from %d in instance %d has new value at Config-Ballot %d.%d.%d", preply.AcceptorId,
			preply.Instance, preply.VBal.Number, preply.VBal.PropID)
	}
	// early learning
	if valWhatDone == CHOSEN {
		dlog.Printf("Preparing instance recognised as chosen (instance %d), returning commit \n", preply.Instance)
		if preply.Bal.IsZero() {
			panic("Why we commit zero ballot???")
		}
		return
	}

	if pbk.propCurBal.GreaterThan(preply.Bal) || pbk.status != PREPARING {
		dlog.Printf("Message in late \n")
		return
	}

	if r.proposerCheckAndHandlePreempt(preply.Instance, preply.Bal, PROMISE, false) {
		dlog.Printf("Another active proposer using config-ballot %d.%d.%d greater than mine\n", preply.Bal)
		r.acceptorPrepareOnBal(preply.Instance, preply.Bal)
		myReply := stdpaxosproto.PrepareReply{
			Instance:   preply.Instance,
			Bal:        preply.Bal,
			VBal:       inst.abk.vBal,
			AcceptorId: r.Id,
			Command:    inst.abk.cmds,
		}
		r.SendMsg(int32(preply.Bal.PropID), r.prepareReplyRPC, &myReply)
		return
	}

	qrm := pbk.proposalInfos[pbk.propCurBal]
	qrm.quorumAdd(preply.AcceptorId)
	dlog.Printf("Added replica's %d promise to qrm", preply.AcceptorId)
	if inst.abk.curBal.GreaterThan(pbk.propCurBal) {
		panic("somehow acceptor has moved on but proposer hasn't")
	}
	if int(qrm.quorumCount()+1) >= r.Replica.ReadQuorumSize() {
		r.propose(preply.Instance)
	}
}

func (r *Replica) propose(inst int32) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if pbk.status == CLOSED || instance.abk.status == COMMITTED {
		panic("oh nnooooooooo")
	}

	pbk.status = READY_TO_PROPOSE
	dlog.Println("Can now propose in instance", inst)
	qrm := pbk.proposalInfos[pbk.propCurBal]
	qrm.quorumClear()
	qrm.qrmType = ACCEPTANCE

	if pbk.maxAcceptedBal.IsZero() {
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

				log.Printf("%d client value(s) received and proposed in instance %d which was recovered \n", len(pbk.clientProposals), inst)
				break
			default:
				pbk.cmds = state.NOOP()
				log.Println("Proposing noop in recovered instance")
			}
		}
	}

	//	if pbk.clientProposals != nil {
	//	if pbk.whatHappenedToClientProposals() == ProposedButNotChosen {
	//		r.requeueClientProposals(inst)
	//		pbk.clientProposals = nil
	//	}
	//}
	pbk.status = PROPOSING
	// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
	if r.fastLearn {
		r.acceptorAcceptOnBal(inst, pbk.propCurBal, pbk.cmds)
		r.proposerCheckAndHandleAcceptedValue(inst, r.Id, pbk.propCurBal, pbk.cmds)
		r.bcastAcceptToAlive(inst)
	} else {
		r.bcastAcceptToAlive(inst)
		r.acceptorAcceptOnBal(inst, pbk.propCurBal, pbk.cmds)
		r.proposerCheckAndHandleAcceptedValue(inst, r.Id, pbk.propCurBal, pbk.cmds)
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

func (r *Replica) handleAccept(accept *stdpaxosproto.Accept) {
	r.checkAndHandleNewlyReceivedInstance(accept.Instance)

	if r.checkAndHandleCommit(accept.Instance, accept.LeaderId) {
		return
	}

	inst := r.instanceSpace[accept.Instance]
	minAcceptableBal := inst.abk.curBal

	// should always be unless there is a restart
	if accept.GreaterThan(minAcceptableBal) || accept.Equal(minAcceptableBal) {
		dlog.Printf("Accepted instance %d on conf-ball %d.%d.%d", accept.Instance, accept.Number, accept.PropID)
		r.acceptorAcceptOnBal(accept.Instance, accept.Ballot, accept.Command)

		//check proposer qrm
		// here is where we can add fast learning bit - also add acceptance by config-bal's owner
		if r.fastLearn {
			r.proposerCheckAndHandleAcceptedValue(accept.Instance, int32(accept.PropID), accept.Ballot, accept.Command) // must go first as acceptor might have already learnt of value
		}
		accValState := r.proposerCheckAndHandleAcceptedValue(accept.Instance, r.Id, accept.Ballot, accept.Command)
		if accValState == CHOSEN {
			return
		}
		r.proposerCheckAndHandlePreempt(accept.Instance, accept.Ballot, ACCEPTANCE, true)
	} else if minAcceptableBal.GreaterThan(accept.Ballot) {
		dlog.Printf("Returning preempt for config-ballot %d.%d.%d < %d.%d.%d in Instance %d\n", accept.Number, accept.PropID, minAcceptableBal.Number, minAcceptableBal.PropID, accept.Instance)
	} else {
		dlog.Printf("Already acknowledged accept request but will return again", accept.Number, accept.PropID, minAcceptableBal.Number, minAcceptableBal.PropID, accept.Instance)
	}

	replyBal := inst.abk.curBal

	areply := &stdpaxosproto.AcceptReply{accept.Instance, r.Id, replyBal, accept.Ballot}
	r.replyAccept(accept.LeaderId, areply)
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

func (r *Replica) handleAcceptReply(areply *stdpaxosproto.AcceptReply) {
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
		dlog.Printf("Acceptance of instance %d at %d.%d.%d by Acceptor %d received\n", areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId)
		r.proposerCheckAndHandleAcceptedValue(areply.Instance, areply.AcceptorId, areply.Cur, pbk.cmds)
		// we can count proposer of value too because they durably accept before sending accept request - only if fast learn is on
		if r.fastLearn {
			r.proposerCheckAndHandleAcceptedValue(areply.Instance, int32(areply.Req.PropID), areply.Cur, pbk.cmds)
		}
	} else if preempted {
		r.proposerCheckAndHandlePreempt(areply.Instance, areply.Cur, ACCEPTANCE, false)
	} else {
		msg := fmt.Sprintf("Somehow cur Conf-Bal of %d is %d.%d.%d when we requested %d.%d.%d for acceptance",
			areply.AcceptorId, areply.Cur.Number, areply.Cur.PropID,
			areply.Req.Number, areply.Req.PropID)
		panic(msg)
	}
}

func (r *Replica) requeueClientProposals(instance int32) {
	inst := r.instanceSpace[instance]
	log.Printf("Requeing client values in instance %d", instance)
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

func (pbk *ProposingBookkeeping) whatHappenedToClientProposals() ClientProposalStory {
	//	abk := r.instanceSpace[inst].abk
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

func (r *Replica) howManyAttemptsToChoose(inst int32) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if pbk.status != CLOSED {
		panic("cannot how many attempts taken to chose value check unless closed")
	}
	attempts := pbk.maxAcceptedBal.Number / r.maxBalInc
	dlog.Printf("Attempts to chose instance %d: %d", inst, attempts)
}

func (r *Replica) proposerCloseCommit(inst int32, chosenAt stdpaxosproto.Ballot, chosenVal []state.Command) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if pbk.status == CLOSED && !cmdsEqual(instance.abk.cmds, chosenVal) {
		panic("somehow another acceptor has closed on a different value :O ")
	}

	pbk.status = CLOSED
	dlog.Printf("Instance %d chosen now\n", inst)

	r.BackoffManager.ClearBackoff(inst)

	if int32(chosenAt.PropID) == r.Id {
		r.timeSinceLastProposedInstance = time.Now()
	}

	pbk.maxAcceptedBal = chosenAt
	pbk.cmds = chosenVal

	switch pbk.whatHappenedToClientProposals() {
	case NotProposed:
		if pbk.clientProposals != nil {
			panic("said not proposed but has")
		}
		break
	case ProposedButNotChosen:
		log.Printf("%d client value(s) proposed in instance %d\n not chosen", len(pbk.clientProposals), inst)
		r.requeueClientProposals(inst)
		pbk.clientProposals = nil
		break
	case ProposedAndChosen:
		log.Printf("%d client value(s) chosen in instance %d\n", len(pbk.clientProposals), inst)
		for i := 0; i < len(pbk.clientProposals); i++ {
			r.clientValueQueue.CloseValue(pbk.clientProposals[i])
		}
		break
	}

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
	r.howManyAttemptsToChoose(inst)

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
						if !cmdsEqual(returnInst.pbk.cmds, returnInst.abk.cmds) {
							panic("client value is wrong")
						}

						if cmdsEqual(returnInst.pbk.cmds, state.NOOP()) {
							panic("somehow returning noop")
						}

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
				returnInst.pbk = nil
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

func (r *Replica) acceptorCommit(instance int32, chosenAt stdpaxosproto.Ballot, cmds []state.Command) {
	inst := r.instanceSpace[instance]
	abk := inst.abk
	dlog.Printf("Committing (crtInstance=%d)\n", instance)

	if inst.pbk.status == CLOSED || inst.abk.status == COMMITTED {
		panic("shouldn't be commiting things multiple times")
	}

	inst.abk.status = COMMITTED
	knowsVal := abk.vBal.Equal(chosenAt)

	abk.vBal = chosenAt
	abk.cmds = cmds

	if !knowsVal {
		r.recordInstanceMetadata(inst)
		r.recordCommands(cmds)
	}
}

func (r *Replica) handleCommit(commit *stdpaxosproto.Commit) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	inst := r.instanceSpace[commit.Instance]

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed \n")
		return
	}

	r.acceptorCommit(commit.Instance, commit.Ballot, commit.Command)
	r.proposerCloseCommit(commit.Instance, commit.Ballot, commit.Command)
}

func (r *Replica) handleCommitShort(commit *stdpaxosproto.CommitShort) {
	r.checkAndHandleNewlyReceivedInstance(commit.Instance)
	inst := r.instanceSpace[commit.Instance]

	if inst.abk.status == COMMITTED {
		dlog.Printf("Already committed \n")
		return
	}

	r.acceptorCommit(commit.Instance, commit.Ballot, inst.abk.cmds)
	r.proposerCloseCommit(commit.Instance, commit.Ballot, inst.abk.cmds)
}