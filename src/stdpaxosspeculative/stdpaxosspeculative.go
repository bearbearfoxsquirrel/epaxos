package stdpaxosspeculative

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
	inst             int32
	proposingConfBal stdpaxosproto.Ballot
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
	initalProposalWait            time.Duration
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
	status   AcceptorStatus
	cmds     []state.Command
	curBal   stdpaxosproto.Ballot
	vConfBal stdpaxosproto.Ballot
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
	proposalInfos      map[stdpaxosproto.Ballot]*QuorumInfo
	maxAcceptedConfBal stdpaxosproto.Ballot // highest maxAcceptedConfBal at which a command was accepted
	cmds               []state.Command      // the accepted command
	propCurBal         stdpaxosproto.Ballot // highest this replica maxAcceptedConfBal tried so far
	clientProposals    []*genericsmr.Propose
	maxKnownBal        stdpaxosproto.Ballot
}

type RetryInfo struct {
	backedoff        bool
	InstToPrep       int32
	attemptedConfBal stdpaxosproto.Ballot
	preempterConfBal stdpaxosproto.Ballot
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
	} else if preempter.GreaterThan(curBackoffInfo.preempterConfBal) || (preempter.Equal(curBackoffInfo.preempterConfBal) && preempterPhase > curBackoffInfo.preempterAt) {
		return true
	} else {
		return false
	}
}
func (bm *BackoffManager) CheckAndHandleBackoff(inst int32, attemptedConfBal stdpaxosproto.Ballot, preempter stdpaxosproto.Ballot, prempterPhase QuorumType) {
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

	dlog.Printf("Beginning backoff of %dus for instance %d on conf-bal %d.%d", next, inst, attemptedConfBal.Number, attemptedConfBal.PropID)
	bm.currentBackoffs[inst] = RetryInfo{
		backedoff:        true,
		InstToPrep:       inst,
		attemptedConfBal: attemptedConfBal,
		preempterConfBal: preempter,
		preempterAt:      prempterPhase,
		backoffus:        next,
		timesPreempted:   preemptNum,
	}

	go func(instance int32, attempted stdpaxosproto.Ballot, preempterConfBal stdpaxosproto.Ballot, preempterP QuorumType, backoff int32, numTimesBackedOff int32) {
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

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, lread bool, dreply bool, durable bool, batchWait int, f int, crtConfig int32, storageLoc string, maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool, factor float64, whoCrash int32, whenCrash time.Duration, howlongCrash time.Duration, initalProposalWait time.Duration, emulatedSS bool, emulatedWriteTime time.Duration) *Replica {
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
		executedUpTo:        0, //get from storage
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
		initalProposalWait:  initalProposalWait,
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
				r.proposerBeginNextConfBal(i)
				nextConfBal := r.instanceSpace[i].pbk.propCurBal
				r.acceptorPrepareOnConfBal(i, nextConfBal)
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
	for i := 0; i < int(r.maxOpenInstances); i++ {
		r.crtOpenedInstances[i] = -1
		r.beginNextInstance()
	}

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
		case retry := <-r.proposableInstances:
			r.retryPropose(retry)
			break
			//	case <-fastClockChan:
			//activate new proposals channel
		//	onOffProposeChan = r.ProposeChan
		//		break
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
	}
}

func (r *Replica) retryPropose(proposalInfo ProposalInfo) {
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
	if pbk.propCurBal.Equal(proposalInfo.proposingConfBal) && pbk.status == READY_TO_PROPOSE {
		if pbk.maxAcceptedConfBal.IsZero() { // || cmdsEqual(pbk.cmds, state.NOOP()){
			switch cliProp := r.clientValueQueue.TryDequeue(); {
			case cliProp != nil:
				numEnqueued := r.clientValueQueue.Len() + 1
				batchSize := numEnqueued
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

				dlog.Printf("%d client value(s) proposed in instance %d \n", len(pbk.clientProposals), inst)
				break
			default:
				pbk.cmds = state.NOOP()
				log.Println("Proposing noop")
				break
			}
		} else {
			//	r.checkAndOpenNewInstance(proposalInfo.inst)
		}

		pbk.status = PROPOSING
		// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
		if r.fastLearn {
			r.acceptorAcceptOnConfBal(proposalInfo.inst, pbk.propCurBal, pbk.cmds)
			r.proposerCheckAndHandleAcceptedValue(proposalInfo.inst, r.Id, pbk.propCurBal, pbk.cmds)
			r.bcastAcceptToAlive(proposalInfo.inst)
		} else {
			r.bcastAcceptToAlive(proposalInfo.inst)
			r.acceptorAcceptOnConfBal(proposalInfo.inst, pbk.propCurBal, pbk.cmds)
			r.proposerCheckAndHandleAcceptedValue(proposalInfo.inst, r.Id, pbk.propCurBal, pbk.cmds)
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

	if (r.BackoffManager.NoHigherBackoff(next) && inst.pbk.status == BACKING_OFF) || !next.backedoff {
		r.proposerBeginNextConfBal(next.InstToPrep)
		nextConfBal := r.instanceSpace[next.InstToPrep].pbk.propCurBal
		if inst.abk.curBal.GreaterThan(inst.pbk.propCurBal) {
			panic("not moved to highest seen conf bal")
		}
		r.acceptorPrepareOnConfBal(next.InstToPrep, nextConfBal)
		r.bcastPrepareToAlive(next.InstToPrep)
		dlog.Printf("Proposing next conf-bal %d.%d to instance %d\n", nextConfBal.Number, nextConfBal.PropID, next.InstToPrep)
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
		// if !r.Alive[r.PreferredPeerOrder[q]] {
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

func (r *Replica) bcastCommitToAll(instance int32, confBal stdpaxosproto.Ballot, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = confBal
	pc.Command = command

	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = confBal
	pcs.Count = int32(len(command))

	argsShort := &pcs

	r.CalculateAlive()
	sent := 0
	for q := 0; q < r.N-1; q++ {
		//	if !r.Alive[r.PreferredPeerOrder[q]] {
		//		continue
		//	}

		_, inQrm := r.instanceSpace[instance].pbk.proposalInfos[confBal].aids[r.PreferredPeerOrder[q]]
		if inQrm {
			r.SendMsg(r.PreferredPeerOrder[q], r.commitShortRPC, argsShort)
		} else {
			r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, &pc)
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

	proposingConfBal := stdpaxosproto.Ballot{next - r.Id, int16(r.Id)}

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
			status:   NOT_STARTED,
			cmds:     nil,
			curBal:   stdpaxosproto.Ballot{-1, -1},
			vConfBal: stdpaxosproto.Ballot{-1, -1},
		},
		pbk: &ProposingBookkeeping{
			status:        NOT_BEGUN,
			proposalInfos: make(map[stdpaxosproto.Ballot]*QuorumInfo),

			maxKnownBal:        stdpaxosproto.Ballot{-1, -1},
			maxAcceptedConfBal: stdpaxosproto.Ballot{-1, -1},
			cmds:               nil,
			propCurBal:         stdpaxosproto.Ballot{-1, -1},
			clientProposals:    nil,
		},
	}
}

func (r *Replica) proposerBeginNextConfBal(inst int32) {
	pbk := r.instanceSpace[inst].pbk
	if pbk.status == CLOSED || r.instanceSpace[inst].abk.status == COMMITTED {
		panic("oh nnooooooooo")
	}
	pbk.status = PREPARING
	nextConfBal := r.getNextProposingBallot(inst)
	pbk.proposalInfos[nextConfBal] = new(QuorumInfo)

	pbk.propCurBal = nextConfBal
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
	// if r.crtInstance % int32(r.N) == r.Id {
	//		initBal := stdpaxosproto.Ballot{r.crtConfig, stdpaxosproto.Ballot{1, int16(r.Id)}}
	//		curInst.pbk.propCurBal = initBal
	//		curInst.pbk.proposalInfos[initBal] = NewQuorumInfo(PROMISE)
	//			r.acceptorPrepareOnConfBal(r.crtInstance, curInst.pbk.propCurBal)
	//		r.propose(r.crtInstance)
	//	} else {
	r.proposerBeginNextConfBal(r.crtInstance)
	r.acceptorPrepareOnConfBal(r.crtInstance, curInst.pbk.propCurBal)
	r.bcastPrepareToAlive(r.crtInstance)
	dlog.Printf("Opened new instance %d\n", r.crtInstance)
	//	}
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	dlog.Printf("Received new client value\n")
	r.clientValueQueue.TryEnqueue(propose)
	//check if any open instances
}

func (r *Replica) acceptorPrepareOnConfBal(inst int32, confBal stdpaxosproto.Ballot) {
	instance := r.instanceSpace[inst]
	abk := instance.abk
	if r.instanceSpace[inst].pbk.status == CLOSED || r.instanceSpace[inst].abk.status == COMMITTED {
		panic("oh nnooooooooo")
	}
	cur := abk.curBal

	if cur.GreaterThan(confBal) && !cur.Equal(confBal) {
		panic("preparing on out of date conf bal")
	}

	r.instanceSpace[inst].abk.status = PREPARED

	dlog.Printf("Acceptor Preparing Config-Ballot %d.%d ", confBal.Number, confBal.PropID)
	r.instanceSpace[inst].abk.curBal = confBal
}

func (r *Replica) acceptorAcceptOnConfBal(inst int32, confBal stdpaxosproto.Ballot, cmds []state.Command) {
	abk := r.instanceSpace[inst].abk
	if r.instanceSpace[inst].pbk.status == CLOSED || abk.status == COMMITTED {
		panic("oh nnooooooooo")
	}
	abk.status = ACCEPTED

	cur := abk.curBal

	if cur.GreaterThan(confBal) {
		panic("accepted outof date conf bal")
	}

	dlog.Printf("Acceptor Accepting Config-Ballot %d.%d ", confBal.Number, confBal.PropID)
	abk.curBal = confBal
	abk.vConfBal = confBal
	abk.cmds = cmds

	r.recordInstanceMetadata(r.instanceSpace[inst])
	r.recordCommands(cmds)
	r.sync()
}

func (pbk *ProposingBookkeeping) isOnInitialConfBal(myID int16) bool {
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

func (r *Replica) openedInstance(inst int32) bool {
	for openedInst := range r.crtOpenedInstances {
		if inst == int32(openedInst) {
			return true
		}
	}
	return false
}

func (r *Replica) proposerCheckAndHandlePreempt(inst int32, preemptingBallot stdpaxosproto.Ballot, preemterPhase QuorumType, shouldRequeue bool) bool {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if pbk.status != CLOSED && preemptingBallot.GreaterThan(pbk.propCurBal) {
		//if pbk.status != BACKING_OFF { // option for add multiple preempts if backing off already?
		r.BackoffManager.CheckAndHandleBackoff(inst, pbk.propCurBal, preemptingBallot, preemterPhase)
		//	}

		//if pbk.status == PREPARING {
		r.checkAndOpenNewInstance(inst)
		//}

		//	if pbk.clientProposals != nil {
		//		r.requeueClientProposals(inst)
		//		pbk.clientProposals = nil
		//	}

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
		//	if instance < r.crtInstance - (int32(r.N) * r.maxOpenInstances) {
		for i := instance; i <= r.crtInstance; i++ {
			returingInst := r.instanceSpace[i]
			if returingInst != nil {
				if returingInst.abk.status == COMMITTED {
					dlog.Printf("Already committed instance %d, returning commit \n", instance)
					var pc stdpaxosproto.Commit
					pc.LeaderId = int32(returingInst.abk.vConfBal.PropID) //prepare.LeaderId
					pc.Instance = i
					pc.Ballot = returingInst.abk.vConfBal
					pc.Command = returingInst.abk.cmds
					args := &pc
					//r.CalculateAlive()
					//r.SendMsg(pc.LeaderId, r.commitRPC, args)
					r.SendMsgNoFlush(whoRespondTo, r.commitRPC, args)
				}
			}
		}
		_ = r.PeerWriters[whoRespondTo].Flush()
		//	} else {
		//		pc.LeaderId = int32(inst.abk.vConfBal.PropID) //prepare.LeaderId
		//		pc.Instance = instance
		//		pc.Ballot = inst.abk.vConfBal
		//		pc.Command = inst.abk.cmds
		//		args := &pc
		//r.CalculateAlive()
		//r.SendMsg(pc.LeaderId, r.commitRPC, args)
		//		r.SendMsg(whoRespondTo, r.commitRPC, args)
		//	}
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
		dlog.Printf("Already prepared on higher Config-Ballot %d.%d < %d.%d", prepare.Number, prepare.PropID, minSafe.Number, minSafe.PropID)
	} else if prepare.Ballot.GreaterThan(minSafe) {
		dlog.Printf("Preparing on prepared on new Config-Ballot %d.%d", prepare.Number, prepare.PropID)
		r.acceptorPrepareOnConfBal(prepare.Instance, prepare.Ballot)
		r.proposerCheckAndHandlePreempt(prepare.Instance, prepare.Ballot, PROMISE, false)
		r.checkAndHandleOldPreempted(prepare.Ballot, minSafe, inst.abk.vConfBal, inst.abk.cmds, prepare.Instance)
		//	r.checkAndOpenNewInstance(prepare.Instance)
		if inst.pbk.propCurBal.GreaterThan(prepare.Ballot) {
			panic("why not proposer acknowledging or is greater than????")
		}

		if inst.pbk.status != BACKING_OFF {
			panic("why not backed off????")
		}
	} else {
		dlog.Printf("Config-Ballot %d.%d already joined, returning same promise", prepare.Number, prepare.PropID)
	}

	newBallot := inst.abk.curBal

	var preply = &stdpaxosproto.PrepareReply{
		Instance:   prepare.Instance,
		Bal:        newBallot,
		VBal:       inst.abk.vConfBal,
		AcceptorId: r.Id,
		Command:    inst.abk.cmds,
	}

	r.replyPrepare(prepare.LeaderId, preply)
}

func (r *Replica) checkAndHandleOldPreempted(new stdpaxosproto.Ballot, old stdpaxosproto.Ballot, accepted stdpaxosproto.Ballot, acceptedVal []state.Command, inst int32) {
	if new.PropID != old.PropID && int32(new.PropID) != r.Id && old.PropID != -1 && new.GreaterThan(old) {
		preemptOldPropMsg := &stdpaxosproto.PrepareReply{
			Instance:   inst,
			Bal:        new,
			VBal:       accepted,
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

func (r *Replica) proposerCheckAndHandleAcceptedValue(inst int32, aid int32, accepted stdpaxosproto.Ballot, val []state.Command) ProposerAccValHandler {
	if accepted.IsZero() {
		return IGNORED
	}
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	if pbk.status == CLOSED {
		return CHOSEN
	}
	if accepted.GreaterThan(pbk.maxKnownBal) {
		pbk.maxKnownBal = accepted
	}

	newVal := false
	if accepted.GreaterThan(pbk.maxAcceptedConfBal) {
		newVal = true
		pbk.maxAcceptedConfBal = accepted
		pbk.cmds = val

		if int32(accepted.PropID) != r.Id {
			r.checkAndOpenNewInstance(inst)
		}
	}

	_, exists := pbk.proposalInfos[accepted]
	if !exists {
		pbk.proposalInfos[accepted] = NewQuorumInfo(ACCEPTANCE)
	}
	pbk.proposalInfos[accepted].quorumAdd(aid)
	// not assumed local acceptor has accepted it
	if int(pbk.proposalInfos[accepted].quorumCount()) >= r.WriteQuorumSize() {
		r.bcastCommitToAll(inst, accepted, val)
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
	if r.fastLearn {
		valWhatDone = r.proposerCheckAndHandleAcceptedValue(preply.Instance, int32(preply.VBal.PropID), preply.VBal, preply.Command)
	}
	if valWhatDone == NEW_VAL {
		dlog.Printf("Promise from %d in instance %d has new value at Config-Ballot %d.%d", preply.AcceptorId,
			preply.Instance, preply.Bal.Number, preply.Bal.PropID)
	}
	// early learning
	if valWhatDone == CHOSEN {
		dlog.Printf("Preparing instance recognised as chosen (instance %d), returning commit \n", preply.Instance)
		if preply.Bal.IsZero() {
			panic("Why we commit zero ballot???")
		}
		return
	}

	minSafe := inst.abk.curBal
	r.checkAndHandleOldPreempted(preply.Bal, minSafe, inst.abk.vConfBal, inst.abk.cmds, preply.Instance)

	if pbk.propCurBal.GreaterThan(preply.Bal) || pbk.status != PREPARING {
		dlog.Printf("Message in late \n")
		return
	}

	if r.proposerCheckAndHandlePreempt(preply.Instance, preply.Bal, PROMISE, false) {
		dlog.Printf("Another active proposer using config-ballot %d.%d greater than mine\n", preply.Bal)
		// todo should keep???
		r.acceptorPrepareOnConfBal(preply.Instance, preply.Bal)
		myReply := stdpaxosproto.PrepareReply{
			Instance:   preply.Instance,
			Bal:        preply.Bal,
			VBal:       inst.abk.vConfBal,
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

	if pbk.maxAcceptedConfBal.IsZero() {
		if r.initalProposalWait > 0 {
			time.Sleep(r.initalProposalWait)
		}

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
			dlog.Printf("%d client value(s) proposed in instance %d \n", len(pbk.clientProposals), inst)
			break
		default:
			if r.noopWaitUs > 0 {
				go func(inst int32, confBal stdpaxosproto.Ballot) {
					timer := time.NewTimer(time.Duration(r.noopWaitUs) * time.Microsecond)
					<-timer.C
					r.proposableInstances <- ProposalInfo{
						inst:             inst,
						proposingConfBal: confBal,
					}
				}(inst, pbk.propCurBal)
				return
			} else {
				//if r.shouldProposeNoop(inst) {
				pbk.cmds = state.NOOP()
				log.Println("Proposing noop")
				//} else {
				//	return
				//}
			}
		}
	} else {

		//r.checkAndOpenNewInstance(inst)
	}
	pbk.status = PROPOSING
	// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn

	if r.fastLearn {
		r.acceptorAcceptOnConfBal(inst, pbk.propCurBal, pbk.cmds)
		r.proposerCheckAndHandleAcceptedValue(inst, r.Id, pbk.propCurBal, pbk.cmds)
		r.bcastAcceptToAlive(inst)
	} else {
		r.bcastAcceptToAlive(inst)
		r.acceptorAcceptOnConfBal(inst, pbk.propCurBal, pbk.cmds)
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
	minAcceptableConfBal := inst.abk.curBal

	// should always be unless there is a restart
	if accept.Ballot.GreaterThan(minAcceptableConfBal) || accept.Ballot.Equal(minAcceptableConfBal) {
		dlog.Printf("Accepted instance %d on conf-ball %d.%d", accept.Instance, accept.Number, accept.PropID)
		r.acceptorAcceptOnConfBal(accept.Instance, accept.Ballot, accept.Command)

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
		r.checkAndHandleOldPreempted(accept.Ballot, minAcceptableConfBal, inst.abk.vConfBal, inst.abk.cmds, accept.Instance)

	} else if minAcceptableConfBal.GreaterThan(accept.Ballot) {
		dlog.Printf("Returning preempt for config-ballot %d.%d < %d.%d in Instance %d\n", accept.Number, accept.PropID, minAcceptableConfBal.Number, minAcceptableConfBal.PropID, accept.Instance)
	} else {
		dlog.Printf("Already acknowledged accept request but will return again", accept.Number, accept.PropID, minAcceptableConfBal.Number, minAcceptableConfBal.PropID, accept.Instance)
	}

	replyConfBal := inst.abk.curBal
	areply := &stdpaxosproto.AcceptReply{accept.Instance, r.Id, replyConfBal, accept.Ballot}
	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) checkAndOpenNewInstance(inst int32) {
	// was instance opened by us
	//	if r.executedUpTo+r.maxOpenInstances*int32(r.N) > r.crtInstance {
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
		dlog.Printf("Acceptance of instance %d at %d.%d by Acceptor %d received\n", areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.AcceptorId)
		// pbk.cmds should be correct value - proposer only gets to this
		// stage if they learn of any previously chosen value and propose it again themselves
		r.proposerCheckAndHandleAcceptedValue(areply.Instance, areply.AcceptorId, areply.Cur, pbk.cmds)
		// we can count proposer of value too because they durably accept before sending accept request
		if r.fastLearn {
			r.proposerCheckAndHandleAcceptedValue(areply.Instance, int32(areply.Req.PropID), areply.Cur, pbk.cmds)
		}

	} else if preempted {
		r.proposerCheckAndHandlePreempt(areply.Instance, areply.Cur, ACCEPTANCE, false)
		minSafe := inst.abk.curBal
		r.checkAndHandleOldPreempted(areply.Cur, minSafe, inst.abk.vConfBal, inst.abk.cmds, areply.Instance)
	} else {
		msg := fmt.Sprintf("Somehow cur Conf-Bal of %d is %d.%d when we requested %d.%d for acceptance",
			areply.AcceptorId, areply.Cur.Number, areply.Cur.PropID,
			areply.Req.Number, areply.Req.PropID)
		panic(msg)
	}
}

func (r *Replica) requeueClientProposals(instance int32) {
	inst := r.instanceSpace[instance]
	dlog.Println("Requeing client value")
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
	attempts := pbk.maxAcceptedConfBal.Number / r.maxBalInc
	log.Printf("Attempts to chose instance %d: %d", inst, attempts)
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

	pbk.maxAcceptedConfBal = chosenAt
	pbk.cmds = chosenVal

	switch pbk.whatHappenedToClientProposals() {
	case NotProposed:
		break
	case ProposedButNotChosen:
		r.requeueClientProposals(inst)
		dlog.Printf("%d client value(s) proposed in instance %d\n not chosen", len(pbk.clientProposals), inst)
		pbk.clientProposals = nil
		break
	case ProposedAndChosen:
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
	r.howManyAttemptsToChoose(inst)
	if r.Exec {
		//	oldExecutedUpTo := r.executedUpTo

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
									attemptedConfBal: r.instanceSpace[k].pbk.propCurBal,
									preempterConfBal: stdpaxosproto.Ballot{-1, stdpaxosproto.Ballot{-1, -1}},
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
				//			if r.executedUpTo > oldExecutedUpTo {
				//				r.recordExecutedUpTo()
				//			}
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
	knowsVal := abk.vConfBal.Equal(chosenAt)
	shouldSync := false

	abk.curBal = chosenAt
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
