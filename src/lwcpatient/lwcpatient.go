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
	"state"
	"sync"
	"time"
)

type ProposerStatus int
type AcceptorStatus int

type ProposalTuples struct {
	cmd      []state.Command
	proposal []*genericsmr.Propose
}

type ProposalInfo struct {
	inst             int32
	proposingConfBal lwcproto.ConfigBal
}

type Replica struct {
	*genericsmr.Replica  // extends a generic Paxos replica
	configChan           chan fastrpc.Serializable
	prepareChan          chan fastrpc.Serializable
	acceptChan           chan fastrpc.Serializable
	commitChan           chan fastrpc.Serializable
	commitShortChan      chan fastrpc.Serializable
	prepareReplyChan     chan fastrpc.Serializable
	acceptReplyChan      chan fastrpc.Serializable
	prepareRPC           uint8
	acceptRPC            uint8
	commitRPC            uint8
	commitShortRPC       uint8
	prepareReplyRPC      uint8
	acceptReplyRPC       uint8
	instanceSpace        []*Instance // the space of all instances (used and not yet used)
	crtInstance          int32       // highest active instance number that this replica knows about
	crtConfig            int32
	Shutdown             bool
	counter              int
	flush                bool
	executedUpTo         int32
	batchWait            int
	maxBalInc            int32
	maxBeganInstances    int32
	crtOpenedInstances   []int32
	proposableInstances  chan ProposalInfo
	clientValueQueue     clientproposalqueue.ClientProposalQueue
	retryInstance        chan RetryInfo
	BackoffManager       BackoffManager
	alwaysNoop           bool
	lastTimeClientChosen time.Time
	lastOpenProposalTime time.Time
}

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
	maxKnownBal        lwcproto.Ballot
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

	log.Printf("Beginning backoff of %dus for instance %d on conf-bal %d.%d.%d", next, inst, attemptedConfBal.Config, attemptedConfBal.Number, attemptedConfBal.PropID)
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

func (bm *BackoffManager) StillRelevant(backoff RetryInfo) bool {
	curBackoff, exists := bm.currentBackoffs[backoff.InstToPrep]

	if !exists {
		return false
	} else {
		stillRelevant := backoff == curBackoff //should update so that inst also has bal backed off
		log.Println("Backoff of instance ", backoff.InstToPrep, "is relevant? ", stillRelevant)
		return stillRelevant
	}
}

func (bm *BackoffManager) ClearBackoff(inst int32) {
	delete(bm.currentBackoffs, inst)
}

func NewReplicaPatient(id int, peerAddrList []string, thrifty bool, exec bool, lread bool, dreply bool, durable bool, batchWait int, f int, crtConfig int32, storageLoc string, maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool, factor float64) *Replica {
	retryInstances := make(chan RetryInfo, maxOpenInstances*10000)
	r := &Replica{
		Replica:            genericsmr.NewReplica(id, peerAddrList, thrifty, exec, lread, dreply, f, storageLoc),
		configChan:         make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareChan:        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptChan:         make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitChan:         make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitShortChan:    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareReplyChan:   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptReplyChan:    make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		prepareRPC:         0,
		acceptRPC:          0,
		commitRPC:          0,
		commitShortRPC:     0,
		prepareReplyRPC:    0,
		acceptReplyRPC:     0,
		instanceSpace:      make([]*Instance, 15*1024*1024),
		crtInstance:        -1, //get from storage
		crtConfig:          crtConfig,
		Shutdown:           false,
		counter:            0,
		flush:              true,
		executedUpTo:       -1, //get from storage
		batchWait:          batchWait,
		maxBalInc:          100,
		maxBeganInstances:  maxOpenInstances,
		crtOpenedInstances: make([]int32, maxOpenInstances),
		clientValueQueue:   clientproposalqueue.ClientProposalQueueInit(),
		retryInstance:      retryInstances,
		BackoffManager:     NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, &retryInstances, factor),
		alwaysNoop:         alwaysNoop,
	}

	r.Durable = durable

	r.prepareRPC = r.RegisterRPC(new(lwcproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(lwcproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(lwcproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(lwcproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(lwcproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(lwcproto.AcceptReply), r.acceptReplyChan)

	//r.submitNewConfig()
	go r.runPatiently()

	return r
}

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

func (r *Replica) runPatiently() {
	r.ConnectToPeers()
	r.RandomisePeerOrder()

	fastClockChan = make(chan bool, 1)
	//Enabled fast clock when batching
	if r.BatchingEnabled() {
		go r.fastClock()
	}

	for i := 0; i < int(r.maxBeganInstances); i++ {
		log.Println("sending begin new")
		r.beginNew <- struct{}{}
		r.crtOpenedInstances[i] = -1
	}

	onOffProposeChan := r.ProposeChan
	//if r.crtConfig > 1 {
	// send to all
	//}
	go r.WaitForClientConnections()

	newPeerOrderTimer := time.NewTimer(1 * time.Second)

	for !r.Shutdown {

		select {
		case <-newPeerOrderTimer.C:
			r.RandomisePeerOrder()
			dlog.Println("Recomputing closest peers")
			newPeerOrderTimer.Reset(100 * time.Millisecond)
			break
		case propose := <-onOffProposeChan:
			//got a Propose from a clien
			for i := 0; i < int(r.maxBeganInstances); i++ {
				r.beginNextInstance()
			}
			r.handlePropose(propose)
			if r.BatchingEnabled() {
				onOffProposeChan = nil
			}
			break
		case next := <-r.retryInstance:
			r.checkAndRetry(next)
			break
		case <-fastClockChan:
			//activate new proposals channel
			onOffProposeChan = r.ProposeChan
			break
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
			// if new or equal backoff again
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

func (r *Replica) checkAndRetry(next RetryInfo) {
	inst := r.instanceSpace[next.InstToPrep]
	if !next.backedoff {
		if inst == nil {
			r.instanceSpace[next.InstToPrep] = r.makeEmptyInstance()
			inst = r.instanceSpace[next.InstToPrep]
		}
	}

	if (r.BackoffManager.StillRelevant(next) || !next.backedoff) && inst.pbk.status != BACKING_OFF && inst.pbk.status != NOT_BEGUN {
		r.proposerBeginNextConfBal(next.InstToPrep)
		nextConfBal := r.instanceSpace[next.InstToPrep].pbk.propCurConfBal
		r.acceptorPrepareOnConfBal(next.InstToPrep, nextConfBal)
		r.bcastPrepare(next.InstToPrep)
		log.Printf("Proposing next conf-bal %d.%d.%d to instance %d\n", nextConfBal.Config, nextConfBal.Number, nextConfBal.PropID, next.InstToPrep)
	} else {
		log.Printf("Skipping retry of instance %d due to preempted again or closed\n", next.InstToPrep)
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
			// is in qrm so if this way chosen then the acceptor should only ever have accepted chosen value
			//(greater tha conf-bal will propose same val)
			r.SendMsg(r.PreferredPeerOrder[q], r.commitShortRPC, argsShort)

		} else {
			r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, &pc)
		}
		sent++
	}
}

func (r *Replica) recordNewConfig(config int32) {
	if !r.Durable {
		return
	}

	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(config))
	r.StableStore.Write(b[:])
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

func (r *Replica) getNextProposingConfigBal(instance int32) lwcproto.ConfigBal {
	pbk := r.instanceSpace[instance].pbk
	min := ((pbk.maxKnownBal.Number/r.maxBalInc)+1)*r.maxBalInc + int32(r.N)
	max := min + r.maxBalInc
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

	//	if (instance % int32(r.N)) == r.Id {
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
			maxKnownBal:   lwcproto.Ballot{-1, -1},
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

func (r *Replica) checkAndHandleNewlyReceivedInstance(instance int32) {
	inst := r.instanceSpace[instance]
	if inst == nil {
		if instance > r.crtInstance {
			r.crtInstance = instance
		}
		r.instanceSpace[instance] = r.makeEmptyInstance()
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
	pbk.maxKnownBal = nextConfBal.Ballot

	pbk.proposalInfos[nextConfBal] = NewQuorumInfo(PROMISE)
}

func (r *Replica) beginNextInstance() {
	r.incToNextOpenInstance()

	//	foundFree := false
	for i := 0; i < len(r.crtOpenedInstances); i++ {
		// allow for an openable instance
		if r.crtOpenedInstances[i] == -1 {
			r.crtOpenedInstances[i] = r.crtInstance
			//foundFree = true
			break
		}
	}

	//if !foundFree {
	//	panic("Opened up too many instances or closed instances not considered closed")
	//}

	r.instanceSpace[r.crtInstance] = r.makeEmptyInstance()
	curInst := r.instanceSpace[r.crtInstance]
	r.proposerBeginNextConfBal(r.crtInstance)
	r.acceptorPrepareOnConfBal(r.crtInstance, curInst.pbk.propCurConfBal)
	r.bcastPrepare(r.crtInstance)
	log.Printf("Opened new instance %d\n", r.crtInstance)
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

func (r *Replica) proposerCheckAndHandlePreempt(inst int32, preemptingConfigBal lwcproto.ConfigBal, preemterPhase QuorumType) bool {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk

	maxKnownConfBal := lwcproto.ConfigBal{
		Config: r.crtConfig,
		Ballot: pbk.maxKnownBal,
	}

	if preemptingConfigBal.GreaterThan(maxKnownConfBal) {
		pbk.maxKnownBal = preemptingConfigBal.Ballot
	}

	if pbk.status != CLOSED && preemptingConfigBal.GreaterThan(maxKnownConfBal) {
		//	r.tryRequeueClientProposal(inst)
		if pbk.status != BACKING_OFF {
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

func (r *Replica) proposerCheckAndHandleAcceptedValue(inst int32, aid int32, accepted lwcproto.ConfigBal, val []state.Command) ProposerAccValHandler {
	if accepted.IsZeroVal() {
		return IGNORED
	}
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	newVal := false

	if accepted.GreaterThan(pbk.maxAcceptedConfBal) {
		newVal = true
		if pbk.clientProposals != nil && !cmdsEqual(pbk.cmds, val) {
			// requeue and open new
		}

		//		log.Println("Client proposal superceded, requeuing")
		//	for i := 0; i < len(pbk.clientProposals); i ++ {
		//		r.clientValueQueue.TryRequeue(pbk.clientProposals[i])
		//	}
		//}
		pbk.maxAcceptedConfBal = accepted
		pbk.cmds = val
	}

	qrm, exists := pbk.proposalInfos[accepted]
	if !exists {
		qrm = NewQuorumInfo(ACCEPTANCE)
	}
	qrm.quorumAdd(aid)
	// not assumed local acceptor has accepted it
	if int(qrm.quorumCount()) >= r.WriteQuorumSize() {
		r.acceptorCommit(inst, accepted, val)
		r.proposerCloseCommit(inst, accepted)
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

func (r *Replica) handlePrepareReplyPatiently(preply *lwcproto.PrepareReply) {
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
		r.acceptorCommit(preply.Instance, preply.ConfigBal, preply.Command)
		r.proposerCloseCommit(preply.Instance, preply.ConfigBal)
		r.bcastCommit(preply.Instance, preply.ConfigBal, preply.Command)
		return
	}

	if pbk.propCurConfBal.GreaterThan(preply.ConfigBal) || pbk.status != PREPARING {
		dlog.Printf("Message in late \n")
		return
	}

	if r.proposerCheckAndHandlePreempt(preply.Instance, preply.ConfigBal) {
		//pbk.nacks++
		//	if pbk.nacks+1 > r.N>>1 { // no qrm
		// we could send promise also through a prepare reply to the owner of the preempting confbal
		dlog.Printf("Another active proposer using config-ballot %d.%d.%d greater than mine\n", preply.ConfigBal)
		if preply.ConfigBal.Config > r.crtConfig {
			r.recordNewConfig(preply.ConfigBal.Config)
			r.sync()
		}
		return
	}

	qrm := pbk.proposalInfos[pbk.propCurConfBal]
	qrm.quorumAdd(preply.AcceptorId)
	if inst.abk.curBal.GreaterThan(pbk.propCurConfBal.Ballot) {
		panic("somehow acceptor has moved on but proposer hasn't")
	}
	if int(qrm.quorumCount()+1) >= r.Replica.ReadQuorumSize() {
		if pbk.status == CLOSED || inst.abk.status == COMMITTED {
			panic("oh nnooooooooo")
		}

		pbk.status = READY_TO_PROPOSE

		dlog.Println("Can now propose in instance", preply.Instance)
		qrm.quorumClear()
		qrm.qrmType = ACCEPTANCE

		if inst.pbk.maxAcceptedConfBal.IsZeroVal() {
			select {
			case cmds := <-r.valuesToPropose:
				pbk.cmds = cmds.cmd
				pbk.clientProposals = cmds.proposal
				break
			default:
				pbk.cmds = state.NOOP()
			}
		}
		pbk.status = PROPOSING
		// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
		r.acceptorAcceptOnConfBal(preply.Instance, preply.ConfigBal, inst.pbk.cmds)
		r.proposerCheckAndHandleAcceptedValue(preply.Instance, r.Id, preply.ConfigBal, inst.pbk.cmds)
		r.bcastAccept(preply.Instance)
	}
}

func (r *Replica) tryRequeueClientProposal(instance int32) {
	inst := r.instanceSpace[instance]
	if inst.pbk.clientProposals != nil {
		log.Println("queueng value 1")
		for i := 0; i < len(inst.pbk.clientProposals); i++ {
			r.clientValueQueue.TryRequeue(inst.pbk.clientProposals[i])
		}
		//inst.pbk.cmds = inst.abk.cmds
		//inst.pbk.clientProposals = nil
	}
}

func (r *Replica) proposerCloseCommit(inst int32, chosenAt lwcproto.ConfigBal) {
	instance := r.instanceSpace[inst]
	pbk := instance.pbk
	pbk.status = CLOSED

	r.BackoffManager.ClearBackoff(inst)
	if chosenAt.Ballot.GreaterThan(pbk.maxKnownBal) {
		pbk.maxKnownBal = chosenAt.Ballot
	}

	if chosenAt.GreaterThan(pbk.maxAcceptedConfBal) {
		pbk.maxAcceptedConfBal = chosenAt
	}

	if pbk.clientProposals != nil {
		if cmdsEqual(instance.abk.cmds, state.NOOP()) || len(pbk.clientProposals) != len(instance.abk.cmds) {
			r.tryRequeueClientProposal(inst)
			pbk.clientProposals = nil

		} else {
			notChosen := false
			for i := 0; i < len(pbk.clientProposals); i++ {
				if pbk.clientProposals[i].Command.String() != instance.abk.cmds[i].String() {
					r.clientValueQueue.TryRequeue(pbk.clientProposals[i])
					notChosen = true
				}
			}
			if notChosen {
				pbk.clientProposals = nil
			} else {
				zeroTime := time.Time{}
				if r.lastTimeClientChosen == zeroTime {
					r.lastTimeClientChosen = time.Now()
				} else {
					timeNow := time.Now()
					timeSinceLastChosen := timeNow.Sub(r.lastTimeClientChosen)
					if timeSinceLastChosen > time.Second {
						panic("we are being starved from chosing client values")
					} else {
						r.lastTimeClientChosen = timeNow
					}
				}
			}
		}
		//
		//
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

	//	if pbk.clientProposals != nil {
	//		for i := 0; i < len(pbk.clientProposals); i++ {
	//			r.clientValueQueue.
	//		}
	//	}

	if r.Exec {
		//	if r.crtInstance - r.executedUpTo > (int32(r.maxBeganInstances) * int32(r.N)){
		//	panic("too many gaps :O")
		//	}
		for i := r.executedUpTo + 1; i <= r.crtInstance; i++ {
			inst := r.instanceSpace[i]
			if inst != nil && inst.abk.status == COMMITTED { //&& inst.abk.cmds != nil {
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
					} else if inst.abk.cmds[j].Op == state.PUT {
						inst.abk.cmds[j].Execute(r.State)
					}

				}
				r.executedUpTo++
				dlog.Printf("Executed up to %d (crtInstance=%d)", r.executedUpTo, r.crtInstance)
				//r.instanceSpace[i] = nil
			}
		}
	}
	r.checkAndOpenNewInstance(inst)

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
	r.proposerCloseCommit(commit.Instance, commit.ConfigBal)

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
	r.proposerCloseCommit(commit.Instance, commit.ConfigBal)

	if inst.pbk.clientProposals != nil && !cmdsEqual(inst.pbk.cmds, inst.abk.cmds) {
		panic("WHHHAATT why these different?")
	}
}
