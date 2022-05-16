package twophase

import (
	"dlog"
	"instanceagentmapper"
	"stats"
	"stdpaxosproto"
	"time"
)

type InstanceInitiator interface {
	startNextInstance(instanceSpace *[]*ProposingBookkeeping) int32
}

type ProposalInitiator interface {
	startNextProposal(initiator *ProposingBookkeeping, inst int32)
}

type RetryDecider interface {
	DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool
}

type ProposalManager interface {
	LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool // returns if proposer's ballot is preempted
	LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot)
}

type CrtInstanceOracle interface {
	GetCrtInstance() int32
}

type InstanceManager interface {
	CrtInstanceOracle
	InstanceInitiator
	ProposalInitiator
	RetryDecider
	ProposalManager
}

type SimplePropsalManager struct {
	crtInstance int32
	BackoffManager
	id      int32
	doStats bool
	*stats.TimeseriesStats
	*stats.ProposalStats
	*stats.InstanceStats
	Balloter
	ProposerQuorumaliser
}

func SimplePropsalManagerNew(id int32, n int32, quoralP ProposerQuorumaliser,
	minBackoff int32, maxInitBackoff int32, maxBackoff int32,
	retries chan RetryInfo, factor float64, softFac bool, constBackoff bool, timeBasedBallots bool,
	doStats bool, tsStats *stats.TimeseriesStats, pStats *stats.ProposalStats, iStats *stats.InstanceStats) InstanceManager {
	return &SimplePropsalManager{
		crtInstance:          -1,
		BackoffManager:       NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, retries, factor, softFac, constBackoff),
		id:                   id,
		doStats:              doStats,
		TimeseriesStats:      tsStats,
		ProposalStats:        pStats,
		InstanceStats:        iStats,
		Balloter:             Balloter{id, n, 10000, time.Time{}, timeBasedBallots},
		ProposerQuorumaliser: quoralP,
	}
}

func (manager *SimplePropsalManager) GetCrtInstance() int32 { return manager.crtInstance }

func (manager *SimplePropsalManager) startNextInstance(instanceSpace *[]*ProposingBookkeeping) int32 {
	manager.crtInstance++
	pbk := getEmptyInstance()
	(*instanceSpace)[manager.crtInstance] = pbk
	return manager.crtInstance
}

func (manager *SimplePropsalManager) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	if pbk.status != BACKING_OFF {
		dlog.AgentPrintfN(manager.id, "Skipping retry of instance %d due to it being closed", retry.inst)
		return false
	}
	if !manager.BackoffManager.StillRelevant(retry) {
		dlog.AgentPrintfN(manager.id, "Skipping retry of instance %d due to being preempted again", retry.inst)
		return false
	}

	return true
}

func beginProposalForPreparing(pbk *ProposingBookkeeping, balloter Balloter) {
	pbk.status = PREPARING
	nextBal := balloter.getNextProposingBal(pbk.maxKnownBal.Number)
	pbk.propCurBal = nextBal
	pbk.maxKnownBal = nextBal
}

func (manager *SimplePropsalManager) startNextProposal(pbk *ProposingBookkeeping, inst int32) {
	beginProposalForPreparing(pbk, manager.Balloter)
	dlog.AgentPrintfN(manager.id, "Starting new proposal for instance %d with ballot %d.%d", inst, pbk.propCurBal.Number, pbk.propCurBal.PropID)
	manager.ProposerQuorumaliser.startPromiseQuorumOnCurBal(pbk, inst)
}

func (manager *SimplePropsalManager) HandleNewInstance(instsanceSpace *[]*ProposingBookkeeping, inst int32) {
	for i := manager.crtInstance + 1; i <= inst; i++ {
		(*instsanceSpace)[i] = getEmptyInstance()
		if i != inst {
			(*instsanceSpace)[i].status = BACKING_OFF
			_, bot := manager.CheckAndHandleBackoff(inst, stdpaxosproto.Ballot{-1, -1}, stdpaxosproto.Ballot{-1, -1}, stdpaxosproto.PROMISE)
			dlog.AgentPrintfN(manager.id, "Backing off newly received instance %d for %d microseconds", inst, bot)
		}
	}
	manager.crtInstance = inst
}

func (manager *SimplePropsalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
	if inst > manager.crtInstance {
		manager.HandleNewInstance(instanceSpace, inst)
	}
	return manager.handleProposalInInstance((*instanceSpace)[inst], inst, ballot, phase)
}

func (manager *SimplePropsalManager) handleProposalInInstance(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
	if pbk.status == CLOSED {
		return false
	}
	if pbk.maxKnownBal.Equal(ballot) || pbk.maxKnownBal.GreaterThan(ballot) {
		return false
	}

	pbk.status = BACKING_OFF
	dlog.AgentPrintfN(manager.id, "Witnessed new maximum ballot %d.%d for instance %d", ballot.Number, ballot.PropID, inst)
	pbk.maxKnownBal = ballot
	// NOTE: HERE WE WANT TO INCREASE BACKOFF EACH TIME THERE IS A NEW PROPOSAL SEEN
	backedOff, botime := manager.BackoffManager.CheckAndHandleBackoff(inst, pbk.propCurBal, ballot, phase)
	if backedOff {
		dlog.AgentPrintfN(manager.id, "Backing off instance %d for %d microseconds because our current ballot %d.%d is preempted by ballot %d.%d",
			inst, botime, pbk.propCurBal.Number, pbk.propCurBal.PropID, ballot.Number, ballot.PropID)
	}
	return true
}

func (manager *SimplePropsalManager) howManyAttemptsToChoose(pbk *ProposingBookkeeping, inst int32) {
	attempts := manager.Balloter.GetAttemptNumber(pbk.proposeValueBal.Number) // (pbk.proposeValueBal.Number / manager.maxBalInc)
	dlog.AgentPrintfN(manager.id, "Instance %d took %d attempts to be chosen", inst, attempts)
}

func (manager *SimplePropsalManager) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, at stdpaxosproto.Ballot) {
	if inst > manager.crtInstance {
		manager.HandleNewInstance(instanceSpace, inst)
	}
	manager.handleBallotChosen((*instanceSpace)[inst], inst, at)
}

func (manager *SimplePropsalManager) handleBallotChosen(pbk *ProposingBookkeeping, inst int32, at stdpaxosproto.Ballot) {
	if pbk.status == CLOSED {
		return
	}
	pbk.status = CLOSED
	dlog.Printf("Instance %d chosen now\n", inst)
	if manager.doStats && pbk.status != BACKING_OFF && !pbk.propCurBal.IsZero() {
		if pbk.propCurBal.GreaterThan(at) {
			manager.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.propCurBal, stats.LOWERPROPOSALCHOSEN)
		} else if at.GreaterThan(pbk.propCurBal) {
			manager.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.propCurBal, stats.HIGHERPROPOSALONGOING)
		} else {
			manager.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.propCurBal, stats.ITWASCHOSEN)
		}
	}
	manager.BackoffManager.ClearBackoff(inst)

	if at.Equal(pbk.propCurBal) && manager.doStats {
		manager.InstanceStats.RecordComplexStatEnd(stats.InstanceID{0, inst}, "Phase 2", "Success")
	}

	pbk.proposeValueBal = at
	if at.GreaterThan(pbk.maxKnownBal) {
		pbk.maxKnownBal = at
	}
	dlog.AgentPrintfN(manager.id, "Instance %d learnt to be chosen at ballot %d.%d", inst, at.Number, at.PropID)
	manager.howManyAttemptsToChoose(pbk, inst)

	if manager.doStats {
		atmts := manager.Balloter.GetAttemptNumber(at.Number) // (pbk.proposeValueBal.Number / manager.maxBalInc)
		manager.InstanceStats.RecordCommitted(stats.InstanceID{0, inst}, atmts, time.Now())
		manager.TimeseriesStats.Update("Instances Learnt", 1)
		if int32(at.PropID) == manager.id {
			manager.TimeseriesStats.Update("Instances I Choose", 1)
			manager.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "I Chose", 1)
		}
	}
}

// Minimal Proposer Proposal Manager
// When F+1 proposals are received greater than ours we stop attempting proposals to the instance.
type MinProposerProposalManager struct {
	*SimplePropsalManager
	minimalProposersShouldMaker
}

func MinProposerProposalManagerNew(f int, id int32, n int32, quoralP ProposerQuorumaliser,
	minBackoff int32, maxInitBackoff int32, maxBackoff int32,
	retries chan RetryInfo, factor float64, softFac bool, constBackoff bool, timeBasedBallots bool,
	doStats bool, tsStats *stats.TimeseriesStats, pStats *stats.ProposalStats, iStats *stats.InstanceStats) InstanceManager {
	return &MinProposerProposalManager{
		SimplePropsalManager: SimplePropsalManagerNew(id, n, quoralP, minBackoff, maxInitBackoff, maxBackoff,
			retries, factor, softFac, constBackoff, timeBasedBallots, doStats, tsStats, pStats, iStats).(*SimplePropsalManager),
		minimalProposersShouldMaker: minimalProposersShouldMaker{
			myBallots:               make(map[int32]stdpaxosproto.Ballot),
			ongoingGreaterProposals: make(map[int32]map[int16]stdpaxosproto.Ballot),
			instancesToForgo:        make(map[int32]struct{}),
			f:                       f,
		},
	}
}

func (decider *MinProposerProposalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
	if int32(ballot.PropID) != decider.PropID && !ballot.IsZero() {
		decider.minimalProposersShouldMaker.ballotReceived(inst, ballot)
		if decider.minimalProposersShouldMaker.shouldSkipInstance(inst) {
			dlog.AgentPrintfN(decider.id, "Stopping making proposals to instance %d because we have witnessed f+1 higher proposals", inst)
			//(*instanceSpace)[inst].status = BACKING_OFF
			//(*instanceSpace)[inst].propCurBal = stdpaxosproto.Ballot{-1, -1}
			//.status = BACKING_OFF
			//decider.BackoffManager.ClearBackoff(inst)
			//decider.BackoffManager.CheckAndHandleBackoff(retry.inst, retry.attemptedBal, retry.preempterBal, retry.preempterAt)
			//return true
		}
	}

	return decider.SimplePropsalManager.LearnOfBallot(instanceSpace, inst, ballot, phase)
}

func (decider *MinProposerProposalManager) startNextProposal(pbk *ProposingBookkeeping, inst int32) {
	if decider.shouldSkipInstance(inst) {
		panic("should not start next proposal on instance fulfilled")
	}
	decider.SimplePropsalManager.startNextProposal(pbk, inst)
	dlog.AgentPrintfN(decider.id, "Starting new proposal for instance %d with ballot %d.%d", inst, pbk.propCurBal.Number, pbk.propCurBal.PropID)
	decider.minimalProposersShouldMaker.startedMyProposal(inst, pbk.propCurBal)
}

func (decider *MinProposerProposalManager) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	shouldSkip := decider.minimalProposersShouldMaker.shouldSkipInstance(retry.inst)
	if shouldSkip {
		if pbk.status == CLOSED {
			dlog.AgentPrintfN(decider.id, "Skipping retry of instance %d due to it being closed", retry.inst)
			return false
		}
		dlog.AgentPrintfN(decider.id, "Skipping retry of instance %d as minimal proposer threshold (f+1) met", retry.inst)
		pbk.status = BACKING_OFF
		decider.BackoffManager.ClearBackoff(retry.inst)
		return false
	}
	return decider.SimplePropsalManager.DecideRetry(pbk, retry)
}

// todo figure our if timeout should be here???
/////////////////////////////////////
// MAPPED
/////////////////////////////////////
type MappedProposerInstanceDecider struct {
	*SimplePropsalManager
	instanceagentmapper.InstanceAgentMapper
}

func MappedProposersProposalManagerNew(id int32, n int32, quoralP ProposerQuorumaliser,
	minBackoff int32, maxInitBackoff int32, maxBackoff int32,
	retries chan RetryInfo, factor float64, softFac bool, constBackoff bool, timeBasedBallots bool,
	doStats bool, tsStats *stats.TimeseriesStats, pStats *stats.ProposalStats, iStats *stats.InstanceStats, agents []int, g int) InstanceManager {

	//todo add dynamic on and off - when not detecting large numbers of proposals turn off F+1 or increase g+1
	simps := &SimplePropsalManager{
		crtInstance:          -1,
		BackoffManager:       NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, retries, factor, softFac, constBackoff),
		id:                   id,
		doStats:              doStats,
		TimeseriesStats:      tsStats,
		ProposalStats:        pStats,
		InstanceStats:        iStats,
		Balloter:             Balloter{id, n, 10000, time.Time{}, timeBasedBallots},
		ProposerQuorumaliser: quoralP,
	}

	return &MappedProposerInstanceDecider{
		SimplePropsalManager: simps,
		InstanceAgentMapper: &instanceagentmapper.InstanceSetMapper{
			Ids: agents,
			G:   g,
			N:   int(n),
		},
	}
}

func (manager *MappedProposerInstanceDecider) startNextInstance(instanceSpace *[]*ProposingBookkeeping) int32 {
	for gotInstance := false; !gotInstance; {
		manager.crtInstance++
		if (*instanceSpace)[manager.crtInstance] == nil {
			(*instanceSpace)[manager.crtInstance] = getEmptyInstance()
		}
		if (*instanceSpace)[manager.crtInstance].status != NOT_BEGUN {
			continue
		}

		mapped := manager.InstanceAgentMapper.GetGroup(int(manager.crtInstance))
		dlog.AgentPrintfN(manager.id, "Proposer group for instance %d is %v", manager.crtInstance, mapped)
		inG := inGroup(mapped, int(manager.id))
		if !inG {
			dlog.AgentPrintfN(manager.id, "Initially backing off instance %d as we are not mapped to it", manager.crtInstance)
			(*instanceSpace)[manager.crtInstance].status = BACKING_OFF
			manager.BackoffManager.CheckAndHandleBackoff(manager.crtInstance, stdpaxosproto.Ballot{-1, -1}, stdpaxosproto.Ballot{-1, -1}, stdpaxosproto.PROMISE)
			continue
		}
		gotInstance = true
		dlog.AgentPrintfN(manager.id, "Starting instance %d as we are mapped to it", manager.crtInstance)
	}
	return manager.crtInstance
}

func (manager *MappedProposerInstanceDecider) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
	manager.checkAndSetNewInstance(instanceSpace, inst, ballot, phase)
	return manager.SimplePropsalManager.handleProposalInInstance((*instanceSpace)[inst], inst, ballot, phase)
}

func (manager *MappedProposerInstanceDecider) checkAndSetNewInstance(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) {
	if (*instanceSpace)[inst] == nil {
		(*instanceSpace)[inst] = getEmptyInstance()
		dlog.AgentPrintfN(manager.id, "Witnessed new instance %d, we have set instance %d now to be our current instance", inst, manager.crtInstance)
	}
}

func (manager *MappedProposerInstanceDecider) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot) {
	if (*instanceSpace)[inst] == nil {
		(*instanceSpace)[inst] = getEmptyInstance()
	}
	manager.SimplePropsalManager.handleBallotChosen((*instanceSpace)[inst], inst, ballot)
}

func inGroup(mapped []int, id int) bool {
	inG := false
	for _, v := range mapped {
		if v == id {
			inG = true
			break
		}
	}
	return inG
}

type DynamicMappedProposalManager struct {
	MappedProposerInstanceDecider
	ids          []int
	n            int
	f            int
	curG         int
	conflictEWMA float64
	ewmaWeight   float64

	ourEWMALat         float64
	othersEWMAlat      float64
	instsObservedConfs map[int32]struct{}
	cooldownT          time.Duration
	curCooldown        *time.Timer
	toStart            chan int32

	//chooseEWMA map[int16]float64
}

func DynamicMappedProposerManagerNew(id int32, n int32, quoralP ProposerQuorumaliser,
	minBackoff int32, maxInitBackoff int32, maxBackoff int32,
	retries chan RetryInfo, factor float64, softFac bool, constBackoff bool, timeBasedBallots bool,
	doStats bool, tsStats *stats.TimeseriesStats, pStats *stats.ProposalStats, iStats *stats.InstanceStats, agents []int, f int) InstanceManager {
	mappedDecider := MappedProposersProposalManagerNew(id, n, quoralP, minBackoff, maxInitBackoff, maxBackoff, retries, factor, softFac, constBackoff, timeBasedBallots, doStats, tsStats, pStats, iStats, agents, int(n))

	dMappedDecicider := &DynamicMappedProposalManager{
		MappedProposerInstanceDecider: *mappedDecider.(*MappedProposerInstanceDecider),
		ids:                           agents,
		n:                             int(n),
		f:                             f,
		curG:                          int(n),
		conflictEWMA:                  float64(0),
		ewmaWeight:                    0.2,
		instsObservedConfs:            make(map[int32]struct{}),
		cooldownT:                     time.Duration(100) * time.Millisecond,
		toStart:                       make(chan int32, 2000),
	}
	return dMappedDecicider
}

func mapper(i, iS, iE float64, oS, oE int32) int32 {
	slope := 1.0 * float64(oE-oS) / (iE - iS)
	o := oS + int32((slope*(i-iS))+0.5)
	return o
}

func (decider *DynamicMappedProposalManager) startNextInstance(instanceSpace *[]*ProposingBookkeeping) int32 {
	decider.updateGroupSize()
	return decider.MappedProposerInstanceDecider.startNextInstance(instanceSpace)
}

func (decider *DynamicMappedProposalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
	decider.MappedProposerInstanceDecider.checkAndSetNewInstance(instanceSpace, inst, ballot, phase)

	pbk := (*instanceSpace)[inst]
	if ballot.GreaterThan(pbk.propCurBal) && !pbk.propCurBal.IsZero() {
		old := decider.conflictEWMA
		decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, +1)
		dlog.AgentPrintfN(decider.id, "Conflict encountered, increasing EWMA from %f to %f", old, decider.conflictEWMA)
	}

	return decider.MappedProposerInstanceDecider.LearnOfBallot(instanceSpace, inst, ballot, phase)
}

func (decider *DynamicMappedProposalManager) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	doRetry := decider.SimplePropsalManager.DecideRetry(pbk, retry)
	if !doRetry {
		return doRetry
	}
	old := decider.conflictEWMA
	decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, -1)
	dlog.AgentPrintfN(decider.id, "Retry needed on instance %d- either because failures are occurring or there is not enough system load, decreasing EWMA from %f to %f", retry.inst, old, decider.conflictEWMA)
	return doRetry
}

func (decider *DynamicMappedProposalManager) updateGroupSize() {
	newG := decider.curG
	if decider.conflictEWMA > 0.5 {
		newG = int(mapper(decider.conflictEWMA, 0.5, 1, int32(decider.curG-1), 1))
		dlog.AgentPrintfN(decider.id, "Current proposer group is of size %d (EWMA is %f)", decider.curG, decider.conflictEWMA)
		dlog.AgentPrintfN(decider.id, "Decreasing proposer group size")
	}
	if decider.conflictEWMA < -0.5 {
		dlog.AgentPrintfN(decider.id, "Increasing proposer group size")
		dlog.AgentPrintfN(decider.id, "Current proposer group is of size %d (EWMA is %f)", decider.curG, decider.conflictEWMA)
		newG = int(mapper(decider.conflictEWMA, -1, -0.5, decider.N, int32(decider.curG+1)))
	}

	if newG < 1 {
		newG = 1
	}

	if newG > decider.n {
		newG = decider.n
	}

	if newG != decider.curG {
		decider.curG = newG
		decider.InstanceAgentMapper = &instanceagentmapper.InstanceSetMapper{
			Ids: decider.ids,
			G:   decider.curG,
			N:   decider.n,
		}
	}
}

func movingPointAvg(a, ob float64) float64 {
	a -= a / 1000
	a += ob / 1000
	return a
}
func ewmaAdd(ewma float64, weight float64, ob float64) float64 {
	return (1-weight)*ewma + weight*ob
}

func (decider *DynamicMappedProposalManager) LearnNoop(inst int32, who int32) {
	if who != decider.id {
		return
	}
	old := decider.conflictEWMA
	decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, -1)
	dlog.AgentPrintfN(decider.id, "Learnt NOOP, decreasing EWMA from %f to %f", old, decider.conflictEWMA)
}

type NoopLearner interface {
	LearnNoop(inst int32, who int32)
}

//
//type Pipeliner struct {
//
//}
//
//
//func LearnNoop() {
//
//}
