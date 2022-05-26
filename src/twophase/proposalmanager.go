package twophase

import (
	"dlog"
	"instanceagentmapper"
	"sort"
	"stats"
	"stdpaxosproto"
	"time"
)

type InstanceInitiator interface {
	startNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32
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

type ProposerGroupGetter interface {
	GetGroup(inst int32) []int // change to map of int32[]struct?
}

type NoopLearner interface {
	LearnNoop(inst int32, who int32)
}

type InstanceProposalHandler interface {
}

type NewInstanceSignalling interface {
}

type NewInstanceHandler interface {
}

type InstanceManager interface {
	ProposerGroupGetter
	CrtInstanceOracle
	InstanceInitiator
	ProposalInitiator
	RetryDecider
	ProposalManager
}

type SimpleProposalManager struct {
	crtInstance int32
	BackoffManager
	id      int32
	doStats bool
	*stats.TimeseriesStats
	*stats.ProposalStats
	*stats.InstanceStats
	Balloter
	ProposerQuorumaliser
	timeSinceLastStarted time.Time
	instsStarted         map[int32]struct{}
	sigNewInst           chan struct{}
}

func SimpleProposalManagerNew(id int32, n int32, quoralP ProposerQuorumaliser,
	minBackoff int32, maxInitBackoff int32, maxBackoff int32,
	retries chan RetryInfo, factor float64, softFac bool, constBackoff bool, timeBasedBallots bool,
	doStats bool, tsStats *stats.TimeseriesStats, pStats *stats.ProposalStats, iStats *stats.InstanceStats, newInstSig chan struct{}) *SimpleProposalManager {
	return &SimpleProposalManager{
		crtInstance:          -1,
		BackoffManager:       NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, retries, factor, softFac, constBackoff),
		id:                   id,
		doStats:              doStats,
		TimeseriesStats:      tsStats,
		ProposalStats:        pStats,
		InstanceStats:        iStats,
		Balloter:             Balloter{id, n, 10000, time.Time{}, timeBasedBallots},
		ProposerQuorumaliser: quoralP,
		sigNewInst:           newInstSig,
		instsStarted:         make(map[int32]struct{}),
	}
}

func (manager *SimpleProposalManager) GetGroup(inst int32) []int {
	g := make([]int, manager.N)
	for i := 0; i < int(manager.N); i++ {
		g[i] = i
	}
	return g
}

func (manager *SimpleProposalManager) GetCrtInstance() int32 { return manager.crtInstance }

func (manager *SimpleProposalManager) startNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32 {
	manager.crtInstance++
	pbk := getEmptyInstance()
	(*instanceSpace)[manager.crtInstance] = pbk
	startFunc(manager.crtInstance)
	manager.instsStarted[manager.crtInstance] = struct{}{}
	return []int32{manager.crtInstance}
}

func (manager *SimpleProposalManager) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	if pbk.status != BACKING_OFF {
		dlog.AgentPrintfN(manager.id, "Skipping retry of instance %d due to it being closed", retry.inst)
		return false
	}
	if !manager.BackoffManager.StillRelevant(retry) || !pbk.propCurBal.Equal(retry.attemptedBal) {
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

func (manager *SimpleProposalManager) startNextProposal(pbk *ProposingBookkeeping, inst int32) {
	beginProposalForPreparing(pbk, manager.Balloter)
	dlog.AgentPrintfN(manager.id, "Starting new proposal for instance %d with ballot %d.%d", inst, pbk.propCurBal.Number, pbk.propCurBal.PropID)
	manager.ProposerQuorumaliser.startPromiseQuorumOnCurBal(pbk, inst)
}

func (manager *SimpleProposalManager) HandleNewInstance(instsanceSpace *[]*ProposingBookkeeping, inst int32) {
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

func (manager *SimpleProposalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
	if inst > manager.crtInstance {
		manager.HandleNewInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	manager.sigNewInstCheck(pbk, inst, ballot, " was preempted")
	return manager.handleInstanceProposal(pbk, inst, ballot, phase)
}

func (manager *SimpleProposalManager) handleInstanceProposal(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
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

func (manager *SimpleProposalManager) sigNewInstCheck(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, reason string) {
	if pbk.status == CLOSED || pbk.maxKnownBal.Equal(ballot) || pbk.maxKnownBal.GreaterThan(ballot) {
		return
	}
	// todo change to attempting instances?
	if _, e := manager.instsStarted[inst]; e {
		dlog.AgentPrintfN(manager.id, "Signalling to open new instance as this instance %d %s", inst, reason)
		go func() { manager.sigNewInst <- struct{}{} }()
		delete(manager.instsStarted, inst)
	}
}

func (manager *SimpleProposalManager) howManyAttemptsToChoose(pbk *ProposingBookkeeping, inst int32) {
	attempts := manager.Balloter.GetAttemptNumber(pbk.proposeValueBal.Number)
	dlog.AgentPrintfN(manager.id, "Instance %d took %d attempts to be chosen", inst, attempts)
}

func (manager *SimpleProposalManager) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, at stdpaxosproto.Ballot) {
	if inst > manager.crtInstance {
		manager.HandleNewInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	manager.sigNewInstCheck(pbk, inst, at, "attempted was chosen by someone else")
	manager.handleBallotChosen(pbk, inst, at)
}

func (manager *SimpleProposalManager) handleBallotChosen(pbk *ProposingBookkeeping, inst int32, at stdpaxosproto.Ballot) {
	if pbk.status == CLOSED {
		return
	}

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
	delete(manager.instsStarted, inst)

	if at.Equal(pbk.propCurBal) && manager.doStats {
		manager.InstanceStats.RecordComplexStatEnd(stats.InstanceID{0, inst}, "Phase 2", "Success")
	}

	pbk.status = CLOSED
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

type EagerProposalManager struct {
	*SimpleProposalManager
	MaxOpenInsts int32
}

// basically the same as simple except that we also

func EagerProposalManagerNew(id int32, n int32, quoralP ProposerQuorumaliser,
	minBackoff int32, maxInitBackoff int32, maxBackoff int32,
	retries chan RetryInfo, factor float64, softFac bool, constBackoff bool, timeBasedBallots bool,
	doStats bool, tsStats *stats.TimeseriesStats, pStats *stats.ProposalStats, iStats *stats.InstanceStats, newInstSig chan struct{}, maxOi int32) *EagerProposalManager {
	man := &EagerProposalManager{
		SimpleProposalManager: SimpleProposalManagerNew(id, n, quoralP, minBackoff, maxInitBackoff, maxBackoff,
			retries, factor, softFac, constBackoff, timeBasedBallots, doStats, tsStats, pStats, iStats, newInstSig),
		MaxOpenInsts: maxOi,
	}
	go func() {
		for i := int32(0); i < maxOi; i++ {
			newInstSig <- struct{}{}
		}
	}()
	return man
}

func (manager *EagerProposalManager) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, at stdpaxosproto.Ballot) {
	if inst > manager.crtInstance {
		manager.HandleNewInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	manager.sigNewInstChosenCheck(pbk, inst, at)
	manager.handleBallotChosen(pbk, inst, at)
}

func (manager *EagerProposalManager) sigNewInstChosenCheck(pbk *ProposingBookkeeping, inst int32, at stdpaxosproto.Ballot) {
	// if opened do
	if pbk.status == CLOSED {
		return
	}
	// todo change to attempting instances?
	if _, e := manager.instsStarted[inst]; e {
		dlog.AgentPrintfN(manager.id, "Signalling to open new instance as this instance %d is chosen", inst)
		go func() { manager.sigNewInst <- struct{}{} }()
		delete(manager.instsStarted, inst)
	}
}

// Dynamic Eager
// chan took to long to propose
// function that detects the length of time to propose a value
//

// Minimal Proposer Proposal Manager
// When F+1 proposals are received greater than ours we stop attempting proposals to the instance.
type MinProposerProposalManager struct {
	*SimpleProposalManager
	minimalProposersShouldMaker
}

func MinProposerProposalManagerNew(f int, id int32, n int32, quoralP ProposerQuorumaliser,
	minBackoff int32, maxInitBackoff int32, maxBackoff int32,
	retries chan RetryInfo, factor float64, softFac bool, constBackoff bool, timeBasedBallots bool,
	doStats bool, tsStats *stats.TimeseriesStats, pStats *stats.ProposalStats, iStats *stats.InstanceStats, sigNewInst chan struct{}) *MinProposerProposalManager {
	return &MinProposerProposalManager{
		SimpleProposalManager: SimpleProposalManagerNew(id, n, quoralP, minBackoff, maxInitBackoff, maxBackoff,
			retries, factor, softFac, constBackoff, timeBasedBallots, doStats, tsStats, pStats, iStats, sigNewInst),
		minimalProposersShouldMaker: minimalProposersShouldMaker{
			myId: int16(id),
			//myBallots:        make(map[int32]stdpaxosproto.Ballot),
			ongoingProposals: make(map[int32]map[int16]stdpaxosproto.Ballot),
			instancesToForgo: make(map[int32]struct{}),
			f:                f,
		},
	}
}

func (decider *MinProposerProposalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
	decider.minimalProposersLearnOfBallot(ballot, inst)
	return decider.SimpleProposalManager.LearnOfBallot(instanceSpace, inst, ballot, phase)
}

func (decider *MinProposerProposalManager) minimalProposersLearnOfBallot(ballot stdpaxosproto.Ballot, inst int32) {
	if int32(ballot.PropID) != decider.PropID && !ballot.IsZero() {
		decider.minimalProposersShouldMaker.ballotReceived(inst, ballot)
		if decider.minimalProposersShouldMaker.shouldSkipInstance(inst) {
			dlog.AgentPrintfN(decider.id, "Stopping making proposals to instance %d because we have witnessed f+1 higher proposals", inst)
		}
	}
}

func (decider *MinProposerProposalManager) startNextProposal(pbk *ProposingBookkeeping, inst int32) {
	if decider.shouldSkipInstance(inst) {
		panic("should not start next proposal on instance fulfilled")
	}
	decider.SimpleProposalManager.startNextProposal(pbk, inst)
	dlog.AgentPrintfN(decider.id, "Starting new proposal for instance %d with ballot %d.%d", inst, pbk.propCurBal.Number, pbk.propCurBal.PropID)
	decider.minimalProposersShouldMaker.startedMyProposal(inst, pbk.propCurBal)
}

func (decider *MinProposerProposalManager) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	shouldSkip := decider.minimalProposersShouldMaker.shouldSkipInstance(retry.inst)
	if shouldSkip {
		decider.handleShouldSkip(pbk, retry)
		return false
	}
	return decider.SimpleProposalManager.DecideRetry(pbk, retry)
}

func (decider *MinProposerProposalManager) handleShouldSkip(pbk *ProposingBookkeeping, retry RetryInfo) {
	if pbk.status == CLOSED {
		dlog.AgentPrintfN(decider.id, "Skipping retry of instance %d due to it being closed", retry.inst)
		return
	}
	dlog.AgentPrintfN(decider.id, "Skipping retry of instance %d as minimal proposer threshold (f+1) met", retry.inst)
	pbk.status = BACKING_OFF
	decider.BackoffManager.ClearBackoff(retry.inst)
	return
}

func (decider *MinProposerProposalManager) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot) {
	delete(decider.instancesToForgo, inst)
	delete(decider.ongoingProposals, inst)
	decider.SimpleProposalManager.LearnBallotChosen(instanceSpace, inst, ballot)
}

func (decider *MinProposerProposalManager) GetGroup(inst int32) []int {
	ongoingProposals := decider.minimalProposersShouldMaker.getOngoingProposals(inst)
	if len(ongoingProposals) < decider.f+1 {
		return decider.SimpleProposalManager.GetGroup(inst)
	}
	topProps := make([]stdpaxosproto.Ballot, 0, len(ongoingProposals))
	topProposers := make([]int, 0, len(ongoingProposals))
	for proposer, bal := range decider.ongoingProposals[inst] {
		topProps = append(topProps, bal)
		topProposers = append(topProposers, int(proposer))
	}
	sort.Slice(topProposers, func(i, j int) bool {
		return topProps[j].GreaterThan(topProps[i]) || topProps[j].Equal(topProps[i])

	})
	return topProposers[:decider.f+1]
}

type MappedProposerInstanceDecider struct {
	*SimpleProposalManager
	instanceagentmapper.InstanceAgentMapper
}

func MappedProposersProposalManagerNew(id int32, n int32, quoralP ProposerQuorumaliser,
	minBackoff int32, maxInitBackoff int32, maxBackoff int32,
	retries chan RetryInfo, factor float64, softFac bool, constBackoff bool, timeBasedBallots bool,
	doStats bool, tsStats *stats.TimeseriesStats, pStats *stats.ProposalStats, iStats *stats.InstanceStats, agents []int, g int, sigNewInst chan struct{}) *MappedProposerInstanceDecider {

	//todo add dynamic on and off - when not detecting large numbers of proposals turn off F+1 or increase g+1
	simps := SimpleProposalManagerNew(id, n, quoralP, minBackoff, maxInitBackoff, maxBackoff, retries, factor, softFac, constBackoff, timeBasedBallots, doStats, tsStats, pStats, iStats, sigNewInst)

	return &MappedProposerInstanceDecider{
		SimpleProposalManager: simps,
		InstanceAgentMapper: &instanceagentmapper.InstanceSetMapper{
			Ids: agents,
			G:   g,
			N:   int(n),
		},
	}
}

func (manager *MappedProposerInstanceDecider) startNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32 {
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
	startFunc(manager.crtInstance)
	return []int32{manager.crtInstance}
}

// todo should retry only if still in group

func (manager *MappedProposerInstanceDecider) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
	manager.checkAndSetNewInstance(instanceSpace, inst, ballot, phase)
	return manager.SimpleProposalManager.handleInstanceProposal((*instanceSpace)[inst], inst, ballot, phase)
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
	manager.SimpleProposalManager.handleBallotChosen((*instanceSpace)[inst], inst, ballot)
}

func (decider *MappedProposerInstanceDecider) GetGroup(inst int32) []int {
	return decider.InstanceAgentMapper.GetGroup(int(inst))
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
	*MappedProposerInstanceDecider
	ids           []int
	n             int
	f             int
	curG          int
	conflictEWMA  float64
	ewmaWeight    float64
	conflictsSeen map[int32]map[stdpaxosproto.Ballot]struct{}
	// want to signal that we do not want to make proposals anymore

	//ourEWMALat    float64
	//othersEWMAlat float64
}

func DynamicMappedProposerManagerNew(id int32, n int32, quoralP ProposerQuorumaliser,
	minBackoff int32, maxInitBackoff int32, maxBackoff int32,
	retries chan RetryInfo, factor float64, softFac bool, constBackoff bool, timeBasedBallots bool,
	doStats bool, tsStats *stats.TimeseriesStats, pStats *stats.ProposalStats, iStats *stats.InstanceStats, agents []int, f int, sigNewInst chan struct{}) *DynamicMappedProposalManager {
	mappedDecider := MappedProposersProposalManagerNew(id, n, quoralP, minBackoff, maxInitBackoff, maxBackoff, retries, factor, softFac, constBackoff, timeBasedBallots, doStats, tsStats, pStats, iStats, agents, int(n), sigNewInst)

	dMappedDecicider := &DynamicMappedProposalManager{
		MappedProposerInstanceDecider: mappedDecider,
		ids:                           agents,
		n:                             int(n),
		f:                             f,
		curG:                          int(n),
		conflictEWMA:                  float64(0),
		ewmaWeight:                    0.1,
		conflictsSeen:                 make(map[int32]map[stdpaxosproto.Ballot]struct{}),
	}
	return dMappedDecicider
}

func mapper(i, iS, iE float64, oS, oE int32) int32 {
	slope := 1.0 * float64(oE-oS) / (iE - iS)
	o := oS + int32((slope*(i-iS))+0.5)
	return o
}

func (decider *DynamicMappedProposalManager) startNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32 {
	decider.updateGroupSize()
	return decider.MappedProposerInstanceDecider.startNextInstance(instanceSpace, startFunc)
}

func (decider *DynamicMappedProposalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
	decider.MappedProposerInstanceDecider.checkAndSetNewInstance(instanceSpace, inst, ballot, phase)

	pbk := (*instanceSpace)[inst]
	if ballot.GreaterThan(pbk.propCurBal) && !pbk.propCurBal.IsZero() {
		if _, e := decider.conflictsSeen[inst]; !e {
			decider.conflictsSeen[inst] = make(map[stdpaxosproto.Ballot]struct{})
		}
		if _, e := decider.conflictsSeen[inst][ballot]; !e {
			old := decider.conflictEWMA
			decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, 1)
			dlog.AgentPrintfN(decider.id, "Conflict encountered, increasing EWMA from %f to %f", old, decider.conflictEWMA)

		}
	}

	return decider.MappedProposerInstanceDecider.LearnOfBallot(instanceSpace, inst, ballot, phase)
}

func (decider *DynamicMappedProposalManager) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	doRetry := decider.SimpleProposalManager.DecideRetry(pbk, retry)
	if !doRetry {
		return doRetry
	}
	old := decider.conflictEWMA
	decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight*3, -1)
	dlog.AgentPrintfN(decider.id, "Retry needed on instance %d because failures are occurring or there is not enough system load, decreasing EWMA from %f to %f", retry.inst, old, decider.conflictEWMA)
	return doRetry
}

func (decider *DynamicMappedProposalManager) updateGroupSize() {
	newG := decider.curG

	dlog.AgentPrintfN(decider.id, "Current proposer group is of size %d (EWMA is %f)", decider.curG, decider.conflictEWMA)
	if decider.conflictEWMA > 0.2 {
		newG = int(mapper(decider.conflictEWMA, 1, 0, int32(decider.f+1), int32(decider.curG)))
		decider.conflictEWMA = 0
	}
	if decider.conflictEWMA < 0 {
		newG = int(mapper(decider.conflictEWMA, 0, -1, int32(decider.curG), int32(decider.n)))
		decider.conflictEWMA = 0
	}

	if newG < 1 {
		newG = 1
	}

	if newG > decider.n {
		newG = decider.n
	}
	if newG != decider.curG {
		if newG > decider.curG {
			dlog.AgentPrintfN(decider.id, "Increasing proposer group size %d", newG)
		} else {
			dlog.AgentPrintfN(decider.id, "Decreasing proposer group size to %d", newG)
		}
		decider.curG = newG
		decider.InstanceAgentMapper = &instanceagentmapper.InstanceSetMapper{
			Ids: decider.ids,
			G:   decider.curG,
			N:   decider.n,
		}
	}
}

func (decider *DynamicMappedProposalManager) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot) {
	pbk := (*instanceSpace)[inst]
	if ballot.GreaterThan(pbk.propCurBal) && !pbk.propCurBal.IsZero() {
		if _, e := decider.conflictsSeen[inst]; !e {
			decider.conflictsSeen[inst] = make(map[stdpaxosproto.Ballot]struct{})
		}
		if _, e := decider.conflictsSeen[inst][ballot]; !e {
			old := decider.conflictEWMA
			decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, 1)
			dlog.AgentPrintfN(decider.id, "Conflict encountered, increasing EWMA from %f to %f", old, decider.conflictEWMA)
		}
	}
	delete(decider.conflictsSeen, inst)
	decider.MappedProposerInstanceDecider.LearnBallotChosen(instanceSpace, inst, ballot)
}

func (decider *DynamicMappedProposalManager) GetGroup(inst int32) []int {
	return decider.MappedProposerInstanceDecider.GetGroup(inst)
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
	//if who != decider.id {
	//	return
	//}
	old := decider.conflictEWMA
	decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, -1)
	dlog.AgentPrintfN(decider.id, "Learnt NOOP, decreasing EWMA from %f to %f", old, decider.conflictEWMA)
}

type DMappedAndMinimalProposalManager struct {
	*DynamicMappedProposalManager
	*MinProposerProposalManager
}

func (man *DMappedAndMinimalProposalManager) startNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32 {
	return man.DynamicMappedProposalManager.startNextInstance(instanceSpace, startFunc)
}

func (man *DMappedAndMinimalProposalManager) startNextProposal(pbk *ProposingBookkeeping, inst int32) {
	if man.MinProposerProposalManager.shouldSkipInstance(inst) {
		panic("should not start next proposal on instance fulfilled")
	}
	man.SimpleProposalManager.startNextProposal(pbk, inst)
	dlog.AgentPrintfN(man.id, "Starting new proposal for instance %d with ballot %d.%d", inst, pbk.propCurBal.Number, pbk.propCurBal.PropID)
	man.minimalProposersShouldMaker.startedMyProposal(inst, pbk.propCurBal)
}

func (man *DMappedAndMinimalProposalManager) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	return man.DynamicMappedProposalManager.DecideRetry(pbk, retry) && man.MinProposerProposalManager.DecideRetry(pbk, retry)
}

func (man *DMappedAndMinimalProposalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
	man.MinProposerProposalManager.minimalProposersLearnOfBallot(ballot, inst)
	return man.DynamicMappedProposalManager.LearnOfBallot(instanceSpace, inst, ballot, phase)
}

func (man *DMappedAndMinimalProposalManager) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot) {
	man.DynamicMappedProposalManager.LearnBallotChosen(instanceSpace, inst, ballot)
	man.MinProposerProposalManager.LearnBallotChosen(instanceSpace, inst, ballot)
}

// mapped
// minimal
// simple
// dmapped
//dmappedminimal

type hedge struct {
	relatedHedges []int32 //includes self
	preempted     bool
	//chosen        bool
}

type SimpleHedgedBets struct {
	*SimpleProposalManager
	id              int32
	n               int32
	currentHedgeNum int32
	conflictEWMA    float64
	ewmaWeight      float64
	max             int32
	currentHedges   map[int32]*hedge
	confs           map[int32]map[int16]struct{}
}

func HedgedBetsProposalManagerNew(id int32, manager *SimpleProposalManager, n int32, initialHedge int32) *SimpleHedgedBets {
	dMappedDecicider := &SimpleHedgedBets{
		SimpleProposalManager: manager,
		id:                    id,
		n:                     n,
		currentHedgeNum:       initialHedge,
		conflictEWMA:          float64(1),
		ewmaWeight:            0.1,
		max:                   2 * n,
		currentHedges:         make(map[int32]*hedge),
		confs:                 make(map[int32]map[int16]struct{}),
	}
	return dMappedDecicider
}

func (decider *SimpleHedgedBets) startNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32 {
	decider.updateHedgeSize()
	opened := make([]int32, 0, decider.currentHedgeNum)
	for i := int32(0); i < decider.currentHedgeNum; i++ {
		opened = append(opened, decider.SimpleProposalManager.startNextInstance(instanceSpace, startFunc)...)
	}

	decider.createHedgeRecordFromOpenedHedges(opened)
	return opened
}

func (decider *SimpleHedgedBets) createHedgeRecordFromOpenedHedges(opened []int32) {
	for _, inst := range opened {
		decider.currentHedges[inst] = &hedge{
			relatedHedges: opened,
			preempted:     false,
			//chosen:        false,
		}
	}
}

func (decider *SimpleHedgedBets) checkInstFailedSig(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot) {
	if pbk.status == CLOSED || pbk.maxKnownBal.Equal(ballot) || pbk.maxKnownBal.GreaterThan(ballot) {
		return
	}
	if pbk.propCurBal.IsZero() || pbk.propCurBal.Equal(ballot) {
		return
	}
	curHedge, e := decider.currentHedges[inst]
	if !e {
		return
	}
	curHedge.preempted = true
	dlog.AgentPrintfN(decider.id, "Noting that instance %d has failed", inst)
}

func (decider *SimpleHedgedBets) checkInstChosenByMeSig(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot) {
	if pbk.status == CLOSED || !ballot.Equal(pbk.propCurBal) { //pbk.maxKnownBal.GreaterThan(ballot) {
		return
	}
	curHedge, e := decider.currentHedges[inst]
	if !e {
		return
	}
	dlog.AgentPrintfN(decider.id, "Noting that instance %d has succeeded, cleaning all related hedges", inst)

	for _, i := range curHedge.relatedHedges {
		delete(decider.currentHedges, i)
	}
}

func (decider *SimpleHedgedBets) checkNeedsSig(pbk *ProposingBookkeeping, inst int32) {
	curHedge, e := decider.currentHedges[inst]
	if !e || pbk.status == CLOSED {
		return
	}

	// if all related hedged failed
	for _, i := range curHedge.relatedHedges {
		if !decider.currentHedges[i].preempted {
			dlog.AgentPrintfN(decider.id, "Not signalling to open new instance %d as not all hedges preempted", inst)
			return
		}
	}
	for _, i := range curHedge.relatedHedges {
		delete(decider.currentHedges, i)
	}
	delete(decider.currentHedges, inst)
	dlog.AgentPrintfN(decider.id, "Signalling to open new instance as all hedged attempts related to %d failed", inst)
	go func() { decider.sigNewInst <- struct{}{} }()
}

func (decider *SimpleHedgedBets) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool {
	if inst > decider.crtInstance {
		decider.SimpleProposalManager.HandleNewInstance(instanceSpace, inst)
	}

	pbk := (*instanceSpace)[inst]
	decider.checkInstFailedSig(pbk, inst, ballot)
	decider.checkNeedsSig(pbk, inst)
	newBallot := decider.SimpleProposalManager.handleInstanceProposal(pbk, inst, ballot, phase) // should only signal if all other instances hedged on are preempted
	decider.updateConfs(inst, ballot)
	return newBallot
}

func (decider *SimpleHedgedBets) updateConfs(inst int32, ballot stdpaxosproto.Ballot) {
	if _, e := decider.confs[inst]; !e {
		decider.confs[inst] = make(map[int16]struct{})
	}
	decider.confs[inst][ballot.PropID] = struct{}{}
	dlog.AgentPrintfN(decider.id, "Now observing %d attempts in instance %d", len(decider.confs[inst]), inst)
}

func (decider *SimpleHedgedBets) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot) {
	if inst > decider.crtInstance {
		decider.SimpleProposalManager.HandleNewInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	decider.checkInstFailedSig(pbk, inst, ballot)
	decider.checkInstChosenByMeSig(pbk, inst, ballot)
	decider.checkNeedsSig(pbk, inst)
	decider.updateConfs(inst, ballot)
	decider.SimpleProposalManager.handleBallotChosen(pbk, inst, ballot)
	decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, float64(len(decider.confs[inst])))
	delete(decider.confs, inst) // will miss some as a result but its okie to avoid too much memory growth
}

func (decider *SimpleHedgedBets) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	doRetry := decider.SimpleProposalManager.DecideRetry(pbk, retry)
	//if !doRetry {
	//	return doRetry
	//}
	//delete(decider.confs[retry.inst], retry.preempterBal.PropID)
	//decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, float64(len(decider.confs[retry.inst]))) // means that the instance has less proposers
	//todo should wait until all attempts have failed??
	return doRetry
}

func (decider *SimpleHedgedBets) updateHedgeSize() {
	// get average number of proposals made per instance
	newHedge := int32(decider.conflictEWMA + 0.5)
	if newHedge < 1 {
		newHedge = 1
	}

	if decider.currentHedgeNum > newHedge {
		dlog.AgentPrintfN(decider.id, "Decreasing hedged size")
	} else if newHedge > decider.currentHedgeNum {
		dlog.AgentPrintfN(decider.id, "Increasing hedged size")
	}

	decider.currentHedgeNum = newHedge
	dlog.AgentPrintfN(decider.id, "Current hedged bet is of size %d (EWMA is %f)", decider.currentHedgeNum, decider.conflictEWMA)
}

func (decider *SimpleHedgedBets) LearnNoop(inst int32, who int32) {
	//if who != decider.id {
	//	return
	//}
	//old := decider.conflictEWMA
	//decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, 1)
	//dlog.AgentPrintfN(decider.id, "Learnt NOOP, decreasing EWMA from %f to %f", old, decider.conflictEWMA)
}

func (decider *SimpleHedgedBets) GetGroup(inst int32) []int {
	return decider.SimpleProposalManager.GetGroup(inst)
}
