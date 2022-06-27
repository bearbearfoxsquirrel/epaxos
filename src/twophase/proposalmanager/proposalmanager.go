package proposalmanager

import (
	"dlog"
	"instanceagentmapper"
	"lwcproto"
	"mathextra"
	"stdpaxosproto"
)

type ProposalManager interface {
	StartNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32
	StartNextProposal(initiator *ProposingBookkeeping, inst int32)
	//ProposingValue(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot)
	LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool // returns if proposer's ballot is preempted
	//LearnOfBallotAccepted(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot)
	LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal)
	DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool
}

type CrtInstanceOracle interface {
	GetCrtInstance() int32
}

type NoopLearner interface {
	LearnNoop(inst int32, who int32)
}

type GlobalInstanceManager interface {
	CrtInstanceOracle
	ProposalManager
}

type SimpleGlobalManager struct {
	n           int32
	crtInstance int32
	SingleInstanceManager
	OpenInstSignal
	*BackoffManager
	id int32
}

func SimpleProposalManagerNew(id int32, signal OpenInstSignal, iManager SingleInstanceManager, backoffManager *BackoffManager) *SimpleGlobalManager {
	return &SimpleGlobalManager{
		crtInstance:           -1,
		id:                    id,
		OpenInstSignal:        signal,
		SingleInstanceManager: iManager,
		BackoffManager:        backoffManager,
	}
}

func (manager *SimpleGlobalManager) GetCrtInstance() int32 { return manager.crtInstance }

func (manager *SimpleGlobalManager) StartNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32 {
	manager.crtInstance++
	(*instanceSpace)[manager.crtInstance] = manager.SingleInstanceManager.InitInstance(manager.crtInstance)
	startFunc(manager.crtInstance)
	opened := []int32{manager.crtInstance}
	manager.OpenInstSignal.Opened(opened)
	return opened
}

func (manager *SimpleGlobalManager) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	return manager.SingleInstanceManager.ShouldRetryInstance(pbk, retry)
}

func (manager *SimpleGlobalManager) StartNextProposal(pbk *ProposingBookkeeping, inst int32) {
	manager.SingleInstanceManager.StartProposal(pbk, inst)
}

func (manager *SimpleGlobalManager) HandleNewInstance(instsanceSpace *[]*ProposingBookkeeping, inst int32) {
	for i := manager.crtInstance + 1; i <= inst; i++ {
		(*instsanceSpace)[i] = GetEmptyInstance()
		// take advantage of inductive backoff property
		if i != inst {
			(*instsanceSpace)[i].Status = BACKING_OFF
			_, bot := manager.CheckAndHandleBackoff(inst, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
			dlog.AgentPrintfN(manager.id, "Backing off newly received instance %d for %d microseconds", inst, bot)
		}
	}
	manager.crtInstance = inst
}

func (manager *SimpleGlobalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	if inst > manager.crtInstance {
		manager.HandleNewInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	manager.OpenInstSignal.CheckBallot(pbk, inst, ballot, phase)
	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

//func (manager *SimpleGlobalManager) LearnOfBallotAccepted(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot) bool {
//	if inst > manager.crtInstance {
//		manager.HandleNewInstance(instanceSpace, inst)
//	}
//	pbk := (*instanceSpace)[inst]
//	//manager.OpenInstSignal.CheckBallotAccepted(pbk, inst, ballot)
//	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot)
//}

func (manager *SimpleGlobalManager) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, at lwcproto.ConfigBal) {
	if inst > manager.crtInstance {
		manager.HandleNewInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	manager.OpenInstSignal.CheckChosen(pbk, inst, at)
	manager.SingleInstanceManager.HandleProposalChosen(pbk, inst, at)
}

// basically the same as simple except that we also

// Dynamic Eager
// chan took to long to propose
// function that detects the length of time to propose a value
//

type MappedGlobalManager struct {
	SingleInstanceManager
	*SimpleGlobalManager
	instanceagentmapper.InstanceAgentMapper
}

func MappedProposersProposalManagerNew(simpleProposalManager *SimpleGlobalManager, iMan SingleInstanceManager, agentMapper instanceagentmapper.InstanceAgentMapper) *MappedGlobalManager {

	//todo add dynamic on and off - when not detecting large numbers of proposals turn off F+1 or increase g+1
	//simps := SimpleProposalManagerNew(id, n, quoralP, minBackoff, maxInitBackoff, maxBackoff, retries, factor, softFac, constBackoff, timeBasedBallots, doStats, tsStats, pStats, iStats, sigNewInst)

	return &MappedGlobalManager{
		SimpleGlobalManager:   simpleProposalManager,
		SingleInstanceManager: iMan,
		InstanceAgentMapper:   agentMapper,
	}
}

func (manager *MappedGlobalManager) GetInstanceMapper() instanceagentmapper.InstanceAgentMapper {
	return manager.InstanceAgentMapper
}

func (manager *MappedGlobalManager) StartNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32 {
	for gotInstance := false; !gotInstance; {
		manager.crtInstance++
		if (*instanceSpace)[manager.crtInstance] == nil {
			(*instanceSpace)[manager.crtInstance] = GetEmptyInstance()
		}
		if (*instanceSpace)[manager.crtInstance].Status != NOT_BEGUN {
			continue
		}

		mapped := manager.InstanceAgentMapper.GetGroup(int(manager.crtInstance))
		dlog.AgentPrintfN(manager.id, "Proposer group for instance %d is %v", manager.crtInstance, mapped)
		inG := inGroup(mapped, int(manager.id))
		if !inG {
			dlog.AgentPrintfN(manager.id, "Initially backing off instance %d as we are not mapped to it", manager.crtInstance)
			(*instanceSpace)[manager.crtInstance].Status = BACKING_OFF
			manager.BackoffManager.CheckAndHandleBackoff(manager.crtInstance, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE) // todo shorter backoff to hear of mapped proposal made
			continue
		}
		gotInstance = true
		dlog.AgentPrintfN(manager.id, "Starting instance %d as we are mapped to it", manager.crtInstance)
	}
	startFunc(manager.crtInstance)
	opened := []int32{manager.crtInstance}
	manager.OpenInstSignal.Opened(opened)
	return opened
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

// todo should retry only if still in group

func (manager *MappedGlobalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	manager.checkAndSetNewInstance(instanceSpace, inst, ballot, phase)
	return manager.SimpleGlobalManager.LearnOfBallot(instanceSpace, inst, ballot, phase)
	//return manager.SingleInstanceManager.HandleReceivedBallot((*instanceSpace)[inst], inst, ballot, phase)
}

func (manager *MappedGlobalManager) checkAndSetNewInstance(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) {
	if (*instanceSpace)[inst] == nil {
		(*instanceSpace)[inst] = GetEmptyInstance()
		dlog.AgentPrintfN(manager.id, "Witnessed new instance %d, we have set instance %d now to be our current instance", inst, manager.crtInstance)
	}
}

func (manager *MappedGlobalManager) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal) {
	if (*instanceSpace)[inst] == nil {
		(*instanceSpace)[inst] = GetEmptyInstance()
	}
	manager.SimpleGlobalManager.LearnBallotChosen(instanceSpace, inst, ballot)
}

type DynamicAgentMapper interface {
	instanceagentmapper.InstanceAgentMapper
	SetGroup(g int)
}

type DynamicInstanceSetMapper struct {
	instanceagentmapper.InstanceSetMapper
}

func (m *DynamicInstanceSetMapper) SetGroup(g int) {
	m.G = g
}

type DynamicMappedGlobalManager struct {
	*MappedGlobalManager
	n             int
	f             int
	curG          int
	conflictEWMA  float64
	ewmaWeight    float64
	conflictsSeen map[int32]map[lwcproto.ConfigBal]struct{}
	DynamicAgentMapper
	// want to signal that we do not want to make proposals anymore
}

func DynamicMappedProposerManagerNew(proposalManager *SimpleGlobalManager, iMan SingleInstanceManager, aMapper DynamicAgentMapper, n int32, f int) *DynamicMappedGlobalManager {
	mappedDecider := MappedProposersProposalManagerNew(proposalManager, iMan, aMapper)
	dMappedDecicider := &DynamicMappedGlobalManager{
		MappedGlobalManager: mappedDecider,
		DynamicAgentMapper:  aMapper,
		n:                   int(n),
		f:                   f,
		curG:                int(n),
		conflictEWMA:        float64(0),
		ewmaWeight:          0.1,
		conflictsSeen:       make(map[int32]map[lwcproto.ConfigBal]struct{}),
	}
	return dMappedDecicider
}

func mapper(i, iS, iE float64, oS, oE int32) int32 {
	slope := 1.0 * float64(oE-oS) / (iE - iS)
	o := oS + int32((slope*(i-iS))+0.5)
	return o
}

func (decider *DynamicMappedGlobalManager) StartNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32 {
	decider.updateGroupSize()
	return decider.MappedGlobalManager.StartNextInstance(instanceSpace, startFunc)
}

func (decider *DynamicMappedGlobalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	decider.MappedGlobalManager.checkAndSetNewInstance(instanceSpace, inst, ballot, phase)

	pbk := (*instanceSpace)[inst]
	if !ballot.Equal(pbk.PropCurBal) && !pbk.PropCurBal.IsZero() { // ballot.GreaterThan(pbk.PropCurBal) &&
		if _, e := decider.conflictsSeen[inst]; !e {
			decider.conflictsSeen[inst] = make(map[lwcproto.ConfigBal]struct{})
		}
		if _, e := decider.conflictsSeen[inst][ballot]; !e {
			old := decider.conflictEWMA
			decider.conflictEWMA = mathextra.EwmaAdd(decider.conflictEWMA, decider.ewmaWeight, 1)
			dlog.AgentPrintfN(decider.id, "Conflict encountered, increasing EWMA from %f to %f", old, decider.conflictEWMA)

		}
	}

	return decider.MappedGlobalManager.LearnOfBallot(instanceSpace, inst, ballot, phase)
}

func (decider *DynamicMappedGlobalManager) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	doRetry := decider.SimpleGlobalManager.DecideRetry(pbk, retry)
	if !doRetry {
		return doRetry
	}
	old := decider.conflictEWMA
	decider.conflictEWMA = mathextra.EwmaAdd(decider.conflictEWMA, decider.ewmaWeight*3, -1)
	dlog.AgentPrintfN(decider.id, "Retry needed on instance %d because failures are occurring or there is not enough system load, decreasing EWMA from %f to %f", retry.Inst, old, decider.conflictEWMA)
	return doRetry
}

func (decider *DynamicMappedGlobalManager) updateGroupSize() {
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
		decider.DynamicAgentMapper.SetGroup(decider.curG)
	}
}

func (decider *DynamicMappedGlobalManager) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal) {
	pbk := (*instanceSpace)[inst]
	if ballot.GreaterThan(pbk.PropCurBal) && !pbk.PropCurBal.IsZero() {
		if _, e := decider.conflictsSeen[inst]; !e {
			decider.conflictsSeen[inst] = make(map[lwcproto.ConfigBal]struct{})
		}
		if _, e := decider.conflictsSeen[inst][ballot]; !e {
			old := decider.conflictEWMA
			decider.conflictEWMA = mathextra.EwmaAdd(decider.conflictEWMA, decider.ewmaWeight, 1)
			dlog.AgentPrintfN(decider.id, "Conflict encountered, increasing EWMA from %f to %f", old, decider.conflictEWMA)
		}
	}
	delete(decider.conflictsSeen, inst)
	decider.MappedGlobalManager.LearnBallotChosen(instanceSpace, inst, ballot)
}

//func (decider *DynamicMappedGlobalManager) GetGroup(Inst int32) []int {
//	return decider.MappedGlobalManager.GetGroup(Inst)
//}

func movingPointAvg(a, ob float64) float64 {
	a -= a / 1000
	a += ob / 1000
	return a
}

func (decider *DynamicMappedGlobalManager) LearnNoop(inst int32, who int32) {
	//if who != decider.id {
	//	return
	//}
	old := decider.conflictEWMA
	decider.conflictEWMA = mathextra.EwmaAdd(decider.conflictEWMA, decider.ewmaWeight, -1)
	dlog.AgentPrintfN(decider.id, "Learnt NOOP, decreasing EWMA from %f to %f", old, decider.conflictEWMA)
}

type hedge struct {
	relatedHedges []int32 //includes self
	preempted     bool
}

type SimpleHedgedBets struct {
	*SimpleGlobalManager
	id              int32
	n               int32
	currentHedgeNum int32
	conflictEWMA    float64
	ewmaWeight      float64
	max             int32
	confs           map[int32]map[int16]struct{}
}

func HedgedBetsProposalManagerNew(id int32, manager *SimpleGlobalManager, n int32, initialHedge int32) *SimpleHedgedBets {
	dMappedDecicider := &SimpleHedgedBets{
		SimpleGlobalManager: manager,
		id:                  id,
		n:                   n,
		currentHedgeNum:     initialHedge,
		conflictEWMA:        float64(1),
		ewmaWeight:          0.1,
		max:                 2 * n,
		confs:               make(map[int32]map[int16]struct{}),
	}
	return dMappedDecicider
}

func (decider *SimpleHedgedBets) StartNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32 {
	decider.updateHedgeSize()
	opened := make([]int32, 0, decider.currentHedgeNum)
	for i := int32(0); i < decider.currentHedgeNum; i++ {
		opened = append(opened, decider.SimpleGlobalManager.StartNextInstance(instanceSpace, startFunc)...)
	}
	decider.OpenInstSignal.Opened(opened)
	return opened
}

func (decider *SimpleHedgedBets) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	if inst > decider.crtInstance {
		decider.SimpleGlobalManager.HandleNewInstance(instanceSpace, inst)
	}

	pbk := (*instanceSpace)[inst]
	decider.updateConfs(inst, ballot)
	decider.OpenInstSignal.CheckBallot(pbk, inst, ballot, phase)
	return decider.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (decider *SimpleHedgedBets) updateConfs(inst int32, ballot lwcproto.ConfigBal) {
	if _, e := decider.confs[inst]; !e {
		decider.confs[inst] = make(map[int16]struct{})
	}
	decider.confs[inst][ballot.PropID] = struct{}{}
	dlog.AgentPrintfN(decider.id, "Now observing %d attempts in instance %d", len(decider.confs[inst]), inst)
}

func (decider *SimpleHedgedBets) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal) {
	if inst > decider.crtInstance {
		decider.SimpleGlobalManager.HandleNewInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	decider.updateConfs(inst, ballot)
	decider.OpenInstSignal.CheckChosen(pbk, inst, ballot)
	decider.SingleInstanceManager.HandleProposalChosen(pbk, inst, ballot)
	decider.conflictEWMA = mathextra.EwmaAdd(decider.conflictEWMA, decider.ewmaWeight, float64(len(decider.confs[inst])))
	delete(decider.confs, inst) // will miss some as a result but its okie to avoid too much memory growth
}

func (decider *SimpleHedgedBets) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	doRetry := decider.SimpleGlobalManager.DecideRetry(pbk, retry)
	//if !doRetry {
	//	return doRetry
	//}
	//delete(decider.confs[retry.Inst], retry.PreempterBal.PropID)
	//decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, float64(len(decider.confs[retry.Inst]))) // means that the instance has less proposers
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

//
//func (decider *SimpleHedgedBets) GetGroup(Inst int32) []int {
//	return decider.SimpleGlobalManager.GetGroup(Inst)
//}

// mapped hedged bets??
