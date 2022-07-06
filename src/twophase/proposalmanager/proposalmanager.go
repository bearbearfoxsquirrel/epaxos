package proposalmanager

import (
	"dlog"
	"genericsmr"
	"instanceagentmapper"
	"lwcproto"
	"math"
	"mathextra"
	"proposerstate"
	"stdpaxosproto"
)

type ProposalManager interface {
	StartNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32
	StartNextProposal(initiator *ProposingBookkeeping, inst int32)
	//ProposingValue(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot)
	LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool // returns if proposer's ballot is preempted
	LearnOfBallotAccepted(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, whosecmds int32)
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

type EagerForwardInductionProposalManager struct {
	n           int32
	crtInstance int32
	SingleInstanceManager
	*EagerSig
	*BackoffManager
	id int32
	*genericsmr.Replica
	stateRpc             uint8
	reservationsMade     map[int32]struct{}
	reservationsGivenOut map[int32]map[int32]struct{}
}

func EagerForwardInductionProposalManagerNew(n int32, id int32, signal *EagerSig, iManager SingleInstanceManager, backoffManager *BackoffManager, replica *genericsmr.Replica, stateRPC uint8) *EagerForwardInductionProposalManager {
	return &EagerForwardInductionProposalManager{
		n:                     n,
		crtInstance:           -1,
		SingleInstanceManager: iManager,
		EagerSig:              signal,
		BackoffManager:        backoffManager,
		id:                    id,
		Replica:               replica,
		stateRpc:              stateRPC,
		reservationsMade:      make(map[int32]struct{}),
		reservationsGivenOut:  make(map[int32]map[int32]struct{}),
	}
}

func (manager *EagerForwardInductionProposalManager) GetCrtInstance() int32 {
	return manager.crtInstance
}

func (manager *EagerForwardInductionProposalManager) StartNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32 {
	min := int32(math.MaxInt32)
	if len(manager.reservationsMade) == 0 {
		min = manager.crtInstance + 1
		manager.crtInstance++
	} else {
		for i, _ := range manager.reservationsMade {
			if i < min {
				min = i
			}
		}
		delete(manager.reservationsMade, min)
	}
	manager.reservationsGivenOut[min] = make(map[int32]struct{})
	(*instanceSpace)[min] = manager.SingleInstanceManager.InitInstance(min)
	startFunc(min)
	opened := []int32{min}
	manager.EagerSig.Opened(opened)
	return opened
}

func (manager *EagerForwardInductionProposalManager) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	return manager.SingleInstanceManager.ShouldRetryInstance(pbk, retry)
}

func (manager *EagerForwardInductionProposalManager) StartNextProposal(pbk *ProposingBookkeeping, inst int32) {
	manager.SingleInstanceManager.StartProposal(pbk, inst)
	if _, e := manager.reservationsMade[inst]; e {
		dlog.AgentPrintfN(manager.id, "No longer reserving instance %d to propose in", inst)
		delete(manager.reservationsMade, inst)
	}
}

func (manager *EagerForwardInductionProposalManager) HandleNewInstance(instsanceSpace *[]*ProposingBookkeeping, inst int32) {
	if manager.crtInstance >= inst {
		return
	}

	for i := manager.crtInstance + 1; i <= inst; i++ {
		manager.reservationsGivenOut[i] = make(map[int32]struct{})

		(*instsanceSpace)[i] = GetEmptyInstance()
		if _, e := manager.reservationsMade[i]; e {
			continue
		}

		if i == inst {
			break
		}

		(*instsanceSpace)[i].Status = BACKING_OFF
		_, bot := manager.CheckAndHandleBackoff(inst, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
		dlog.AgentPrintfN(manager.id, "Backing off newly received instance %d for %d microseconds", inst, bot)
	}
	manager.crtInstance = inst
}

func (manager *EagerForwardInductionProposalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	manager.HandleNewInstance(instanceSpace, inst)
	// reservation taken away
	pbk := (*instanceSpace)[inst]
	// given reservation
	if manager.checkIfGivenReservation(ballot, pbk, inst) {
		return false
	}
	manager.checkIfLostReservation(ballot, inst)

	manager.EagerSig.CheckOngoingBallot(pbk, inst, ballot, phase)
	newBal := manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)

	// Should not give out reservation?
	if int32(ballot.PropID) == manager.id || ballot.PropID == -1 {
		return newBal
	}
	if _, e := manager.reservationsGivenOut[inst][int32(ballot.PropID)]; e {
		return newBal
	}
	if ballot.GreaterThan(pbk.MaxKnownBal) || ballot.Equal(pbk.MaxKnownBal) || !pbk.ProposeValueBal.IsZero() {
		return newBal
	}

	// Give reservation
	manager.reservationsGivenOut[inst][int32(ballot.PropID)] = struct{}{}
	manager.Replica.SendMsg(int32(ballot.PropID), manager.stateRpc, &proposerstate.State{
		ProposerID:      manager.id,
		CurrentInstance: manager.crtInstance + 1,
	})
	dlog.AgentPrintfN(manager.id, "Assuming that proposer %d is going to make a new proposal to instance %d following this preempt or acceptance message", ballot.PropID, manager.crtInstance+1)
	manager.LearnOfBallot(instanceSpace, manager.crtInstance+1, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: 10000, PropID: -1}}, stdpaxosproto.PROMISE)
	return newBal
}

func (manager *EagerForwardInductionProposalManager) checkIfGivenReservation(ballot lwcproto.ConfigBal, pbk *ProposingBookkeeping, inst int32) bool {
	if ballot.Equal(lwcproto.ConfigBal{Config: -2, Ballot: stdpaxosproto.Ballot{Number: -2, PropID: -2}}) {
		if pbk.MaxKnownBal.GreaterThan(lwcproto.ConfigBal{-1, stdpaxosproto.Ballot{10000, -1}}) { // have we reserved for someone else??
			dlog.AgentPrintfN(manager.id, "ignoring invitation of reservation for %d to propose in ", inst)
			return true
		}
		manager.reservationsMade[inst] = struct{}{}
		dlog.AgentPrintfN(manager.id, "Reserving instance %d to attempt next", inst)
		return true
	}
	return false
}

func (manager *EagerForwardInductionProposalManager) checkIfLostReservation(ballot lwcproto.ConfigBal, inst int32) {
	if !ballot.Equal(lwcproto.ConfigBal{Config: -2, Ballot: stdpaxosproto.Ballot{Number: -2, PropID: -2}}) {
		if _, e := manager.reservationsMade[inst]; e {
			dlog.AgentPrintfN(manager.id, "No longer reserving instance %d to attempt next", inst)
			delete(manager.reservationsMade, inst)
		}
	}
}

func (manager *EagerForwardInductionProposalManager) LearnOfBallotAccepted(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	manager.HandleNewInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.EagerSig.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)

	if _, e := manager.reservationsMade[inst]; e {
		dlog.AgentPrintfN(manager.id, "No longer reserving instance %d to attempt next", inst)
		delete(manager.reservationsMade, inst)
	}

	if _, e := manager.reservationsGivenOut[inst][int32(ballot.PropID)]; e {
		return
	}
	if int32(ballot.PropID) == manager.id {
		return
	}

	manager.reservationsGivenOut[inst][int32(ballot.PropID)] = struct{}{}
	manager.Replica.SendMsg(int32(ballot.PropID), manager.stateRpc, &proposerstate.State{
		ProposerID:      manager.id,
		CurrentInstance: manager.crtInstance + 1,
	})
	dlog.AgentPrintfN(manager.id, "Assuming that proposer %d is going to make a new proposal to instance %d following this notification of already acceptance", ballot.PropID, manager.crtInstance+1)
	manager.LearnOfBallot(instanceSpace, manager.crtInstance+1, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: 10000, PropID: -1}}, stdpaxosproto.PROMISE)

}

func (manager *EagerForwardInductionProposalManager) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, at lwcproto.ConfigBal) {
	if _, e := manager.reservationsMade[inst]; e {
		dlog.AgentPrintfN(manager.id, "No longer reserving instance %d to propose in", inst)
		delete(manager.reservationsMade, inst)
	}

	manager.HandleNewInstance(instanceSpace, inst)
	if at.IsZero() {
		panic("asjdflasdlkfj")
	}
	pbk := (*instanceSpace)[inst]
	manager.EagerSig.CheckChosen(pbk, inst, at)
	manager.SingleInstanceManager.HandleProposalChosen(pbk, inst, at)

	if int32(at.PropID) == manager.id {
		return
	}
	if _, e := manager.reservationsGivenOut[inst][int32(at.PropID)]; e {
		return
	}

	manager.reservationsGivenOut[inst][int32(at.PropID)] = struct{}{}
	manager.Replica.SendMsg(int32(at.PropID), manager.stateRpc, &proposerstate.State{
		ProposerID:      manager.id,
		CurrentInstance: manager.crtInstance + 1,
	})

	dlog.AgentPrintfN(manager.id, "Assuming that proposer %d is going to make a new proposal to instance %d following this chosen receipt", at.PropID, manager.crtInstance+1)
	manager.LearnOfBallot(instanceSpace, manager.crtInstance+1, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: 10000, PropID: -1}}, stdpaxosproto.PROMISE)
	//}
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

func (manager *SimpleGlobalManager) UpdateCurrentInstance(instsanceSpace *[]*ProposingBookkeeping, inst int32) {
	if inst <= manager.crtInstance {
		return
	}
	// take advantage of inductive backoff property
	for i := manager.crtInstance + 1; i <= inst; i++ {
		(*instsanceSpace)[i] = GetEmptyInstance()
		if i == inst {
			break
		}
		(*instsanceSpace)[i].Status = BACKING_OFF
		_, bot := manager.CheckAndHandleBackoff(inst, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
		dlog.AgentPrintfN(manager.id, "Backing off newly received instance %d for %d microseconds", inst, bot)
	}
	dlog.AgentPrintfN(manager.id, "Setting instance %d as current instance", inst)
	manager.crtInstance = inst
}

func (manager *SimpleGlobalManager) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.OpenInstSignal.CheckOngoingBallot(pbk, inst, ballot, phase)
	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (manager *SimpleGlobalManager) LearnOfBallotAccepted(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	if inst > manager.crtInstance {
		manager.UpdateCurrentInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	manager.OpenInstSignal.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
	//return //manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot)
}

func (manager *SimpleGlobalManager) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, at lwcproto.ConfigBal) {
	manager.UpdateCurrentInstance(instanceSpace, inst)
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
	// todo check if all prior instances are opened?
	if (*instanceSpace)[inst] == nil {
		(*instanceSpace)[inst] = GetEmptyInstance()
		dlog.AgentPrintfN(manager.id, "Witnessed new instance %d, we have set instance %d now to be our current instance", inst, manager.crtInstance)
	}
}

func (manager *MappedGlobalManager) LearnOfBallotAccepted(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	if (*instanceSpace)[inst] == nil {
		(*instanceSpace)[inst] = GetEmptyInstance()
	}
	pbk := (*instanceSpace)[inst]
	manager.OpenInstSignal.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
	//return //manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot)
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

func (manager *SimpleHedgedBets) StartNextInstance(instanceSpace *[]*ProposingBookkeeping, startFunc func(inst int32)) []int32 {
	manager.updateHedgeSize()
	opened := make([]int32, 0, manager.currentHedgeNum)
	for i := int32(0); i < manager.currentHedgeNum; i++ {
		opened = append(opened, manager.SimpleGlobalManager.StartNextInstance(instanceSpace, startFunc)...)
	}
	manager.OpenInstSignal.Opened(opened)
	return opened
}

func (manager *SimpleHedgedBets) LearnOfBallot(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	if inst > manager.crtInstance {
		manager.SimpleGlobalManager.UpdateCurrentInstance(instanceSpace, inst)
	}

	pbk := (*instanceSpace)[inst]
	manager.updateConfs(inst, ballot)
	manager.OpenInstSignal.CheckOngoingBallot(pbk, inst, ballot, phase)
	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (manager *SimpleHedgedBets) LearnOfBallotAccepted(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	if inst > manager.crtInstance {
		manager.SimpleGlobalManager.UpdateCurrentInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	manager.updateConfs(inst, ballot)
	manager.OpenInstSignal.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
	//return //manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot)
}

func (manager *SimpleHedgedBets) updateConfs(inst int32, ballot lwcproto.ConfigBal) {
	if _, e := manager.confs[inst]; !e {
		manager.confs[inst] = make(map[int16]struct{})
	}
	manager.confs[inst][ballot.PropID] = struct{}{}
	dlog.AgentPrintfN(manager.id, "Now observing %d attempts in instance %d", len(manager.confs[inst]), inst)
}

func (manager *SimpleHedgedBets) LearnBallotChosen(instanceSpace *[]*ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal) {
	if inst > manager.crtInstance {
		manager.SimpleGlobalManager.UpdateCurrentInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	manager.updateConfs(inst, ballot)
	manager.OpenInstSignal.CheckChosen(pbk, inst, ballot)
	manager.SingleInstanceManager.HandleProposalChosen(pbk, inst, ballot)
	manager.conflictEWMA = mathextra.EwmaAdd(manager.conflictEWMA, manager.ewmaWeight, float64(len(manager.confs[inst])))
	delete(manager.confs, inst) // will miss some as a result but its okie to avoid too much memory growth
}

func (manager *SimpleHedgedBets) DecideRetry(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	doRetry := manager.SimpleGlobalManager.DecideRetry(pbk, retry)
	//if !doRetry {
	//	return doRetry
	//}
	//delete(decider.confs[retry.Inst], retry.PreempterBal.PropID)
	//decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, float64(len(decider.confs[retry.Inst]))) // means that the instance has less proposers
	//todo should wait until all attempts have failed??
	return doRetry
}

func (manager *SimpleHedgedBets) updateHedgeSize() {
	// get average number of proposals made per instance
	newHedge := int32(manager.conflictEWMA + 0.5)
	if newHedge < 1 {
		newHedge = 1
	}

	if manager.currentHedgeNum > newHedge {
		dlog.AgentPrintfN(manager.id, "Decreasing hedged size")
	} else if newHedge > manager.currentHedgeNum {
		dlog.AgentPrintfN(manager.id, "Increasing hedged size")
	}

	manager.currentHedgeNum = newHedge
	dlog.AgentPrintfN(manager.id, "Current hedged bet is of size %d (EWMA is %f)", manager.currentHedgeNum, manager.conflictEWMA)
}

func (manager *SimpleHedgedBets) LearnNoop(inst int32, who int32) {
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
