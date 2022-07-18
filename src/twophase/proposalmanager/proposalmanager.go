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
	StartNextInstance(instanceSpace *[]*PBK, startFunc func(inst int32)) []int32
	StartNextProposal(initiator *PBK, inst int32)
	//ProposingValue(instanceSpace *[]*PBK, inst int32, ballot stdpaxosproto.Ballot)
	LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool // returns if proposer's ballot is preempted
	LearnOfBallotAccepted(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32)
	LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal)
	DecideRetry(pbk *PBK, retry RetryInfo) bool
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

//type ProposerEager struct {
//	reservationRPC  uint8
//	reservationChan chan fastrpc.Serializable
//	n               int32
//	crtInst         int32
//	id              int32
//
//	reservationsFrom  map[int32][]int32            // each instance attempted has a list of reservations returned -- choose max
//	reservationsGiven map[int32]map[int32]struct{} // avoid duplication
//	reservationOwners map[int32]int32              // instances and who has reserved them
//}

type Eager struct {
	n           int32
	crtInstance int32
	SingleInstanceManager
	*EagerSig
	*BackoffManager
	id int32
	*genericsmr.Replica
	stateRpc             uint8
	reservationsMade     map[int32]struct{}           // our reservations
	reservationsGivenOut map[int32]map[int32]struct{} // avoid duplication in response to prepare, accept, and chosen per instance
	propWithReservation  map[int32]int32              // reservation on an instance given out to others -- need to track to break ties
	instanceagentmapper.InstanceSetMapper

	//expectingProposers map[int32]map[int32]struct{}
	//expectingInsts

	theshold     map[int32]*int32
	reservedFrom map[int32]map[int32]struct{}

	//proposerReservations []int32 // if we get a proposer in a reserved instance -- decrement
	// if we get a proposer who is not reserved -- increment (as long as less than oi?)

	//reservedInstances map[int32]struct{} // if we get a
	//reservedProps     []int32

	//propReservations [][]int32 // for each proposer maintain a list of instances reserved for them
}

// todo, if got proposal for ix, and have give reservations for ix, then move to icrt + 1 for reservations

func EagerForwardInductionProposalManagerNew(n int32, id int32, signal *EagerSig, iManager SingleInstanceManager, backoffManager *BackoffManager, replica *genericsmr.Replica, stateRPC uint8) *Eager {
	ids := make([]int, n)
	for i := 0; i < int(n); i++ {
		ids[i] = i
	}
	e := &Eager{
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
		propWithReservation:   make(map[int32]int32),
		//propReservationke([][]int32, n),
		InstanceSetMapper: instanceagentmapper.InstanceSetMapper{
			Ids: ids,
			G:   int(n),
			N:   int(n),
		},
	}
	//for i := 0; i < int(n); i++ {
	//	e.propReservations[i] = make([]int32, 0, e.MaxOpenInsts)
	//}
	return e
}

//func (manager *Eager) isReserved(pid int, inst int32) bool {
//	for i := 0; i < len(manager.propReservations[pid]); i++ {
//		if manager.propReservations[pid][i] == inst {
//			return true
//		}
//	}
//	return false
//}

func (manager *Eager) GetCrtInstance() int32 {
	return manager.crtInstance
}

func (manager *Eager) StartNextInstance(instanceSpace *[]*PBK, startFunc func(inst int32)) []int32 {
	next := int32(-1)
	if len(manager.reservationsMade) == 0 {
		dlog.AgentPrintfN(manager.id, "Starting next instance in log")
		next = manager.crtInstance + 1
		manager.crtInstance++
		if (*instanceSpace)[next] != nil {
			panic("alsdjfalksdjf;e")
		}

	} else {
		//min := int32(0)
		//for i, _ := range manager.reservationsMade { // does this improve likelihood that we choose correct reservation?
		//	if i > min {
		//		min = i
		//	}
		//}
		//toDel := len(manager.reservationsMade) - int(manager.MaxOpenInsts)
		//dlog.AgentPrintfN(manager.id, "Need to delete %d instances from reservation", toDel)
		//for d := toDel; d > 0; d-- {
		//	min := int32(math.MaxInt32)
		//	for i, _ := range manager.reservationsMade {
		//		if i < min {
		//			min = i
		//		}
		//	}
		//	dlog.AgentPrintfN(manager.id, "Not proposing to instance %d as we are going to assume someone else is proposing to it", min)
		//	delete(manager.reservationsMade, min)
		//}

		min := int32(math.MaxInt32)
		for i, _ := range manager.reservationsMade {
			if i < min {
				min = i
			}
		}
		delete(manager.reservationsMade, min)
		next = min
		dlog.AgentPrintfN(manager.id, "Starting reserved instance")

		if next > manager.crtInstance {
			panic("oh noonono")
			//manager.HandleNewInstance(instanceSpace, next)
		}
	}

	//if  + manager.n *

	//if (*instanceSpace)[next] == nil {
	//	panic("adfjasldkfjalskdjfals;dkfjals;kdfjasl;kdfjasl;kdfjasl;kdfjasl;dkfjasdkl;fjds")
	//}
	if (*instanceSpace)[next] != nil {
		pbk := (*instanceSpace)[next]
		if pbk.Status != BACKING_OFF && pbk.Status != NOT_BEGUN {
			panic("aslkjflaksdfj")
		}
		if !pbk.MaxKnownBal.Equal(getReservationGivenBal()) && !pbk.MaxKnownBal.IsZero() {
			panic("asldkfjalskdfjlkasdflaksdjf;laksdjf;lkjel;kjfa;slkdfj")
		}
	}

	manager.reservationsGivenOut[next] = make(map[int32]struct{})
	(*instanceSpace)[next] = manager.SingleInstanceManager.InitInstance(next)
	startFunc(next)
	opened := []int32{next}
	manager.EagerSig.Opened(opened)
	return opened
}

func (manager *Eager) DecideRetry(pbk *PBK, retry RetryInfo) bool {
	return manager.SingleInstanceManager.ShouldRetryInstance(pbk, retry)
}

func (manager *Eager) StartNextProposal(pbk *PBK, inst int32) {
	manager.SingleInstanceManager.StartProposal(pbk, inst)
	//if _, e := manager.reservationsMade[inst]; e {
	//	dlog.AgentPrintfN(manager.id, "No longer reserving instance %d to propose in", inst)
	delete(manager.reservationsMade, inst)
	//}
}

func (manager *Eager) HandleNewInstance(iSpace *[]*PBK, inst int32) {
	if manager.crtInstance >= inst {
		return
	}

	for i := manager.crtInstance + 1; i <= inst; i++ {
		manager.reservationsGivenOut[i] = make(map[int32]struct{})
		(*iSpace)[i] = GetEmptyInstance()
		if _, e := manager.reservationsMade[i]; e {
			continue
		}

		if inst == i { // will be handled by caller
			break
		}

		(*iSpace)[i].Status = BACKING_OFF
		nilB := lwcproto.GetNilConfigBal()
		_, bot := manager.CheckAndHandleBackoff(i, nilB, nilB, stdpaxosproto.PROMISE)
		if bot == -1 {
			panic("??")
		}
		dlog.AgentPrintfN(manager.id, "Backing off newly received instance %d for %d microseconds", i, bot)
	}
	manager.crtInstance = inst
}

func (manager *Eager) prepareShouldGiveOutReservation(ballot lwcproto.ConfigBal, inst int32, pbk *PBK) bool {
	if int32(ballot.PropID) == manager.id || ballot.PropID == -1 {
		return false
	}
	if _, e := manager.reservationsGivenOut[inst][int32(ballot.PropID)]; e {
		return false
	}
	if (ballot.GreaterThan(pbk.MaxKnownBal) || ballot.Equal(pbk.MaxKnownBal)) && pbk.ProposeValueBal.IsZero() { //|| !pbk.ProposeValueBal.IsZero() {
		return false
	}
	return true
}

func (manager *Eager) acceptShouldGiveOutReservation(ballot lwcproto.ConfigBal, inst int32) bool {
	if int32(ballot.PropID) == manager.id || ballot.PropID == -1 {
		return false
	}
	if _, e := manager.reservationsGivenOut[inst][int32(ballot.PropID)]; e {
		return false
	}
	return true
}

func (manager *Eager) acceptedShouldGiveReservation(inst int32, ballot lwcproto.ConfigBal) bool {
	if int32(ballot.PropID) == manager.id {
		return false
	}
	if _, e := manager.reservationsGivenOut[inst][int32(ballot.PropID)]; e {
		return false
	}
	return true
}

func (manager *Eager) chosenShouldGiveOutReservation(at lwcproto.ConfigBal, inst int32) bool {
	if int32(at.PropID) == manager.id {
		return false
	}
	if _, e := manager.reservationsGivenOut[inst][int32(at.PropID)]; e {
		return false
	}
	return true
}

func (manager *Eager) checkIfIsGivenReservation(iSpace *[]*PBK, ballot lwcproto.ConfigBal, pbk *PBK, inst int32) bool {
	if ballot.Number == -2 {
		manager.HandleNewInstance(iSpace, inst)
		dlog.AgentPrintfN(manager.id, "Got invitation to reserve instance %d from %d", inst, ballot.PropID)

		if pbk != nil {
			rBal := getReservationGivenBal()
			if pbk.MaxKnownBal.GreaterThan(rBal) { // have we reserved for someone else??
				dlog.AgentPrintfN(manager.id, "Ignoring invitation of reservation for instance %d to propose in. We've received another proposal for instance", inst)
				//manager.reservationsMade[manager.crtInstance+1] = struct{}{}
				//manager.HandleNewInstance(iSpace, manager.crtInstance+1)
				return true
			}
		}

		//if givenTo, e := manager.propWithReservation[inst]; e {
		//	g := manager.InstanceSetMapper.GetGroup(int(inst))
		//	for _, pid := range g {
		//		if pid == int(givenTo) {
		//			dlog.AgentPrintfN(manager.id, "Ignoring invitation as we have given out before to someone with priority (ranking is %v)", g)
		//			//manager.reservationsMade[manager.crtInstance+1] = struct{}{}
		//			//manager.HandleNewInstance(iSpace, manager.crtInstance+1)
		//			return true
		//		}
		//
		//		if pid == int(manager.id) {
		//			break
		//		}
		//	}
		//}

		manager.reservationsMade[inst] = struct{}{}
		dlog.AgentPrintfN(manager.id, "Reserving instance %d to attempt next", inst)
		// todo if give reservation, then make sure that the next ones we give follow it
		//manager.EagerSig.Close(inst)
		//manager.EagerSig.DoSig()

		return true
	}
	return false
}

func (manager *Eager) giveReservationTo(instanceSpace *[]*PBK, becauseInst int32, prop int32, reason string) {
	manager.reservationsGivenOut[becauseInst][prop] = struct{}{}
	manager.Replica.SendMsg(prop, manager.stateRpc, manager.GetReservationMsg())
	dlog.AgentPrintfN(manager.id, "Assuming that proposer %d is going to make a new proposal to instance %d following %s in instance %d", prop, manager.crtInstance+1, reason, becauseInst)
	if _, e := manager.propWithReservation[manager.crtInstance+1]; e {
		panic("askdjfllkeja;")
	}
	manager.propWithReservation[manager.crtInstance+1] = prop
	manager.LearnOfBallot(instanceSpace, manager.crtInstance+1, getReservationGivenBal(), stdpaxosproto.PROMISE)
}

func (manager *Eager) GetReservationMsg() *proposerstate.State {
	return &proposerstate.State{
		ProposerID:      manager.id,
		CurrentInstance: manager.crtInstance + 1,
	}
}

func getReservationGivenBal() lwcproto.ConfigBal {
	return lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: 10000, PropID: -1}}
}

func (manager *Eager) checkIfLostReservation(ballot lwcproto.ConfigBal, inst int32) {
	if _, e := manager.reservationsMade[inst]; e {
		dlog.AgentPrintfN(manager.id, "No longer reserving instance %d to attempt next", inst)
		delete(manager.reservationsMade, inst)
	}
}

func (manager *Eager) LearnOfBallot(iSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	if manager.checkIfIsGivenReservation(iSpace, ballot, (*iSpace)[inst], inst) {
		return false
	}

	// actual proposal
	manager.HandleNewInstance(iSpace, inst)
	pbk := (*iSpace)[inst]
	manager.checkIfLostReservation(ballot, inst)
	manager.EagerSig.CheckOngoingBallot(pbk, inst, ballot, phase)

	oldProp := int32(pbk.MaxKnownBal.PropID)
	newProp := int32(ballot.PropID)
	acked := manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)

	if !acked {
		// handle giving reservation/preempt
		manager.giveReservationTo(iSpace, inst, newProp, "is preempted")
		return acked
	}

	// handle old proposer preempt
	if oldProp != -1 && oldProp != manager.id && oldProp != int32(ballot.PropID) {
		manager.giveReservationTo(iSpace, inst, newProp, "has been preempted by newely recevied proposal")
		// give reservation to
	}

	// if inst + 1 is reserved, then don't give out

	// This is an acknoweldeged msg
	//if manager.reservedProps[newProp] > 0 {
	//	manager.reservedProps[newProp] = manager.reservedProps[newProp] - 1
	//}

	// has taken up reservation
	//_, instReserved := manager.reservedInstances[inst]
	//if instReserved {
	//	delete(manager.reservedInstances, inst)
	//}

	//if manager.propReservations

	//manager.reservedInstances[]

	// give reservation to oldie
	//	manager.giveReservationTo(iSpace, inst, oldProp, "preempting of previous ballot")
	//	return acked
	//}

	// give reservation to newbie
	//if phase == stdpaxosproto.PROMISE && !manager.prepareShouldGiveOutReservation(ballot, inst, pbk) {
	//	return acked
	//}
	//if phase == stdpaxosproto.ACCEPTANCE && !manager.acceptShouldGiveOutReservation(ballot, inst) {
	//	return acked
	//}
	//str := "preempt"
	//if phase == stdpaxosproto.ACCEPTANCE && pbk.MaxKnownBal.Equal(ballot) {
	//	str = "acceptance"
	//}
	//manager.giveReservationTo(iSpace, inst, int32(ballot.PropID), fmt.Sprintf("this %s message", str))
	return acked
}

func (manager *Eager) LearnOfBallotAccepted(iSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	manager.HandleNewInstance(iSpace, inst)
	pbk := (*iSpace)[inst]
	manager.EagerSig.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
	manager.checkIfLostReservation(ballot, inst)
	//if !manager.acceptedShouldGiveReservation(inst, ballot) {
	//	return
	//}
	//manager.giveReservationTo(iSpace, inst, int32(ballot.PropID), "this notification of acceptance")
}

func (manager *Eager) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, at lwcproto.ConfigBal) {
	manager.HandleNewInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.checkIfLostReservation(at, inst)
	if at.IsZero() {
		panic("asjdflasdlkfj")
	}
	manager.EagerSig.CheckChosen(pbk, inst, at)
	manager.SingleInstanceManager.HandleProposalChosen(pbk, inst, at)

	//if !manager.chosenShouldGiveOutReservation(at, inst) {
	//	return
	//}
	//
	//manager.giveReservationTo(instanceSpace, inst, int32(at.PropID), fmt.Sprintf("learning instance %d is chosen", inst))
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

func (manager *SimpleGlobalManager) StartNextInstance(instanceSpace *[]*PBK, startFunc func(inst int32)) []int32 {
	manager.crtInstance = manager.crtInstance + 1

	if (*instanceSpace)[manager.crtInstance] != nil {
		panic("aslkdjfalksjflekjals;kdfj")
	}

	(*instanceSpace)[manager.crtInstance] = manager.SingleInstanceManager.InitInstance(manager.crtInstance)
	startFunc(manager.crtInstance)
	opened := []int32{manager.crtInstance}
	manager.OpenInstSignal.Opened(opened)
	return opened
}

func (manager *SimpleGlobalManager) DecideRetry(pbk *PBK, retry RetryInfo) bool {
	return manager.SingleInstanceManager.ShouldRetryInstance(pbk, retry)
}

func (manager *SimpleGlobalManager) StartNextProposal(pbk *PBK, inst int32) {
	manager.SingleInstanceManager.StartProposal(pbk, inst)
	//if pbk.PropCurBal.Number > 20000 {
	//	panic("askdfjasldkfj")
	//}
}

func (manager *SimpleGlobalManager) UpdateCurrentInstance(instsanceSpace *[]*PBK, inst int32) {
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
		_, bot := manager.CheckAndHandleBackoff(i, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
		dlog.AgentPrintfN(manager.id, "Backing off newly received instance %d for %d microseconds", i, bot)
	}
	dlog.AgentPrintfN(manager.id, "Setting instance %d as current instance", inst)
	manager.crtInstance = inst
}

func (manager *SimpleGlobalManager) LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.OpenInstSignal.CheckOngoingBallot(pbk, inst, ballot, phase)
	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (manager *SimpleGlobalManager) LearnOfBallotAccepted(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	//if inst > manager.crtInstance {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	//}
	pbk := (*instanceSpace)[inst]
	manager.OpenInstSignal.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
	return //manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot)
}

func (manager *SimpleGlobalManager) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, at lwcproto.ConfigBal) {
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

func (manager *MappedGlobalManager) StartNextInstance(instanceSpace *[]*PBK, startFunc func(inst int32)) []int32 {
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

func (manager *MappedGlobalManager) LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	manager.checkAndSetNewInstance(instanceSpace, inst, ballot, phase)
	return manager.SimpleGlobalManager.LearnOfBallot(instanceSpace, inst, ballot, phase)
	//return manager.SingleInstanceManager.HandleReceivedBallot((*instanceSpace)[inst], inst, ballot, phase)
}

func (manager *MappedGlobalManager) checkAndSetNewInstance(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) {
	// todo check if all prior instances are opened?
	if (*instanceSpace)[inst] == nil {
		(*instanceSpace)[inst] = GetEmptyInstance()
		dlog.AgentPrintfN(manager.id, "Witnessed new instance %d, we have set instance %d now to be our current instance", inst, manager.crtInstance)
	}
}

func (manager *MappedGlobalManager) LearnOfBallotAccepted(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	if (*instanceSpace)[inst] == nil {
		(*instanceSpace)[inst] = GetEmptyInstance()
	}
	pbk := (*instanceSpace)[inst]
	manager.OpenInstSignal.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
	//return //manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot)
}

func (manager *MappedGlobalManager) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal) {
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

func (decider *DynamicMappedGlobalManager) StartNextInstance(instanceSpace *[]*PBK, startFunc func(inst int32)) []int32 {
	decider.updateGroupSize()
	return decider.MappedGlobalManager.StartNextInstance(instanceSpace, startFunc)
}

func (decider *DynamicMappedGlobalManager) LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
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

func (decider *DynamicMappedGlobalManager) DecideRetry(pbk *PBK, retry RetryInfo) bool {
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

func (decider *DynamicMappedGlobalManager) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal) {
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

func (manager *SimpleHedgedBets) StartNextInstance(instanceSpace *[]*PBK, startFunc func(inst int32)) []int32 {
	manager.updateHedgeSize()
	opened := make([]int32, 0, manager.currentHedgeNum)
	for i := int32(0); i < manager.currentHedgeNum; i++ {
		opened = append(opened, manager.SimpleGlobalManager.StartNextInstance(instanceSpace, startFunc)...)
	}
	manager.OpenInstSignal.Opened(opened)
	return opened
}

func (manager *SimpleHedgedBets) LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	if inst > manager.crtInstance {
		manager.SimpleGlobalManager.UpdateCurrentInstance(instanceSpace, inst)
	}

	pbk := (*instanceSpace)[inst]
	manager.updateConfs(inst, ballot)
	manager.OpenInstSignal.CheckOngoingBallot(pbk, inst, ballot, phase)
	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (manager *SimpleHedgedBets) LearnOfBallotAccepted(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
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

func (manager *SimpleHedgedBets) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal) {
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

func (manager *SimpleHedgedBets) DecideRetry(pbk *PBK, retry RetryInfo) bool {
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
