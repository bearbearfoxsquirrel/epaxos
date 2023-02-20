package proposer

import (
	"epaxos/dlog"
	"epaxos/instanceagentmapper"
	"epaxos/lwcproto"
	"epaxos/mathextra"
	"epaxos/stdpaxosproto"
	"epaxos/twophase/balloter"
	"epaxos/twophase/mapper"
	"sort"
)

// PROPOSER INTERFACES
type ProposalManager interface {
	StartNextInstance(instanceSpace *[]*PBK) []int32
	StartNextProposal(initiator *PBK, inst int32)
	LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool // returns if proposer's ballot is preempted
	LearnOfBallotAccepted(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32)
	//LearntOfBallotValue(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32)
	LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32)
	DecideRetry(pbk *PBK, retry RetryInfo) bool
}

type CrtInstanceOracle interface {
	GetCrtInstance() int32
}

type NoopLearner interface {
	LearnNoop(inst int32, who int32)
}

type Proposer interface {
	CrtInstanceOracle
	ProposalManager
	GetBalloter() *balloter.Balloter
	GetStartInstanceSignaller() <-chan struct{}
}

type EagerByExecProposer interface {
	Proposer
	GetExecSignaller() ExecOpenInstanceSignal
}

// PROPOSER IMPLEMENTATIONS
// BASELINE PROPOSER
type BaselineManager struct {
	CrtInstance int32
	SingleInstanceManager
	BallotOpenInstanceSignal
	*BackoffManager
	Id int32
	OpenInstSignal
	*balloter.Balloter
}

func (manager *BaselineManager) GetStartInstanceSignaller() <-chan struct{} {
	return manager.OpenInstSignal.GetSignaller()
}

func (manager *BaselineManager) GetBalloter() *balloter.Balloter {
	return manager.Balloter
}

func BaselineProposerNew(id int32, sig OpenInstSignal, signal BallotOpenInstanceSignal, iManager SingleInstanceManager, backoffManager *BackoffManager, balloter *balloter.Balloter) *BaselineManager {
	return &BaselineManager{
		CrtInstance:              -1,
		SingleInstanceManager:    iManager,
		BallotOpenInstanceSignal: signal,
		BackoffManager:           backoffManager,
		Id:                       id,
		OpenInstSignal:           sig,
		Balloter:                 balloter,
	}
}

func (manager *BaselineManager) GetCrtInstance() int32 { return manager.CrtInstance }

func (manager *BaselineManager) StartNextInstance(instanceSpace *[]*PBK) []int32 {
	manager.CrtInstance = manager.CrtInstance + 1
	for (*instanceSpace)[manager.CrtInstance] != nil {
		manager.CrtInstance = manager.CrtInstance + 1
	}
	if (*instanceSpace)[manager.CrtInstance] != nil {
		panic("instance already started")
	}
	(*instanceSpace)[manager.CrtInstance] = manager.SingleInstanceManager.InitInstance(manager.CrtInstance)
	pbk := (*instanceSpace)[manager.CrtInstance]
	opened := []int32{manager.CrtInstance}
	manager.OpenInstSignal.Opened(opened)
	manager.StartNextProposal(pbk, manager.CrtInstance)
	return opened
}

func (manager *BaselineManager) DecideRetry(pbk *PBK, retry RetryInfo) bool {
	return manager.SingleInstanceManager.ShouldRetryInstance(pbk, retry)
}

func (manager *BaselineManager) StartNextProposal(pbk *PBK, inst int32) {
	manager.SingleInstanceManager.StartProposal(pbk, inst)
}

func (manager *BaselineManager) UpdateCurrentInstance(instsanceSpace *[]*PBK, inst int32) {
	if inst <= manager.CrtInstance {
		return
	}
	if (*instsanceSpace)[inst] != nil {
		return
	}
	(*instsanceSpace)[inst] = GetEmptyInstance()
	for (*instsanceSpace)[manager.CrtInstance+1] != nil {
		manager.CrtInstance += 1
	}
	dlog.AgentPrintfN(manager.Id, "Setting instance %d as current instance", inst)
}

func (manager *BaselineManager) LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.BallotOpenInstanceSignal.CheckOngoingBallot(pbk, inst, ballot, phase)
	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (manager *BaselineManager) LearnOfBallotAccepted(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.BallotOpenInstanceSignal.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
	return //manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot)
}

func (manager *BaselineManager) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.BallotOpenInstanceSignal.CheckChosen(pbk, inst, ballot, whoseCmds)
	manager.SingleInstanceManager.HandleProposalChosen(pbk, inst, ballot)
}

// INDUCTIVE CONFLICTS PROPOSER
type InductiveConflictsManager struct {
	*BaselineManager
}

func NewInductiveConflictsManager(baselineManager *BaselineManager) *InductiveConflictsManager {
	return &InductiveConflictsManager{BaselineManager: baselineManager}
}

func (manager *InductiveConflictsManager) StartNextInstance(instanceSpace *[]*PBK) []int32 {
	manager.CrtInstance = manager.CrtInstance + 1
	if (*instanceSpace)[manager.CrtInstance] != nil {
		panic("aslkdjfalksjflekjals;kdfj")
	}
	(*instanceSpace)[manager.CrtInstance] = manager.SingleInstanceManager.InitInstance(manager.CrtInstance)
	pbk := (*instanceSpace)[manager.CrtInstance]
	opened := []int32{manager.CrtInstance}
	manager.OpenInstSignal.Opened(opened)
	manager.StartNextProposal(pbk, manager.CrtInstance)
	return opened
}

func (manager *InductiveConflictsManager) UpdateCurrentInstance(instsanceSpace *[]*PBK, inst int32) {
	if inst <= manager.CrtInstance {
		return
	}

	// take advantage of inductive backoff property
	for i := manager.CrtInstance + 1; i <= inst; i++ {
		(*instsanceSpace)[i] = GetEmptyInstance()
		if i == inst {
			break
		}
		(*instsanceSpace)[i].Status = BACKING_OFF
		_, bot := manager.CheckAndHandleBackoff(i, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
		dlog.AgentPrintfN(manager.Id, "Backing off induced instance %d for %d microseconds", i, bot)
	}
	dlog.AgentPrintfN(manager.Id, "Setting instance %d as current instance", inst)
	manager.CrtInstance = inst
}

func (manager *InductiveConflictsManager) LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.BallotOpenInstanceSignal.CheckOngoingBallot(pbk, inst, ballot, phase)
	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (manager *InductiveConflictsManager) LearnOfBallotAccepted(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.BallotOpenInstanceSignal.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
	return //manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot)
}

func (manager *InductiveConflictsManager) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.BallotOpenInstanceSignal.CheckChosen(pbk, inst, ballot, whoseCmds)
	manager.SingleInstanceManager.HandleProposalChosen(pbk, inst, ballot)
}

type Eager struct {
	InductiveConflictsManager
	ExecOpenInstanceSignal
}

func (e Eager) GetExecSignaller() ExecOpenInstanceSignal {
	return e.ExecOpenInstanceSignal
}

func EagerManagerNew(manager InductiveConflictsManager, signal ExecOpenInstanceSignal) *Eager {
	return &Eager{InductiveConflictsManager: manager, ExecOpenInstanceSignal: signal}
}

type EagerFI struct {
	CrtInstance int32

	InducedUpTo       int32
	Induced           map[int32]map[int32]stdpaxosproto.Ballot // for each instance
	DoChosenFWI       bool
	DoValueFWI        bool
	DoLateProposalFWI bool

	Id int32
	N  int32

	SingleInstanceManager
	*BackoffManager
	Windy map[int32][]int32

	*EagerExecUpToSig
	*balloter.Balloter

	Forwarding int32
	MaxStarted int32
	MaxAt      map[int32]int32
}

func (manager *EagerFI) GetStartInstanceSignaller() <-chan struct{} {
	return manager.EagerExecUpToSig.GetSignaller()
}

func (manager *EagerFI) GetCrtInstance() int32 {
	return manager.CrtInstance
}

func (manager *EagerFI) DecideRetry(pbk *PBK, retry RetryInfo) bool {
	return manager.SingleInstanceManager.ShouldRetryInstance(pbk, retry)
}

func (manager *EagerFI) GetBalloter() *balloter.Balloter {
	return manager.Balloter
}

func (manager *EagerFI) GetStartInstanceChan() <-chan struct{} {
	return manager.sigNewInst
}

func (manager *EagerFI) GetExecSignaller() ExecOpenInstanceSignal {
	return manager.EagerExecUpToSig
}

func (manager *EagerFI) addToWindy(index int, inst int32, pid int32) {
	//manager.Windy[pid] := append(manager.Windy[pid], inst)
	//sort.Slice(manager.Windy[pid], func(i, j int) bool {
	//	return i < j
	//})

	//sort.IntsAreSorted(manager.Windy[pid])
	//sort

	for i := 1; i <= index; i++ {
		manager.Windy[pid][i-1] = manager.Windy[pid][i]
	} //index, wInst := range manager.Windy[pid] {
	manager.Windy[pid][index] = inst
	dlog.AgentPrintfN(manager.id, "Pos %d in windy updated to %d", index, manager.Windy[pid][index])
	for i := 0; i < len(manager.Windy[pid]); i++ {
		dlog.AgentPrintfN(manager.id, "Pos %d in windy is %d", i, manager.Windy[pid][i])
	}
	for i := 0; i < len(manager.Windy[pid])-1; i++ {
		if manager.Windy[pid][i] > manager.Windy[pid][i+1] {
			panic("bad order")
		}
	}
}

func (manager *EagerFI) UpdateCurrentInstance(instsanceSpace *[]*PBK, inst int32, pid int32) {
	if manager.CrtInstance >= inst {
		return
	}
	if pid == manager.PropID || pid == -1 {
		return
	}
	// Create instance
	if (*instsanceSpace)[inst] == nil {
		(*instsanceSpace)[inst] = GetEmptyInstance()
	}

	// Update Windy
	for i := 0; i < len(manager.Windy[pid]); i++ {
		if manager.Windy[pid][i] == inst {
			return
		}
	}
	//dlog.AgentPrintfN(manager.id, "old window is %v", manager.Windy[pid])
	manager.Windy[pid] = append(manager.Windy[pid], inst)
	//dlog.AgentPrintfN(manager.id, "uncompressed window is %v", manager.Windy[pid])
	sort.Slice(manager.Windy[pid], func(i, j int) bool {
		return i < j
	})
	manager.Windy[pid] = manager.Windy[pid][1:]
	//dlog.AgentPrintfN(manager.id, "new window is %v", manager.Windy[pid])

	newcrt := manager.CrtInstance
	for _, windy := range manager.Windy {
		//for i := 0; i < len(manager.Windy[pid]); i++ {
		//	dlog.AgentPrintfN(manager.id, "for proposer %d pos %d in windy is %d", prop, i, windy[i])
		//}
		if newcrt >= windy[0] {
			continue
		}
		newcrt = windy[0]
	}

	if manager.CrtInstance == newcrt {
		return
	}

	for i := manager.CrtInstance + 1; i <= newcrt; i++ {
		if (*instsanceSpace)[i] != nil {
			continue
		}
		(*instsanceSpace)[i] = GetEmptyInstance()
		if i == inst {
			break
		}
		(*instsanceSpace)[i].Status = BACKING_OFF
		_, bot := manager.CheckAndHandleBackoff(i, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
		dlog.AgentPrintfN(manager.Id, "Backing off induced instance %d for %d microseconds", i, bot)
	}
	manager.CrtInstance = newcrt
	dlog.AgentPrintfN(manager.Id, "Setting instance %d as current instance", manager.CrtInstance)
}

func (manager *EagerFI) StartNextInstance(instanceSpace *[]*PBK) []int32 {
	for {
		manager.CrtInstance = manager.CrtInstance + 1
		if (*instanceSpace)[manager.CrtInstance] != nil {
			continue
		}

		inductUpTo := manager.GetInducedInstances(instanceSpace)
		if inductUpTo == 0 {
			break
		}

		dlog.AgentPrintfN(manager.Id, "Skipping the next %d instances (until instance %d) as we have deduced someone will propose to them", inductUpTo, manager.CrtInstance+inductUpTo)
		for i := manager.CrtInstance; i < manager.CrtInstance+inductUpTo; i++ {
			if (*instanceSpace)[i] != nil {
				continue
			}
			(*instanceSpace)[i] = manager.SingleInstanceManager.InitInstance(i)
			(*instanceSpace)[i].Status = BACKING_OFF
			_, bot := manager.CheckAndHandleBackoff(i, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
			dlog.AgentPrintfN(manager.Id, "Backing off instance %d for %d microseconds as we expect someone else to propose to it", i, bot)
		}
		manager.CrtInstance += inductUpTo - 1
		dlog.AgentPrintfN(manager.Id, "Setting instance %d as current instance", manager.CrtInstance)
	}

	if (*instanceSpace)[manager.CrtInstance] != nil {
		panic("Cannot open already started instance")
	}

	(*instanceSpace)[manager.CrtInstance] = manager.SingleInstanceManager.InitInstance(manager.CrtInstance)
	pbk := (*instanceSpace)[manager.CrtInstance]
	opened := []int32{manager.CrtInstance}
	manager.EagerExecUpToSig.Opened(opened)
	manager.StartNextProposal(pbk, manager.CrtInstance)
	return opened
}

func (manager *EagerFI) StartNextProposal(pbk *PBK, inst int32) {
	manager.SingleInstanceManager.StartProposal(pbk, inst)
}

func (manager *EagerFI) GetInducedInstances(instanceSpace *[]*PBK) int32 {
	inductUpTo := int32(0)
	if manager.CrtInstance < manager.Forwarding*manager.N {
		return inductUpTo
	}
	for i := int32(1); i <= manager.Forwarding*manager.N; i++ { // will return 0 for all negative i values
		k := int32(len(manager.Induced[manager.CrtInstance-i])) - (i - 1) // extend this to being in the proposer?
		if !(*instanceSpace)[manager.CrtInstance-i].PropCurBal.IsZero() {
			k -= 1
		}
		dlog.AgentPrintfN(manager.Id, "For instance %d, there are %d induced proposals", manager.CrtInstance-(i), len(manager.Induced[manager.CrtInstance-i]))
		if k <= 0 {
			continue
		}
		inductUpTo += k
	}
	return inductUpTo
}

// if closed and get new proposal then increment
func (manager *EagerFI) UpdateChosenFI(pbk *PBK, inst int32, ballot stdpaxosproto.Ballot) {
	if !manager.DoChosenFWI {
		return
	}
	if !manager.RelevantToNextStartingInstance(inst) {
		return
	}
	if manager.CrtInstance >= inst {
		return
	}
	if pbk.Status == CLOSED {
		return
	}
	if pbk.Status == BACKING_OFF && !pbk.MaxKnownBal.IsZero() {
		return // don't induce after signalling to start new instance
	}
	pid := int32(ballot.PropID)
	if pid == manager.Id {
		return
	}
	if _, e := manager.Induced[inst]; !e {
		manager.Induced[inst] = make(map[int32]stdpaxosproto.Ballot)
	}
	if _, e := manager.Induced[inst][pid]; e {
		return
	}
	manager.Induce(pid, inst, ballot, "it was chosen")
}

func (manager *EagerFI) Induce(pid int32, inst int32, ballot stdpaxosproto.Ballot, because string) {
	if pid < 0 || pid > manager.N || pid == manager.Id {
		panic("Bad proposer number")
	}
	if ballot.IsZero() {
		panic("Bad ballot")
	}
	manager.Induced[inst][pid] = ballot
	dlog.AgentPrintfN(manager.Id, "Inducing forward proposer %d from instance %d at ballot %d.%d because %s", pid, inst, ballot.Number, ballot.PropID, because)
}

func (manager *EagerFI) RelevantToNextStartingInstance(inst int32) bool {
	return manager.CrtInstance+1 >= inst-(manager.Forwarding*manager.N)
}

func (manager *EagerFI) UpdateValueFI(pbk *PBK, inst int32, ballot stdpaxosproto.Ballot) {
	if !manager.DoValueFWI {
		return
	}
	if !manager.RelevantToNextStartingInstance(inst) {
		return
	}
	pid := int32(ballot.PropID)

	if pid == manager.Id {
		return
	}
	if ballot.IsZero() {
		return
	}

	if pbk.Status == BACKING_OFF && !pbk.MaxKnownBal.IsZero() { //.GreaterThan(pbk.PropCurBal) {
		return // don't induce after signalling to start new instance
	}

	if _, e := manager.Induced[inst]; !e {
		manager.Induced[inst] = make(map[int32]stdpaxosproto.Ballot)
	}
	if _, e := manager.Induced[inst][pid]; e {
		return
	}
	manager.Induce(pid, inst, ballot, "a value was proposed to it")
	//dlog.AgentPrintfN(manager.Id, "Iduced cus of value")
}

func (manager *EagerFI) GetPreempted(pbk *PBK, bal stdpaxosproto.Ballot) (int32, stdpaxosproto.Ballot) {
	if pbk.MaxKnownBal.Ballot.GreaterThan(bal) {
		return int32(bal.PropID), bal
	}
	return int32(pbk.MaxKnownBal.PropID), pbk.MaxKnownBal.Ballot
}

// (Inducing|Starting new|Received a (Prepare|Accept))
func (manager *EagerFI) UpdateProposalFI(pbk *PBK, inst int32, ballot stdpaxosproto.Ballot) {
	//if !manager.RelevantToNextStartingInstance(inst) {
	//	return
	//}
	if ballot.IsZero() || pbk.MaxKnownBal.Ballot.IsZero() {
		return
	} // is invalid ballot or first ballot received
	if ballot.Equal(pbk.MaxKnownBal.Ballot) {
		return
	} // is repeat of previously acknowledged ballot
	if int32(ballot.PropID) == manager.id {
		return
	}

	if pbk.Status == BACKING_OFF && pbk.MaxKnownBal.GreaterThan(pbk.PropCurBal) {
		return // don't induce after signalling to start new instance
	}

	if _, e := manager.Induced[inst]; !e {
		manager.Induced[inst] = make(map[int32]stdpaxosproto.Ballot)
	}
	if pbk.Status == CLOSED {
		if !manager.DoLateProposalFWI {
			return
		}
		//return
		_, priorInduced := manager.Induced[inst][int32(ballot.PropID)]
		if priorInduced {
			return
		}
		manager.Induce(int32(ballot.PropID), inst, ballot, "it is already chosen")
		return
	}
	//if pbk.Status == BACKING_OFF && pbk.MaxKnownBal.GreaterThan(pbk.PropCurBal) {
	//	return // don't induce after signalling to start new instance
	//}

	preemptedPiD, bal := manager.GetPreempted(pbk, ballot)
	if preemptedPiD == manager.Id {
		return
	}
	_, priorInduced := manager.Induced[inst][preemptedPiD]
	if priorInduced {
		// happens if preempted pid is preempted after proposing
		return
	}
	manager.Induce(preemptedPiD, inst, bal, "of a preemption")
}

func (manager *EagerFI) LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	manager.UpdateCurrentInstance(instanceSpace, inst, int32(ballot.PropID))
	pbk := (*instanceSpace)[inst]
	manager.UpdateProposalFI(pbk, inst, ballot.Ballot)
	manager.EagerExecUpToSig.CheckOngoingBallot(pbk, inst, ballot, phase)
	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (manager *EagerFI) LearnOfBallotAccepted(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	manager.UpdateCurrentInstance(instanceSpace, inst, int32(ballot.PropID))
	pbk := (*instanceSpace)[inst]
	manager.UpdateValueFI(pbk, inst, ballot.Ballot)
	manager.EagerExecUpToSig.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
	return
}

func (manager *EagerFI) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	manager.UpdateCurrentInstance(instanceSpace, inst, int32(ballot.PropID))
	pbk := (*instanceSpace)[inst]
	manager.UpdateChosenFI(pbk, inst, ballot.Ballot)
	manager.EagerExecUpToSig.CheckChosen(pbk, inst, ballot, whoseCmds)
	manager.SingleInstanceManager.HandleProposalChosen(pbk, inst, ballot)
}

type LoLProposer struct {
	*InductiveConflictsManager
	n          int32
	newInstSig chan<- struct{}
}

func NewLoLProposer(manager *InductiveConflictsManager, n int32, sig chan<- struct{}) *LoLProposer {
	return &LoLProposer{InductiveConflictsManager: manager, n: n, newInstSig: sig}
}

func (manager *LoLProposer) LookBackInstanceOwner(instanceSpace *[]*PBK, i int32) int32 {
	if i < manager.n*10 {
		p := i % manager.n
		dlog.AgentPrintfN(manager.Id, "Ballot 0 for instance %d is allocated to proposer %d", i, p)
		return p
	}
	lbI := i - (manager.n * 10)
	lbInst := (*instanceSpace)[lbI]
	if lbInst.Status != CLOSED {
		p := int32(-1)
		dlog.AgentPrintfN(manager.Id, "Ballot 0 for instance %d is allocated to proposer %d", i, p)
		return p
	}
	p := int32(lbInst.ProposeValueBal.PropID)
	dlog.AgentPrintfN(manager.Id, "Ballot 0 for instance %d is allocated to proposer %d", i, p)
	return p
}

func (manager *LoLProposer) StartNextInstance(instanceSpace *[]*PBK) []int32 {
	iSpace := *instanceSpace
	for {
		manager.CrtInstance += 1
		if iSpace[manager.CrtInstance] != nil {
			continue
		}
		iSpace[manager.CrtInstance] = GetEmptyInstance()
		owner := manager.LookBackInstanceOwner(instanceSpace, manager.CrtInstance)
		pbk := iSpace[manager.CrtInstance]
		if owner != -1 && owner != manager.Id {
			// someone elses
			// backoff
			pbk.Status = BACKING_OFF
			_, bot := manager.CheckAndHandleBackoff(manager.CrtInstance, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
			dlog.AgentPrintfN(manager.Id, "Backing off instance %d for %d microseconds as we expect someone else to propose to it", manager.CrtInstance, bot)
			continue
		}
		if owner == -1 {
			manager.StartNextProposal(pbk, manager.CrtInstance)
			break
		}
		pbk.PropCurBal = lwcproto.ConfigBal{-1, stdpaxosproto.Ballot{0, int16(manager.Id)}}
		pbk.MaxKnownBal = lwcproto.ConfigBal{-1, stdpaxosproto.Ballot{0, int16(manager.Id)}}
		pbk.Status = READY_TO_PROPOSE

		break
		// start in phase 2
	}
	return []int32{manager.CrtInstance}
	// if we are assigned one, we should look ahead another n to see if there is one unassigned????
}

func (manager *LoLProposer) UpdateCurrentInstance(instsanceSpace *[]*PBK, inst int32) {
	if inst <= manager.CrtInstance {
		return
	}

	//if (*instsanceSpace)[inst] == nil {
	//	(*instsanceSpace)[inst] = GetEmptyInstance()
	//}
	// take advantage of inductive backoff property

	// if we know that we are the owner of this instance, then we do not backoff
	// create list of instances that we don't know enough about to inductively backoff, then when opening instance we backoff

	for i := manager.CrtInstance + 1; i <= inst; i++ {
		if (*instsanceSpace)[inst] != nil {
			continue
		}
		(*instsanceSpace)[i] = GetEmptyInstance()
		if manager.LookBackInstanceOwner(instsanceSpace, inst) == manager.Id {
			go func() { manager.newInstSig <- struct{}{} }()
		}
		//if i == inst {
		//	break
		//}

	}

	//if manager.LookBackInstanceOwner(instsanceSpace, i) == manager.Id {
	//manager.instsToOpen = append(manager.instsToOpen, i)
	//if len(manager.instsToOpen) == 0 {
	//manager.Sig <- struct{}{}
	//}
	//continue
	//pbk
	//}

	// if we own it then start it

	//(*instsanceSpace)[i].Status = BACKING_OFF
	//_, bot := manager.CheckAndHandleBackoff(i, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
	//dlog.AgentPrintfN(manager.Id, "Backing off newly received instance %d for %d microseconds", i, bot)
	//
	//}
	//dlog.AgentPrintfN(manager.Id, "Setting instance %d as current instance", inst)
	//manager.CrtInstance = inst
}

func (manager *LoLProposer) LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.BallotOpenInstanceSignal.CheckOngoingBallot(pbk, inst, ballot, phase)
	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (manager *LoLProposer) LearnOfBallotAccepted(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.BallotOpenInstanceSignal.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
	return //manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot)
}

func (manager *LoLProposer) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	manager.UpdateCurrentInstance(instanceSpace, inst)
	pbk := (*instanceSpace)[inst]
	manager.BallotOpenInstanceSignal.CheckChosen(pbk, inst, ballot, whoseCmds)
	manager.SingleInstanceManager.HandleProposalChosen(pbk, inst, ballot)
}

// basically the same as simple except that we also

// Dynamic EagerFI
// chan took to long to propose
// function that detects the length of time to propose a value
//

//type

type SingleLeaderStaticMapped struct {
	// log manager
	// instanceagentmapper.InstanceAgentMapper
	//

	// multiplex instances with groups // global instance mapper maps groups to logs
	// log mapps instances to other things

	// signal applies to group, or applies to log??? -- could specify a log, or could specify -- watch instance for failure and trigger reproposal, open instance does it for
	// get batch signals instance
	// failure signals retry
	//
}

type StaticMappedProposalManager struct {
	SingleInstanceManager
	*Eager
	instanceagentmapper.InstanceAgentMapper
	ManualSignaller
	openInstToCatchUp bool
	//newInstSig chan<- struct{}
}

func MappedProposersProposalManagerNew(sig ManualSignaller, eagerGlobalManag *Eager, iMan SingleInstanceManager, agentMapper instanceagentmapper.InstanceAgentMapper, openInstToCatchUp bool) *StaticMappedProposalManager {
	return &StaticMappedProposalManager{
		ManualSignaller:       sig,
		Eager:                 eagerGlobalManag,
		SingleInstanceManager: iMan,
		InstanceAgentMapper:   agentMapper,
		openInstToCatchUp:     openInstToCatchUp,
	}
}

func (manager *StaticMappedProposalManager) GetProposerInstanceMapper() instanceagentmapper.InstanceAgentMapper {
	return manager.InstanceAgentMapper
}

func (manager *StaticMappedProposalManager) StartNextInstance(instanceSpace *[]*PBK) []int32 {
	for gotInstance := false; !gotInstance; {
		manager.CrtInstance++
		if (*instanceSpace)[manager.CrtInstance] == nil {
			(*instanceSpace)[manager.CrtInstance] = GetEmptyInstance()
		}
		if (*instanceSpace)[manager.CrtInstance].Status != NOT_BEGUN {
			continue
		}
		mapped := manager.InstanceAgentMapper.GetGroup(manager.CrtInstance)
		dlog.AgentPrintfN(manager.Id, "Proposer group for instance %d is %v", manager.CrtInstance, mapped)
		inG := inGroup(mapped, manager.Id)
		if !inG {
			dlog.AgentPrintfN(manager.Id, "Skipping instance %d as we are not mapped to it", manager.CrtInstance)
			(*instanceSpace)[manager.CrtInstance].Status = BACKING_OFF
			continue
		}
		gotInstance = true
		dlog.AgentPrintfN(manager.Id, "Starting instance %d as we are mapped to it", manager.CrtInstance)
		manager.StartNextProposal((*instanceSpace)[manager.CrtInstance], manager.CrtInstance)
	}
	opened := []int32{manager.CrtInstance}
	manager.OpenInstSignal.Opened(opened)
	return opened
}

func inGroup(mapped []int32, id int32) bool {
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
func (manager *StaticMappedProposalManager) LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	manager.checkAndSetNewInstance(instanceSpace, inst, ballot, phase)
	pbk := (*instanceSpace)[inst]
	manager.BallotOpenInstanceSignal.CheckOngoingBallot(pbk, inst, ballot, phase)
	if !manager.iInGroup(inst) {
		return false
	}
	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (manager *StaticMappedProposalManager) checkAndSetNewInstance(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) {
	if manager.CrtInstance >= inst {
		return
	}

	for i := manager.CrtInstance + 1; i <= inst; i++ {
		if (*instanceSpace)[i] != nil {
			continue
		}
		weIn, themIn := manager.whoMapped(i, int32(ballot.PropID))
		(*instanceSpace)[i] = GetEmptyInstance()
		if weIn {
			if themIn {
				if i == inst {
					continue
				}
				// can backoff as they will have attempted
				(*instanceSpace)[i].Status = BACKING_OFF
				_, bot := manager.CheckAndHandleBackoff(i, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}, stdpaxosproto.PROMISE)
				dlog.AgentPrintfN(manager.Id, "Backing off instance %d for %d microseconds as we are mapped to it", i, bot)
			} else {
				// open new instance
				if !manager.openInstToCatchUp {
					continue
				}
				manager.ManualSignaller.SignalNext()
			}
		} else {
			//don't need to consider instance as we aren't in it
			(*instanceSpace)[i].Status = BACKING_OFF
			dlog.AgentPrintfN(manager.Id, "Skipping instance %d as we are not mapped to it", i)
		}
	}
}

func (manager *StaticMappedProposalManager) iInGroup(i int32) bool {
	g := manager.GetProposerInstanceMapper().GetGroup(i)
	for _, pid := range g {
		if manager.Id == pid {
			return true
		}
	}
	return false
}

func (manager *StaticMappedProposalManager) whoMapped(i int32, them int32) (weIn bool, themIn bool) {
	g := manager.GetProposerInstanceMapper().GetGroup(i)
	dlog.AgentPrintfN(manager.Id, "Proposer group for instance %d is %v", i, g)
	for _, pid := range g {
		if manager.Id == pid {
			weIn = true
		}
		if pid == them {
			themIn = true
		}
	}
	return
}

func (manager *StaticMappedProposalManager) LearnOfBallotAccepted(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	manager.checkAndSetNewInstance(instanceSpace, inst, ballot, stdpaxosproto.ACCEPTANCE)
	pbk := (*instanceSpace)[inst]
	if !manager.iInGroup(inst) {
		return
	}
	manager.BallotOpenInstanceSignal.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
}

func (manager *StaticMappedProposalManager) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	manager.checkAndSetNewInstance(instanceSpace, inst, ballot, stdpaxosproto.ACCEPTANCE)
	pbk := (*instanceSpace)[inst]
	manager.BallotOpenInstanceSignal.CheckChosen(pbk, inst, ballot, whoseCmds)
	manager.SingleInstanceManager.HandleProposalChosen(pbk, inst, ballot)
}

type DynamicAgentMapper interface {
	instanceagentmapper.InstanceAgentMapper
	SetGroup(g int32)
}

type DynamicInstanceSetMapper struct {
	instanceagentmapper.DetRandInstanceSetMapper
}

func (m *DynamicInstanceSetMapper) SetGroup(g int32) {
	m.G = g
}

type DynamicMappedGlobalManager struct {
	*StaticMappedProposalManager
	n             int32
	f             int32
	curG          int32
	conflictEWMA  float64
	ewmaWeight    float64
	conflictsSeen map[int32]map[lwcproto.ConfigBal]struct{}
	DynamicAgentMapper
	// want to signal that we do not want to make proposals anymore
}

func DynamicMappedProposerManagerNew(sig ManualSignaller, proposalManager *Eager, iMan SingleInstanceManager, aMapper DynamicAgentMapper, n int32, f int32) *DynamicMappedGlobalManager {
	mappedDecider := MappedProposersProposalManagerNew(sig, proposalManager, iMan, aMapper, false)
	dMappedDecicider := &DynamicMappedGlobalManager{
		StaticMappedProposalManager: mappedDecider,
		DynamicAgentMapper:          aMapper,
		n:                           n,
		f:                           f,
		curG:                        n,
		conflictEWMA:                float64(0),
		ewmaWeight:                  0.1,
		conflictsSeen:               make(map[int32]map[lwcproto.ConfigBal]struct{}),
	}
	return dMappedDecicider
}

func (decider *DynamicMappedGlobalManager) StartNextInstance(instanceSpace *[]*PBK) []int32 {
	decider.updateGroupSize()
	return decider.StaticMappedProposalManager.StartNextInstance(instanceSpace)
}

func (decider *DynamicMappedGlobalManager) LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	decider.StaticMappedProposalManager.checkAndSetNewInstance(instanceSpace, inst, ballot, phase)

	pbk := (*instanceSpace)[inst]
	if !ballot.Equal(pbk.PropCurBal) && !pbk.PropCurBal.IsZero() { // ballot.GreaterThan(pbk.PropCurBal) &&
		if _, e := decider.conflictsSeen[inst]; !e {
			decider.conflictsSeen[inst] = make(map[lwcproto.ConfigBal]struct{})
		}
		if _, e := decider.conflictsSeen[inst][ballot]; !e {
			old := decider.conflictEWMA
			decider.conflictEWMA = mathextra.EwmaAdd(decider.conflictEWMA, decider.ewmaWeight, 1)
			dlog.AgentPrintfN(decider.Id, "Conflict encountered. Increasing EWMA from %f to %f", old, decider.conflictEWMA)

		}
	}

	return decider.StaticMappedProposalManager.LearnOfBallot(instanceSpace, inst, ballot, phase)
}

func (decider *DynamicMappedGlobalManager) DecideRetry(pbk *PBK, retry RetryInfo) bool {
	doRetry := decider.DecideRetry(pbk, retry)
	if !doRetry {
		return doRetry
	}
	old := decider.conflictEWMA
	decider.conflictEWMA = mathextra.EwmaAdd(decider.conflictEWMA, decider.ewmaWeight*3, -1)
	dlog.AgentPrintfN(decider.Id, "Retry needed on instance %d because failures are occurring or there is not enough system load. Decreasing EWMA from %f to %f", retry.Inst, old, decider.conflictEWMA)
	return doRetry
}

func (decider *DynamicMappedGlobalManager) updateGroupSize() {
	newG := decider.curG

	dlog.AgentPrintfN(decider.Id, "Current proposer group is of size %d (EWMA is %f)", decider.curG, decider.conflictEWMA)
	if decider.conflictEWMA > 0.2 {
		newG = mapper.Mapper(decider.conflictEWMA, 1, 0, decider.f+1, decider.curG)
		decider.conflictEWMA = 0
	}
	if decider.conflictEWMA < 0 {
		newG = mapper.Mapper(decider.conflictEWMA, 0, -1, decider.curG, decider.n)
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
			dlog.AgentPrintfN(decider.Id, "Increasing proposer group size %d", newG)
		} else {
			dlog.AgentPrintfN(decider.Id, "Decreasing proposer group size to %d", newG)
		}
		decider.curG = newG
		decider.DynamicAgentMapper.SetGroup(decider.curG)
	}
}

func (decider *DynamicMappedGlobalManager) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	pbk := (*instanceSpace)[inst]
	if ballot.GreaterThan(pbk.PropCurBal) && !pbk.PropCurBal.IsZero() {
		if _, e := decider.conflictsSeen[inst]; !e {
			decider.conflictsSeen[inst] = make(map[lwcproto.ConfigBal]struct{})
		}
		if _, e := decider.conflictsSeen[inst][ballot]; !e {
			old := decider.conflictEWMA
			decider.conflictEWMA = mathextra.EwmaAdd(decider.conflictEWMA, decider.ewmaWeight, 1)
			dlog.AgentPrintfN(decider.Id, "Conflict encountered. Increasing EWMA from %f to %f", old, decider.conflictEWMA)
		}
	}
	delete(decider.conflictsSeen, inst)
	decider.StaticMappedProposalManager.LearnBallotChosen(instanceSpace, inst, ballot, whoseCmds)
}

//func (decider *DynamicMappedGlobalManager) GetAckersGroup(Inst int32) []int {
//	return decider.StaticMappedProposalManager.GetAckersGroup(Inst)
//}

func movingPointAvg(a, ob float64) float64 {
	a -= a / 1000
	a += ob / 1000
	return a
}

func (decider *DynamicMappedGlobalManager) LearnNoop(inst int32, who int32) {
	//if who != decider.Id {
	//	return
	//}
	old := decider.conflictEWMA
	decider.conflictEWMA = mathextra.EwmaAdd(decider.conflictEWMA, decider.ewmaWeight, -1)
	dlog.AgentPrintfN(decider.Id, "Learnt NOOP. Decreasing EWMA from %f to %f", old, decider.conflictEWMA)
}

type hedge struct {
	relatedHedges []int32 //includes self
	preempted     bool
}

type SimpleHedgedBets struct {
	*InductiveConflictsManager
	id              int32
	n               int32
	currentHedgeNum int32
	conflictEWMA    float64
	ewmaWeight      float64
	max             int32
	confs           map[int32]map[int16]struct{}
}

func HedgedBetsProposalManagerNew(id int32, manager *InductiveConflictsManager, n int32, initialHedge int32) *SimpleHedgedBets {
	dMappedDecicider := &SimpleHedgedBets{
		InductiveConflictsManager: manager,
		id:                        id,
		n:                         n,
		currentHedgeNum:           initialHedge,
		conflictEWMA:              float64(1),
		ewmaWeight:                0.1,
		max:                       2 * n,
		confs:                     make(map[int32]map[int16]struct{}),
	}
	return dMappedDecicider
}

func (manager *SimpleHedgedBets) StartNextInstance(instanceSpace *[]*PBK) []int32 {
	manager.updateHedgeSize()
	opened := make([]int32, 0, manager.currentHedgeNum)
	for i := int32(0); i < manager.currentHedgeNum; i++ {
		opened = append(opened, manager.InductiveConflictsManager.StartNextInstance(instanceSpace)...)
	}
	manager.OpenInstSignal.Opened(opened)
	return opened
}

func (manager *SimpleHedgedBets) LearnOfBallot(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	if inst > manager.CrtInstance {
		manager.UpdateCurrentInstance(instanceSpace, inst)
	}

	pbk := (*instanceSpace)[inst]
	manager.updateConfs(inst, ballot)
	manager.BallotOpenInstanceSignal.CheckOngoingBallot(pbk, inst, ballot, phase)
	return manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (manager *SimpleHedgedBets) LearnOfBallotAccepted(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	if inst > manager.CrtInstance {
		manager.UpdateCurrentInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	manager.updateConfs(inst, ballot)
	manager.BallotOpenInstanceSignal.CheckAcceptedBallot(pbk, inst, ballot, whosecmds)
	//return //manager.SingleInstanceManager.HandleReceivedBallot(pbk, inst, ballot)
}

func (manager *SimpleHedgedBets) updateConfs(inst int32, ballot lwcproto.ConfigBal) {
	if _, e := manager.confs[inst]; !e {
		manager.confs[inst] = make(map[int16]struct{})
	}
	manager.confs[inst][ballot.PropID] = struct{}{}
	dlog.AgentPrintfN(manager.id, "Now observing %d attempts in instance %d", len(manager.confs[inst]), inst)
}

func (manager *SimpleHedgedBets) LearnBallotChosen(instanceSpace *[]*PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	if inst > manager.CrtInstance {
		manager.UpdateCurrentInstance(instanceSpace, inst)
	}
	pbk := (*instanceSpace)[inst]
	manager.updateConfs(inst, ballot)
	manager.BallotOpenInstanceSignal.CheckChosen(pbk, inst, ballot, whoseCmds)
	manager.SingleInstanceManager.HandleProposalChosen(pbk, inst, ballot)
	manager.conflictEWMA = mathextra.EwmaAdd(manager.conflictEWMA, manager.ewmaWeight, float64(len(manager.confs[inst])))
	delete(manager.confs, inst) // will miss some as a result but its okie to avoid too much memory growth
}

func (manager *SimpleHedgedBets) DecideRetry(pbk *PBK, retry RetryInfo) bool {
	doRetry := manager.InductiveConflictsManager.DecideRetry(pbk, retry)
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
	//if who != decider.Id {
	//	return
	//}
	//old := decider.conflictEWMA
	//decider.conflictEWMA = ewmaAdd(decider.conflictEWMA, decider.ewmaWeight, 1)
	//dlog.AgentPrintfN(decider.Id, "Learnt NOOP, decreasing EWMA from %f to %f", old, decider.conflictEWMA)

}

//
//func (decider *SimpleHedgedBets) GetAckersGroup(Inst int32) []int {
//	return decider.BaselineManager.GetAckersGroup(Inst)
//}

// mapped hedged bets??
