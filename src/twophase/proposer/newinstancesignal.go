package proposer

import (
	"epaxos/dlog"
	"epaxos/lwcproto"
	"epaxos/stdpaxosproto"
	"epaxos/twophase/logfmt"
)

type OpenInstSignal interface {
	Opened(opened []int32)
	GetSignaller() <-chan struct{}
}

type BallotOpenInstanceSignal interface {
	CheckOngoingBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase)
	CheckAcceptedBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32)
	CheckChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32)
}

type ExecOpenInstanceSignal interface {
	CheckExec(informer ExecInformer)
}

type SimpleSig struct {
	instsStarted map[int32]struct{}
	sigNewInst   chan struct{}
	id           int32
}

func SimpleSigNew(newInstSig chan struct{}, id int32) *SimpleSig {
	return &SimpleSig{
		instsStarted: make(map[int32]struct{}),
		sigNewInst:   newInstSig,
		id:           id,
	}
}

func (sig *SimpleSig) GetSignaller() <-chan struct{} {
	return sig.sigNewInst
}

func (sig *SimpleSig) Opened(opened []int32) {
	if len(opened) == 0 {
		panic("asjdlfjasldf")
	}
	for _, i := range opened {
		sig.instsStarted[i] = struct{}{}
	}
}

func (sig *SimpleSig) CheckOngoingBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) {
	if !sig.ballotShouldOpen(pbk, inst, ballot, phase) {
		return
	}
	sig.sigNextInst(inst)
}

func (sig *SimpleSig) ballotShouldOpen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	if int32(ballot.PropID) == sig.id {
		return false
	}
	if _, e := sig.instsStarted[inst]; !e {
		return false
	}
	if pbk.PropCurBal.Equal(ballot) || pbk.PropCurBal.GreaterThan(ballot) {
		return false
	}
	//if pbk.Status == PROPOSING && phase == stdpaxosproto.ACCEPTANCE { // we are already proopsing our own value
	//	return
	//}
	//if  pbk.Status == PROPOSING && phase == stdpaxosproto.ACCEPTANCE {
	//	return false
	//}

	dlog.AgentPrintfN(sig.id, "Signalling to open new instance as instance %d %s", inst, "as it is preempted")
	return true
}

func (sig *SimpleSig) sigNextInst(inst int32) {
	delete(sig.instsStarted, inst)
	//dlog.AgentPrintfN(sig.id, "sigging %d", inst)
	go func() { sig.sigNewInst <- struct{}{} }()
}

func (sig *SimpleSig) CheckAcceptedBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	if !sig.acceptedShouldSignal(pbk, inst, ballot, whosecmds) {
		return
	}
	sig.sigNextInst(inst)
}

func (sig *SimpleSig) acceptedShouldSignal(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) bool {
	if _, e := sig.instsStarted[inst]; !e {
		return false
	}
	if int32(ballot.PropID) == sig.id && (whosecmds == sig.id || whosecmds == -1) {
		return false
	}
	if pbk.Status == PROPOSING && pbk.PropCurBal.GreaterThan(ballot) { // late acceptance that we've ignored (got promise quorum and learnt no value was chosen)
		return false
	}
	dlog.AgentPrintfN(sig.id, "Signalling to open new instance as instance %d %s", inst, "as there is an accepted ballot")
	return true
}

func (sig *SimpleSig) CheckChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	// either we choose or didn't start the instance
	if !sig.chosenShouldSignal(inst, ballot, whoseCmds) {
		return
	}
	sig.sigNextInst(inst)
}

func (sig *SimpleSig) chosenShouldSignal(inst int32, ballot lwcproto.ConfigBal, whoseCmd int32) bool {
	if _, e := sig.instsStarted[inst]; !e {
		//dlog.AgentPrintfN(Sig.Id, "not siging inst %d cause 2", inst)
		return false
	}
	if int32(ballot.PropID) == sig.id && (whoseCmd == sig.id || whoseCmd == -1) {
		return false
	}

	dlog.AgentPrintfN(sig.id, "Signalling to open new instance as instance %d attempted was chosen by someone else or proposed someone else's value ", inst)
	return true
}

type EagerExecUpToSig struct {
	EagerSig
	n         float32
	fac       float32
	sigged    int32
	myMaxInst int32
	execUpTo  int32
}

func EagerExecUpToSigNew(eagerSig *EagerSig, n float32, fac float32) *EagerExecUpToSig {
	return &EagerExecUpToSig{
		EagerSig:  *eagerSig,
		n:         n,
		fac:       fac,
		myMaxInst: -1,
		execUpTo:  -1,
		sigged:    eagerSig.MaxOpenInsts,
	}
}

func (sig *EagerExecUpToSig) updateMyMaxInst(inst int32) {
	if sig.myMaxInst >= inst {
		return
	}
	sig.myMaxInst = inst
}

func (sig *EagerExecUpToSig) Opened(o []int32) {
	if sig.sigged <= 0 {
		panic("No recorded signals to open")
	}
	for _, i := range o {
		sig.updateMyMaxInst(i)
	}
	sig.SimpleSig.Opened(o)
	sig.sigged -= int32(len(o))
}

func (sig *EagerExecUpToSig) sigNextInst(inst int32) {
	if _, e := sig.instsStarted[inst]; !e {
		panic("signalling for instance not started")
	}
	sig.SimpleSig.sigNextInst(inst)
	sig.sigged += 1
}

type ManualSignaller interface {
	SignalNext()
}

func (sig *EagerExecUpToSig) SignalNext() {
	//sig.SimpleSig.sigNextInst(inst)
	if sig.PipelineTooLong() {
		return
	}
	sig.sigged += 1
	go func() { sig.sigNewInst <- struct{}{} }()
}

func (sig *EagerExecUpToSig) CheckOngoingBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) {
	//manager.updateMyMaxInst(inst)
	if !sig.ballotShouldOpen(pbk, inst, ballot, phase) {
		return
	}
	sig.sigNextInst(inst)
}

func (sig *EagerExecUpToSig) CheckAcceptedBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	//manager.updateMyMaxInst(inst)
	if !sig.acceptedShouldSignal(pbk, inst, ballot, whosecmds) {
		return
	}
	sig.sigNextInst(inst)
}

func (sig *EagerExecUpToSig) CheckChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	//manager.updateMyMaxInst(inst)
	if _, e := sig.instsStarted[inst]; !e {
		return
	}
	if int32(ballot.PropID) == sig.id {
		if sig.PipelineTooLong() {
			return
		}
		sig.sigNextInst(inst)
		return
	}

	dlog.AgentPrintfN(sig.id, "Signalling to open new instance as instance %d attempted was chosen by someone else", inst)
	sig.sigNextInst(inst)
}

func (sig *EagerExecUpToSig) CheckExec(informer ExecInformer) {
	if sig.execUpTo >= informer.GetExecutedUpTo() {
		return
	}
	sig.execUpTo = informer.GetExecutedUpTo()
	dlog.AgentPrintfN(sig.id, "Checking whether to open up new instances")
	if sig.PipelineTooLong() {
		return
	}

	//if int32(len(sig.instsStarted)) > sig.MaxOpenInsts {
	//	panic("too long")
	//}

	for i, _ := range sig.instsStarted {
		if i > informer.GetExecutedUpTo() {
			continue
		}
		delete(sig.instsStarted, i)
	}
	toOpen := sig.MaxOpenInsts - (int32(len(sig.instsStarted)) + sig.sigged)
	if toOpen+int32(len(sig.instsStarted))+sig.sigged > sig.MaxOpenInsts {
		panic("going to make a too long pipeline")
	}
	if toOpen <= 0 {
		dlog.AgentPrintfN(sig.id, "Not opening up new instances as we have currently opened %d instances in the pipeline", len(sig.instsStarted))
		return
	}

	//manager.sigged += toOpen

	dlog.AgentPrintfN(sig.id, "Signalling to open %d new instance(s) as executed instance has caught up with current", toOpen)
	sig.sigged += toOpen
	go func(toOpen int32) {
		for i := int32(0); i < toOpen; i++ {
			sig.sigNewInst <- struct{}{}
		}
	}(toOpen)
}

func (sig *EagerExecUpToSig) PipelineTooLong() bool {
	if sig.myMaxInst >= sig.GetMaxPipelineLen() {
		dlog.AgentPrintfN(sig.id, "Not opening up new instances as executed instance %d hasn't caught up with current instance %d", sig.execUpTo, sig.myMaxInst)
		return true
	}
	return false
}

func (sig *EagerExecUpToSig) GetMaxPipelineLen() int32 {
	return sig.execUpTo + int32(float32(sig.MaxOpenInsts)*sig.n*sig.fac)
}

type EagerSig struct {
	*SimpleSig
	MaxOpenInsts int32
}

func EagerSigNew(simpleSig *SimpleSig, maxOI int32) *EagerSig {
	e := &EagerSig{
		SimpleSig:    simpleSig,
		MaxOpenInsts: maxOI,
	}
	go func() {
		for i := int32(0); i < maxOI; i++ {
			e.sigNewInst <- struct{}{}
		}
	}()
	return e
}

//func (Sig *EagerSig) Opened(o []int32) {
//	Sig.SimpleSig.Opened(o)
//}

func (manager *EagerSig) Close(inst int32) {
	delete(manager.instsStarted, inst)
}

func (manager *EagerSig) CheckOngoingBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) {
	if !manager.ballotShouldOpen(pbk, inst, ballot, phase) {
		return
	}
	manager.tryOpenNewInst(inst)
}

func (manager *EagerSig) CheckAcceptedBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	if !manager.acceptedShouldSignal(pbk, inst, ballot, whosecmds) {
		return
	}
	manager.tryOpenNewInst(inst)
}

func (manager *EagerSig) CheckChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	if pbk.Status == CLOSED {
		return
	}
	if _, e := manager.instsStarted[inst]; !e {
		return
	}
	logfmt.OpenInstanceSignalChosen(manager.id, inst, ballot.Ballot)
	manager.tryOpenNewInst(inst)
}

func (manager *EagerSig) tryOpenNewInst(inst int32) {
	delete(manager.instsStarted, inst)
	if len(manager.instsStarted) >= int(manager.MaxOpenInsts) {
		return
	}
	go func() { manager.sigNewInst <- struct{}{} }()
}

func (sig *EagerSig) DoSig() {
	go func() { sig.sigNewInst <- struct{}{} }()
}

type HedgedSig struct {
	sigNewInst    chan struct{}
	id            int32
	currentHedges map[int32]*hedge
}

func HedgedSigNew(id int32, newInstSig chan struct{}) *HedgedSig {
	return &HedgedSig{
		sigNewInst:    newInstSig,
		id:            id,
		currentHedges: make(map[int32]*hedge),
	}
}

func (sig *HedgedSig) Opened(opened []int32) {
	for _, inst := range opened {
		sig.currentHedges[inst] = &hedge{
			relatedHedges: opened,
			preempted:     false,
		}
	}
}

func (sig *HedgedSig) CheckOngoingBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) {
	curHedge, e := sig.currentHedges[inst]
	if !e {
		return
	}

	// we have the most recent ballot preparing
	if phase == stdpaxosproto.PROMISE && (pbk.PropCurBal.Equal(ballot) || pbk.PropCurBal.GreaterThan(ballot)) || int32(ballot.PropID) == sig.id {
		return
	}

	//if phase == stdpaxosproto.ACCEPTANCE && (pbk.Status == PROPOSING || int32(ballot.PropID) == Sig.Id) {
	//	return
	//}

	curHedge.preempted = true
	dlog.AgentPrintfN(sig.id, "Noting that instance %d has failed", inst)
	sig.checkNeedsSig(pbk, inst)
}

func (sig *HedgedSig) CheckChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	//if pbk.Status == CLOSED {
	//	return
	//}
	//Sig.checkInstFailed(pbk, inst, ballot)
	curHedge, e := sig.currentHedges[inst]
	if !e {
		return
	}

	if !ballot.Equal(pbk.PropCurBal) {
		curHedge.preempted = true
		dlog.AgentPrintfN(sig.id, "Noting that instance %d has failed", inst)
		sig.checkNeedsSig(pbk, inst)
		return
	}

	dlog.AgentPrintfN(sig.id, "Noting that instance %d has succeeded. Cleaning all related hedges", inst)
	for _, i := range curHedge.relatedHedges {
		delete(sig.currentHedges, i)
	}

}

func (sig *HedgedSig) CheckAcceptedBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {

}

func (sig *HedgedSig) checkNeedsSig(pbk *PBK, inst int32) {
	curHedge, e := sig.currentHedges[inst]
	if !e || pbk.Status == CLOSED {
		return
	}

	// if all related hedged failed
	for _, i := range curHedge.relatedHedges {
		if !sig.currentHedges[i].preempted {
			dlog.AgentPrintfN(sig.id, "Not signalling to open new instance %d as not all hedges preempted", inst)
			return
		}
	}
	for _, i := range curHedge.relatedHedges {
		delete(sig.currentHedges, i)
	}
	delete(sig.currentHedges, inst)
	dlog.AgentPrintfN(sig.id, "Signalling to open new instance as all hedged attempts related to %d failed", inst)
	go func() { sig.sigNewInst <- struct{}{} }()
}
