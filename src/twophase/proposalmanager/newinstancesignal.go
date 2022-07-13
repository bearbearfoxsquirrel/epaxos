package proposalmanager

import (
	"dlog"
	"lwcproto"
	"stdpaxosproto"
)

type OpenInstSignal interface {
	Opened(opened []int32)
	CheckOngoingBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase)
	CheckAcceptedBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32)
	CheckChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal)
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

func (sig *SimpleSig) Opened(opened []int32) {
	if len(opened) == 0 {
		panic("asjdlfjasldf")
	}
	for _, i := range opened {
		sig.instsStarted[i] = struct{}{}
	}
}

func (sig *SimpleSig) CheckOngoingBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) {
	if pbk.Status == CLOSED {
		return
	}
	if int32(ballot.PropID) == sig.id {
		return
	}
	if _, e := sig.instsStarted[inst]; !e {
		return
	}
	if (pbk.PropCurBal.Equal(ballot) || pbk.PropCurBal.GreaterThan(ballot)) && phase == stdpaxosproto.PROMISE {
		return
	}
	if pbk.Status == PROPOSING && phase == stdpaxosproto.ACCEPTANCE { // we are already proopsing our own value
		return
	}
	//if pbk.PropCurBal.GreaterThan(ballot) && pbk.Status == PROPOSING && phase == stdpaxosproto.ACCEPTANCE {
	//	return
	//}

	dlog.AgentPrintfN(sig.id, "Signalling to open new instance as instance %d %s", inst, "as it is preempted")
	delete(sig.instsStarted, inst)
	go func() { sig.sigNewInst <- struct{}{} }()
}

func (sig *SimpleSig) CheckAcceptedBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, whosecmds int32) {
	if pbk.Status == CLOSED {
		return
	}
	if int32(ballot.PropID) == sig.id {
		return
	}
	if _, e := sig.instsStarted[inst]; !e {
		return
	}
	if whosecmds == sig.id {
		return
	}

	dlog.AgentPrintfN(sig.id, "Signalling to open new instance as instance %d %s", inst, "as there is an accepted ballot")
	delete(sig.instsStarted, inst)
	go func() { sig.sigNewInst <- struct{}{} }()
}

func (sig *SimpleSig) CheckChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal) {
	if pbk.Status == CLOSED {
		return
	}
	// either we choose or didn't start the instance
	if _, e := sig.instsStarted[inst]; !e {
		//dlog.AgentPrintfN(sig.id, "not siging inst %d cause 2", inst)
		return
	}
	if int32(ballot.PropID) == sig.id {
		return
	}

	dlog.AgentPrintfN(sig.id, "Signalling to open new instance as instance %d attempted was chosen by someone else", inst)
	delete(sig.instsStarted, inst)
	go func() { sig.sigNewInst <- struct{}{} }()
}

//func (sig *SimpleSig) CheckPreempted(pbk *PBK, inst int32, ballot stdpaxosproto.Ballot, reason string) {
//
//}

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

func (sig *EagerSig) Opened(o []int32) {
	if int32(len(sig.instsStarted)) >= sig.MaxOpenInsts {
		panic("saldkjfakl;jfls;kdfj")
	}
	sig.SimpleSig.Opened(o)
}

//func (sig *EagerSig) CheckOngoingBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) {
//	if pbk.Status == CLOSED {
//		return
//	}
//
//	_, e := sig.instsStarted[inst]
//	if !e {
//		return
//	}
//
//	if int32(ballot.PropID) == sig.id && pbk.Status != PROPOSING {
//		return
//	}
//
//	if (pbk.PropCurBal.Equal(ballot) || pbk.PropCurBal.GreaterThan(ballot)) && phase == stdpaxosproto.PROMISE {
//		return
//	}
//
//	if pbk.Status == PROPOSING && phase == stdpaxosproto.ACCEPTANCE { // we are already proopsing our own value
//		return
//	}
//
//
//	//if pbk.PropCurBal.GreaterThan(ballot) && pbk.Status == PROPOSING && phase == stdpaxosproto.ACCEPTANCE {
//	//	return
//	//}
//
//	dlog.AgentPrintfN(sig.id, "Signalling to open new instance as instance %d %s", inst, "as it is preempted")
//	go func() { sig.sigNewInst <- struct{}{} }()
//	delete(sig.instsStarted, inst)
//}
func (manager *EagerSig) Close(inst int32) {
	delete(manager.instsStarted, inst)
}

func (manager *EagerSig) CheckChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal) {
	if pbk.Status == CLOSED {
		return
	}
	if _, e := manager.instsStarted[inst]; !e {
		return
	}
	dlog.AgentPrintfN(manager.id, "Signalling to open new instance as this instance %d is chosen", inst)
	delete(manager.instsStarted, inst)
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
	//sig.checkInstFailed(pbk, inst, ballot)
	//if pbk.Status == CLOSED {
	//	return
	//}

	curHedge, e := sig.currentHedges[inst]
	if !e {
		return
	}

	// we have the most recent ballot preparing
	if phase == stdpaxosproto.PROMISE && (pbk.PropCurBal.Equal(ballot) || pbk.PropCurBal.GreaterThan(ballot)) || int32(ballot.PropID) == sig.id {
		return
	}

	//if phase == stdpaxosproto.ACCEPTANCE && (pbk.Status == PROPOSING || int32(ballot.PropID) == sig.id) {
	//	return
	//}

	curHedge.preempted = true
	dlog.AgentPrintfN(sig.id, "Noting that instance %d has failed", inst)
	sig.checkNeedsSig(pbk, inst)
}

func (sig *HedgedSig) CheckChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal) {
	//if pbk.Status == CLOSED {
	//	return
	//}
	//sig.checkInstFailed(pbk, inst, ballot)
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

	dlog.AgentPrintfN(sig.id, "Noting that instance %d has succeeded, cleaning all related hedges", inst)
	for _, i := range curHedge.relatedHedges {
		delete(sig.currentHedges, i)
	}

	// check percentage of choosing
	// if we are above a threshold then reduce
	// if we are below, then increase

	//sig.checkInstChosenByMe(pbk, inst, ballot)
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

// if proposed noop in opened instance, then reduce pipeline (ignore one sig?)

// eager hedged?
