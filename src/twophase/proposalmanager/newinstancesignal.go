package proposalmanager

import (
	"dlog"
	"lwcproto"
	"stdpaxosproto"
)

type OpenInstSignal interface {
	Opened(opened []int32) // todo decide whether to make two interfaces for hedged variants or not?
	CheckBallot(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase)
	//CheckAcceptedBallot(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot)
	CheckChosen(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal)
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

func (sig *SimpleSig) CheckBallot(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) {
	if pbk.Status == CLOSED {
		return
	}
	if _, e := sig.instsStarted[inst]; !e {
		//dlog.AgentPrintfN(sig.id, "not siging inst %d cause 1", inst)
		return
	}
	// we have the most recent ballot preparing
	if phase == stdpaxosproto.PROMISE && (pbk.PropCurBal.Equal(ballot) || pbk.PropCurBal.GreaterThan(ballot)) || int32(ballot.PropID) == sig.id {
		//dlog.AgentPrintfN(sig.id, "not siging inst %d cause 2", inst)
		return
	}

	if phase == stdpaxosproto.ACCEPTANCE && (pbk.Status == PROPOSING || int32(ballot.PropID) == sig.id) {
		//dlog.AgentPrintfN(sig.id, "not siging inst %d cause 3", inst)
		return
	}

	dlog.AgentPrintfN(sig.id, "Signalling to open new instance as instance %d %s", inst, "as it is preempted or there is an accepted ballot")
	go func() { sig.sigNewInst <- struct{}{} }()
	delete(sig.instsStarted, inst)
}

func (sig *SimpleSig) CheckChosen(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal) {
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
	go func() { sig.sigNewInst <- struct{}{} }()
	delete(sig.instsStarted, inst)
}

//func (sig *SimpleSig) CheckPreempted(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, reason string) {
//
//}

type EagerSig struct {
	*SimpleSig
	MaxOpenInsts int32
}

func EagerSigNew(simpleSig *SimpleSig, maxOI int32) *EagerSig {
	return &EagerSig{
		SimpleSig:    simpleSig,
		MaxOpenInsts: maxOI,
	}
}

func (manager *EagerSig) CheckChosen(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal) {
	if pbk.Status == CLOSED {
		return
	}
	if _, e := manager.instsStarted[inst]; e {
		dlog.AgentPrintfN(manager.id, "Signalling to open new instance as this instance %d is chosen", inst)
		go func() { manager.sigNewInst <- struct{}{} }()
		delete(manager.instsStarted, inst)
	}
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

func (sig *HedgedSig) CheckBallot(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) {
	//sig.checkInstFailed(pbk, inst, ballot)
	if pbk.Status == CLOSED {
		return
	}

	curHedge, e := sig.currentHedges[inst]
	if !e {
		return
	}

	// we have the most recent ballot preparing
	if phase == stdpaxosproto.PROMISE && (pbk.PropCurBal.Equal(ballot) || pbk.PropCurBal.GreaterThan(ballot)) || int32(ballot.PropID) == sig.id {
		return
	}

	if phase == stdpaxosproto.ACCEPTANCE && (pbk.Status == PROPOSING || int32(ballot.PropID) == sig.id) {
		return
	}
	curHedge.preempted = true
	dlog.AgentPrintfN(sig.id, "Noting that instance %d has failed", inst)
	sig.checkNeedsSig(pbk, inst)
}

func (sig *HedgedSig) CheckChosen(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal) {
	if pbk.Status == CLOSED {
		return
	}
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
	//sig.checkInstChosenByMe(pbk, inst, ballot)
}

func (sig *HedgedSig) checkNeedsSig(pbk *ProposingBookkeeping, inst int32) {
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
