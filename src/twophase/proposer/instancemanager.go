package proposer

import (
	"epaxos/dlog"
	"epaxos/lwcproto"
	"epaxos/stdpaxosproto"
	"epaxos/twophase/balloter"
)

type SingleInstanceManager interface {
	InitInstance(inst int32) *PBK
	StartProposal(pbk *PBK, inst int32)
	ShouldRetryInstance(pbk *PBK, retry RetryInfo) bool
	HandleReceivedBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool
	//HandleAcceptedBallot(pbk *PBK, inst int32, ballot stdpaxosproto.Ballot, cmd []state.Command)
	HandleProposalChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal)
}

type SimpleInstanceManager struct {
	id int32
	*BackoffManager
	*balloter.Balloter
	ProposerInstanceQuorumaliser
}

func SimpleInstanceManagerNew(id int32, manager *BackoffManager, balloter *balloter.Balloter, qrm ProposerInstanceQuorumaliser) *SimpleInstanceManager {
	return &SimpleInstanceManager{
		id:                           id,
		BackoffManager:               manager,
		Balloter:                     balloter,
		ProposerInstanceQuorumaliser: qrm,
	}
}

func (man *SimpleInstanceManager) InitInstance(inst int32) *PBK {
	return GetEmptyInstance()
}

func (man *SimpleInstanceManager) StartProposal(pbk *PBK, inst int32) {
	pbk.Status = PREPARING
	nextBal := man.Balloter.GetNextProposingBal(pbk.MaxKnownBal.Config, pbk.MaxKnownBal.Number)
	pbk.PropCurBal = nextBal
	pbk.MaxKnownBal = nextBal
	man.ProposerInstanceQuorumaliser.StartPromiseQuorumOnCurBal(pbk, inst)
	dlog.AgentPrintfN(man.id, "Starting new proposal for instance %d with ballot %d.%d", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
}

func (man *SimpleInstanceManager) ShouldRetryInstance(pbk *PBK, retry RetryInfo) bool {
	if pbk.Status != BACKING_OFF {
		dlog.AgentPrintfN(man.id, "Skipping retry of instance %d due to it no longer being backed off", retry.Inst)
		return false
	}
	if !man.BackoffManager.StillRelevant(retry) || !pbk.PropCurBal.Equal(retry.AttemptedBal) {
		dlog.AgentPrintfN(man.id, "Skipping retry of instance %d due to being preempted again", retry.Inst)
		return false
	}
	return true
}

func (man *SimpleInstanceManager) HandleReceivedBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	if pbk.Status == CLOSED {
		return false
	}
	if int32(ballot.PropID) == man.id {
		return false
	}
	//if pbk.PropCurBal.Equal(ballot) {
	//	return false
	//}

	if pbk.MaxKnownBal.GreaterThan(ballot) || pbk.MaxKnownBal.Equal(ballot) {
		return false
	} // want to backoff in face of new phase

	phaseAt := 0
	if pbk.Status == PREPARING || pbk.Status == READY_TO_PROPOSE {
		phaseAt = 1
	} else if pbk.Status == PROPOSING {
		phaseAt = 2
	}

	pbk.Status = BACKING_OFF
	if ballot.GreaterThan(pbk.MaxKnownBal) {
		dlog.AgentPrintfN(man.id, "Witnessed new maximum ballot %d.%d for instance %d", ballot.Number, ballot.PropID, inst)
	}
	pbk.MaxKnownBal = ballot
	//NOTE: HERE WE WANT TO INCREASE BACKOFF EACH TIME THERE IS A NEW PROPOSAL SEEN
	backedOff, botime := man.BackoffManager.CheckAndHandleBackoff(inst, pbk.PropCurBal, ballot, phase)

	if backedOff {
		dlog.AgentPrintfN(man.id, "Backing off instance %d for %d microseconds because our current ballot %d.%d in phase %d is preempted by ballot %d.%d",
			inst, botime, pbk.PropCurBal.Number, pbk.PropCurBal.PropID, phaseAt, ballot.Number, ballot.PropID)
	}
	return true
}

func (man *SimpleInstanceManager) HandleProposalChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal) {
	if pbk.Status == CLOSED {
		return
	}

	man.BackoffManager.StopBackoffs(inst)

	pbk.Status = CLOSED
	pbk.ProposeValueBal = ballot
	if ballot.GreaterThan(pbk.MaxKnownBal) {
		pbk.MaxKnownBal = ballot
	}

	if ballot.PropID == int16(man.id) {
		man.Balloter.UpdateProposalChosen()
	}
}

type MinimalProposersInstanceManager struct {
	*SimpleInstanceManager
	*MinimalProposersShouldMaker
}

func MinimalProposersInstanceManagerNew(manager *SimpleInstanceManager, minimalProposer *MinimalProposersShouldMaker) *MinimalProposersInstanceManager {
	return &MinimalProposersInstanceManager{
		SimpleInstanceManager:       manager,
		MinimalProposersShouldMaker: minimalProposer,
	}
}

func (man *MinimalProposersInstanceManager) StartProposal(pbk *PBK, inst int32) {
	if man.ShouldSkipInstance(inst) {
		panic("should not start next proposal on instance fulfilled")
	}
	man.SimpleInstanceManager.StartProposal(pbk, inst)
	dlog.AgentPrintfN(man.id, "Starting new proposal for instance %d with ballot %d.%d", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
	man.MinimalProposersShouldMaker.startedMyProposal(inst, pbk.PropCurBal)
}

func (man *MinimalProposersInstanceManager) ShouldRetryInstance(pbk *PBK, retry RetryInfo) bool {
	if man.MinimalProposersShouldMaker.ShouldSkipInstance(retry.Inst) {
		if pbk.Status == CLOSED {
			dlog.AgentPrintfN(man.id, "Skipping retry of instance %d due to it being closed", retry.Inst)
			return false
		}
		dlog.AgentPrintfN(man.id, "Skipping retry of instance %d as minimal proposer threshold (f+1) met", retry.Inst)
		pbk.Status = BACKING_OFF
		//man.BackoffManager.ClearBackoff(retry.Inst)
		man.StopBackoffs(retry.Inst)
		return false
	}
	return man.SimpleInstanceManager.ShouldRetryInstance(pbk, retry)
}

func (man *MinimalProposersInstanceManager) HandleReceivedBallot(pbk *PBK, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	man.minimalProposersLearnOfBallot(ballot, inst)
	return man.SimpleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (man *MinimalProposersInstanceManager) HandleProposalChosen(pbk *PBK, inst int32, ballot lwcproto.ConfigBal) {
	man.MinimalProposersShouldMaker.Cleanup(inst)
	man.SimpleInstanceManager.HandleProposalChosen(pbk, inst, ballot)
}

func (man *MinimalProposersInstanceManager) minimalProposersLearnOfBallot(ballot lwcproto.ConfigBal, inst int32) {
	if int32(ballot.PropID) != man.PropID && !ballot.IsZero() {
		man.MinimalProposersShouldMaker.BallotReceived(inst, ballot)
		if man.MinimalProposersShouldMaker.ShouldSkipInstance(inst) {
			dlog.AgentPrintfN(man.id, "Stopping making proposals to instance %d because we have witnessed f+1 higher proposals", inst)
		}
	}
}

//
//type ConfigInstanceManager struct {
//	crtConfig int32
//	SingleInstanceManager
//}
//
//
//func (man *ConfigInstanceManager) InitInstance(inst int32) *ConfigProposingBookkeeping {
//
//}
//
//
//StartProposal(pbk *PBK, inst int32)
//ShouldRetryInstance(pbk *PBK, retry RetryInfo) bool
//HandleReceivedBallot(pbk *PBK, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool
//
////HandleAcceptedBallot(pbk *PBK, inst int32, ballot stdpaxosproto.Ballot, cmd []state.Command)
//HandleProposalChosen(pbk *PBK, inst int32, ballot stdpaxosproto.Ballot)
