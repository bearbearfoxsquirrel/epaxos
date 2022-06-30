package proposalmanager

import (
	"dlog"
	"lwcproto"
	"stats"
	"stdpaxosproto"
	"time"
)

type SingleInstanceManager interface {
	InitInstance(inst int32) *ProposingBookkeeping
	StartProposal(pbk *ProposingBookkeeping, inst int32)
	ShouldRetryInstance(pbk *ProposingBookkeeping, retry RetryInfo) bool
	HandleReceivedBallot(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool
	//HandleAcceptedBallot(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, cmd []state.Command)
	HandleProposalChosen(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal)
}

type SimpleInstanceManager struct {
	doStats bool
	id      int32
	*BackoffManager
	Balloter
	ProposerQuorumaliser
	*stats.TimeseriesStats
	*stats.ProposalStats
	*stats.InstanceStats
}

func SimpleInstanceManagerNew(id int32, manager *BackoffManager, balloter Balloter, doStats bool, qrm ProposerQuorumaliser, tsStats *stats.TimeseriesStats, proposalStats *stats.ProposalStats, instanceStats *stats.InstanceStats) *SimpleInstanceManager {
	return &SimpleInstanceManager{
		doStats:              doStats,
		id:                   id,
		BackoffManager:       manager,
		Balloter:             balloter,
		ProposerQuorumaliser: qrm,
		TimeseriesStats:      tsStats,
		ProposalStats:        proposalStats,
		InstanceStats:        instanceStats,
	}
}

func (man *SimpleInstanceManager) InitInstance(inst int32) *ProposingBookkeeping {
	return GetEmptyInstance()
}

func (man *SimpleInstanceManager) StartProposal(pbk *ProposingBookkeeping, inst int32) {
	pbk.Status = PREPARING
	nextBal := man.Balloter.GetNextProposingBal(pbk.MaxKnownBal.Config, pbk.MaxKnownBal.Number)
	pbk.PropCurBal = nextBal
	pbk.MaxKnownBal = nextBal
	man.ProposerQuorumaliser.StartPromiseQuorumOnCurBal(pbk, inst)
	dlog.AgentPrintfN(man.id, "Starting new proposal for instance %d with ballot %d.%d", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
}

func (man *SimpleInstanceManager) ShouldRetryInstance(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	if pbk.Status != BACKING_OFF {
		dlog.AgentPrintfN(man.id, "Skipping retry of instance %d due to it being closed", retry.Inst)
		return false
	}
	if !man.BackoffManager.StillRelevant(retry) || !pbk.PropCurBal.Equal(retry.AttemptedBal) {
		dlog.AgentPrintfN(man.id, "Skipping retry of instance %d due to being preempted again", retry.Inst)
		return false
	}
	return true
}

func (man *SimpleInstanceManager) HandleReceivedBallot(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	if pbk.Status == CLOSED {
		return false
	}
	if int32(ballot.PropID) == man.id {
		return false
	}
	//if pbk.PropCurBal.Equal(ballot) {
	//	return false
	//}

	if pbk.MaxKnownBal.GreaterThan(ballot) {
		return false
	} // want to backoff in face of new phase

	pbk.Status = BACKING_OFF
	if ballot.GreaterThan(pbk.MaxKnownBal) {
		dlog.AgentPrintfN(man.id, "Witnessed new maximum ballot %d.%d for instance %d", ballot.Number, ballot.PropID, inst)
	}
	pbk.MaxKnownBal = ballot

	// NOTE: HERE WE WANT TO INCREASE BACKOFF EACH TIME THERE IS A NEW PROPOSAL SEEN
	backedOff, botime := man.BackoffManager.CheckAndHandleBackoff(inst, pbk.PropCurBal, ballot, phase)

	//if ballot.IsZero() {
	//	panic("askldjfalskdjf")
	//}
	if backedOff {
		dlog.AgentPrintfN(man.id, "Backing off instance %d for %d microseconds because our current ballot %d.%d is preempted by ballot %d.%d",
			inst, botime, pbk.PropCurBal.Number, pbk.PropCurBal.PropID, ballot.Number, ballot.PropID)
	}
	return true
}

func (man *SimpleInstanceManager) howManyAttemptsToChoose(pbk *ProposingBookkeeping, inst int32) {
	attempts := man.Balloter.GetAttemptNumber(pbk.ProposeValueBal.Number)
	dlog.AgentPrintfN(man.id, "Instance %d took %d attempts to be chosen", inst, attempts)
}

func (man *SimpleInstanceManager) HandleProposalChosen(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal) {
	if pbk.Status == CLOSED {
		return
	}

	if man.doStats && pbk.Status != BACKING_OFF && !pbk.PropCurBal.IsZero() {
		if pbk.PropCurBal.GreaterThan(ballot) {
			man.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.PropCurBal, stats.LOWERPROPOSALCHOSEN)
		} else if ballot.GreaterThan(pbk.PropCurBal) {
			man.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.PropCurBal, stats.HIGHERPROPOSALONGOING)
		} else {
			man.ProposalStats.CloseAndOutput(stats.InstanceID{0, inst}, pbk.PropCurBal, stats.ITWASCHOSEN)
		}
	}
	man.BackoffManager.ClearBackoff(inst)

	if ballot.Equal(pbk.PropCurBal) && man.doStats {
		man.InstanceStats.RecordComplexStatEnd(stats.InstanceID{0, inst}, "Phase 2", "Success")
	}

	pbk.Status = CLOSED
	pbk.ProposeValueBal = ballot
	if ballot.GreaterThan(pbk.MaxKnownBal) {
		pbk.MaxKnownBal = ballot
	}
	dlog.AgentPrintfN(man.id, "Instance %d learnt to be chosen at ballot %d.%d", inst, ballot.Number, ballot.PropID)
	man.howManyAttemptsToChoose(pbk, inst)

	if man.doStats {
		atmts := man.Balloter.GetAttemptNumber(ballot.Number) // (pbk.pbk.ProposeValueBal.Number / man.maxBalInc)
		man.InstanceStats.RecordCommitted(stats.InstanceID{0, inst}, atmts, time.Now())
		man.TimeseriesStats.Update("Instances Learnt", 1)
		if int32(ballot.PropID) == man.id {
			man.TimeseriesStats.Update("Instances I Choose", 1)
			man.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "I Chose", 1)
		}
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

func (man *MinimalProposersInstanceManager) StartProposal(pbk *ProposingBookkeeping, inst int32) {
	if man.ShouldSkipInstance(inst) {
		panic("should not start next proposal on instance fulfilled")
	}
	man.SimpleInstanceManager.StartProposal(pbk, inst)
	dlog.AgentPrintfN(man.id, "Starting new proposal for instance %d with ballot %d.%d", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
	man.MinimalProposersShouldMaker.startedMyProposal(inst, pbk.PropCurBal)
}

func (man *MinimalProposersInstanceManager) ShouldRetryInstance(pbk *ProposingBookkeeping, retry RetryInfo) bool {
	if man.MinimalProposersShouldMaker.ShouldSkipInstance(retry.Inst) {
		if pbk.Status == CLOSED {
			dlog.AgentPrintfN(man.id, "Skipping retry of instance %d due to it being closed", retry.Inst)
			return false
		}
		dlog.AgentPrintfN(man.id, "Skipping retry of instance %d as minimal proposer threshold (f+1) met", retry.Inst)
		pbk.Status = BACKING_OFF
		man.BackoffManager.ClearBackoff(retry.Inst)
		return false
	}
	return man.SimpleInstanceManager.ShouldRetryInstance(pbk, retry)
}

func (man *MinimalProposersInstanceManager) HandleReceivedBallot(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal, phase stdpaxosproto.Phase) bool {
	man.minimalProposersLearnOfBallot(ballot, inst)
	return man.SimpleInstanceManager.HandleReceivedBallot(pbk, inst, ballot, phase)
}

func (man *MinimalProposersInstanceManager) HandleProposalChosen(pbk *ProposingBookkeeping, inst int32, ballot lwcproto.ConfigBal) {
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
//StartProposal(pbk *ProposingBookkeeping, inst int32)
//ShouldRetryInstance(pbk *ProposingBookkeeping, retry RetryInfo) bool
//HandleReceivedBallot(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, phase stdpaxosproto.Phase) bool
//
////HandleAcceptedBallot(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, cmd []state.Command)
//HandleProposalChosen(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot)
