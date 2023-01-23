package proposer

import (
	"epaxos/batching"
	"epaxos/stdpaxosproto"
	"fmt"
)

func UIDFmt(batch batching.ProposalBatch) string {
	return fmt.Sprintf("UID %d (length %d values)", batch.GetUID(), len(batch.GetCmds()))
}

func BatchFmt(batch batching.ProposalBatch) string {
	return fmt.Sprintf("batch with %s", UIDFmt(batch))
}

func ProposingValue(id int32, pbk *PBK, inst int32, atmptNum int32, disklessNoop bool) string {
	if pbk.WhoseCmds == -1 {
		if disklessNoop { // no diskless noop should be always bcastaccept as we will have always verified there is no conflict (at least with synchronous acceptor)
			return fmt.Sprintf("Proposing diskless noop in instance %d at ballot %d.%d", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
		} else {
			return fmt.Sprintf("Proposing persistent noop in instance %d at ballot %d.%d", inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
		}
	} else {
		if pbk.WhoseCmds == id {
			if atmptNum > 1 {
				return fmt.Sprintf("Proposing again batch with UID %d (length %d values) in instance %d at ballot %d.%d", pbk.ClientProposals.GetUID(), len(pbk.ClientProposals.GetCmds()), inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
			} else {
				return fmt.Sprintf("Proposing batch with UID %d (length %d values) in instance %d at ballot %d.%d", pbk.ClientProposals.GetUID(), len(pbk.ClientProposals.GetCmds()), inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
			}
		} else {
			return fmt.Sprintf("Proposing previous value from ballot %d.%d with whose command %d in instance %d at ballot %d.%d",
				pbk.ProposeValueBal.Number, pbk.ProposeValueBal.PropID, pbk.WhoseCmds, inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
		}
	}
}

func RequeuingBatchFmt(batch batching.ProposalBatch) string {
	return fmt.Sprintf("Requeuing %s", BatchFmt(batch))
}

func RequeuingBatchPreempted(inst int32, ballot stdpaxosproto.Ballot, batch batching.ProposalBatch) string {
	return fmt.Sprintf("Encountered preempting ballot in instance %d ballot %d.%d. %s",
		inst, ballot.Number, ballot.PropID, RequeuingBatchFmt(batch))
}
func RequeuingBatchAcceptedValue(inst int32, ballot stdpaxosproto.Ballot, whose int32, batch batching.ProposalBatch) string {
	return fmt.Sprintf("Encountered previously accepted value in instance %d whose value %d from ballot %d.%d. %s",
		inst, whose, ballot.Number, ballot.PropID, RequeuingBatchFmt(batch))
}
func RequeuingBatchDifferentValueChosen(inst int32, whose int32, batch batching.ProposalBatch) string {
	return fmt.Sprintf("Instance %d chosen with different value than ours (whose command %d). %s",
		inst, whose, RequeuingBatchFmt(batch))
}

func LearntFmt(inst int32, ballot stdpaxosproto.Ballot, proposer Proposer, whose int32) string {
	balloter := proposer.GetBalloter()
	attempts := balloter.GetAttemptNumber(ballot.Number)
	return fmt.Sprintf("Learnt instance %d at ballot %d.%d (%d attempts) with whose commands %d", inst, ballot.Number, ballot.PropID, attempts, whose)
}

func LearntInlineFmt(inst int32, ballot stdpaxosproto.Ballot, proposer Proposer, whose int32) string {
	balloter := proposer.GetBalloter()
	attempts := balloter.GetAttemptNumber(ballot.Number)
	return fmt.Sprintf("learnt instance %d at ballot %d.%d (%d attempts) with whose commands %d", inst, ballot.Number, ballot.PropID, attempts, whose)
}

func LearntBatchFmt(inst int32, ballot stdpaxosproto.Ballot, proposer Proposer, whose int32, batch batching.ProposalBatch) string {
	return fmt.Sprintf("%s (%s)", LearntFmt(inst, ballot, proposer, whose), BatchFmt(batch))
}

func LearntBatchAgainFmt(inst int32, ballot stdpaxosproto.Ballot, proposer Proposer, whose int32, batch batching.ProposalBatch) string {
	return fmt.Sprintf("%s (again learnt %s)", LearntFmt(inst, ballot, proposer, whose), BatchFmt(batch))
}

func ExecutingFmt(inst int32, whose int32) string {
	return fmt.Sprintf("Executing instance %d with whose commands %d", inst, whose)
}

func ExecutingBatchFmt(inst int32, whose int32, batch batching.ProposalBatch) string {
	return fmt.Sprintf("%s (%s)", ExecutingFmt(inst, whose), BatchFmt(batch))
}
