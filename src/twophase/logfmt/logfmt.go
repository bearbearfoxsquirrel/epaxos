package logfmt

import (
	"epaxos/batching"
	"epaxos/dlog"
	"epaxos/stdpaxosproto"
	"epaxos/twophase/balloter"
	"fmt"
)

func UIDFmt(batch batching.ProposalBatch) string {
	return fmt.Sprintf("UID %d (length %d values)", batch.GetUID(), len(batch.GetCmds()))
}

func BatchFmt(batch batching.ProposalBatch) string {
	return fmt.Sprintf("batch with %s", UIDFmt(batch))
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

func LearntFmt(inst int32, ballot stdpaxosproto.Ballot, balloter *balloter.Balloter, whose int32) string {
	attempts := balloter.GetAttemptNumber(ballot.Number)
	return fmt.Sprintf("Learnt instance %d at ballot %d.%d (%d attempts) with whose commands %d", inst, ballot.Number, ballot.PropID, attempts, whose)
}

func LearntBatchFmt(inst int32, ballot stdpaxosproto.Ballot, balloter *balloter.Balloter, whose int32, batch batching.ProposalBatch) string {
	return fmt.Sprintf("%s (%s)", LearntFmt(inst, ballot, balloter, whose), BatchFmt(batch))
}

func LearntBatchAgainFmt(inst int32, ballot stdpaxosproto.Ballot, balloter *balloter.Balloter, whose int32, batch batching.ProposalBatch) string {
	return fmt.Sprintf("%s (again learnt %s)", LearntFmt(inst, ballot, balloter, whose), BatchFmt(batch))
}

func ExecutingFmt(inst int32, whose int32) string {
	return fmt.Sprintf("Executing instance %d with whose commands %d", inst, whose)
}

func ExecutingBatchFmt(inst int32, whose int32, batch batching.ProposalBatch) string {
	return fmt.Sprintf("%s (%s)", ExecutingFmt(inst, whose), BatchFmt(batch))
}

func OpenInstanceSignalChosen(id int32, inst int32, chosenAt stdpaxosproto.Ballot) {
	s := "(by someone else)"
	if id == int32(chosenAt.PropID) {
		s = "(by us)"
	}
	dlog.AgentPrintfN(id, "Signalling to open new instance as attempted instance %d is chosen %s", inst, s)
}
