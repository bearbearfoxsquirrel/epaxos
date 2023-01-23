package logfmt

import (
	"epaxos/dlog"
	"epaxos/stdpaxosproto"
	"fmt"
)

func OpenInstanceSignalChosen(id int32, inst int32, chosenAt stdpaxosproto.Ballot) {
	s := "(by someone else)"
	if id == int32(chosenAt.PropID) {
		s = "(by us)"
	}
	dlog.AgentPrintfN(id, "Signalling to open new instance as attempted instance %d is chosen %s", inst, s)
}

// Receive Messages
func ReceivePrepareFmt(prepare *stdpaxosproto.Prepare) string {
	return fmt.Sprintf("Replica received a Prepare from Replica %d in instance %d at ballot %d.%d", prepare.PropID, prepare.Instance, prepare.Number, prepare.PropID)
}
func ReceivePrepareReplyFmt(preply *stdpaxosproto.PrepareReply) string {
	return fmt.Sprintf("Replica received a Prepare Reply from Replica %d in instance %d at requested ballot %d.%d, current ballot %d.%d and value ballot %d.%d with whose commands %d", preply.AcceptorId, preply.Instance, preply.Req.Number, preply.Req.PropID, preply.Cur.Number, preply.Cur.PropID, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd)
}

func ReceiveAcceptFmt(accept *stdpaxosproto.Accept) string {
	return fmt.Sprintf("Replica received Accept from Replica %d in instance %d at ballot %d.%d", accept.PropID, accept.Instance, accept.Number, accept.PropID)
}
func ReceiveAcceptReplyFmt(areply *stdpaxosproto.AcceptReply) string {
	return fmt.Sprintf("Replica received Accept Reply from Replica %d in instance %d at requested ballot %d.%d and current ballot %d.%d", areply.AcceptorId, areply.Instance, areply.Req.Number, areply.Req.PropID, areply.Cur.Number, areply.Cur.PropID)
}
func ReceiveCommitFmt(commit *stdpaxosproto.Commit) string {
	return fmt.Sprintf("Replica received Commit from Replica %d in instance %d at ballot %d.%d with whose commands %d", commit.PropID, commit.Instance, commit.Ballot.Number, commit.Ballot.PropID, commit.WhoseCmd)
}

func ReceiveCommitShortFmt(commit *stdpaxosproto.CommitShort) string {
	return fmt.Sprintf("Replica received Commit Short from Replica %d in instance %d at ballot %d.%d with whose commands %d", commit.PropID, commit.Instance, commit.Ballot.Number, commit.Ballot.PropID, commit.WhoseCmd)
}
