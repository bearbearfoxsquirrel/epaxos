package stdpaxosproto

import (
	"state"
)

func (bal Ballot) GreaterThan(cmp Ballot) bool {
	return bal.Number > cmp.Number || (bal.Number == cmp.Number && bal.PropID > cmp.PropID)
}

func (bal Ballot) Equal(cmp Ballot) bool {
	return bal.Number == cmp.Number && bal.PropID == cmp.PropID
}

func (bal Ballot) IsZero() bool {
	return bal.Equal(Ballot{
		Number: -1,
		PropID: -1,
	})
}

type Ballot struct {
	Number int32
	PropID int16
}

type Prepare struct {
	LeaderId int32
	Instance int32
	Ballot
}
type PrepareReply struct {
	Instance   int32
	Bal        Ballot
	VBal       Ballot
	AcceptorId int32
	Command    []state.Command
}

type Accept struct {
	LeaderId int32
	Instance int32
	Ballot
	Command []state.Command
}

type AcceptReply struct {
	Instance   int32
	AcceptorId int32
	Cur        Ballot
	Req        Ballot
}

type Commit struct {
	LeaderId int32
	Instance int32
	Ballot
	Command []state.Command
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	Ballot
	Count int32
}
