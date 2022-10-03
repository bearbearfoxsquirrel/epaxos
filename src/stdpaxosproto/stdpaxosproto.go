package stdpaxosproto

import (
	"epaxos/state"
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

type Phase int32

const (
	//	NIL
	PROMISE Phase = iota
	ACCEPTANCE
	UNKNOWN
)

func (p Phase) int32() int32 {
	return int32(p)
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
	Req        Ballot
	Cur        Ballot
	CurPhase   Phase
	VBal       Ballot
	AcceptorId int32
	WhoseCmd   int32
	Command    []*state.Command
}

type Accept struct {
	LeaderId int32
	Instance int32
	Ballot
	WhoseCmd int32
	Command  []*state.Command
}

type AcceptReply struct {
	Instance   int32
	AcceptorId int32
	Cur        Ballot
	Req        Ballot
	CurPhase   Phase
	WhoseCmd   int32
}

type Commit struct {
	LeaderId int32
	Instance int32
	Ballot
	WhoseCmd   int32
	MoreToCome int32
	Command    []*state.Command
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	Ballot
	Count    int32
	WhoseCmd int32
}
