package lwcproto

import (
	"state"
)

type ConfigBal struct {
	Config int32
	Ballot
}

func (confBal ConfigBal) IsZero() bool {
	zero := ConfigBal{
		Config: -1,
		Ballot: Ballot{-1, -1},
	}
	return confBal.Equal(zero)
}

func (configBal ConfigBal) GreaterThan(cmp ConfigBal) bool {
	return configBal.Config > cmp.Config || (configBal.Config == cmp.Config && configBal.Ballot.GreaterThan(cmp.Ballot))
}

func (configBal ConfigBal) Equal(cmp ConfigBal) bool {
	return configBal.Config == cmp.Config && configBal.Ballot.Equal(cmp.Ballot)
}

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
	ConfigBal
}
type PrepareReply struct {
	Instance   int32
	ConfigBal  ConfigBal
	VConfigBal ConfigBal
	AcceptorId int32
	WhoseCmd   int32
	Command    []state.Command
}

type Accept struct {
	LeaderId int32
	Instance int32
	ConfigBal
	WhoseCmd int32
	Command  []state.Command
}

type AcceptReply struct {
	Instance   int32
	AcceptorId int32
	Cur        ConfigBal
	Req        ConfigBal
	WhoseCmd   int32
}

type Commit struct {
	LeaderId int32
	Instance int32
	ConfigBal
	WhoseCmd   int32
	MoreToCome int32
	Command    []state.Command
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	ConfigBal
	Count    int32
	WhoseCmd int32
}
