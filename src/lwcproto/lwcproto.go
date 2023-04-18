package lwcproto

import (
	"epaxos/state"
	"epaxos/stdpaxosproto"
)

type ConfigBal struct {
	Config int32
	stdpaxosproto.Ballot
}

func (confBal ConfigBal) IsZero() bool {
	zero := ConfigBal{
		Config: -1,
		Ballot: stdpaxosproto.Ballot{-1, -1},
	}
	return confBal.Equal(zero)
}

func (configBal ConfigBal) GreaterThan(cmp ConfigBal) bool {
	return configBal.Config > cmp.Config || (configBal.Config == cmp.Config && configBal.Ballot.GreaterThan(cmp.Ballot))
}

func (configBal ConfigBal) Equal(cmp ConfigBal) bool {
	return configBal.Config == cmp.Config && configBal.Ballot.Equal(cmp.Ballot)
}

type Prepare struct {
	LeaderId int32
	Instance int32
	ConfigBal
}
type PrepareReply struct {
	Instance   int32
	Req        ConfigBal
	Cur        ConfigBal
	CurPhase   stdpaxosproto.Phase
	VBal       ConfigBal
	AcceptorId int32
	WhoseCmd   int32
	Command    []*state.Command
}

type Accept struct {
	LeaderId int32
	Instance int32
	ConfigBal
	WhoseCmd int32
	Command  []*state.Command
}

type AcceptReply struct {
	Instance   int32
	AcceptorId int32
	Cur        ConfigBal
	Req        ConfigBal
	CurPhase   stdpaxosproto.Phase
	WhoseCmd   int32
}

type Commit struct {
	LeaderId int32
	Instance int32
	ConfigBal
	WhoseCmd   int32
	MoreToCome int32
	Command    []*state.Command
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	ConfigBal
	Count    int32
	WhoseCmd int32
}
