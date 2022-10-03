package paxosproto

import (
	"epaxos/state"
)

const (
	PREPARE uint8 = iota
	PREPARE_REPLY
	ACCEPT
	ACCEPT_REPLY
	COMMIT
	COMMIT_SHORT
)

type Prepare struct {
	LeaderId int32
	Instance int32
	Ballot   int32
}

type PrepareReply struct {
	LBallot       int32
	Instance      int32
	Ballot        int32
	VBallot       int32
	DefaultBallot int32
	AcceptorId    int32
	Command       []*state.Command
}

type Accept struct {
	LeaderId int32
	Instance int32
	Ballot   int32
	Command  []*state.Command
}

type AcceptReply struct {
	Instance int32
	Ballot   int32
	Who      int32
}

type Commit struct {
	LeaderId int32
	Instance int32
	Ballot   int32
	Command  []*state.Command
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	Count    int32
	Ballot   int32
}
