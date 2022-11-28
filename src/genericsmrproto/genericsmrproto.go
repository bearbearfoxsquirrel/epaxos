package genericsmrproto

import (
	"epaxos/state"
)

const (
	PROPOSE uint8 = iota
	PROPOSE_REPLY
	READ
	READ_REPLY
	PROPOSE_AND_READ
	PROPOSE_AND_READ_REPLY
	UDP_TIBSL_ACK
	GENERIC_SMR_BEACON
	GENERIC_SMR_BEACON_REPLY
	STATS
)

type Propose struct {
	CommandId int32
	//	CliID     int32
	Command   state.Command
	Timestamp int64
}

type ProposeReply struct {
	OK        uint8
	CommandId int32
}

type ProposeReplyTS struct {
	OK uint8
	//	CliID     int32
	CommandId int32
	Value     state.Value
	Timestamp int64
}

type Read struct {
	CommandId int32
	Key       state.Key
}

type ReadReply struct {
	CommandId int32
	Value     state.Value
}

type ProposeAndRead struct {
	CommandId int32
	Command   state.Command
	Key       state.Key
}

type ProposeAndReadReply struct {
	OK        uint8
	CommandId int32
	Value     state.Value
}

// handling stalls and failures

type Beacon struct {
	Timestamp int64
}

type BeaconReply struct {
	Timestamp int64
}

type PingArgs struct {
	ActAsLeader uint8
}

type PingReply struct {
}

type BeTheLeaderArgs struct {
}

type BeTheLeaderReply struct {
}

type Stats struct {
	M map[string]int `json:"stats"`
}
