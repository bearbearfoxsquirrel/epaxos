package learner

import (
	"epaxos/dlog"
	"epaxos/instanceagentmapper"
	"epaxos/quorum"
	"epaxos/quorumsystem"
)

type AQConstructor interface {
	CreateTally(inst int32) quorum.QuorumTally
}

type StaticDefined struct {
	instanceagentmapper.FixedInstanceAgentMapping
	quorumsystem.AcceptanceQuorumsConstructor
}

func GetStaticDefinedAQConstructor(instags [][]int32, constructor quorumsystem.AcceptanceQuorumsConstructor) StaticDefined {
	return StaticDefined{
		FixedInstanceAgentMapping:    instanceagentmapper.FixedInstanceAgentMapping{instags},
		AcceptanceQuorumsConstructor: constructor,
	}

}

type Minimal struct {
	AcceptorMapper instanceagentmapper.InstanceAgentMapper
	quorumsystem.AcceptanceQuorumsConstructor
	MyID int32
}

func GetMinimalGroupAQConstructorr(n, f int32, ids []int32, constructor quorumsystem.AcceptanceQuorumsConstructor, me int32) Minimal {
	return Minimal{
		AcceptorMapper: &instanceagentmapper.DetRandInstanceSetMapper{
			Ids: ids,
			G:   2*f + 1,
			N:   n,
		},
		AcceptanceQuorumsConstructor: constructor,
		MyID:                         me,
	}
}

func (q *Minimal) CreateTally(inst int32) quorum.QuorumTally {
	group := q.AcceptorMapper.GetGroup(inst)
	dlog.AgentPrintfN(q.MyID, "Minimal acceptor group for instance %d is %v", inst, group)
	return q.AcceptanceQuorumsConstructor.ConstructAQ(group)
}

type Standard struct {
	quorumsystem.AcceptanceQuorumsConstructor
	Aids []int32
	MyID int32
}

func GetStandardGroupAQConstructorr(ids []int32, constructor quorumsystem.AcceptanceQuorumsConstructor, me int32) Standard {
	return Standard{
		AcceptanceQuorumsConstructor: constructor,
		Aids:                         ids,
		MyID:                         me,
	}
}

func (q *Standard) CreateTally(inst int32) quorum.QuorumTally {
	return q.AcceptanceQuorumsConstructor.ConstructAQ(q.Aids)
}
