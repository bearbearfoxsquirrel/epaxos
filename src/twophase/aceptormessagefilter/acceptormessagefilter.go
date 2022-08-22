package aceptormessagefilter

import (
	"instanceagentmapper"
)

type AcceptorMessageFilter interface {
	ShouldFilterMessage(aid int32, inst int32) bool
}

func NoFilterNew() AcceptorMessageFilter {
	return &NoFilter{}
}

type NoFilter struct {
}

func (NoFilter) ShouldFilterMessage(aid int32, inst int32) bool {
	return false
}

func MinimalAcceptorFilterNew(minimalAcceptor instanceagentmapper.InstanceAgentMapper) AcceptorMessageFilter {
	return &MinimalAcceptorFilter{
		InstanceAgentMapper: minimalAcceptor,
		minimalGroups:       make(map[int32][]int32),
	}
}

type MinimalAcceptorFilter struct {
	instanceagentmapper.InstanceAgentMapper
	minimalGroups map[int32][]int32
}

func (m *MinimalAcceptorFilter) ShouldFilterMessage(aid int32, inst int32) bool {
	if _, exists := m.minimalGroups[inst]; !exists {
		m.minimalGroups[inst] = m.GetGroup(inst)
	}

	for i := 0; i < len(m.minimalGroups[inst]); i++ {
		if m.minimalGroups[inst][i] == aid {
			return false
		}
	}
	return true
}

type MinimalAcceptorQuorumMinimalAcceptorFilter struct {
	instanceAcceptorGroupMapper instanceagentmapper.InstanceAgentMapper
	minimalGroups               map[int32][]int32
	f                           int32
}
