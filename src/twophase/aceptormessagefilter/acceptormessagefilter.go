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

func MinimalAcceptorFilterNew(minimalAcceptor instanceagentmapper.InstanceAcceptorMapper) AcceptorMessageFilter {
	return &MinimalAcceptorFilter{
		InstanceAcceptorMapper: minimalAcceptor,
		minimalGroups:          make(map[int32][]int),
	}
}

type MinimalAcceptorFilter struct {
	instanceagentmapper.InstanceAcceptorMapper
	minimalGroups map[int32][]int
}

func (m *MinimalAcceptorFilter) ShouldFilterMessage(aid int32, inst int32) bool {
	if _, exists := m.minimalGroups[inst]; !exists {
		m.minimalGroups[inst] = m.GetGroup(int(inst))
	}

	for i := 0; i < len(m.minimalGroups[inst]); i++ {
		if m.minimalGroups[inst][i] == int(aid) {
			return false
		}
	}
	return true
}

type MinimalAcceptorQuorumMinimalAcceptorFilter struct {
	instanceAcceptorGroupMapper instanceagentmapper.InstanceAcceptorMapper
	minimalGroups               map[int32][]int
	f                           int32
}
