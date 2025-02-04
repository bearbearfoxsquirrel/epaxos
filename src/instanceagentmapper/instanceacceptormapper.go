package instanceagentmapper

import (
	"math/rand"
)

type InstanceAgentMapper interface {
	GetGroup(inst int32) []int32
}

type FixedInstanceAgentMapping struct {
	Groups [][]int32
}

func (mapper *FixedInstanceAgentMapping) GetGroup(inst int32) []int32 {
	return mapper.Groups[int(inst)%len(mapper.Groups)]
}

type InstanceAcceptorSetMapper struct {
	Acceptors []int32
	F         int32
	N         int32
}

type InstanceNegativeAcceptorSetMapper struct {
	Acceptors []int32
	F         int32
	N         int32
}

type DetRandInstanceSetMapper struct {
	Ids []int32
	G   int32
	N   int32
}

func (mapper *DetRandInstanceSetMapper) GetGroup(inst int32) []int32 {
	group := make([]int32, mapper.G)
	rem := make([]int32, len(mapper.Ids))
	copy(rem, mapper.Ids)
	group = getGroup(inst, mapper.G, mapper.N, rem, group)
	//log.Println("got group for instance", inst, "group is", group)
	return group
}

func (mapper *InstanceNegativeAcceptorSetMapper) GetGroup(inst int32) []int32 {
	// will pick from the same group as the acceptor group mapper as uses same function
	group := make([]int32, mapper.F+1)
	rem := make([]int32, len(mapper.Acceptors))

	copy(rem, mapper.Acceptors)
	group = getGroup(inst, mapper.F+1, mapper.N, rem, group)
	//log.Println("got negative group for instance", inst, "group is", group)
	return group
}

func getGroup(inst int32, numToGet int32, numAgents int32, rem []int32, group []int32) []int32 {
	if numToGet > numAgents {
		panic("Too many agents to get for how many agents provided")
	}
	for i := int32(0); i < numToGet; i++ {
		//get hashed aid from rank
		random := rand.New(rand.NewSource(int64(inst + i)))
		r := random.Int31()
		selectedRank := r % (numAgents - i)
		selectedAid := rem[selectedRank]

		// add to group
		group[i] = selectedAid

		// do not consider aid any more
		rem = remove(rem, selectedRank)
	}
	return group
}

func (mapper *InstanceAcceptorSetMapper) GetGroup(inst int32) []int32 {
	group := make([]int32, 2*mapper.F+1)
	rem := make([]int32, len(mapper.Acceptors))
	copy32(rem, mapper.Acceptors)
	group = getGroup(inst, 2*mapper.F+1, mapper.N, rem, group)
	//log.Println("got group for instance", inst, "group is", group)
	return group
}

func copy32(dst []int32, src []int32) {
	for i := 0; i < len(src); i++ {
		dst[i] = src[i]
	}
}

type InstanceAcceptorGridMapper struct {
	Acceptors []int32
	F         int32
	N         int32
}

func remove(s []int32, i int32) []int32 {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func (mapper *InstanceAcceptorGridMapper) GetGroup(inst int32) []int32 {
	gridSize := (mapper.F + 1) * (mapper.F + 1)
	group := make([]int32, gridSize)
	rem := make([]int32, len(mapper.Acceptors))
	copy(rem, mapper.Acceptors)

	for i := int32(0); i < gridSize; i++ {
		//get hashed aid from rank
		random := rand.New(rand.NewSource(int64(inst + i)))
		selectedRank := int32(random.Int()) % (mapper.N - i)
		selectedAid := rem[selectedRank]

		// add to group
		group[i] = selectedAid

		// do not consider aid any more
		rem = remove(rem, selectedRank)
	}
	return group
}
