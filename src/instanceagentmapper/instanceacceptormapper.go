package instanceagentmapper

import (
	"math/rand"
)

type InstanceAgentMapper interface {
	GetGroup(inst int) []int
}

type InstanceAcceptorSetMapper struct {
	Acceptors []int
	F         int
	N         int
}

type InstanceNegativeAcceptorSetMapper struct {
	Acceptors []int
	F         int
	N         int
}

type InstanceSetMapper struct {
	Ids []int
	G   int
	N   int
}

func (mapper *InstanceSetMapper) GetGroup(inst int) []int {
	group := make([]int, mapper.G)
	rem := make([]int, len(mapper.Ids))
	copy(rem, mapper.Ids)
	group = getGroup(inst, mapper.G, mapper.N, rem, group)
	//log.Println("got group for instance", inst, "group is", group)
	return group
}

func (mapper *InstanceNegativeAcceptorSetMapper) GetGroup(inst int) []int {
	// will pick from the same group as the acceptor group mapper as uses same function
	group := make([]int, mapper.F+1)
	rem := make([]int, len(mapper.Acceptors))
	copy(rem, mapper.Acceptors)
	group = getGroup(inst, mapper.F+1, mapper.N, rem, group)
	//log.Println("got negative group for instance", inst, "group is", group)
	return group
}

func getGroup(inst int, numToGet int, numAgents int, rem []int, group []int) []int {
	if numToGet > numAgents {
		panic("Too many agents to get for how many agents provided")
	}
	for i := 0; i < numToGet; i++ {
		//get hashed aid from rank
		random := rand.New(rand.NewSource(int64(inst + i)))
		selectedRank := random.Int() % (numAgents - i)
		selectedAid := rem[selectedRank]

		// add to group
		group[i] = selectedAid

		// do not consider aid any more
		rem = remove(rem, selectedRank)
	}
	return group
}

func (mapper *InstanceAcceptorSetMapper) GetGroup(inst int) []int {
	group := make([]int, 2*mapper.F+1)
	rem := make([]int, len(mapper.Acceptors))
	copy(rem, mapper.Acceptors)
	group = getGroup(inst, 2*mapper.F+1, mapper.N, rem, group)
	//log.Println("got group for instance", inst, "group is", group)
	return group
}

type InstanceAcceptorGridMapper struct {
	Acceptors []int
	F         int
	N         int
}

func remove(s []int, i int) []int {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func (mapper *InstanceAcceptorGridMapper) GetGroup(inst int) []int {
	gridSize := (mapper.F + 1) * (mapper.F + 1)
	group := make([]int, gridSize)
	rem := make([]int, len(mapper.Acceptors))
	copy(rem, mapper.Acceptors)

	for i := 0; i < gridSize; i++ {
		//get hashed aid from rank
		random := rand.New(rand.NewSource(int64(inst + i)))
		selectedRank := random.Int() % (mapper.N - i)
		selectedAid := rem[selectedRank]

		// add to group
		group[i] = selectedAid

		// do not consider aid any more
		rem = remove(rem, selectedRank)
	}
	return group
}
