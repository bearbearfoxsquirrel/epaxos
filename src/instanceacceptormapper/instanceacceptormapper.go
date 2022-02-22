package instanceacceptormapper

import (
	"math/rand"
)

type InstanceAcceptorMapper interface {
	GetGroup(inst int) []int
}

type InstanceAcceptorSetMapper struct {
	Acceptors []int
	F         int
	N         int
}

func (mapper *InstanceAcceptorSetMapper) GetGroup(inst int) []int {
	group := make([]int, 2*mapper.F+1)
	rem := make([]int, len(mapper.Acceptors))
	copy(rem, mapper.Acceptors)

	for i := 0; i < 2*mapper.F+1; i++ {
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

func main() {
	mapper := InstanceAcceptorGridMapper{
		Acceptors: []int{0, 1, 2, 3, 4, 5, 6},
		F:         1,
		N:         6,
	}

	x := make([][]int, 100)
	for i := 0; i < 100; i++ {
		x[i] = mapper.GetGroup(i)
		//		log.Println(x[i])
	}

}
