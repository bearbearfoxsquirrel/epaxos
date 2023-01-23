package twophase

import (
	"epaxos/dlog"
	"epaxos/instanceagentmapper"
	"epaxos/lwcproto"
	"epaxos/stdpaxosproto"
	"epaxos/twophase/proposer"
	"sort"
	"time"
)

//	type AwaitingGroup interface {
//		GetAwaitingGroup(inst int32) []int32
//	}
type ProposerGroupGetter interface {
	GetGroup(inst int32) []int32 // change to map of int32[]struct?
}

type SimpleProposersAwaitingGroup struct {
	aids []int32
}

func SimpleProposersAwaitingGroupGetterNew(aids []int32) *SimpleProposersAwaitingGroup {
	return &SimpleProposersAwaitingGroup{
		aids: aids,
	}
}

func (g *SimpleProposersAwaitingGroup) GetGroup(inst int32) []int32 {
	return g.aids
}

type MinimalProposersAwaitingGroup struct {
	*proposer.MinimalProposersShouldMaker
	//*proposer.MinimalProposersInstanceManager
	*SimpleProposersAwaitingGroup
	f int32
}

func MinimalProposersAwaitingGroupNew(simpleG *SimpleProposersAwaitingGroup, maker *proposer.MinimalProposersShouldMaker, f int32) *MinimalProposersAwaitingGroup {
	return &MinimalProposersAwaitingGroup{
		MinimalProposersShouldMaker:  maker,
		SimpleProposersAwaitingGroup: simpleG,
		f:                            f,
	}
}

func (g *MinimalProposersAwaitingGroup) GetGroup(inst int32) []int32 {
	ongoingProposals := g.MinimalProposersShouldMaker.GetOngoingProposals(inst)
	if int32(len(ongoingProposals)) < g.f+1 {
		return g.SimpleProposersAwaitingGroup.GetGroup(inst)
	}
	topProps := make([]lwcproto.ConfigBal, 0, len(ongoingProposals))
	topProposers := make([]int32, 0, len(ongoingProposals))
	for proposer, bal := range g.GetOngoingProposals(inst) {
		topProps = append(topProps, bal)
		topProposers = append(topProposers, int32(proposer))
	}
	sort.Slice(topProposers, func(i, j int) bool {
		return topProps[j].GreaterThan(topProps[i]) || topProps[j].Equal(topProps[i])

	})
	return topProposers[:g.f+1]
}

type MappedProposersAwaitingGroup struct {
	instanceagentmapper.InstanceAgentMapper
}

func MappedProposersAwaitingGroupNew(mapper instanceagentmapper.InstanceAgentMapper) *MappedProposersAwaitingGroup {
	return &MappedProposersAwaitingGroup{
		InstanceAgentMapper: mapper,
	}
}

func (decider *MappedProposersAwaitingGroup) GetGroup(inst int32) []int32 {
	return decider.InstanceAgentMapper.GetGroup(inst)
}

type MinimalMappedProposersAwaitingGroup struct {
	MinimalProposersAwaitingGroup
	MappedProposersAwaitingGroup
}

func MinimalMappedProposersAwaitingGroupNew(minimalGroup MinimalProposersAwaitingGroup, awaitingGroup MappedProposersAwaitingGroup) *MinimalMappedProposersAwaitingGroup {
	return &MinimalMappedProposersAwaitingGroup{
		MinimalProposersAwaitingGroup: minimalGroup,
		MappedProposersAwaitingGroup:  awaitingGroup,
	}
}

func (decider *MinimalMappedProposersAwaitingGroup) GetGroup(inst int32) []int32 {
	minimalG := decider.MinimalProposersAwaitingGroup.GetGroup(inst)
	if int32(len(minimalG)) == decider.f+1 {
		return minimalG
	}
	return decider.MappedProposersAwaitingGroup.GetGroup(inst)
}

type patientProposals struct {
	myId                int32
	promisesRequestedAt map[int32]map[stdpaxosproto.Ballot]time.Time
	pidsPropRecv        map[int32]map[int32]struct{}
	doPatient           bool
	Ewma                []float64
	closed              map[int32]struct{}
	ProposerGroupGetter
}

func (patient *patientProposals) learnOfProposal(inst int32, ballot stdpaxosproto.Ballot) {
	//if _, e := patient.promisesRequestedAt[inst]; !e {
	//	dlog.AgentPrintfN(patient.myId, "No longer considering proposals received for instance %d in patient proposals", inst)
	//	return

	//}
	if _, e := patient.closed[inst]; e {
		dlog.AgentPrintfN(patient.myId, "No longer considering proposals received for instance %d in patient proposals", inst)
		return
	}

	if _, e := patient.pidsPropRecv[inst]; !e {
		patient.pidsPropRecv[inst] = make(map[int32]struct{})
	}
	patient.pidsPropRecv[inst][int32(ballot.PropID)] = struct{}{}
}

func (patient *patientProposals) alreadyHeardFrom(inst int32, id int32) bool {
	pidsRecv, e := patient.pidsPropRecv[inst]
	if !e {
		return false
	}
	_, pidE := pidsRecv[id]
	return pidE
}

func (patient *patientProposals) gotPromise(inst int32, ballot stdpaxosproto.Ballot, from int32) {
	if _, e := patient.closed[inst]; e {
		dlog.AgentPrintfN(patient.myId, "No longer considering promises received for instance %d in patient proposals", inst)
		return
	}

	t := time.Now().UnixNano() - patient.promisesRequestedAt[inst][ballot].UnixNano()
	dlog.AgentPrintfN(patient.myId, "It took %d microseconds to receive prepare reply from replica %d in instance %d at ballot %d.%d", time.Duration(t).Microseconds(), from, inst, ballot.Number, ballot.PropID)
	patient.Ewma[from] = (1-0.3)*patient.Ewma[from] + 0.3*float64(t)
}

func (patient *patientProposals) startedProposal(inst int32, ballot stdpaxosproto.Ballot) {
	if _, e := patient.promisesRequestedAt[inst]; !e {
		patient.promisesRequestedAt[inst] = make(map[stdpaxosproto.Ballot]time.Time)
	}
	patient.promisesRequestedAt[inst][ballot] = time.Now()
}

func (patient *patientProposals) stoppingProposals(inst int32, ballot stdpaxosproto.Ballot) {
	delete(patient.promisesRequestedAt, inst)
	delete(patient.pidsPropRecv, inst)
	patient.closed[inst] = struct{}{}
}

// Maximum latency we should expect from a replica -- based on who is alive
func (patient *patientProposals) getMaxExpectedLatency(inst int32) time.Duration {
	maxAlive := time.Duration(0)
	g := patient.ProposerGroupGetter.GetGroup(inst)

	for i, ewma := range patient.Ewma { // need to know that no all acceptors will promise and so will have different latencies
		lat := time.Duration(ewma)
		inG := false
		for _, gMem := range g {
			if gMem == int32(i) {
				inG = true
				break
			}
		}
		if maxAlive >= lat || i == int(patient.myId) || !inG || patient.alreadyHeardFrom(inst, int32(i)) {
			continue
		}
		maxAlive = lat
	}
	return maxAlive
}

func (patient *patientProposals) getTimeToDelayProposal(inst int32, ballot stdpaxosproto.Ballot) time.Duration {
	if _, e := patient.promisesRequestedAt[inst]; !e {
		patient.promisesRequestedAt[inst] = make(map[stdpaxosproto.Ballot]time.Time)
	}
	begunAt, exists := patient.promisesRequestedAt[inst][ballot]
	if !exists {
		panic("how can we try propose to an instance we haven't yet begin?")
	}

	expectedMaxLatency := patient.getMaxExpectedLatency(inst)
	acqLat := time.Now().Sub(begunAt)
	dlog.AgentPrintfN(patient.myId, "It took %d microseconds (expected %d microseconds) to acquire a promise quorum in instance %d at ballot %d.%d",
		acqLat.Microseconds(), expectedMaxLatency.Microseconds(), inst, ballot.Number, ballot.PropID)
	if !patient.doPatient {
		return 0
	}
	return expectedMaxLatency - acqLat
}
