package proposalmanager

import (
	"epaxos/lwcproto"
)

// does not protect against own ballots - user must make sure ballot is not own
type MinimalProposersShouldMaker struct {
	//myBallots        map[int32]stdpaxosproto.Ballot
	myId             int16
	ongoingProposals map[int32]map[int16]lwcproto.ConfigBal
	instancesToForgo map[int32]struct{}
	f                int
}

func MinimalProposersShouldMakerNew(id int16, f int) *MinimalProposersShouldMaker {
	return &MinimalProposersShouldMaker{
		myId:             id,
		ongoingProposals: make(map[int32]map[int16]lwcproto.ConfigBal),
		instancesToForgo: make(map[int32]struct{}),
		f:                f,
	}
}

func (p *MinimalProposersShouldMaker) Cleanup(inst int32) {
	delete(p.ongoingProposals, inst)
	delete(p.instancesToForgo, inst)
}

func (p *MinimalProposersShouldMaker) SetOngoingProposals(inst int32, ballot lwcproto.ConfigBal) {
	if _, e := p.ongoingProposals[inst]; !e {
		p.ongoingProposals[inst] = make(map[int16]lwcproto.ConfigBal)
	}
	p.ongoingProposals[inst][ballot.PropID] = ballot
}

func (p *MinimalProposersShouldMaker) GetOngoingProposal(inst int32, pid int16) lwcproto.ConfigBal {
	if _, e := p.ongoingProposals[inst]; !e {
		p.ongoingProposals[inst] = make(map[int16]lwcproto.ConfigBal)
	}
	return p.ongoingProposals[inst][pid]
}

func (p *MinimalProposersShouldMaker) GetOngoingProposals(inst int32) map[int16]lwcproto.ConfigBal {
	if _, e := p.ongoingProposals[inst]; !e {
		p.ongoingProposals[inst] = make(map[int16]lwcproto.ConfigBal)
	}
	return p.ongoingProposals[inst]
}

func (p *MinimalProposersShouldMaker) BallotReceived(inst int32, ballot lwcproto.ConfigBal) {
	_, forgoneAlready := p.instancesToForgo[inst]                                         // don't need to track instance as we will not make more proposals to it
	if !ballot.GreaterThan(p.GetOngoingProposal(inst, ballot.PropID)) || forgoneAlready { // || p.myBallots[Inst].GreaterThan(ballot) {
		return
	}
	p.SetOngoingProposals(inst, ballot)
	greaterThan := 0
	myBal := p.GetOngoingProposal(inst, p.myId)
	for pid, bal := range p.ongoingProposals[inst] {
		if myBal.GreaterThan(bal) || pid == p.myId {
			continue
		}
		greaterThan++
	}
	if greaterThan < p.f+1 {
		return
	}
	p.instancesToForgo[inst] = struct{}{}
}

func (p *MinimalProposersShouldMaker) ShouldSkipInstance(inst int32) bool {
	_, exists := p.instancesToForgo[inst]
	return exists

	// there are f+1 proposals greater than ours
}

func (p *MinimalProposersShouldMaker) startedMyProposal(inst int32, ballot lwcproto.ConfigBal) {
	if ballot.PropID != p.myId {
		panic("Not my id")
	}
	if p.ShouldSkipInstance(inst) { //len(p.ongoingProposals[Inst]) > p.f+1 {
		panic("Should not make proposal once f+1 greater proposals wittnessed")
	}
	for _, ongoingBal := range p.ongoingProposals[inst] {
		if ongoingBal.GreaterThan(ballot) || ballot.Equal(ongoingBal) {
			panic("New ballot not greater than")
		}
	}
	p.SetOngoingProposals(inst, ballot)
	delete(p.instancesToForgo, inst)
}

func (p *MinimalProposersShouldMaker) GetGroup(inst int32) {

}
