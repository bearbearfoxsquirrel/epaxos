package twophase

import (
	"stdpaxosproto"
)

// does not protect against own ballots - user must make sure ballot is not own
type minimalProposersShouldMaker struct {
	//myBallots        map[int32]stdpaxosproto.Ballot
	myId             int16
	ongoingProposals map[int32]map[int16]stdpaxosproto.Ballot
	instancesToForgo map[int32]struct{}
	f                int
}

func (p *minimalProposersShouldMaker) setOngoingProposals(inst int32, ballot stdpaxosproto.Ballot) {
	if _, e := p.ongoingProposals[inst]; !e {
		p.ongoingProposals[inst] = make(map[int16]stdpaxosproto.Ballot)
	}
	p.ongoingProposals[inst][ballot.PropID] = ballot
}

func (p *minimalProposersShouldMaker) getOngoingProposal(inst int32, pid int16) stdpaxosproto.Ballot {
	if _, e := p.ongoingProposals[inst]; !e {
		p.ongoingProposals[inst] = make(map[int16]stdpaxosproto.Ballot)
	}
	return p.ongoingProposals[inst][pid]
}

func (p *minimalProposersShouldMaker) getOngoingProposals(inst int32) map[int16]stdpaxosproto.Ballot {
	if _, e := p.ongoingProposals[inst]; !e {
		p.ongoingProposals[inst] = make(map[int16]stdpaxosproto.Ballot)
	}
	return p.ongoingProposals[inst]
}

func (p *minimalProposersShouldMaker) ballotReceived(inst int32, ballot stdpaxosproto.Ballot) {
	_, forgoneAlready := p.instancesToForgo[inst]                                         // don't need to track instance as we will not make more proposals to it
	if !ballot.GreaterThan(p.getOngoingProposal(inst, ballot.PropID)) || forgoneAlready { // || p.myBallots[inst].GreaterThan(ballot) {
		return
	}
	p.setOngoingProposals(inst, ballot)
	greaterThan := 0
	myBal := p.getOngoingProposal(inst, p.myId)
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

func (p *minimalProposersShouldMaker) shouldSkipInstance(inst int32) bool {
	_, exists := p.instancesToForgo[inst]
	return exists

	// there are f+1 proposals greater than ours
}

func (p *minimalProposersShouldMaker) startedMyProposal(inst int32, ballot stdpaxosproto.Ballot) {
	if ballot.PropID != p.myId {
		panic("Not my id")
	}
	if p.shouldSkipInstance(inst) { //len(p.ongoingProposals[inst]) > p.f+1 {
		panic("Should not make proposal once f+1 greater proposals wittnessed")
	}
	for _, ongoingBal := range p.ongoingProposals[inst] {
		if ongoingBal.GreaterThan(ballot) || ballot.Equal(ongoingBal) {
			panic("New ballot not greater than")
		}
	}
	p.setOngoingProposals(inst, ballot)
	delete(p.instancesToForgo, inst)
}
