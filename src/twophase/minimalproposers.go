package twophase

import (
	"stdpaxosproto"
)

// does not protect against own ballots - user must make sure ballot is not own
type minimalProposersShouldMaker struct {
	myBallots               map[int32]stdpaxosproto.Ballot
	ongoingGreaterProposals map[int32]map[int16]stdpaxosproto.Ballot
	instancesToForgo        map[int32]struct{}
	f                       int
}

func (p *minimalProposersShouldMaker) setOngoingProposals(inst int32, ballot stdpaxosproto.Ballot) {
	if _, e := p.ongoingGreaterProposals[inst]; !e {
		p.ongoingGreaterProposals[inst] = make(map[int16]stdpaxosproto.Ballot)
	}
	p.ongoingGreaterProposals[inst][ballot.PropID] = ballot
}

func (p *minimalProposersShouldMaker) getOngoingProposal(inst int32, pid int16) stdpaxosproto.Ballot {
	if _, e := p.ongoingGreaterProposals[inst]; !e {
		p.ongoingGreaterProposals[inst] = make(map[int16]stdpaxosproto.Ballot)
	}
	return p.ongoingGreaterProposals[inst][pid]
}

func (p *minimalProposersShouldMaker) ballotReceived(inst int32, ballot stdpaxosproto.Ballot) {
	_, forgoneAlready := p.instancesToForgo[inst]
	if p.getOngoingProposal(inst, ballot.PropID).GreaterThan(ballot) || forgoneAlready || p.myBallots[inst].GreaterThan(ballot) {
		return
	}
	//p.ongoingGreaterProposals[inst][ballot.PropID] = ballot
	p.setOngoingProposals(inst, ballot)
	if len(p.ongoingGreaterProposals[inst]) < p.f+1 {
		return
	}
	p.instancesToForgo[inst] = struct{}{}
}

func (p *minimalProposersShouldMaker) shouldSkipInstance(inst int32) bool {
	_, exists := p.instancesToForgo[inst]
	return exists
}

func (p *minimalProposersShouldMaker) startedMyProposal(inst int32, ballot stdpaxosproto.Ballot) {
	if len(p.ongoingGreaterProposals[inst]) > p.f+1 {
		panic("Should not make proposal once f+1 wittnessed")
	}
	for _, ongoingBal := range p.ongoingGreaterProposals[inst] {
		if ongoingBal.GreaterThan(ballot) || ballot.Equal(ongoingBal) {
			panic("New ballot not greater than")
		}
	}
	if p.myBallots[inst].GreaterThan(ballot) {
		panic("my new ballot is out of date")
	}
	p.myBallots[inst] = ballot
	delete(p.instancesToForgo, inst)
	delete(p.ongoingGreaterProposals, inst)
}
