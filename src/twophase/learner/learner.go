package learner

import (
	"quorum"
	"state"
	"stdpaxosproto"
)

type Learner interface {
	//ClientValueProposed(inst32, batching.ProposalBatch)
	ProposalAccepted(inst int32, ballot stdpaxosproto.Ballot, by int32)
	ProposalValue(inst int32, ballot stdpaxosproto.Ballot, cmd []*state.Command, whosecmd int32)
	ProposalChosen(inst int32, ballot stdpaxosproto.Ballot)
	IsChosen(int32) bool       // has chosen ballot
	HasLearntValue(int32) bool // has learnt value
	GetChosen(int32) (stdpaxosproto.Ballot, []*state.Command, int32)
}

type executor struct {
	executedUpTo int32
	exec         bool
}

type cmdProposed struct {
	whose int32
	cmds  []*state.Command
}
type DesignatedLearner struct {
	AQConstructor
	qrm    map[int32]map[stdpaxosproto.Ballot]quorum.QuorumTally
	val    map[int32]map[stdpaxosproto.Ballot]cmdProposed
	learnt map[int32]stdpaxosproto.Ballot
}

func GetDesignedLearner(qrmC AQConstructor) DesignatedLearner {
	return DesignatedLearner{
		AQConstructor: qrmC,
		qrm:           make(map[int32]map[stdpaxosproto.Ballot]quorum.QuorumTally),
		val:           make(map[int32]map[stdpaxosproto.Ballot]cmdProposed),
		learnt:        make(map[int32]stdpaxosproto.Ballot),
	}
}

//proposalChosen
func (l *DesignatedLearner) proposalChosen(inst int32, bal stdpaxosproto.Ballot) {
	if bal.IsZero() {
		return
	}

	if _, e := l.learnt[inst]; e {
		return
	}
	l.learnt[inst] = bal
}

func (l *DesignatedLearner) ProposalAccepted(inst int32, ballot stdpaxosproto.Ballot, aid int32) {
	if ballot.IsZero() {
		return
	}

	if _, e := l.learnt[inst]; e {
		return
	}
	if _, e := l.qrm[inst]; !e {
		l.qrm[inst] = make(map[stdpaxosproto.Ballot]quorum.QuorumTally)
	}
	if _, e := l.qrm[inst][ballot]; !e {
		l.qrm[inst][ballot] = l.AQConstructor.CreateTally(inst)
	}
	l.qrm[inst][ballot].Add(aid)
	if !l.qrm[inst][ballot].Reached() {
		return
	}
	if _, e := l.val[inst][ballot]; !e {
		panic("Designated learner should not learn instance with acceptances without having a value to also learn")
	}
	l.learnt[inst] = ballot
}

func (l *DesignatedLearner) ProposalValue(inst int32, ballot stdpaxosproto.Ballot, commands []*state.Command, whose int32) {
	if ballot.IsZero() {
		return
	}
	if _, e := l.learnt[inst]; e {
		v := l.val[inst][l.learnt[inst]].cmds
		if ballot.GreaterThan(l.learnt[inst]) && !state.CommandsEqual(commands, v) {
			panic("ProposedBatch value different from chosen value in previous ballot")
		}
		return
	}
	if _, e := l.val[inst]; !e {
		l.val[inst] = make(map[stdpaxosproto.Ballot]cmdProposed)
	}
	if _, e := l.val[inst][ballot]; e {
		if !state.CommandsEqual(commands, l.val[inst][ballot].cmds) {
			panic("Proposal value not equal to one present for ballot")
		}
		return
	}
	l.val[inst][ballot] = cmdProposed{whose: whose, cmds: commands}
}

func (l *DesignatedLearner) ProposalChosen(inst int32, ballot stdpaxosproto.Ballot) {
	if ballot.IsZero() {
		return
	}
	if _, e := l.learnt[inst]; e {
		return
	}
	if _, e := l.val[inst][ballot]; !e {
		panic("Designated learner should not learn instance with acceptances without having a value to also learn")
	}
	l.learnt[inst] = ballot
}

func (l *DesignatedLearner) IsChosen(inst int32) bool {
	if _, e := l.learnt[inst]; e {
		return true
	}
	return false
}

func (l *DesignatedLearner) HasLearntValue(inst int32) bool {
	if _, e := l.learnt[inst]; e {
		return true
	}
	return false
}

func (l *DesignatedLearner) GetChosen(inst int32) (stdpaxosproto.Ballot, []*state.Command, int32) {
	if _, e := l.learnt[inst]; !e {
		panic("Cannot return chosen because there is no chosen")
	}
	c := l.val[inst][l.learnt[inst]]
	return l.learnt[inst], c.cmds, c.whose
}

// find out that a ballot is chosen, find out the value of a ballot equal to or greater than
type BcastAcceptLearner struct {
	val map[int32]map[stdpaxosproto.Ballot]cmdProposed
	AQConstructor
	qrm     map[int32]map[stdpaxosproto.Ballot]quorum.QuorumTally
	learntB map[int32]map[stdpaxosproto.Ballot]struct{}
	learntV map[int32]cmdProposed
}

func GetBcastAcceptLearner(qrmC AQConstructor) BcastAcceptLearner {
	return BcastAcceptLearner{
		val:           make(map[int32]map[stdpaxosproto.Ballot]cmdProposed),
		AQConstructor: qrmC,
		qrm:           make(map[int32]map[stdpaxosproto.Ballot]quorum.QuorumTally),
		learntB:       make(map[int32]map[stdpaxosproto.Ballot]struct{}),
		learntV:       make(map[int32]cmdProposed),
	}
}

func (l *BcastAcceptLearner) ProposalAccepted(inst int32, ballot stdpaxosproto.Ballot, aid int32) {
	if ballot.IsZero() {
		return
	}
	if l.IsChosen(inst) && l.HasLearntValue(inst) {
		return
	}
	if _, e := l.qrm[inst]; !e {
		l.qrm[inst] = make(map[stdpaxosproto.Ballot]quorum.QuorumTally)
	}
	if _, e := l.qrm[inst][ballot]; !e {
		l.qrm[inst][ballot] = l.AQConstructor.CreateTally(inst)
	}
	l.qrm[inst][ballot].Add(aid)
	if !l.qrm[inst][ballot].Reached() {
		return
	}
	if _, e := l.learntB[inst]; !e {
		l.learntB[inst] = make(map[stdpaxosproto.Ballot]struct{})
	}
	l.learntB[inst][ballot] = struct{}{}
	if _, e := l.val[inst]; !e {
		//dlog.AgentPrintfN(l.id, "Can")
		return
	}
	l.updateLearntV(inst, ballot)
}

func (l *BcastAcceptLearner) updateLearntV(inst int32, ballot stdpaxosproto.Ballot) {
	for b, v := range l.val[inst] {
		if ballot.GreaterThan(b) {
			continue
		}
		// proposed value will be equal to previously chosen value
		l.learntV[inst] = v
		break
	}
}

func (l *BcastAcceptLearner) ProposalValue(inst int32, ballot stdpaxosproto.Ballot, commands []*state.Command, whose int32) {
	if ballot.IsZero() {
		return
	}
	if l.IsChosen(inst) && l.HasLearntValue(inst) {
		return
	}
	if _, e := l.val[inst]; !e {
		l.val[inst] = make(map[stdpaxosproto.Ballot]cmdProposed)
	}
	l.val[inst][ballot] = cmdProposed{
		whose: whose,
		cmds:  commands,
	}

	// if there is any chosen ballots, check if any of them are less than or equal to this one
	for b, _ := range l.learntB[inst] {
		if b.GreaterThan(ballot) {
			continue
		}
		l.learntV[inst] = cmdProposed{
			whose: whose,
			cmds:  commands,
		}
		break
	}
}

func (l *BcastAcceptLearner) ProposalChosen(inst int32, ballot stdpaxosproto.Ballot) {
	if ballot.IsZero() {
		return
	}
	if l.IsChosen(inst) && l.HasLearntValue(inst) {
		return
	}
	if _, e := l.learntB[inst]; !e {
		l.learntB[inst] = make(map[stdpaxosproto.Ballot]struct{})
	}
	l.learntB[inst][ballot] = struct{}{}
	if _, e := l.val[inst]; !e {
		//dlog.AgentPrintfN(l.id, "Can")
		return
	}
	l.updateLearntV(inst, ballot)
}

func (l *BcastAcceptLearner) IsChosen(inst int32) bool {
	if _, e := l.learntB[inst]; e {
		return true
	}
	return false
}

func (l *BcastAcceptLearner) HasLearntValue(inst int32) bool {
	if _, e := l.learntV[inst]; e {
		return true
	}
	return false
}

func (l *BcastAcceptLearner) GetChosen(inst int32) (stdpaxosproto.Ballot, []*state.Command, int32) {
	if !l.IsChosen(inst) || !l.HasLearntValue(inst) {
		panic("Cannot return chosen because there is no chosen")
	}
	cB := stdpaxosproto.Ballot{}
	for b, _ := range l.learntB[inst] {
		cB = b
		break
	}
	return cB, l.learntV[inst].cmds, l.learntV[inst].whose
}
