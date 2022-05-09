package twophase

import (
	"batching"
	"dlog"
	"state"
	"stats"
	"stdpaxosproto"
	"time"
)

type ValuePreemptedHandler interface {
	learnOfBallot(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot)
}

type AcceptedValueHandler interface {
	learnOfAcceptedBallot(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, whoseCmds int32)
}

type ValueChosenHandler interface {
	valueChosen(pbk *ProposingBookkeeping, inst int32, whoseCmds int32, cmds []*state.Command)
}

// ProposedClientValuesManager handles information that could affect proposed values and uses it to inform future values to be
// proposed
type ProposedClientValuesManager interface {
	AcceptedValueHandler
	ValuePreemptedHandler
	ValueChosenHandler
}

type SimpleManager struct {
	id    int32
	stats *stats.TimeseriesStats
	Requeueing
	doStats    bool
	requeuedAt map[int32]map[stdpaxosproto.Ballot]struct{}
}

func ProposedClientValuesManagerNew(id int32, tsStats *stats.TimeseriesStats, doStats bool, q Queueing) ProposedClientValuesManager {
	return &SimpleManager{
		id:         id,
		stats:      tsStats,
		Requeueing: q,
		doStats:    doStats,
		requeuedAt: make(map[int32]map[stdpaxosproto.Ballot]struct{}),
	}
}

// is this after or before
func (manager *SimpleManager) learnOfBallot(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot) {
	//todo log
	if pbk.clientProposals == nil {
		return
	}
	if pbk.status == NOT_BEGUN || pbk.status == CLOSED {
		return
	}
	if pbk.propCurBal.GreaterThan(ballot) || pbk.propCurBal.Equal(ballot) {
		return
	}

	if _, e := manager.requeuedAt[inst][pbk.propCurBal]; e {
		return
	}

	dlog.AgentPrintfN(manager.id, "Encountered preempting ballot in instance %d. Requeuing batch with UID %d",
		ballot.Number, ballot.PropID, pbk.propCurBal.Number, pbk.propCurBal.PropID, inst, pbk.clientProposals.GetUID())
	manager.Requeue(pbk.clientProposals)
	manager.setRequeuedAt(inst, pbk.propCurBal)
}

// is this after or before
func (manager *SimpleManager) learnOfAcceptedBallot(pbk *ProposingBookkeeping, inst int32, ballot stdpaxosproto.Ballot, whoseCmds int32) {
	if pbk.clientProposals == nil {
		return
	}
	if whoseCmds == manager.id {
		return
	}
	if pbk.status == NOT_BEGUN || pbk.status == CLOSED || pbk.status == PROPOSING {
		return
	}
	if _, e := manager.requeuedAt[inst][pbk.propCurBal]; e {
		return
	}
	dlog.AgentPrintfN(manager.id, "Encountered previously accepted value in instance %d, whose value %d from ballot %d.%d. Requeuing batch with UID %d",
		inst, whoseCmds, ballot.Number, ballot.PropID, pbk.clientProposals.GetUID())
	//todo log
	manager.Requeue(pbk.clientProposals)
	manager.setRequeuedAt(inst, pbk.propCurBal)
}

func (manager *SimpleManager) setRequeuedAt(inst int32, bal stdpaxosproto.Ballot) {
	if _, e := manager.requeuedAt[inst]; !e {
		manager.requeuedAt[inst] = make(map[stdpaxosproto.Ballot]struct{})
	}
	manager.requeuedAt[inst][bal] = struct{}{}
}

type ClientProposalStory int

const (
	NotProposed ClientProposalStory = iota
	ProposedButNotChosen
	ProposedAndChosen
)

func whatHappenedToClientProposals(pbk *ProposingBookkeeping, whoseCmds int32, myId int32) ClientProposalStory {
	if whoseCmds != myId && pbk.clientProposals != nil {
		return ProposedButNotChosen
	} else if pbk.clientProposals != nil && whoseCmds == myId {
		return ProposedAndChosen
	} else {
		return NotProposed
	}
}

func (manager *SimpleManager) valueChosen(pbk *ProposingBookkeeping, inst int32, whoseCmds int32, cmds []*state.Command) {
	if pbk.whoseCmds == manager.id && pbk.clientProposals == nil {
		panic("client values chosen but we won't recognise that")
	}

	dlog.AgentPrintfN(manager.id, "Instance %d learnt to be chosen with whose commands %d", inst, whoseCmds)
	switch whatHappenedToClientProposals(pbk, whoseCmds, manager.id) {
	case NotProposed:
		break
	case ProposedButNotChosen:
		dlog.AgentPrintfN(manager.id, "%d client value(s) proposed in instance %d not chosen", len(pbk.clientProposals.GetCmds()), inst)
		if _, e := manager.requeuedAt[inst][pbk.propCurBal]; !e {
			manager.Requeue(pbk.clientProposals)
		}
		pbk.clientProposals = nil
		break
	case ProposedAndChosen:
		dlog.AgentPrintfN(manager.id, "%d client value(s) chosen in instance %d", len(pbk.clientProposals.GetCmds()), inst)
		break
	}
	delete(manager.requeuedAt, inst)
}

// MyBatchLearner Wants to answer whether a batch has been chosen or not
type MyBatchLearner interface {
	Learn(bat batching.ProposalBatch)
}

func (b *Balloter) Learn(bat batching.ProposalBatch) {
	b.timeSinceValueLastSelected = time.Now()
}
