package twophase

import (
	"batching"
	"dlog"
	"lwcproto"
	"state"
	"stats"
	"twophase/proposalmanager"
)

type ValuePreemptedHandler interface {
	learnOfBallot(pbk *proposalmanager.PBK, inst int32, ballot lwcproto.ConfigBal)
}

type AcceptedValueHandler interface {
	learnOfAcceptedBallot(pbk *proposalmanager.PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32)
}

type ValueChosenHandler interface {
	valueChosen(pbk *proposalmanager.PBK, inst int32, whoseCmds int32, cmds []*state.Command)
}

type ClientBatchProposedHandler interface {
	intendingToProposeBatch(pbk *proposalmanager.PBK, inst int32, batch batching.ProposalBatch)
}

// ProposedClientValuesManager handles information that could affect proposed values and uses it to inform future values to be
// proposed
type ProposedClientValuesManager interface {
	AcceptedValueHandler
	ValuePreemptedHandler
	ValueChosenHandler
	ClientBatchProposedHandler
}

type SimpleBatchManager struct {
	id    int32
	stats *stats.TimeseriesStats
	Requeueing
	doStats    bool
	requeuedAt map[int32]map[lwcproto.ConfigBal]struct{}
}

func ProposedClientValuesManagerNew(id int32, tsStats *stats.TimeseriesStats, doStats bool, q Queueing) ProposedClientValuesManager {
	return &SimpleBatchManager{
		id:         id,
		stats:      tsStats,
		Requeueing: q,
		doStats:    doStats,
		requeuedAt: make(map[int32]map[lwcproto.ConfigBal]struct{}),
	}
}

func (manager *SimpleBatchManager) intendingToProposeBatch(pbk *proposalmanager.PBK, inst int32, batch batching.ProposalBatch) {
	pbk.PutBatch(batch)
}

// is this after or before
func (manager *SimpleBatchManager) learnOfBallot(pbk *proposalmanager.PBK, inst int32, ballot lwcproto.ConfigBal) {
	//todo log
	if pbk.ClientProposals == nil {
		return
	}
	if pbk.Status == proposalmanager.NOT_BEGUN || pbk.Status == proposalmanager.CLOSED {
		return
	}
	//if pbk.Status == proposalmanager.PROPOSING {
	//	return
	//}
	if pbk.PropCurBal.GreaterThan(ballot) || pbk.PropCurBal.Equal(ballot) {
		return
	}

	if _, e := manager.requeuedAt[inst][pbk.PropCurBal]; e {
		return
	}

	dlog.AgentPrintfN(manager.id, "Encountered preempting ballot in instance %d. Requeuing batch with UID %d",
		inst, pbk.ClientProposals.GetUID())
	manager.Requeue(pbk.ClientProposals)
	manager.setRequeuedAt(inst, pbk.PropCurBal)
}

// is this after or before
func (manager *SimpleBatchManager) learnOfAcceptedBallot(pbk *proposalmanager.PBK, inst int32, ballot lwcproto.ConfigBal, whoseCmds int32) {
	if pbk.ClientProposals == nil {
		return
	}
	if whoseCmds == manager.id {
		return
	}
	if pbk.Status == proposalmanager.NOT_BEGUN || pbk.Status == proposalmanager.CLOSED { // || pbk.Status == PROPOSING {
		return
	}
	if _, e := manager.requeuedAt[inst][pbk.PropCurBal]; e {
		return
	}
	dlog.AgentPrintfN(manager.id, "Encountered previously accepted value in instance %d, whose value %d from ballot %d.%d. Requeuing batch with UID %d",
		inst, whoseCmds, ballot.Number, ballot.PropID, pbk.ClientProposals.GetUID())
	//todo log
	manager.Requeue(pbk.ClientProposals)
	manager.setRequeuedAt(inst, pbk.PropCurBal)
}

func (manager *SimpleBatchManager) setRequeuedAt(inst int32, bal lwcproto.ConfigBal) {
	if _, e := manager.requeuedAt[inst]; !e {
		manager.requeuedAt[inst] = make(map[lwcproto.ConfigBal]struct{})
	}
	manager.requeuedAt[inst][bal] = struct{}{}
}

type ClientProposalStory int

const (
	NotProposed ClientProposalStory = iota
	ProposedButNotChosen
	ProposedAndChosen
)

func whatHappenedToClientProposals(pbk *proposalmanager.PBK, whoseCmds int32, myId int32) ClientProposalStory {
	if whoseCmds != myId && pbk.ClientProposals != nil {
		return ProposedButNotChosen
	} else if pbk.ClientProposals != nil && whoseCmds == myId {
		return ProposedAndChosen
	} else {
		return NotProposed
	}
}

func (manager *SimpleBatchManager) valueChosen(pbk *proposalmanager.PBK, inst int32, whoseCmds int32, cmds []*state.Command) {
	if pbk.WhoseCmds == manager.id && pbk.ClientProposals == nil {
		panic("client values chosen but we won't recognise that")
	}

	dlog.AgentPrintfN(manager.id, "Instance %d learnt to be chosen with whose commands %d", inst, whoseCmds)
	switch whatHappenedToClientProposals(pbk, whoseCmds, manager.id) {
	case NotProposed:
		break
	case ProposedButNotChosen:
		dlog.AgentPrintfN(manager.id, "%d client value(s) proposed in instance %d not chosen", len(pbk.ClientProposals.GetCmds()), inst)
		if _, e := manager.requeuedAt[inst][pbk.PropCurBal]; !e {
			manager.Requeue(pbk.ClientProposals)
		}
		pbk.ClientProposals = nil
		break
	case ProposedAndChosen:
		dlog.AgentPrintfN(manager.id, "%d client value(s) chosen in instance %d", len(pbk.ClientProposals.GetCmds()), inst)
		break
	}
	delete(manager.requeuedAt, inst)
}

// MyBatchLearner Wants to answer whether a batch has been chosen or not
//
//type HedgedBetsBatchManager struct {
//	id    int32
//	stats *stats.TimeseriesStats
//	Requeueing
//	doStats            bool
//	chosen             map[int32]map[int32]struct{}
//	attemptedInstances map[int32]map[int32]struct{}     // keep track of how many instances we have proposed a batch to
//	attemptedBatches   map[int32]batching.ProposalBatch // reverse look up of insts and their batches attempted
//	// only requeue once all attempts have preempted or are requesting requeue
//	// todo add in preempt as a failure
//	failedAttempts map[int32]map[int32]struct{} // instances that a batch has been chosen and preempted (or requesting requeuing)
//}
//
//func HedgedBetsBatchManagerNew(id int32, tsStats *stats.TimeseriesStats, doStats bool, q Queueing) *HedgedBetsBatchManager {
//	return &HedgedBetsBatchManager{
//		id:                 id,
//		stats:              tsStats,
//		Requeueing:         q,
//		doStats:            doStats,
//		attemptedBatches:   make(map[int32]batching.ProposalBatch),
//		chosen:             make(map[int32]map[int32]struct{}),
//		attemptedInstances: make(map[int32]map[int32]struct{}),
//		failedAttempts:     make(map[int32]map[int32]struct{}),
//	}
//}
//
//func (manager *HedgedBetsBatchManager) intendingToProposeBatch(pbk *proposalmanager.PBK, inst int32, batch batching.ProposalBatch) {
//	// this counts up how many times we are attempting to tryPropose a batch at the same time
//	if pbk.Status == proposalmanager.CLOSED {
//		panic("Should not be attempting instance that is chosen")
//	}
//	if _, e := manager.chosen[batch.GetUID()]; e {
//		if len(manager.chosen[batch.GetUID()]) > 0 {
//			panic("Should not be attempting chosen batches")
//		}
//	}
//
//	pbk.ClientProposals = batch
//	dlog.AgentPrintfN(manager.id, "Attempting batch with UID %d in instance %d", batch.GetUID(), inst)
//	if _, e := manager.attemptedInstances[batch.GetUID()]; !e {
//		manager.attemptedInstances[batch.GetUID()] = make(map[int32]struct{})
//	}
//
//	// can occur that we are attempting one batch and then want to attempt another
//	// -- for example hedged batch, then decide not to tryPropose because aldeady did so in another batch
//	// -- then receive another batch to tryPropose instead
//	// in this case, stop following it cause it was never proposed
//	if oldB, e := manager.attemptedBatches[inst]; e {
//		delete(manager.attemptedInstances[oldB.GetUID()], inst)
//		delete(manager.failedAttempts[oldB.GetUID()], inst)
//		//panic("multiple attempted batches here")
//	}
//	manager.attemptedBatches[inst] = batch
//	manager.attemptedInstances[batch.GetUID()][inst] = struct{}{}
//}
//
//// is this after or before
//func (manager *HedgedBetsBatchManager) learnOfBallot(pbk *proposalmanager.PBK, inst int32, ballot stdpaxosproto.Ballot) {
//	// should we do anything here? - in future track preempting?
//	if pbk.ClientProposals == nil {
//		return
//	}
//	if pbk.Status == proposalmanager.NOT_BEGUN || pbk.Status == proposalmanager.CLOSED {
//		return
//	}
//	if pbk.PropCurBal.GreaterThan(ballot) || pbk.PropCurBal.Equal(ballot) {
//		return
//	}
//
//	dlog.AgentPrintfN(manager.id, "Encountered preempting ballot in instance %d for batch with UID %d",
//		inst, pbk.ClientProposals.GetUID())
//	batch, attempted := manager.attemptedBatches[inst]
//	if !attempted {
//		return
//	}
//	manager.markedFailed(inst, batch)
//	if manager.shouldRequeue(batch) {
//		manager.Requeueing.Requeue(batch)
//	}
//
//	if !manager.isAllAccountedFor(batch) {
//		return
//	}
//	// once all current attempts at a proposal have been accounted for delete all tracking of past attempts
//	delete(manager.attemptedInstances, batch.GetUID())
//	delete(manager.chosen, batch.GetUID())
//	delete(manager.failedAttempts, batch.GetUID())
//}
//
//func (manager *HedgedBetsBatchManager) markedFailed(inst int32, batch batching.ProposalBatch) {
//	uid := batch.GetUID()
//	if _, e := manager.failedAttempts[uid]; !e {
//		manager.failedAttempts[uid] = make(map[int32]struct{})
//	}
//	manager.failedAttempts[uid][inst] = struct{}{}
//}
//
//// is this after or before
//func (manager *HedgedBetsBatchManager) learnOfAcceptedBallot(pbk *proposalmanager.PBK, inst int32, ballot stdpaxosproto.Ballot, whoseCmds int32) {
//	// consider the instance preempted?
//	if pbk.ClientProposals == nil {
//		return
//	}
//	if whoseCmds == manager.id {
//		return
//	}
//	if pbk.Status == proposalmanager.NOT_BEGUN || pbk.Status == proposalmanager.CLOSED || pbk.Status == proposalmanager.PROPOSING {
//		return
//	}
//	dlog.AgentPrintfN(manager.id, "Encountered accepted value in instance %d, whose value %d from ballot %d.%d when attempting batch with UID %d",
//		inst, whoseCmds, ballot.Number, ballot.PropID, pbk.ClientProposals.GetUID())
//	batch, attempted := manager.attemptedBatches[inst]
//	if !attempted {
//		return
//	}
//	manager.markedFailed(inst, batch)
//	if manager.shouldRequeue(batch) {
//		manager.Requeueing.Requeue(batch)
//	}
//
//	if !manager.isAllAccountedFor(batch) {
//		return
//	}
//	// once all current attempts at a proposal have been accounted for delete all tracking of past attempts
//	delete(manager.attemptedInstances, batch.GetUID())
//	delete(manager.chosen, batch.GetUID())
//	delete(manager.failedAttempts, batch.GetUID())
//}
//
//func (manager *HedgedBetsBatchManager) isAllAccountedFor(batch batching.ProposalBatch) bool {
//	return len(manager.chosen[batch.GetUID()])+len(manager.failedAttempts[batch.GetUID()]) >= len(manager.attemptedInstances[batch.GetUID()])
//}
//
//func (manager *HedgedBetsBatchManager) shouldRequeue(batch batching.ProposalBatch) bool {
//	return len(manager.chosen[batch.GetUID()]) == 0 && manager.isAllAccountedFor(batch)
//}
//
//func (manager *HedgedBetsBatchManager) valueChosen(pbk *proposalmanager.PBK, inst int32, whoseCmds int32, cmds []*state.Command) {
//	if pbk.WhoseCmds == manager.id && pbk.ClientProposals == nil {
//		panic("client values chosen but we won't recognise that")
//	}
//	dlog.AgentPrintfN(manager.id, "Instance %d learnt to be chosen with whose commands %d", inst, whoseCmds)
//	batchProposed := pbk.ClientProposals
//	whatHappened := whatHappenedToClientProposals(pbk, whoseCmds, manager.id)
//
//	batch, attempted := manager.attemptedBatches[inst]
//	if !attempted {
//		return
//	}
//	uid := batch.GetUID()
//	if whatHappened == NotProposed {
//		manager.markedFailed(inst, batch)
//	}
//	if whatHappened == ProposedButNotChosen {
//		dlog.AgentPrintfN(manager.id, "%d client value(s) proposed in instance %d not chosen", len(batchProposed.GetCmds()), inst)
//		manager.markedFailed(inst, batch)
//		pbk.ClientProposals = nil
//	}
//
//	if whatHappened == ProposedAndChosen {
//		dlog.AgentPrintfN(manager.id, "%d client value(s) chosen in instance %d", len(batchProposed.GetCmds()), inst)
//		if _, e := manager.chosen[uid]; !e {
//			manager.chosen[uid] = make(map[int32]struct{})
//		}
//		manager.chosen[uid][inst] = struct{}{}
//	}
//
//	if manager.shouldRequeue(batch) {
//		// all current intentions have preempted
//		manager.Requeueing.Requeue(batch)
//	}
//
//	if !manager.isAllAccountedFor(batch) {
//		return
//	}
//	// once all current attempts at a proposal have been accounted for delete all tracking of past attempts
//	delete(manager.attemptedBatches, inst)
//	delete(manager.attemptedInstances, batch.GetUID())
//	delete(manager.chosen, batch.GetUID())
//	delete(manager.failedAttempts, batch.GetUID())
//}

//func (manager *HedgedBetsBatchManager) shouldPropose(pbk *PBK, inst int32, batch batching.ProposalBatch) bool {
//	return false
//}
//func (manager *HedgedBetsBatchManager) tryPropose(pbk *PBK, inst int32) bool {
//	if !pbk.ProposeValueBal.IsZero() {
//		//if r.doStats {
//		//	r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Previous Value Proposed", 1)
//		//	r.TimeseriesStats.Update("Times Previous Value Proposed", 1)
//		//	r.ProposalStats.RecordPreviousValueProposed(stats.InstanceID{0, inst}, pbk.PropCurBal, len(pbk.Cmds))
//		//}
//		dlog.AgentPrintfN(manager.id, "Proposing previous value from ballot %d.%d with whose command %d in instance %d at ballot %d.%d",
//			pbk.ProposeValueBal.Number, pbk.ProposeValueBal.PropID, pbk.WhoseCmds, inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
//		return true
//	}
//
//	if pbk.ClientProposals == nil {
//		for foundVal := false; !foundVal; {
//			select {
//			case b := <-manager.Queueing.GetHead():
//				proposeF := func() {
//					if !manager.shouldPropose(pbk, inst, b) {
//						return
//					}
//					manager.intendingToProposeBatch(pbk, inst, b)
//					foundVal = true
//				}
//				manager.Queueing.Dequeued(b, proposeF)
//				break
//			default:
//				return false
//			}
//		}
//	}
//
//	if pbk.ClientProposals != nil {
//		if !r.ProposeBatchOracle.ShouldPropose(pbk.ClientProposals) {
//			pbk.ClientProposals = nil
//			r.tryPropose(inst, priorAttempts)
//			return
//		}
//		setProposingValue(pbk, r.Id, pbk.PropCurBal, pbk.ClientProposals.GetCmds())
//		if r.doStats {
//			r.InstanceStats.RecordOccurrence(stats.InstanceID{0, inst}, "Client Value Proposed", 1)
//			r.ProposalStats.RecordClientValuesProposed(stats.InstanceID{0, inst}, pbk.PropCurBal, len(pbk.Cmds))
//			r.TimeseriesStats.Update("Times Client Values Proposed", 1)
//		}
//		dlog.AgentPrintfN(r.Id, "%d client value(s) from batch with UID %d received and proposed in instance %d at ballot %d.%d \n", len(pbk.ClientProposals.GetCmds()), pbk.ClientProposals.GetUID(), inst, pbk.PropCurBal.Number, pbk.PropCurBal.PropID)
//	}
//
//	if pbk.Cmds == nil {
//		panic("there must be a previously chosen value")
//	}
//
//	pbk.setNowProposing()
//	return true
//}
