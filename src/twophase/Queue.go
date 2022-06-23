package twophase

import (
	"batching"
	"dlog"
	"errors"
	"fmt"
)

type QueueingError struct {
	error
	code int32
}

type Requeueing interface {
	Requeue(bat batching.ProposalBatch) error
}

type Queueing interface {
	Requeueing
	//Enqueue(bat ProposalBatch)
	GetHead() <-chan batching.ProposalBatch
	GetTail() chan<- batching.ProposalBatch
	Dequeued(bat batching.ProposalBatch, onSuccess func()) error // func that should be called once a value is pulled from the queue in a select statement. func is what happens afterwards
}

type UniqueQ struct {
	requeued map[int32]struct{}
	q        chan batching.ProposalBatch
	id       int32
}

type ProposedObserver interface {
	ObserveProposed(proposed batching.ProposalBatch)
}

type ProposeBatchOracle interface {
	ShouldPropose(bat batching.ProposalBatch) bool
}

func (q *UniqueQ) ShouldPropose(bat batching.ProposalBatch) bool {
	return true
}

func (q *UniqueQ) GetTail() chan<- batching.ProposalBatch {
	return q.q
}

func (q *UniqueQ) GetHead() <-chan batching.ProposalBatch {
	return q.q
}

func (q *UniqueQ) Dequeued(bat batching.ProposalBatch, do func()) error {
	delete(q.requeued, bat.GetUID())
	do()
	return nil
}

func (q *UniqueQ) Requeue(bat batching.ProposalBatch) error {
	//dlog.AgentPrintfN(q.id, "Attempting to requeue batch with UID %d", bat.GetUID())
	if _, exists := q.requeued[bat.GetUID()]; exists {
		dlog.AgentPrintfN(q.id, "Not requeuing batch with UID %d as it is already requeued", bat.GetUID())
		return &QueueingError{errors.New(fmt.Sprintf("Not Requeued as batch with UID %d is already in queue", bat.GetUID())), 1}
	}
	//dlog.AgentPrintfN(q.id, "Requeued batch with UID %d", bat.GetUID())
	go func() { q.q <- bat }()
	return nil
}

type ChosenUniqueQ struct {
	chosen map[int32]struct{}
	*UniqueQ
}

func (q *ChosenUniqueQ) ShouldPropose(bat batching.ProposalBatch) bool {
	if _, exists := q.chosen[bat.GetUID()]; exists {
		dlog.AgentPrintfN(q.id, "Batch with UID %d received to propose has been chosen so throwing out", bat.GetUID())
		return false
	}
	return true
}

func (q *ChosenUniqueQ) Dequeued(bat batching.ProposalBatch, do func()) error {
	if _, exists := q.chosen[bat.GetUID()]; exists {
		dlog.AgentPrintfN(q.id, "Batch with UID %d received to propose has been chosen so throwing out", bat.GetUID())
		return &QueueingError{errors.New(fmt.Sprintf("Not Requeued as batch with UID %d is chosen", bat.GetUID())), 2}
	}
	q.UniqueQ.Dequeued(bat, do)
	return nil
}

func (q *ChosenUniqueQ) Requeue(bat batching.ProposalBatch) error {
	//dlog.AgentPrintfN(q.id, "Attempting to requeue batch with UID %d", bat.GetUID())
	if _, exists := q.chosen[bat.GetUID()]; exists {
		dlog.AgentPrintfN(q.id, "Not requeuing batch with UID %d as it is chosen", bat.GetUID())
		return &QueueingError{errors.New(fmt.Sprintf("Not Requeued batch with UID %d as it is chosen", bat.GetUID())), 2}
	}
	if _, exists := q.requeued[bat.GetUID()]; exists {
		dlog.AgentPrintfN(q.id, "Not requeuing batch with UID %d as it is already requeued", bat.GetUID())
		return &QueueingError{errors.New(fmt.Sprintf("Not Requeued batch with UID %d is already in queue", bat.GetUID())), 1}
	}
	//dlog.AgentPrintfN(q.id, "Requeued batch with UID %d", bat.GetUID())
	go func() { q.q <- bat }()
	return nil
}

func (q *ChosenUniqueQ) Learn(bat batching.ProposalBatch) {
	if _, e := q.chosen[bat.GetUID()]; e {
		dlog.AgentPrintfN(q.id, "Batch with UID %d learnt again", bat.GetUID())
		return
	}
	q.chosen[bat.GetUID()] = struct{}{}
}

func UniqueQNew(rId int32, initLen int) *UniqueQ {
	return &UniqueQ{make(map[int32]struct{}), make(chan batching.ProposalBatch, initLen), rId}
}

func ChosenUniqueQNew(rId int32, initLen int) *ChosenUniqueQ {
	return &ChosenUniqueQ{make(map[int32]struct{}), UniqueQNew(rId, initLen)}
}

type ProposingChosenUniqueQ struct {
	proposed map[int32]struct{}
	*ChosenUniqueQ
}

func ProposingChosenUniqueueQNew(rId int32, initLen int) *ProposingChosenUniqueQ {
	return &ProposingChosenUniqueQ{
		proposed:      make(map[int32]struct{}),
		ChosenUniqueQ: ChosenUniqueQNew(rId, initLen),
	}
}

func (q *ProposingChosenUniqueQ) ObserveProposed(bat batching.ProposalBatch) {
	q.proposed[bat.GetUID()] = struct{}{}
}

func (q *ProposingChosenUniqueQ) Requeue(bat batching.ProposalBatch) error {
	err := q.ChosenUniqueQ.Requeue(bat)
	delete(q.proposed, bat.GetUID())
	return err
}

func (q *ProposingChosenUniqueQ) Dequeued(bat batching.ProposalBatch, onSuccess func()) error {
	if _, exists := q.proposed[bat.GetUID()]; exists {
		dlog.AgentPrintfN(q.id, "Batch with UID %d is proposed already so throwing it out", bat.GetUID())
		return &QueueingError{errors.New(fmt.Sprintf("Not Requeued batch with UID %d as it is chosen", bat.GetUID())), 3}
	}
	return q.ChosenUniqueQ.Dequeued(bat, onSuccess)
}

func (q *ProposingChosenUniqueQ) ShouldPropose(bat batching.ProposalBatch) bool {
	if _, exists := q.proposed[bat.GetUID()]; exists {
		dlog.AgentPrintfN(q.id, "Batch with UID %d is proposed already so throwing it out", bat.GetUID())
		return false
	}
	return q.ChosenUniqueQ.ShouldPropose(bat)
}

type ProposingSeveralChosenUniqueQ struct {
	proposed map[int32]int32
	*ChosenUniqueQ
	copies int32
}

func ProposingSeveralChosenUniqueQNew(rId int32, initLen int, proposingCopies int32) *ProposingSeveralChosenUniqueQ {
	return &ProposingSeveralChosenUniqueQ{
		proposed:      make(map[int32]int32),
		ChosenUniqueQ: ChosenUniqueQNew(rId, initLen),
		copies:        proposingCopies,
	}
}

func (q *ProposingSeveralChosenUniqueQ) ObserveProposed(bat batching.ProposalBatch) {
	q.proposed[bat.GetUID()] = q.proposed[bat.GetUID()] + 1
}

func (q *ProposingSeveralChosenUniqueQ) Requeue(bat batching.ProposalBatch) error {
	q.proposed[bat.GetUID()] = q.proposed[bat.GetUID()] - 1
	if q.proposed[bat.GetUID()] == -1 {
		panic("Negative number of times batch proposed now")
	}
	if q.proposed[bat.GetUID()] > 0 {
		return &QueueingError{
			error: errors.New(fmt.Sprintf("Still %d proposals for batch with UID %d remaining outstanding so not requeueing", q.proposed[bat.GetUID()], bat.GetUID())),
			code:  4,
		}
	}
	err := q.ChosenUniqueQ.Requeue(bat)
	return err
}

func (q *ProposingSeveralChosenUniqueQ) Dequeued(bat batching.ProposalBatch, onSuccess func()) error {
	if _, exists := q.proposed[bat.GetUID()]; exists {
		dlog.AgentPrintfN(q.id, "Batch with UID %d is proposed already so throwing it out", bat.GetUID())
		return &QueueingError{errors.New(fmt.Sprintf("Not Requeued batch with UID %d as it is chosen", bat.GetUID())), 3}
	}
	return q.ChosenUniqueQ.Dequeued(bat, onSuccess)
}

func (q *ProposingSeveralChosenUniqueQ) ShouldPropose(bat batching.ProposalBatch) bool {
	if _, exists := q.proposed[bat.GetUID()]; exists {
		dlog.AgentPrintfN(q.id, "Batch with UID %d is proposed already so throwing it out", bat.GetUID())
		return false
	}
	return q.ChosenUniqueQ.ShouldPropose(bat)
}

func (q *ProposingChosenUniqueQ) Learn(bat batching.ProposalBatch) {
	q.ChosenUniqueQ.Learn(bat)
	delete(q.proposed, bat.GetUID())
}
