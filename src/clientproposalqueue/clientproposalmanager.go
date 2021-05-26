package clientproposalqueue

import (
	"genericsmr"
	"log"
)

type UID struct {
	commandID int32
	timestamp int64
}
type ClientProposalQueue struct {
	proposalsQueue   chan *genericsmr.Propose
	reproposalsQueue chan *genericsmr.Propose

	// way to check if reproposal que value isn't closed
	//
	queued      map[UID]struct{}
	outstanding map[UID]*genericsmr.Propose
	closed      map[UID]struct{}
}

func ClientProposalQueueInit() ClientProposalQueue {
	return ClientProposalQueue{
		proposalsQueue:   make(chan *genericsmr.Propose, 1000),
		reproposalsQueue: make(chan *genericsmr.Propose, 1000),
		queued:           make(map[UID]struct{}),
		outstanding:      make(map[UID]*genericsmr.Propose),
		closed:           make(map[UID]struct{}),
	}
}

func (q *ClientProposalQueue) isClosed(propose *genericsmr.Propose) bool {
	uid := UID{
		commandID: propose.CommandId,
		timestamp: propose.Timestamp,
	}

	if _, closed := q.closed[uid]; closed {
		return true
	} else {
		return false
	}
}

func (q *ClientProposalQueue) removeClosed(propose *genericsmr.Propose) {

}

func (q *ClientProposalQueue) tryAddQueuedUID(propose *genericsmr.Propose) bool {
	uid := UID{
		commandID: propose.CommandId,
		timestamp: propose.Timestamp,
	}
	_, exists := q.queued[uid]
	if exists {
		return false
	} else {
		q.queued[uid] = struct{}{}
		return true
	}
}

func (q *ClientProposalQueue) isOutstanding(propose *genericsmr.Propose) bool {
	uid := UID{
		commandID: propose.CommandId,
		timestamp: propose.Timestamp,
	}

	if _, outstanding := q.outstanding[uid]; outstanding {
		return true
	} else {
		return false
	}
}

func (q *ClientProposalQueue) TryEnqueue(propose *genericsmr.Propose) {
	if q.isOutstanding(propose) {
		panic("Attempting to queue value for first time that is outstanding")
	}
	if !q.tryAddQueuedUID(propose) {
		return
	}
	select {
	case q.proposalsQueue <- propose:
		break
	default:
		panic("No space left to TryEnqueue :O")
	}
}

func (q *ClientProposalQueue) TryRequeue(propose *genericsmr.Propose) {
	if !q.isOutstanding(propose) {
		panic("Attempting to requeue value that is not outstanding")
	}

	if !q.tryAddQueuedUID(propose) {
		log.Print("Not able to requeue")
		return
	}
	select {
	case q.reproposalsQueue <- propose:
		log.Print("requeued client proposal")
		break
	default:
		panic("No space left to TryRequeue")
	}
}
func (q *ClientProposalQueue) tryRemoveQueuedUID(propose *genericsmr.Propose) {
	uid := UID{
		commandID: propose.CommandId,
		timestamp: propose.Timestamp,
	}
	_, exists := q.queued[uid]
	if !exists {
		panic("already added")
	} else {
		delete(q.queued, uid)
	}
}

func (q *ClientProposalQueue) setOutstanding(propose *genericsmr.Propose) {
	uid := UID{
		commandID: propose.CommandId,
		timestamp: propose.Timestamp,
	}

	if test, outstanding := q.outstanding[uid]; outstanding {
		print(test)
		panic("Setting value outstanding that is already outstanding")
	} else {
		q.outstanding[uid] = propose
	}
}

func (q *ClientProposalQueue) TryDequeue() *genericsmr.Propose {
	if len(q.queued) == 0 {
		return nil
	}
	for {
		select {
		case proposal := <-q.reproposalsQueue:
			q.tryRemoveQueuedUID(proposal)
			if q.isClosed(proposal) {
				q.setOutstanding(proposal)
				log.Print("Unqueued retry client proposal")
				return proposal
			} else {
				continue
			}
		default:
			// done this way to ensure preference in reproposals
			select {
			case proposal := <-q.proposalsQueue:
				q.tryRemoveQueuedUID(proposal)
				if q.isClosed(proposal) {
					q.setOutstanding(proposal)
					log.Print("unqueued client proposal")
					return proposal
				} else {
					continue
				}
			default:
				panic("should be able to dequeue but can't")
				return nil
			}
		}
	}
}

func (q *ClientProposalQueue) isQueued(propose *genericsmr.Propose) bool {
	uid := UID{
		commandID: propose.CommandId,
		timestamp: propose.Timestamp,
	}
	_, exists := q.queued[uid]
	if !exists {
		return true
	} else {
		return false
	}
}
func (q *ClientProposalQueue) CloseValue(propose *genericsmr.Propose) {
	if !q.isOutstanding(propose) { // need to change if allowing multiple proposals of outstanding value
		panic("Closing value that is outstanding")
	} else {
		uid := UID{
			commandID: propose.CommandId,
			timestamp: propose.Timestamp,
		}

		//	if q.isQueued(propose) {

		//		panic("closing value that is queued for reproposal") // could have closed map to remove these as they are dequed
		//	}
		q.closed[uid] = struct{}{}

		delete(q.outstanding, uid)
	}
}

func (q *ClientProposalQueue) Len() int {
	return len(q.queued)
}
