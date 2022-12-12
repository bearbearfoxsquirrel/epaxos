package twophase

import (
	"epaxos/batching"
	"epaxos/dlog"
	"epaxos/genericsmr"
	"epaxos/state"
	"math"
	"time"
)

type ClientRequestBatcher struct {
	ProposedClientValuesManager
	id                         int32
	nextBatch                  curBatch
	sleepingInsts              map[int32]time.Time
	constructedAwaitingBatches []batching.ProposalBatch
	chosenBatches              map[int32]struct{}
}

type curBatch struct {
	maxLength  int
	cmds       []*state.Command
	clientVals []*genericsmr.Propose
	//next       int
	uid int32
}

func (b *ClientRequestBatcher) PutBatch(batch batching.ProposalBatch) {
	if _, e := b.chosenBatches[batch.GetUID()]; e {
		return
	}
	b.constructedAwaitingBatches = append(b.constructedAwaitingBatches, batch)
	// put back in
	// not if batch is chosen
}

func remove(s []batching.ProposalBatch, i int) []batching.ProposalBatch {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func (b *ClientRequestBatcher) GetBatchToPropose() batching.ProposalBatch {
	var batch batching.ProposalBatch = nil
	batID := int32(math.MaxInt32)
	selI := -1
	for i, bat := range b.constructedAwaitingBatches {
		if _, e := b.chosenBatches[bat.GetUID()]; e {
			continue
		}
		if bat.GetUID() > batID {
			continue
		}
		batch = bat
		batID = bat.GetUID()
		selI = i
	}

	if selI != -1 {
		b.constructedAwaitingBatches = remove(b.constructedAwaitingBatches, selI)
	}
	return batch
}

func GetBatcher(id int32, maxBatchSize int) ClientRequestBatcher {
	//ma:= InstanceProposeValueTimeout{}
	b := ClientRequestBatcher{
		ProposedClientValuesManager: nil,
		nextBatch: curBatch{
			maxLength:  maxBatchSize,
			cmds:       make([]*state.Command, 0, maxBatchSize),
			clientVals: make([]*genericsmr.Propose, 0, maxBatchSize),
			uid:        0,
		},
		sleepingInsts:              make(map[int32]time.Time),
		constructedAwaitingBatches: make([]batching.ProposalBatch, 0, 100),
		chosenBatches:              make(map[int32]struct{}),
		id:                         id,
	}
	return b
}

func (b *ClientRequestBatcher) startNextBatch() {
	b.nextBatch.cmds = make([]*state.Command, 0, b.nextBatch.maxLength)
	b.nextBatch.clientVals = make([]*genericsmr.Propose, 0, b.nextBatch.maxLength)
	b.nextBatch.uid += 1
}

func (b *ClientRequestBatcher) addBatchToQueue() batching.Batch {
	batch := batching.Batch{
		Proposals: b.nextBatch.clientVals,
		Cmds:      b.nextBatch.cmds,
		Uid:       b.nextBatch.uid,
	}
	b.constructedAwaitingBatches = append(b.constructedAwaitingBatches, &batch)
	return batch
}

func (b *ClientRequestBatcher) tryFillRestOfBatch(othersAwaiting <-chan *genericsmr.Propose) {
	if len(b.nextBatch.cmds) < b.nextBatch.maxLength {
		l := len(othersAwaiting)
		for i := 0; i < l; i++ {
			v := <-othersAwaiting // assumes no one else is reading otherwise, need timeout or to remove func
			b.addToBatch(v)
			if len(b.nextBatch.cmds) == b.nextBatch.maxLength {
				break
			}
		}
	}
}

func (b *ClientRequestBatcher) AddProposal(clientRequest *genericsmr.Propose, othersAwaiting <-chan *genericsmr.Propose) bool {
	b.addToBatch(clientRequest)
	b.tryFillRestOfBatch(othersAwaiting)

	if len(b.nextBatch.cmds) != b.nextBatch.maxLength {
		return false
	}
	batch := b.addBatchToQueue()
	dlog.AgentPrintfN(b.id, "Assembled full batch with UID %d (length %d values)", batch.Uid, len(batch.GetCmds()))
	b.startNextBatch()
	return true
}

func (b *ClientRequestBatcher) addToBatch(clientRequest *genericsmr.Propose) {
	b.nextBatch.cmds = append(b.nextBatch.cmds, &clientRequest.Command)
	b.nextBatch.clientVals = append(b.nextBatch.clientVals, clientRequest)
}

func (b *ClientRequestBatcher) CurrentBatchLen() int {
	return len(b.nextBatch.cmds)
}

func (b *ClientRequestBatcher) GetNumBatchesMade() int {
	// if next batch len > 0 add 1
	return len(b.constructedAwaitingBatches)
}
