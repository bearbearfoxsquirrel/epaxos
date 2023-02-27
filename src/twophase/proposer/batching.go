package proposer

import (
	"epaxos/batching"
	"epaxos/dlog"
	"epaxos/genericsmr"
	"epaxos/state"
	"math"
	"time"
)

type StartProposalBatcher struct {
	Sig chan<- struct{}
	SimpleBatcher
}

func (b *StartProposalBatcher) AddProposal(clientRequest *genericsmr.Propose,
	othersAwaiting <-chan *genericsmr.Propose) bool {
	if b.CurrentBatchLen() == 0 {
		go func() { b.Sig <- struct{}{} }()
	}
	return b.SimpleBatcher.AddProposal(clientRequest, othersAwaiting)
}

type SimpleBatcher struct {
	ProposedClientValuesManager
	id                         int32
	nextBatch                  CurBatch
	sleepingInsts              map[int32]time.Time
	constructedAwaitingBatches []batching.ProposalBatch
	chosenBatches              map[int32]struct{}
}

type CurBatch struct {
	MaxLength  int
	Cmds       []*state.Command
	ClientVals []*genericsmr.Propose
	Uid        int32
}

func (b *SimpleBatcher) PutBatch(batch batching.ProposalBatch) {
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

func (b *SimpleBatcher) GetBatchToPropose() batching.ProposalBatch {
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

func GetBatcher(id int32, maxBatchSize int) SimpleBatcher {
	b := SimpleBatcher{
		ProposedClientValuesManager: nil,
		nextBatch: CurBatch{
			MaxLength:  maxBatchSize,
			Cmds:       make([]*state.Command, 0, maxBatchSize),
			ClientVals: make([]*genericsmr.Propose, 0, maxBatchSize),
			Uid:        0,
		},
		sleepingInsts:              make(map[int32]time.Time),
		constructedAwaitingBatches: make([]batching.ProposalBatch, 0, 100),
		chosenBatches:              make(map[int32]struct{}),
		id:                         id,
	}
	return b
}

func (b *SimpleBatcher) startNextBatch() {
	b.nextBatch.Cmds = make([]*state.Command, 0, b.nextBatch.MaxLength)
	b.nextBatch.ClientVals = make([]*genericsmr.Propose, 0, b.nextBatch.MaxLength)
	b.nextBatch.Uid += 1
}

func (b *SimpleBatcher) addBatchToQueue() batching.Batch {
	batch := batching.Batch{
		Proposals: b.nextBatch.ClientVals,
		Cmds:      b.nextBatch.Cmds,
		Uid:       b.nextBatch.Uid,
	}
	b.constructedAwaitingBatches = append(b.constructedAwaitingBatches, &batch)
	return batch
}

func (b *SimpleBatcher) tryFillRestOfBatch(othersAwaiting <-chan *genericsmr.Propose) {
	if len(b.nextBatch.Cmds) < b.nextBatch.MaxLength {
		l := len(othersAwaiting)
		for i := 0; i < l; i++ {
			v := <-othersAwaiting // assumes no one else is reading otherwise, need timeout or to remove func
			b.addToBatch(v)
			if len(b.nextBatch.Cmds) == b.nextBatch.MaxLength {
				break
			}
		}
	}
}

func (b *SimpleBatcher) AddProposal(clientRequest *genericsmr.Propose, othersAwaiting <-chan *genericsmr.Propose) bool {
	b.addToBatch(clientRequest)
	b.tryFillRestOfBatch(othersAwaiting)

	if len(b.nextBatch.Cmds) != b.nextBatch.MaxLength {
		return false
	}
	batch := b.addBatchToQueue()
	dlog.AgentPrintfN(b.id, "Assembled full batch with UID %d (length %d values)", batch.Uid, len(batch.GetCmds()))
	b.startNextBatch()
	return true
}

func (b *SimpleBatcher) addToBatch(clientRequest *genericsmr.Propose) {
	b.nextBatch.Cmds = append(b.nextBatch.Cmds, &clientRequest.Command)
	b.nextBatch.ClientVals = append(b.nextBatch.ClientVals, clientRequest)
}

func (b *SimpleBatcher) CurrentBatchLen() int {
	return len(b.nextBatch.Cmds)
}

func (b *SimpleBatcher) GetNumBatchesMade() int {
	// if next batch len > 0 add 1
	return len(b.constructedAwaitingBatches)
}