package batching

import (
	"dlog"
	"genericsmr"
	"state"
	"time"
)

type ProposalBatch interface {
	GetProposals() []*genericsmr.Propose
	GetCmds() []*state.Command
	GetUID() int32
}

type Batchable interface {
	popBatch() ProposalBatch // get and remove
	putBatch(bat ProposalBatch)
	getBatch() ProposalBatch
}

type batch struct {
	proposals []*genericsmr.Propose
	cmds      []*state.Command
	uid       int32
}

func (p *batch) GetProposals() []*genericsmr.Propose {
	return p.proposals
}

func (p *batch) GetCmds() []*state.Command {
	return p.cmds
}

func (p *batch) GetUID() int32 {
	return p.uid
}

type proposalBatcher struct {
	curBatchCmds            []*state.Command
	curBatchProposals       []*genericsmr.Propose
	curBatchSize            int
	curUID                  int32
	curTimeout              *time.Timer
	maxBatchWait            time.Duration
	maxBatchBytes           int
	expectedBatchedRequests int
	unbatchedProposals      <-chan *genericsmr.Propose
	batchedProposals        chan<- ProposalBatch
	myId                    int32
}

func StartBatching(myId int32, in <-chan *genericsmr.Propose, out chan<- ProposalBatch, expectedBatchedRequests int32, maxBatchSizeBytes int, maxBatchWait time.Duration, onBatch func()) {
	batcher := proposalBatcher{
		myId:               myId,
		unbatchedProposals: in,
		batchedProposals:   out,
		curBatchProposals:  make([]*genericsmr.Propose, 0, expectedBatchedRequests),
		curBatchCmds:       make([]*state.Command, 0, expectedBatchedRequests),
		curBatchSize:       0,
		curUID:             int32(0),
		curTimeout:         time.NewTimer(maxBatchWait),
		maxBatchWait:       maxBatchWait,
		maxBatchBytes:      maxBatchSizeBytes,
	}
	for {
		select {
		case v := <-batcher.unbatchedProposals:
			batcher.addToBatch(v)
			if !batcher.isBatchSizeMet() {
				break
			}
			batchC := batcher.getBatch()
			batcher.batchedProposals <- batchC
			onBatch()
			dlog.AgentPrintfN(batcher.myId, "Client proposal batch of length %d bytes satisfied, now handing over batch with UID %d to replica", batcher.curBatchSize, batchC.GetUID())
			batcher.startNextBatch()
			break
		case <-batcher.curTimeout.C:
			if !batcher.hasBatch() {
				batcher.resetTimeout()
				break
			}
			batchC := batcher.getBatch()
			dlog.AgentPrintfN(batcher.myId, "Timed out on acquiring a client proposal batch of length %d bytes, now handing over partly filled batch with UID %d to replica", batcher.curBatchSize, batchC.GetUID())

			batcher.batchedProposals <- batchC
			onBatch()
			batcher.startNextBatch()
			break
		}
	}
}

func (b *proposalBatcher) hasBatch() bool {
	return b.curBatchSize > 0
}

func (b *proposalBatcher) resetTimeout() {
	b.curTimeout.Reset(b.maxBatchWait)
}

func (b *proposalBatcher) isBatchSizeMet() bool {
	return b.curBatchSize >= b.maxBatchBytes
}

func (b *proposalBatcher) addToBatch(v *genericsmr.Propose) {
	b.curBatchProposals = append(b.curBatchProposals, v)
	b.curBatchCmds = append(b.curBatchCmds, &v.Command)
	b.curBatchSize += len(v.Command.V) + 16 + 2
}

func (b *proposalBatcher) startNextBatch() {
	b.curBatchProposals = make([]*genericsmr.Propose, 0, b.expectedBatchedRequests)
	b.curBatchCmds = make([]*state.Command, 0, b.expectedBatchedRequests)
	b.curBatchSize = 0
	b.curUID += 1
	b.resetTimeout()
}

func (b *proposalBatcher) getBatch() *batch {
	batchC := &batch{
		proposals: b.curBatchProposals,
		cmds:      b.curBatchCmds,
		uid:       b.curUID,
	}
	return batchC
}

// todo
//func whatisbatchload?
