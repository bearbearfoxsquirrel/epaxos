package batching

import (
	"epaxos/genericsmr"
	"epaxos/state"
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

type Batch struct {
	Proposals []*genericsmr.Propose
	Cmds      []*state.Command
	Uid       int32
}

func (p *Batch) GetProposals() []*genericsmr.Propose {
	return p.Proposals
}

func (p *Batch) GetCmds() []*state.Command {
	return p.Cmds
}

func (p *Batch) GetUID() int32 {
	return p.Uid
}

type ProposalBatcher struct {
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
	doEager                 bool
}

type EagerNudgeRecv struct {
	cancel <-chan struct{}
	batch  chan<- ProposalBatch
}

type EagerNudgeSend struct {
}

//func StartEagerBatching(myId int32, in <-chan *genericsmr.Propose, out chan<- ProposalBatch, expectedBatchedRequests int32, maxBatchSizeBytes int, maxBatchWait time.Duration, onBatch func(), nudge <-chan EagerNudgeRecv) {
//	batcher := ProposalBatcher{
//		myId:               myId,
//		unbatchedProposals: in,
//		batchedProposals:   out,
//		curBatchProposals:  make([]*genericsmr.Propose, 0, expectedBatchedRequests),
//		curBatchCmds:       make([]*state.Command, 0, expectedBatchedRequests),
//		curBatchSize:       0,
//		curUID:             int32(0),
//		curTimeout:         time.NewTimer(maxBatchWait),
//		maxBatchWait:       maxBatchWait,
//		maxBatchBytes:      maxBatchSizeBytes,
//	}
//	for {
//		select {
//		case v := <-batcher.unbatchedProposals:
//			batcher.addToBatch(v)
//			if !batcher.isBatchSizeMet() {
//				break
//			}
//			batchC := batcher.getBatch()
//			batcher.batchedProposals <- batchC
//			onBatch()
//			dlog.AgentPrintfN(batcher.myId, "Batcher client proposal Batch of length %d bytes satisfied, now handing over Batch with UID %d to replica", batcher.curBatchSize, batchC.GetUID())
//			batcher.startNextBatch()
//			break
//
//		//case <-batcher.curTimeout.C:
//		//	if !batcher.hasBatch() {
//		//		batcher.resetTimeout()
//		//		break
//		//	}
//		//	batchC := batcher.getBatch()
//		//	dlog.AgentPrintfN(batcher.myId, "Batcher timed out on acquiring a client proposal Batch of length %d bytes, now handing over partly filled Batch with UID %d to replica", batcher.curBatchSize, batchC.GetUID())
//		//	batcher.batchedProposals <- batchC
//		//	onBatch()
//		//	batcher.startNextBatch()
//		//	break
//		case n <-nudge:
//			select {
//
//			}
//			//if !batcher.hasBatch() {
//			//	dlog.AgentPrintfN(batcher.myId, "Batcher ignoring nudge as there is not Batch to pass on to replica")
//			//	break
//			//}
//			//batchC := batcher.getBatch()
//			//dlog.AgentPrintfN(batcher.myId, "Batcher nudged so giving client proposal Batch of length %d bytes, now handing over partly filled Batch with UID %d to replica", batcher.curBatchSize, batchC.GetUID())
//			//batcher.batchedProposals <- batchC
//			//onBatch()
//			//batcher.startNextBatch()
//			//break
//		}
//	}
//}

func StartBatching(myId int32, in <-chan *genericsmr.Propose, out chan<- ProposalBatch, expectedBatchedRequests int32, maxBatchSizeBytes int, maxBatchWait time.Duration, onBatch func(), nudge <-chan chan ProposalBatch, eager bool) {
	batcher := ProposalBatcher{
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
		doEager:            eager,
	}
	if batcher.doEager {
		batcher.curTimeout.C = nil
	}
	for {
		select {
		case v := <-batcher.unbatchedProposals:
			if !batcher.hasBatch() {
				onBatch()
			}
			batcher.addToBatch(v)
			if !batcher.isBatchSizeMet() {
				break
			}
			batchC := batcher.getBatch()
			batcher.batchedProposals <- batchC
			//dlog.AgentPrintfN(batcher.myId, "Batcher client proposal Batch of length %d bytes satisfied, now handing over Batch with UID %d to replica", batcher.curBatchSize, batchC.GetUID())
			batcher.startNextBatch()
			break
		case <-batcher.curTimeout.C:
			if !batcher.hasBatch() {
				batcher.resetTimeout()
				break
			}
			batchC := batcher.getBatch()
			//dlog.AgentPrintfN(batcher.myId, "Batcher timed out on acquiring a client proposal Batch of length %d bytes, now handing over partly filled Batch with UID %d to replica", batcher.curBatchSize, batchC.GetUID())
			batcher.batchedProposals <- batchC
			batcher.startNextBatch()
			break
		case ret := <-nudge:
			if !batcher.hasBatch() {
				//dlog.AgentPrintfN(batcher.myId, "Batcher ignoring nudge as there is not Batch to pass on to replica")
				go func() { ret <- nil }()
				break
			}
			batchC := batcher.getBatch()
			//dlog.AgentPrintfN(batcher.myId, "Batcher nudged so giving client proposal Batch of length %d bytes, now handing over partly filled Batch with UID %d to replica", batcher.curBatchSize, batchC.GetUID())
			go func(b *Batch) { ret <- b }(batchC)
			batcher.startNextBatch()
			break
			//case requeue := <-batcher.requeue:
			//	if batcher.requeued[requeue.UID()] =
			//	break
			//case chosen := <-batcher.chosen:
			//	batcher.chosenI[chosen] = struct{}{}
			//	break
		}
	}
}

func (b *ProposalBatcher) hasBatch() bool {
	return b.curBatchSize > 0
}

func (b *ProposalBatcher) resetTimeout() {
	b.curTimeout.Reset(b.maxBatchWait)
}

func (b *ProposalBatcher) isBatchSizeMet() bool {
	return b.curBatchSize >= b.maxBatchBytes
}

func (b *ProposalBatcher) addToBatch(v *genericsmr.Propose) {
	b.curBatchProposals = append(b.curBatchProposals, v)
	b.curBatchCmds = append(b.curBatchCmds, &v.Command)
	b.curBatchSize += len(v.Command.V) + 16 + 2
}

func (b *ProposalBatcher) startNextBatch() {
	b.curBatchProposals = make([]*genericsmr.Propose, 0, b.expectedBatchedRequests)
	b.curBatchCmds = make([]*state.Command, 0, b.expectedBatchedRequests)
	b.curBatchSize = 0
	b.curUID += 1
	if b.doEager {
		return
	}
	b.resetTimeout()
}

func (b *ProposalBatcher) getBatch() *Batch {
	batchC := &Batch{
		Proposals: b.curBatchProposals,
		Cmds:      b.curBatchCmds,
		Uid:       b.curUID,
	}
	return batchC
}

// todo
//func whatisbatchload?

//type
