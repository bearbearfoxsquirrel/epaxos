package acceptor

import (
	"dlog"
	"encoding/binary"
	"math"
	"stablestore"
	"stdpaxosproto"
	"time"
)

type prewriter struct {
	maxInst int32

	iBound int32
	bBound int32

	iWriteAhead int32
	bWriteAhead int32

	classesToWriteAhead map[int32]int32
	preWrittens         map[int32]stdpaxosproto.Ballot
	maxPrewrittenC      int32
	proposerLeasing     chan PromiseLease
	stablestore.StableStore
}

func PrewriterNew(promiseLeasesRet chan PromiseLease, iWriteAhead int32, stableStore stablestore.StableStore) *prewriter {
	bWriteAhead := int32(50000)

	return &prewriter{
		maxInst:             0,
		iBound:              300,
		bBound:              10000,
		iWriteAhead:         iWriteAhead,
		bWriteAhead:         bWriteAhead,
		classesToWriteAhead: make(map[int32]int32),
		preWrittens:         make(map[int32]stdpaxosproto.Ballot),
		maxPrewrittenC:      -1,
		proposerLeasing:     promiseLeasesRet,
		StableStore:         stableStore,
	}
}

func (a *prewriter) updateMaxInstance(inst int32) {
	if inst <= a.maxInst {
		return
	}
	a.maxInst = inst
}

func (a *prewriter) updateInstancesToPrewriteBasedOnIBound(meId int32) {
	oldMaxWriteAheadI := a.maxPrewrittenC * a.iWriteAhead
	if a.maxInst-oldMaxWriteAheadI < a.iBound { // maxInstance is updated through promise and accept
		//dlog.AgentPrintfN(a.meID, "Max instance is %d. No need to write ahead new promises", a.maxInstance)
		return
	}
	dlog.AgentPrintfN(meId, "Max instance %d has broken write ahead bound (written up to instance %d), writing now ahead to instance %d", a.maxInst, a.maxPrewrittenC*a.iWriteAhead, (a.maxPrewrittenC+1)*a.iWriteAhead)
	a.prewriteNextClass()
}

func (a *prewriter) getClass(inst int32) int32 {
	return inst / a.iWriteAhead
}

func (a *prewriter) getWrittenAheadBallot(inst int32) stdpaxosproto.Ballot {
	c := a.getClass(inst)
	writtenTo, e := a.preWrittens[c]
	if !e {
		return stdpaxosproto.Ballot{-1, -1}
	}
	return writtenTo
}

func (a *prewriter) isPrewritten(inst int32, ballot stdpaxosproto.Ballot) bool {
	c := a.getClass(inst)
	lB, e := a.preWrittens[c]
	if !e {
		return false
	}
	if ballot.GreaterThan(lB) {
		return false
	}
	return true
}

func (a *prewriter) shouldSendWriteAheadSignalForInstClass(inst int32, ballot stdpaxosproto.Ballot) bool {
	c := a.getClass(inst)
	writtenTo, e := a.preWrittens[c]
	if !e {
		return true
	}
	lB := stdpaxosproto.Ballot{
		Number: writtenTo.Number - a.bBound,
		PropID: writtenTo.PropID,
	}
	if ballot.GreaterThan(lB) {
		return true
	}
	return false
}

func (a *prewriter) prewriteNextClass() {
	a.classesToWriteAhead[a.maxPrewrittenC+1] = a.bWriteAhead
}

func (a *prewriter) returnNewPromiseLeases() {
	for c, b := range a.classesToWriteAhead {
		a.proposerLeasing <- PromiseLease{
			From:           c * a.iWriteAhead,
			MaxBalPromises: stdpaxosproto.Ballot{b, math.MaxInt16},
		}
	}
}

func (a *prewriter) writePrewritesToStableStorageUNSAFE() {
	for c, b := range a.classesToWriteAhead {
		var w [8]byte
		binary.LittleEndian.PutUint32(w[0:4], uint32(c))
		binary.LittleEndian.PutUint32(w[4:8], uint32(b))
		_, _ = a.StableStore.Write(w[:])
		a.preWrittens[c] = stdpaxosproto.Ballot{b, math.MaxInt16}
		if c > a.maxPrewrittenC {
			a.maxPrewrittenC = c
		}
	}
}

func (a *prewriter) updateClassesToWriteAhead(inst int32, ballot stdpaxosproto.Ballot) {
	if a.shouldSendWriteAheadSignalForInstClass(inst, ballot) {
		toWriteAheadToB := a.getWrittenAheadBallot(inst).Number
		if toWriteAheadToB == -1 { // doesn't really matter but did it anyway **shrug**
			toWriteAheadToB = 0
		}
		toWriteAheadToB += a.bWriteAhead
		a.classesToWriteAhead[a.getClass(inst)] = toWriteAheadToB
	}
}

func (a *prewriter) clearPendingPrewrites() {
	a.classesToWriteAhead = make(map[int32]int32)
}

// todo handle case where doing write in commit -- could piggyback promises
type prewritePromiseAcceptor struct {
	standard
	prewriter
	proactivePreempt bool
}

//todo add proactive preempt

func PrewritePromiseAcceptorNew(file stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration, id int32, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8, commitShortRPC uint8, catchupOnProceedingCommits bool, promiseLeasesRet chan PromiseLease, iWriteAhead int32, proactivePreemptOnNewB bool) *prewritePromiseAcceptor {
	a := &prewritePromiseAcceptor{
		standard:         *StandardAcceptorNew(file, durable, emulatedSS, emulatedWriteTime, id, prepareReplyRPC, acceptReplyRPC, commitRPC, commitShortRPC, catchupOnProceedingCommits),
		prewriter:        *PrewriterNew(promiseLeasesRet, iWriteAhead, file),
		proactivePreempt: proactivePreemptOnNewB,
	}

	a.prewriter.updateInstancesToPrewriteBasedOnIBound(a.meID)
	a.prewriter.writePrewritesToStableStorageUNSAFE()
	a.prewriter.returnNewPromiseLeases()
	a.prewriter.clearPendingPrewrites()

	return a
}

func (a *prewritePromiseAcceptor) RecvPrepareRemote(prepare *stdpaxosproto.Prepare) <-chan Message {
	dlog.AgentPrintfN(a.meID, "Acceptor received prepare request for instance %d from %d at ballot %d.%d", prepare.Instance, prepare.PropID, prepare.Ballot.Number, prepare.Ballot.PropID)
	inst, bal, requestor := prepare.Instance, prepare.Ballot, int32(prepare.PropID)
	responseC := make(chan Message, 2)
	checkAndCreateInstanceState(&a.instanceState, inst)
	abk := a.instanceState[inst]

	if abk.status == COMMITTED {
		a.handleMsgAlreadyCommitted(requestor, inst, responseC)
		return responseC
	}

	a.prewriter.updateClassesToWriteAhead(prepare.Instance, prepare.Ballot)

	if !a.isPrewritten(prepare.Instance, prepare.Ballot) {
		a.prewriter.updateMaxInstance(a.maxInst)
		a.writePrewritesToStableStorageUNSAFE()
		fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
		a.returnNewPromiseLeases()
		a.clearPendingPrewrites()
	}

	//if bal.Equal(abk.curBal) && abk.status == ACCEPTED {
	// ignore
	//}

	if bal.GreaterThan(abk.curBal) {
		if !abk.curBal.IsZero() && a.proactivePreempt {
			a.returnPreemptMsg(inst, prepare.Ballot, stdpaxosproto.PROMISE, responseC)
		}
		abk.status = PREPARED
		abk.curBal = bal
	}

	msg := a.getPrepareReply(prepare, inst)
	a.returnPrepareReply(msg, responseC)
	close(responseC)
	return responseC
}

func (a *prewritePromiseAcceptor) returnPreemptMsg(inst int32, newBal stdpaxosproto.Ballot, newPhase stdpaxosproto.Phase, responseC chan<- Message) {
	abk := a.instanceState[inst]
	if abk.status == PREPARED {
		preply := &stdpaxosproto.PrepareReply{
			Instance:   inst,
			Req:        abk.curBal,
			Cur:        newBal,
			CurPhase:   newPhase,
			VBal:       abk.vBal,
			AcceptorId: a.meID,
			WhoseCmd:   abk.whoseCmds,
			Command:    abk.cmds,
		}
		a.returnPrepareReply(preply, responseC)
		return
	}

	areply := &stdpaxosproto.AcceptReply{
		Instance:   inst,
		Req:        abk.curBal,
		Cur:        newBal,
		CurPhase:   newPhase,
		AcceptorId: a.meID,
		WhoseCmd:   abk.whoseCmds,
	}
	a.returnAcceptReply(areply, responseC)
}

func (a *prewritePromiseAcceptor) RecvAcceptRemote(accept *stdpaxosproto.Accept) <-chan Message {
	dlog.AgentPrintfN(a.meID, "Acceptor received accept request for instance %d from %d at ballot %d.%d", accept.Instance, accept.PropID, accept.Ballot.Number, accept.Ballot.PropID)

	inst, bal, requestor := accept.Instance, accept.Ballot, int32(accept.PropID)
	responseC := make(chan Message, 2)
	checkAndCreateInstanceState(&a.instanceState, inst)
	abk := a.instanceState[inst]

	if abk.status == COMMITTED {
		a.handleMsgAlreadyCommitted(requestor, inst, responseC)
		return responseC
	}

	if bal.GreaterThan(abk.curBal) || bal.Equal(abk.curBal) {
		if !abk.curBal.IsZero() && a.proactivePreempt && bal.GreaterThan(abk.curBal) {
			a.returnPreemptMsg(inst, accept.Ballot, stdpaxosproto.PROMISE, responseC)
		}

		a.prewriter.updateMaxInstance(a.maxInst)
		a.writePrewritesToStableStorageUNSAFE()
		nonDurableAccept(accept, abk, a.stableStore, a.durable)
		fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
		a.returnNewPromiseLeases()
		a.clearPendingPrewrites()
	}

	acptReply := a.getAcceptReply(inst, accept)
	a.returnAcceptReply(acptReply, responseC)
	close(responseC)
	return responseC
}

type prewrittenBatcher struct {
	batchingAcceptor
	maxAcceptanceBatchWait time.Duration
	maxPrepareBatchWait    time.Duration
	prewriter
}

type PrewritePromiseBatchingAcceptor struct {
	ProposerIDs []int32
	batcher     prewrittenBatcher
}

func PrewrittenBatcherAcceptorNew(file stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration, id int32, maxBatchWait time.Duration, proposerIDs []int32, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8, commitShortRPC uint8, catchupOnProceedingCommits bool, promiseLeasesRet chan PromiseLease, iWriteAhead int32) *PrewritePromiseBatchingAcceptor {
	bat := batchingAcceptor{
		meID:                       id,
		maxInstance:                -1,
		instanceState:              make(map[int32]*AcceptorBookkeeping, 100),
		awaitingPrepareReplies:     make(map[int32]map[int32]outgoingPromiseResponses),
		awaitingAcceptReplies:      make(map[int32]map[int32]outgoingAcceptResponses),
		promiseRequests:            make(chan incomingPromiseRequests, 1000),
		acceptRequests:             make(chan incomingAcceptRequests, 1000),
		commits:                    make(chan incomingCommitMsgs, 1000),
		commitShorts:               make(chan incomingCommitShortMsgs, 1000),
		catchupOnProceedingCommits: catchupOnProceedingCommits,
		acceptReplyRPC:             acceptReplyRPC,
		prepareReplyRPC:            prepareReplyRPC,
		commitRPC:                  commitRPC,
		commitShortRPC:             commitShortRPC,
		stableStore:                file,
		durable:                    durable,
		emuatedWriteTime:           emulatedWriteTime,
		emulatedSS:                 emulatedSS,
		maxBatchWait:               maxBatchWait,
	}

	a := &PrewritePromiseBatchingAcceptor{
		ProposerIDs: proposerIDs,
		batcher: prewrittenBatcher{
			batchingAcceptor:       bat,
			maxAcceptanceBatchWait: maxBatchWait,
			maxPrepareBatchWait:    0,
			prewriter:              *PrewriterNew(promiseLeasesRet, iWriteAhead, file),
		},
	}

	a.batcher.prewriter.updateInstancesToPrewriteBasedOnIBound(a.batcher.meID)
	a.batcher.prewriter.writePrewritesToStableStorageUNSAFE()
	a.batcher.prewriter.returnNewPromiseLeases()
	a.batcher.prewriter.clearPendingPrewrites()

	go a.batching()
	return a

}

func (a *PrewritePromiseBatchingAcceptor) RecvPrepareRemote(prepare *stdpaxosproto.Prepare) <-chan Message {
	c := make(chan Message, 1)
	go func() {
		a.batcher.promiseRequests <- incomingPromiseRequests{
			retMsg:      c,
			incomingMsg: prepare,
		}
	}()
	return c
}

func (a *PrewritePromiseBatchingAcceptor) RecvAcceptRemote(accept *stdpaxosproto.Accept) <-chan Message {
	c := make(chan Message, 1)
	go func() {
		a.batcher.acceptRequests <- incomingAcceptRequests{
			retMsg:      c,
			incomingMsg: accept,
		}
	}()
	return c
}

func (a *PrewritePromiseBatchingAcceptor) RecvCommitRemote(commit *stdpaxosproto.Commit) <-chan struct{} {
	c := make(chan struct{}, 1)
	go func() {
		a.batcher.commits <- incomingCommitMsgs{
			done:        c,
			incomingMsg: commit,
		}
	}()
	return c
}

func (a *PrewritePromiseBatchingAcceptor) RecvCommitShortRemote(commit *stdpaxosproto.CommitShort) <-chan struct{} {
	c := make(chan struct{}, 1)
	go func() {
		a.batcher.commitShorts <- incomingCommitShortMsgs{
			done:        c,
			incomingMsg: commit,
		}
	}()
	return c
}

func (a *PrewritePromiseBatchingAcceptor) batching() {
	var prepareTimeout *time.Timer = nil
	if a.batcher.maxAcceptanceBatchWait > 0 {
		prepareTimeout = time.NewTimer(a.batcher.maxPrepareBatchWait)
	}
	acceptTimeout := time.NewTimer(a.batcher.maxAcceptanceBatchWait)

	for {
		select {
		// todo track whether each writtenbatch is chosen so can reclaim memory
		case inc := <-a.batcher.commitShorts:
			a.batcher.handleCommitShort(inc)
			break
		case inc := <-a.batcher.commits:
			a.batcher.handleCommit(inc)
			break
		case inc := <-a.batcher.promiseRequests:
			a.batcher.recvPromiseRequest(inc)
			if a.batcher.maxPrepareBatchWait == 0 {
				a.batcher.doPrepareBatch()
			}
			break
		case inc := <-a.batcher.acceptRequests:
			a.batcher.recvAcceptRequest(inc)
			break
		case <-prepareTimeout.C:
			didPersist := a.batcher.doPrepareBatch()
			if didPersist {
				acceptTimeout.Reset(a.batcher.maxAcceptanceBatchWait)
			}
			prepareTimeout.Reset(a.batcher.maxPrepareBatchWait)
			break
		case <-acceptTimeout.C:
			a.batcher.doAcceptBatch()
			acceptTimeout.Reset(a.batcher.maxAcceptanceBatchWait)
		}
	}
}

func (a *prewrittenBatcher) doPrepareBatch() bool {
	// first check if it is safe to return response
	writeForSafety := a.updateClassesToWriteBasedOnAwaitingPrepares()
	a.prewriter.updateMaxInstance(a.maxInstance)
	if writeForSafety {
		dlog.AgentPrintfN(a.meID, "Promises in batch past prewriten ballots. Must write to storage before responding.")
		a.updateInstancesToPrewriteBasedOnIBound(a.meID)
		a.persistAll()
		dlog.AgentPrintfN(a.meID, "Acceptor batch performed, now returning prepare and accept responses for %d instances", len(a.awaitingPrepareReplies)+len(a.awaitingAcceptReplies))
		a.returnPrepareAndAcceptRepliesAndClear()
		dlog.AgentPrintfN(a.meID, "Acceptor done returning responses")
		return true
	}

	a.returnPrepareRepliesAndClear()
	a.awaitingPrepareReplies = make(map[int32]map[int32]outgoingPromiseResponses)
	dlog.AgentPrintfN(a.meID, "Acceptor done returning responses")
	return false
}

func (a *prewrittenBatcher) doAcceptBatch() {
	a.updateIfBallotBoundsBrokeOnAwaitingAccept()
	a.prewriter.updateMaxInstance(a.maxInstance)
	a.updateInstancesToPrewriteBasedOnIBound(a.meID)
	a.persistAll()
	dlog.AgentPrintfN(a.meID, "Acceptor batch performed, now returning responses for %d instances", len(a.awaitingAcceptReplies))
	a.returnAcceptRepliesAndClear()
	dlog.AgentPrintfN(a.meID, "Acceptor done returning responses")
	return
}

func (a *prewrittenBatcher) persistAll() {
	a.writePrewritesToStableStorageUNSAFE()
	for instToResp, _ := range a.awaitingAcceptReplies {
		abk := a.instanceState[instToResp]
		recordInstanceMetadata(abk, a.stableStore, a.durable)
		if abk.cmds != nil {
			recordCommands(abk.cmds, a.stableStore, a.durable)
		}
	}
	fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
	a.returnNewPromiseLeases()
	a.clearPendingPrewrites()

}

func (a *prewrittenBatcher) updateIfBallotBoundsBrokeOnAwaitingAccept() {
	for instToResp, instResps := range a.awaitingAcceptReplies {
		for _, requestorResp := range instResps {
			a.updateClassesToWriteAhead(instToResp, requestorResp.Ballot)
		}
	}
}

func (a *prewrittenBatcher) updateClassesToWriteBasedOnAwaitingPrepares() bool {
	writeForSafety := false
	for instToResp, instResps := range a.awaitingPrepareReplies {
		for _, requestorResp := range instResps {
			if !a.isPrewritten(instToResp, requestorResp.Ballot) {
				writeForSafety = true
			}
			if a.shouldSendWriteAheadSignalForInstClass(instToResp, requestorResp.Ballot) {
				toWriteAheadToB := a.getWrittenAheadBallot(instToResp).Number
				if toWriteAheadToB == -1 { // doesn't really matter but did it anyway **shrug**
					toWriteAheadToB = 0
				}
				toWriteAheadToB += a.bWriteAhead
				a.classesToWriteAhead[a.getClass(instToResp)] = toWriteAheadToB
			}
		}
	}
	return writeForSafety
}
