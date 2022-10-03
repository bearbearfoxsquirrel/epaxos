package acceptor

import (
	"encoding/binary"
	"epaxos/dlog"
	"epaxos/lwcproto"
	"epaxos/stablestore"
	"epaxos/state"
	"epaxos/stdpaxosproto"
	"sync"
	"time"
)

type ConfigAcceptor interface {
	RecvPrepareRemote(prepare *lwcproto.Prepare) <-chan Message
	RecvAcceptRemote(accept *lwcproto.Accept) <-chan Message
	RecvCommitRemote(commit *lwcproto.Commit) <-chan struct{}          // bool
	RecvCommitShortRemote(short *lwcproto.CommitShort) <-chan struct{} // bool
}

//
//func standardConfAcceptorNew(file stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration, id int32, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8, commitShortRPC uint8, catchupOnProceedingCommits bool) *standardConf {
//	return &standardConf{
//		instanceState:    make(map[int32]*AcceptorBookkeeping),
//		stableStore:      file,
//		durable:          durable,
//		emuatedWriteTime: emulatedWriteTime,
//		emulatedSS:       emulatedSS,
//		meID:             id,
//		prepareReplyRPC:  prepareReplyRPC,
//		acceptReplyRPC:   acceptReplyRPC,
//		commitRPC:        commitRPC,
//		commitShortRPC:   commitShortRPC,
//	}
//}
//
//
//func betterBatchingConfAcceptorNew(file stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration, id int32, maxBatchWait time.Duration, proposerIDs []int32, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8, commitShortRPC uint8, catchupOnProceedingCommits bool) *betterBatchingConf {
//	bat := &betterBatchingConf{
//
//		batcher: &batchingAcceptor{
//			instanceState:              make(map[int32]*AcceptorBookkeeping, 100),
//			awaitingPrepareReplies:     make(map[int32]map[int32]outgoingPromiseResponses),
//			awaitingAcceptReplies:      make(map[int32]map[int32]outgoingAcceptResponses),
//			promiseRequests:            make(chan incomingPromiseRequests, 1000),
//			acceptRequests:             make(chan incomingAcceptRequests, 1000),
//			commits:                    make(chan incomingCommitMsgs, 1000),
//			commitShorts:               make(chan incomingCommitShortMsgs, 1000),
//			prepareReplyRPC:            prepareReplyRPC,
//			acceptReplyRPC:             acceptReplyRPC,
//			commitRPC:                  commitRPC,
//			commitShortRPC:             commitShortRPC,
//			maxInstance:                -1,
//			meID:                       id,
//			catchupOnProceedingCommits: catchupOnProceedingCommits,
//			stableStore:                file,
//			durable:                    durable,
//			emuatedWriteTime:           emulatedWriteTime,
//			emulatedSS:                 emulatedSS,
//			maxBatchWait:               maxBatchWait,
//		},
//		ProposerIDs: proposerIDs,
//	}
//	go bat.batchPersister()
//	return bat
//}

type LWCAcceptorBookkeeping struct {
	vConf int32
	AcceptorBookkeeping
}

type ConfigHolder interface {
	getConfig() int32
	setConfig(config int32)
}

func (abk *LWCAcceptorBookkeeping) getCurBal(a ConfigHolder) lwcproto.ConfigBal {
	return lwcproto.ConfigBal{
		Config: a.getConfig(),
		Ballot: abk.curBal,
	}
}

func (abk *LWCAcceptorBookkeeping) getVBal() lwcproto.ConfigBal {
	return lwcproto.ConfigBal{
		Config: abk.vConf,
		Ballot: abk.vBal,
	}
}

func (abk *LWCAcceptorBookkeeping) setVCBal(ballot lwcproto.ConfigBal) {
	abk.vConf = ballot.Config
	abk.vBal = ballot.Ballot
}

func (abk *LWCAcceptorBookkeeping) setCurCBal(a ConfigHolder, ballot lwcproto.ConfigBal) {
	a.setConfig(ballot.Config)
	abk.curBal = ballot.Ballot
}

func recordConfig(config int32, stableStore stablestore.StableStore, durable bool) {
	if !durable {
		return
	}
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(config))
	_, _ = stableStore.Write(b[:])
}

// append a log entry to stable storage
func recordConfigInstanceMetadata(abk *LWCAcceptorBookkeeping, stableStore stablestore.StableStore, durable bool) {
	if !durable {
		return
	}

	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(abk.vConf))
	_, _ = stableStore.Write(b[:])
}

func checkAndCreateConfigInstanceState(instanceState *map[int32]*LWCAcceptorBookkeeping, inst int32) {
	if _, exists := (*instanceState)[inst]; !exists {
		(*instanceState)[inst] = &LWCAcceptorBookkeeping{
			vConf: -1,
			AcceptorBookkeeping: AcceptorBookkeeping{
				status: NOT_STARTED,
				cmds:   nil,
				curBal: stdpaxosproto.Ballot{Number: -1, PropID: -1},
				vBal:   stdpaxosproto.Ballot{Number: -1, PropID: -1},
			},
		}
	}
}

type standardConf struct {
	instanceState              map[int32]*LWCAcceptorBookkeeping
	stableStore                stablestore.StableStore
	durable                    bool
	emuatedWriteTime           time.Duration
	emulatedSS                 bool
	meID                       int32
	acceptReplyRPC             uint8
	prepareReplyRPC            uint8
	commitRPC                  uint8
	commitShortRPC             uint8
	catchupOnProceedingCommits bool
	maxInstance                int32
	crtConfig                  int32
}

func (a *standardConf) getConfig() int32 {
	return a.crtConfig
}

func (a *standardConf) setConfig(config int32) {
	if a.crtConfig >= config {
		panic("safety broken as config is lower")
	}
	a.crtConfig = config
}

func (a *standardConf) handleMsgAlreadyCommitted(requestor int32, inst int32, respHand chan Message) {
	if requestor == a.meID { // todo change to have a local accept and prepare and a remote one too
		close(respHand)
		return
	}

	maxInstToTryReturn := a.maxInstance
	if !a.catchupOnProceedingCommits {
		maxInstToTryReturn = inst
	}

	wg := sync.WaitGroup{}
	for i := maxInstToTryReturn; i >= inst; i-- {
		iState, exists := a.instanceState[i]
		if !exists {
			continue
		}
		if iState.status != COMMITTED {
			continue
		}

		wg.Add(1)
		go func(instI int32) {
			respHand <- protoMessage{
				towhom:     func() int32 { return requestor },
				gettype:    func() uint8 { return a.commitRPC },
				isnegative: func() bool { return true },
				Serializable: &lwcproto.Commit{
					LeaderId:   int32(iState.vBal.PropID),
					Instance:   instI,
					ConfigBal:  iState.getCurBal(a),
					WhoseCmd:   iState.whoseCmds,
					MoreToCome: 0,
					Command:    iState.cmds,
				},
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(respHand)
	}()
}

func (a *standardConf) RecvPrepareRemote(prepare *lwcproto.Prepare) <-chan Message {
	dlog.AgentPrintfN(a.meID, "Acceptor received prepare request for instance %d from %d at ballot %d.%d", prepare.Instance, prepare.PropID, prepare.Ballot.Number, prepare.Ballot.PropID)
	inst, bal, requestor := prepare.Instance, prepare.ConfigBal, int32(prepare.PropID)
	responseC := make(chan Message, 1)
	checkAndCreateConfigInstanceState(&a.instanceState, inst)
	abk := a.instanceState[inst]

	if abk.status == COMMITTED {
		a.handleMsgAlreadyCommitted(requestor, inst, responseC)
		return responseC
	}

	if bal.GreaterThan(abk.getCurBal(a)) {
		abk.status = PREPARED
		abk.curBal = bal.Ballot

		if bal.Config > a.crtConfig {
			recordConfig(a.crtConfig, a.stableStore, a.durable)
			recordConfigInstanceMetadata(abk, a.stableStore, a.durable)
			fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
		}
	}

	msg := a.getPrepareReply(prepare, inst)
	go a.returnPrepareReply(msg, responseC)

	return responseC
}

func (a *standardConf) returnPrepareReply(msg *lwcproto.PrepareReply, responseC chan Message) {
	toWhom := int32(msg.Req.PropID)
	funcRet := func() uint8 { return a.prepareReplyRPC }
	funcNeg := func() bool {
		return !msg.Cur.Equal(msg.Req)
	}
	toRet := &protoMessage{giveToWhom(toWhom), funcRet, funcNeg, msg}
	responseC <- toRet
	close(responseC)
}

func (a *standardConf) getPrepareReply(prepare *lwcproto.Prepare, inst int32) *lwcproto.PrepareReply {
	return &lwcproto.PrepareReply{
		Instance:   inst,
		Req:        prepare.ConfigBal,
		Cur:        a.instanceState[inst].getCurBal(a),
		CurPhase:   a.getPhase(inst),
		VBal:       a.instanceState[inst].getVBal(),
		AcceptorId: a.meID,
		WhoseCmd:   a.instanceState[inst].whoseCmds,
		Command:    a.instanceState[inst].cmds,
	}
}

func (a *standardConf) getPhase(inst int32) stdpaxosproto.Phase {
	var phase stdpaxosproto.Phase
	if a.instanceState[inst].status == ACCEPTED {
		phase = stdpaxosproto.ACCEPTANCE
	} else if a.instanceState[inst].status == PREPARED {
		phase = stdpaxosproto.PROMISE
	} else {
		panic("should not return phase when neither promised nor accepted")
	}
	return phase
}

func (a *standardConf) RecvAcceptRemote(accept *lwcproto.Accept) <-chan Message {
	dlog.AgentPrintfN(a.meID, "Acceptor received accept request for instance %d from %d at ballot %d.%d", accept.Instance, accept.PropID, accept.Ballot.Number, accept.Ballot.PropID)
	inst, bal, requestor := accept.Instance, accept.Ballot, int32(accept.PropID)
	responseC := make(chan Message, 1)
	checkAndCreateConfigInstanceState(&a.instanceState, inst)
	abk := a.instanceState[inst]

	if abk.status == COMMITTED {
		a.handleMsgAlreadyCommitted(requestor, inst, responseC)
		return responseC
	}

	if bal.GreaterThan(abk.curBal) || bal.Equal(abk.curBal) {
		nonDurableConfAccept(a, accept, abk, a.stableStore, a.durable)
		fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
	}

	acptReply := a.getAcceptReply(inst, accept)
	go a.returnAcceptReply(acptReply, responseC)

	return responseC
}

func (a *standardConf) getAcceptReply(inst int32, accept *lwcproto.Accept) *lwcproto.AcceptReply {
	return &lwcproto.AcceptReply{
		Instance:   inst,
		AcceptorId: a.meID,
		WhoseCmd:   a.instanceState[inst].whoseCmds,
		Req:        accept.ConfigBal,
		Cur:        a.instanceState[inst].getCurBal(a),
		CurPhase:   a.getPhase(inst),
	}
}

func (a *standardConf) returnAcceptReply(acceptReply *lwcproto.AcceptReply, responseC chan Message) {
	responseC <- protoMessage{
		towhom:       giveToWhom(int32(acceptReply.Req.PropID)),
		isnegative:   func() bool { return !acceptReply.Cur.Equal(acceptReply.Req) },
		gettype:      func() uint8 { return a.acceptReplyRPC },
		Serializable: acceptReply,
	}
	close(responseC)
}

func nonDurableConfAccept(a ConfigHolder, accept *lwcproto.Accept, abk *LWCAcceptorBookkeeping, stableStore stablestore.StableStore, durable bool) {
	abk.status = ACCEPTED
	abk.setCurCBal(a, accept.ConfigBal)
	abk.setVCBal(accept.ConfigBal)
	abk.curBal = accept.Ballot
	abk.vBal = accept.Ballot
	abk.cmds = accept.Command
	abk.whoseCmds = accept.WhoseCmd
	recordConfigInstanceMetadata(abk, stableStore, durable)
	recordCommands(abk.cmds, stableStore, durable)
}

func (a *standardConf) handleCommit(inst int32, ballot stdpaxosproto.Ballot, cmds []*state.Command, whoseCmds int32) {
	checkAndCreateConfigInstanceState(&a.instanceState, inst)
	if a.instanceState[inst].status != COMMITTED {
		if ballot.GreaterThan(a.instanceState[inst].vBal) {
			a.instanceState[inst].status = COMMITTED
			a.instanceState[inst].curBal = ballot
			a.instanceState[inst].vBal = ballot
			a.instanceState[inst].cmds = cmds
			a.instanceState[inst].whoseCmds = whoseCmds
			recordConfigInstanceMetadata(a.instanceState[inst], a.stableStore, a.durable)
			recordCommands(a.instanceState[inst].cmds, a.stableStore, a.durable)
			//	fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
		} else {
			// if a ballot was chosen, then all proceeding ballots propose the same value
			a.instanceState[inst].status = COMMITTED
			a.instanceState[inst].curBal = ballot
			a.instanceState[inst].vBal = ballot
			recordConfigInstanceMetadata(a.instanceState[inst], a.stableStore, a.durable)
			// no need for fsync as if we lose data, old safe state is restored
		}
	}
}

func (a *standardConf) RecvCommitRemote(commit *stdpaxosproto.Commit) <-chan struct{} {
	c := make(chan struct{}, 1)
	a.handleCommit(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
	go func() {
		c <- struct{}{}
		close(c)
	}()
	return c
}

func (a *standardConf) RecvCommitShortRemote(commit *stdpaxosproto.CommitShort) <-chan struct{} {
	if a.instanceState[commit.Instance].cmds != nil || commit.GreaterThan(a.instanceState[commit.Instance].vBal) {
		c := make(chan struct{}, 1)
		a.handleCommit(commit.Instance, commit.Ballot, a.instanceState[commit.Instance].cmds, commit.WhoseCmd)
		go func() {
			c <- struct{}{}
			close(c)
		}()
		return c
	} else {
		panic("Got a short commits msg but we don't have any record of the value")
	}
}

//
//type incomingCCommitMsgs struct {
//	done        chan struct{}
//	incomingMsg *lwcproto.Commit
//}
//
//type incomingCCommitShortMsgs struct {
//	done        chan struct{}
//	incomingMsg *lwcproto.CommitShort
//}
//
//type incomingCPromiseRequests struct {
//	retMsg      chan Message
//	incomingMsg *lwcproto.Prepare
//}
//
//type incomingCAcceptRequests struct {
//	retMsg      chan Message
//	incomingMsg *lwcproto.Accept
//}
//
//type outgoingCPromiseResponses struct {
//	//	done       chan bool
//	retMsgChan chan Message
//	lwcproto.ConfigBal
//}
//
//type outgoingCAcceptResponses struct {
//	//	done       chan bool
//	retMsgChan chan Message
//	lwcproto.ConfigBal
//}
//
//type batchingConfAcceptor struct {
//	meID                   int32
//	maxInstance            int32
//	instanceState          map[int32]*AcceptorBookkeeping
//	awaitingPrepareReplies map[int32]map[int32]outgoingPromiseResponses
//	awaitingAcceptReplies  map[int32]map[int32]outgoingAcceptResponses
//	promiseRequests        chan incomingPromiseRequests
//	acceptRequests         chan incomingAcceptRequests
//	commits                chan incomingCommitMsgs
//	commitShorts           chan incomingCommitShortMsgs
//
//	catchupOnProceedingCommits bool
//
//	acceptReplyRPC  uint8
//	prepareReplyRPC uint8
//	commitRPC       uint8
//	commitShortRPC  uint8
//
//	stableStore      stablestore.StableStore
//	durable          bool
//	emuatedWriteTime time.Duration
//	emulatedSS       bool
//	maxBatchWait     time.Duration
//}
//
//type betterBatchingConf struct {
//	ProposerIDs []int32
//	batcher     *batchingAcceptor
//}
//
//func (a *betterBatchingConf) RecvPrepareRemote(prepare *stdpaxosproto.Prepare) <-chan Message {
//	c := make(chan Message, 1)
//	go func() {
//		a.batcher.promiseRequests <- incomingPromiseRequests{
//			retMsg:      c,
//			incomingMsg: prepare,
//		}
//	}()
//	return c
//}
//
//func (a *betterBatchingConf) RecvAcceptRemote(accept *stdpaxosproto.Accept) <-chan Message {
//	c := make(chan Message, 1)
//	go func() {
//		a.batcher.acceptRequests <- incomingAcceptRequests{
//			retMsg:      c,
//			incomingMsg: accept,
//		}
//	}()
//	return c
//}
//
//func (a *betterBatchingConf) RecvCommitRemote(commit *stdpaxosproto.Commit) <-chan struct{} {
//	c := make(chan struct{}, 1)
//	go func() {
//		a.batcher.commits <- incomingCommitMsgs{
//			done:        c,
//			incomingMsg: commit,
//		}
//	}()
//	return c
//}
//
//func (a *betterBatchingConf) RecvCommitShortRemote(commit *stdpaxosproto.CommitShort) <-chan struct{} {
//	c := make(chan struct{}, 1)
//	go func() {
//		a.batcher.commitShorts <- incomingCommitShortMsgs{
//			done:        c,
//			incomingMsg: commit,
//		}
//	}()
//	return c
//}
//
//func (a *batchingAcceptor) checkAndUpdateMaxInstance(inst int32) {
//	if a.maxInstance < inst {
//		a.maxInstance = inst
//	}
//}
//
//func (b *batchingAcceptor) handleCommitShort(inc incomingCommitShortMsgs) {
//	cmtMsg, resp := inc.incomingMsg, inc.done
//	inst := cmtMsg.Instance
//	b.checkAndUpdateMaxInstance(inst)
//
//	instState := b.getInstState(inst)
//	if instState.status == COMMITTED {
//		close(resp)
//		return
//	}
//
//	if instState.cmds == nil {
//		panic("committing with no value!!!")
//	}
//	// store commit
//	instState.status = COMMITTED
//	instState.curBal = cmtMsg.Ballot
//	instState.vBal = cmtMsg.Ballot
//
//	instState.whoseCmds = cmtMsg.WhoseCmd
//
//	go func() {
//		resp <- struct{}{}
//		close(resp)
//	}()
//}
//
//func (a *batchingAcceptor) handleCommit(inc incomingCommitMsgs) {
//	// store all data
//	cmtMsg, resp := inc.incomingMsg, inc.done
//	inst := cmtMsg.Instance
//	a.checkAndUpdateMaxInstance(inst)
//	instState := a.getInstState(inst)
//	if instState.status == COMMITTED {
//		go func() {
//			close(resp)
//		}()
//		return
//	}
//
//	// store commit
//	instState.status = COMMITTED
//	instState.curBal = cmtMsg.Ballot
//	instState.vBal = cmtMsg.Ballot
//	instState.cmds = cmtMsg.Command
//	instState.whoseCmds = cmtMsg.WhoseCmd
//	go func() {
//		resp <- struct{}{}
//		close(resp)
//	}()
//}
//
//func (a *betterBatchingConf) batchPersister() {
//	timeout := time.NewTimer(a.batcher.maxBatchWait)
//	for {
//		select {
//		case inc := <-a.batcher.commitShorts:
//			a.batcher.handleCommitShort(inc)
//			break
//		case inc := <-a.batcher.commits:
//			a.batcher.handleCommit(inc)
//		case inc := <-a.batcher.promiseRequests:
//			a.batcher.recvPromiseRequest(inc)
//			break
//		case inc := <-a.batcher.acceptRequests:
//			a.batcher.recvAcceptRequest(inc)
//			break
//		case <-timeout.C:
//			// return responses for max and not max
//			a.batcher.doBatch()
//			timeout.Reset(a.batcher.maxBatchWait)
//			break
//
//		}
//
//	}
//}
//
//func (a *batchingAcceptor) doBatch() {
//	dlog.AgentPrintfN(a.meID, "Acceptor batch starting, now persisting received ballots")
//	for instToResp, _ := range a.awaitingPrepareReplies {
//		abk := a.instanceState[instToResp]
//		recordInstanceMetadata(abk, a.stableStore, a.durable)
//		//if abk.cmds != nil {
//		//	recordCommands(abk.cmds, a.stableStore, a.durable)
//		//}
//	}
//	for instToResp, _ := range a.awaitingAcceptReplies {
//		abk := a.instanceState[instToResp]
//		recordInstanceMetadata(abk, a.stableStore, a.durable)
//		if abk.cmds != nil {
//			recordCommands(abk.cmds, a.stableStore, a.durable)
//		}
//	}
//
//	fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
//	dlog.AgentPrintfN(a.meID, "Acceptor batch performed, now returning responses for %d instances", len(a.awaitingPrepareReplies)+len(a.awaitingAcceptReplies))
//
//	a.returnPrepareAndAcceptRepliesAndClear()
//	dlog.AgentPrintfN(a.meID, "Acceptor done returning responses")
//}
//
//func (a *batchingAcceptor) returnPrepareAndAcceptRepliesAndClear() {
//	a.returnPrepareRepliesAndClear()
//	a.returnAcceptRepliesAndClear()
//}
//
//func (a *batchingAcceptor) returnAcceptRepliesAndClear() {
//	for instToResp, awaitingResps := range a.awaitingAcceptReplies {
//		abk := a.instanceState[instToResp]
//		if abk.status == COMMITTED {
//			dlog.AgentPrintfN(a.meID, "Acceptor %d returning no responses for instance %d as it has been committed in this batch", a.meID, instToResp)
//			for _, prepResp := range awaitingResps {
//				close(prepResp.retMsgChan)
//			}
//			continue
//		}
//
//		returnAcceptRepliesAwaiting(instToResp, a.meID, awaitingResps, abk, a.acceptReplyRPC)
//	}
//
//	a.awaitingAcceptReplies = make(map[int32]map[int32]outgoingAcceptResponses, 100)
//}
//
//func (a *batchingAcceptor) returnPrepareRepliesAndClear() {
//	for instToResp, awaitingResps := range a.awaitingPrepareReplies {
//		abk := a.instanceState[instToResp]
//		if abk.status == COMMITTED {
//			dlog.AgentPrintfN(a.meID, "Acceptor %d returning no responses for instance %d as it has been committed in this batch", a.meID, instToResp)
//			for _, prepResp := range awaitingResps {
//				close(prepResp.retMsgChan)
//			}
//			continue
//		}
//		returnPrepareRepliesAwaiting(instToResp, a.meID, awaitingResps, abk, a.prepareReplyRPC)
//	}
//	a.awaitingPrepareReplies = make(map[int32]map[int32]outgoingPromiseResponses, 100)
//}
//
//func (a *batchingAcceptor) getAwaitingPrepareReplies(inst int32) map[int32]outgoingPromiseResponses {
//	_, awaitingRespsExists := a.awaitingPrepareReplies[inst]
//	if !awaitingRespsExists {
//		a.awaitingPrepareReplies[inst] = make(map[int32]outgoingPromiseResponses)
//	}
//	return a.awaitingPrepareReplies[inst]
//}
//
//func (a *batchingAcceptor) getAwaitingAcceptReplies(inst int32) map[int32]outgoingAcceptResponses {
//	_, awaitingRespsExists := a.awaitingAcceptReplies[inst]
//	if !awaitingRespsExists {
//		a.awaitingAcceptReplies[inst] = make(map[int32]outgoingAcceptResponses)
//	}
//	return a.awaitingAcceptReplies[inst]
//}
//
//func (a *batchingAcceptor) recvAcceptRequest(inc incomingAcceptRequests) {
//	dlog.AgentPrintfN(a.meID, "Acceptor received accept request for instance %d from %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.PropID, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
//	accMsg, respHand, requestor := inc.incomingMsg, inc.retMsg, int32(inc.incomingMsg.PropID)
//	inst := accMsg.Instance
//
//	a.checkAndUpdateMaxInstance(inst)
//	instState := a.getInstState(inst)
//
//	if instState.status == COMMITTED {
//		a.handleMsgAlreadyCommitted(requestor, inst, respHand)
//		return
//	}
//
//	instAwaitingAcceptReplies := a.getAwaitingAcceptReplies(inst)
//	instAwaitingPrepareReplies := a.getAwaitingPrepareReplies(inst)
//	if instState.curBal.GreaterThan(accMsg.Ballot) {
//		dlog.AgentPrintfN(a.meID, "Acceptor intending to preempt ballot in instance %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
//		//check if most recent ballot from proposer to return preempt to
//		prevAcc, wasPrevAcc := instAwaitingAcceptReplies[requestor]
//		if wasPrevAcc {
//			if prevAcc.GreaterThan(accMsg.Ballot) {
//				close(respHand)
//				return
//			}
//		}
//		prevPrep, wasPrevPrep := instAwaitingPrepareReplies[requestor]
//		if wasPrevPrep {
//			if prevPrep.GreaterThan(accMsg.Ballot) {
//				close(respHand)
//				return
//			}
//		}
//		closeAllPreviousResponseToProposer(instAwaitingPrepareReplies, instAwaitingAcceptReplies, requestor)
//		instAwaitingAcceptReplies[requestor] = outgoingAcceptResponses{
//			retMsgChan: respHand,
//			Ballot:     accMsg.Ballot,
//		}
//		return
//	}
//	dlog.AgentPrintfN(a.meID, "Acceptor intending to accept ballot in instance %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
//
//	instState.status = ACCEPTED
//	instState.curBal = accMsg.Ballot
//	instState.vBal = accMsg.Ballot
//	instState.cmds = accMsg.Command
//	instState.whoseCmds = accMsg.WhoseCmd
//
//	// close any previous awaiting responses and store this current one
//	closeAllPreviousResponseToProposer(instAwaitingPrepareReplies, instAwaitingAcceptReplies, requestor)
//	instAwaitingAcceptReplies[requestor] = outgoingAcceptResponses{
//		retMsgChan: respHand,
//		Ballot:     accMsg.Ballot,
//	}
//}
//
//func (a *batchingAcceptor) recvPromiseRequest(inc incomingPromiseRequests) {
//	dlog.AgentPrintfN(a.meID, "Acceptor received promise request for instance %d from %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.PropID, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
//	prepMsg, respHand, requestor := inc.incomingMsg, inc.retMsg, int32(inc.incomingMsg.PropID)
//
//	inst := prepMsg.Instance
//	a.checkAndUpdateMaxInstance(inst)
//	instState := a.getInstState(inst)
//
//	if instState.status == COMMITTED {
//		a.handleMsgAlreadyCommitted(requestor, inst, respHand)
//		return
//	}
//
//	instAwaitingAcceptReplies := a.getAwaitingAcceptReplies(inst)
//	instAwaitingPrepareReplies := a.getAwaitingPrepareReplies(inst)
//	if instState.curBal.GreaterThan(prepMsg.Ballot) || (instState.curBal.Equal(prepMsg.Ballot) && instState.status == ACCEPTED) {
//		dlog.AgentPrintfN(a.meID, "Acceptor intending to preempt ballot in instance %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
//		// check if most recent ballot from proposer to return preempt to
//		prevAcc, wasPrevAcc := instAwaitingAcceptReplies[requestor]
//		if wasPrevAcc {
//			if prevAcc.GreaterThan(prepMsg.Ballot) || prevAcc.Equal(prepMsg.Ballot) {
//				close(respHand)
//				return
//			}
//		}
//
//		prevPrep, wasPrevPrep := instAwaitingPrepareReplies[requestor]
//		if wasPrevPrep {
//			if prevPrep.GreaterThan(prepMsg.Ballot) {
//				close(respHand)
//				return
//			}
//		}
//		closeAllPreviousResponseToProposer(instAwaitingPrepareReplies, instAwaitingAcceptReplies, requestor)
//		instAwaitingPrepareReplies[requestor] = outgoingPromiseResponses{
//			retMsgChan: respHand,
//			Ballot:     prepMsg.Ballot,
//		}
//		return
//	}
//	dlog.AgentPrintfN(a.meID, "Acceptor intending to promise ballot in instance %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
//
//	instState.status = PREPARED
//	instState.curBal = prepMsg.Ballot
//	//recordInstanceMetadata(instState, a.stableStore, a.durable)
//
//	// close any previous awaiting responses and store this current one
//	closeAllPreviousResponseToProposer(instAwaitingPrepareReplies, instAwaitingAcceptReplies, requestor)
//	instAwaitingPrepareReplies[requestor] = outgoingPromiseResponses{
//		retMsgChan: respHand,
//		Ballot:     prepMsg.Ballot,
//	}
//}
//
//func (a *batchingAcceptor) handleMsgAlreadyCommitted(requestor int32, inst int32, respHand chan Message) {
//	if requestor == a.meID { // todo change to have a local accept and prepare and a remote one too
//		close(respHand)
//		return
//	}
//
//	maxInstToTryReturn := a.maxInstance
//	if !a.catchupOnProceedingCommits {
//		maxInstToTryReturn = inst
//	}
//
//	wg := sync.WaitGroup{}
//	for i := maxInstToTryReturn; i >= inst; i-- {
//		iState, exists := a.instanceState[i]
//		if !exists {
//			continue
//		}
//		if iState.status != COMMITTED {
//			continue
//		}
//
//		wg.Add(1)
//		go func(instI int32) {
//			respHand <- protoMessage{
//				towhom:     func() int32 { return requestor },
//				gettype:    func() uint8 { return a.commitRPC },
//				isnegative: func() bool { return true },
//				Serializable: &stdpaxosproto.Commit{
//					LeaderId:   int32(iState.vBal.PropID),
//					Instance:   instI,
//					Ballot:     iState.vBal,
//					WhoseCmd:   iState.whoseCmds,
//					MoreToCome: 0,
//					Command:    iState.cmds},
//			}
//			wg.Done()
//		}(i)
//	}
//
//	go func() {
//		wg.Wait()
//		close(respHand)
//	}()
//}
//
//func (a *batchingAcceptor) getInstState(inst int32) *AcceptorBookkeeping {
//	_, exists := a.instanceState[inst]
//	if !exists {
//		a.instanceState[inst] = &AcceptorBookkeeping{
//			status:    0,
//			cmds:      nil,
//			curBal:    stdpaxosproto.Ballot{-1, -1},
//			vBal:      stdpaxosproto.Ballot{-1, -1},
//			whoseCmds: -1,
//			//Mutex:     sync.Mutex{},
//		}
//	}
//	return a.instanceState[inst]
//}
//
////func (a *batchingAcceptor) getAwaitingResponses(inst int32) *responsesAwaiting {
////	_, awaitingRespsExists := a.awaitingResponses[inst]
////	if !awaitingRespsExists {
////		a.awaitingResponses[inst] = &responsesAwaiting{
////			propsPrepResps:   make(map[int32]*outgoingPromiseResponses),
////			propsAcceptResps: make(map[int32]*outgoingAcceptResponses),
////		}
////	}
////	return a.awaitingResponses[inst]
////}
//
//func closeAllPreviousResponseToProposer(awaitingPrepareReplies map[int32]outgoingPromiseResponses, awaitingAcceptReplies map[int32]outgoingAcceptResponses, proposer int32) {
//	prop, existsp := awaitingPrepareReplies[proposer]
//	//go func() {
//	if existsp {
//		close(prop.retMsgChan)
//		delete(awaitingPrepareReplies, proposer)
//	}
//	acc, existsa := awaitingAcceptReplies[proposer]
//	if existsa {
//		close(acc.retMsgChan)
//		delete(awaitingAcceptReplies, proposer)
//	}
//	//}()
//}
//
//func (a *betterBatchingConf) getPhase(inst int32) stdpaxosproto.Phase {
//	var phase stdpaxosproto.Phase
//	if a.batcher.instanceState[inst].status == ACCEPTED {
//		phase = stdpaxosproto.ACCEPTANCE
//	} else if a.batcher.instanceState[inst].status == PREPARED {
//		phase = stdpaxosproto.PROMISE
//	} else {
//		panic("should not return phase when neither promised nor accepted")
//	}
//	return phase
//}
//
//func (abk *AcceptorBookkeeping) getPhase() stdpaxosproto.Phase {
//	var phase stdpaxosproto.Phase
//	if abk.status == ACCEPTED {
//		phase = stdpaxosproto.ACCEPTANCE
//	} else if abk.status == PREPARED {
//		phase = stdpaxosproto.PROMISE
//	} else {
//		panic("should not return phase when neither promised nor accepted")
//	}
//	return phase
//}
//
////func returnPrepareAndAcceptResponses(inst int32, myId int32, awaitingPromiseResps map[int32]outgoingPromiseResponses, awaitingAcceptResps map[int32]outgoingAcceptResponses, abk *AcceptorBookkeeping, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8) {
////	dlog.AgentPrintfN(myId, "Acceptor %d returning batch of %d responses for instance %d", myId, len(awaitingPromiseResps)+len(awaitingAcceptResps), inst)
////	if abk.status == COMMITTED {
////		dlog.AgentPrintfN(myId, "Acceptor %d returning no responses for instance %d as it has been committed in this batch", myId, inst)
////		for _, prepResp := range awaitingPromiseResps {
////			close(prepResp.retMsgChan)
////
////		}
////		for _, accResp := range awaitingAcceptResps {
////			close(accResp.retMsgChan)
////		}
////		return
////	}
////
////	returnPrepareRepliesAwaiting(inst, myId, awaitingPromiseResps, abk, prepareReplyRPC)
////
////	returnAcceptRepliesAwaiting(inst, myId, awaitingAcceptResps, abk, acceptReplyRPC)
////}
//
//func returnAcceptRepliesAwaiting(inst int32, myId int32, awaitingResps map[int32]outgoingAcceptResponses, abk *AcceptorBookkeeping, acceptReplyRPC uint8) {
//	outOfOrderAccept := false
//	for pid, accResp := range awaitingResps {
//
//		if abk.curBal.GreaterThan(accResp.Ballot) {
//			outOfOrderAccept = true
//		}
//
//		// if we accepted anything here but cur bal has changed, it is safe to respond positively to accept request
//		//   -- we are telling proposers of the greater ballot of this acceptance
//		curPhase := abk.getPhase()
//		curBal := abk.curBal
//		if abk.vBal.Equal(accResp.Ballot) {
//			curPhase = stdpaxosproto.ACCEPTANCE
//			curBal = abk.vBal
//		}
//		acc := &stdpaxosproto.AcceptReply{
//			Instance:   inst,
//			AcceptorId: myId,
//			CurPhase:   curPhase,
//			Cur:        curBal,
//			Req:        accResp.Ballot,
//			WhoseCmd:   abk.whoseCmds,
//		}
//
//		preempt := !acc.Cur.Equal(acc.Req)
//		respTypeS := "acceptance"
//		if preempt {
//			respTypeS = "preempt"
//		}
//		dlog.AgentPrintfN(myId, "Acceptor returning ConfigAccept Reply (%s) to Replica %d for instance %d at current ballot %d.%d when requested %d.%d", respTypeS,
//			pid, inst, abk.curBal.Number, abk.curBal.PropID, accResp.Number, accResp.PropID)
//
//		resp, lpid := accResp, pid
//		go func() {
//			resp.retMsgChan <- protoMessage{
//				towhom:       giveToWhom(lpid),
//				gettype:      func() uint8 { return acceptReplyRPC },
//				isnegative:   func() bool { return preempt },
//				Serializable: acc,
//			}
//			close(resp.retMsgChan)
//		}()
//	}
//
//	if outOfOrderAccept {
//		dlog.AgentPrintfN(myId, "Acceptor %d has acknowledged ballots out of order in instance %d", myId, inst)
//	}
//}
//
//func returnPrepareRepliesAwaiting(inst int32, myId int32, awaitingResps map[int32]outgoingPromiseResponses, abk *AcceptorBookkeeping, prepareReplyRPC uint8) {
//	for pid, prepResp := range awaitingResps {
//		prep := &stdpaxosproto.PrepareReply{
//			Instance:   inst,
//			Cur:        abk.curBal,
//			CurPhase:   abk.getPhase(),
//			Req:        prepResp.Ballot,
//			VBal:       abk.vBal,
//			AcceptorId: myId,
//			WhoseCmd:   abk.whoseCmds,
//			Command:    abk.cmds,
//		}
//		preempt := !prep.Cur.Equal(prep.Req)
//		typeRespS := "promise"
//		if preempt {
//			typeRespS = "preempt"
//		}
//		dlog.AgentPrintfN(myId, "Acceptor returning ConfigPrepare Reply (%s) to Replica %d for instance %d at current ballot %d.%d when requested %d.%d", typeRespS, pid,
//			inst, abk.curBal.Number, abk.curBal.PropID, prepResp.Number, prepResp.PropID)
//
//		resp, lpid := prepResp, pid
//		go func() {
//			resp.retMsgChan <- protoMessage{
//				towhom:       giveToWhom(lpid),
//				gettype:      func() uint8 { return prepareReplyRPC },
//				isnegative:   func() bool { return preempt },
//				Serializable: prep,
//			}
//			close(resp.retMsgChan)
//		}()
//	}
//}
