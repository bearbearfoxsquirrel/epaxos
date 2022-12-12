package acceptor

//
//import (
//	"dlog"
//	"stablestore"
//	"state"
//	"stdpaxosproto"
//	"sync"
//	"time"
//)
//
//type AltAcceptor interface {
//	RecvPrepareRemote(prepare *stdpaxosproto.Prepare, ret chan<- Message)       //<-chan Message
//	RecvAcceptRemote(accept *stdpaxosproto.Accept, ret chan<- Message)          //<-chan Message
//	RecvCommitRemote(commit *stdpaxosproto.Commit, ret chan<- Message)          //<-chan struct{} // bool
//	RecvCommitShortRemote(short *stdpaxosproto.CommitShort, ret chan<- Message) //<-chan struct{} // bool
//}
//
//func AltStandardAcceptorNew(file stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration, id int32, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8, commitShortRPC uint8, catchupOnProceedingCommits bool) *altstandard {
//	return &altstandard{
//		instanceState:    make(map[int32]*InstanceBookkeeping),
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
//func AltBetterBatchingAcceptorNew(file stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration, id int32, maxAcceptanceBatchWait time.Duration, proposerIDs []int32, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8, commitShortRPC uint8, catchupOnProceedingCommits bool) *altbetterBatching {
//	bat := &altbetterBatching{
//		stableStore:      file,
//		durable:          durable,
//		emuatedWriteTime: emulatedWriteTime,
//		emulatedSS:       emulatedSS,
//		maxAcceptanceBatchWait:     maxAcceptanceBatchWait,
//		batcher: &altbetterBatcher{
//			awaitingResponses: make(map[int32]*responsesAwaiting, 100),
//			instanceState:     make(map[int32]*InstanceBookkeeping, 100),
//			promiseRequests:   make(chan incomingPromiseRequests, 1000),
//			acceptRequests:    make(chan incomingAcceptRequests, 1000),
//			commits:           make(chan incomingCommitMsgs, 1000),
//			commitShorts:      make(chan incomingCommitShortMsgs, 1000),
//		},
//		ProposerIDs:                proposerIDs,
//		meID:                       id,
//		prepareReplyRPC:            prepareReplyRPC,
//		acceptReplyRPC:             acceptReplyRPC,
//		commitRPC:                  commitRPC,
//		commitShortRPC:             commitShortRPC,
//		maxInstance:                -1,
//		catchupOnProceedingCommits: catchupOnProceedingCommits,
//	}
//	go bat.batchPersister()
//	return bat
//}
//
//type altstandard struct {
//	instanceState              map[int32]*InstanceBookkeeping
//	stableStore                stablestore.StableStore
//	durable                    bool
//	emuatedWriteTime           time.Duration
//	emulatedSS                 bool
//	meID                       int32
//	acceptReplyRPC             uint8
//	prepareReplyRPC            uint8
//	commitRPC                  uint8
//	commitShortRPC             uint8
//	catchupOnProceedingCommits bool
//	maxInstance                int32
//}
//
//func (a *altstandard) handleMsgAlreadyCommitted(requestor int32, inst int32, respHand chan<- Message) {
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
//	//wg := sync.WaitGroup{}
//	for i := maxInstToTryReturn; i >= inst; i-- {
//		iState, exists := a.instanceState[i]
//		if !exists {
//			continue
//		}
//		if iState.status != COMMITTED {
//			continue
//		}
//
//		//wg.Add(1)
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
//			//wg.Done()
//		}(i)
//	}
//
//	//go func() {
//	//	wg.Wait()
//	//close(respHand)
//	//}()
//	//	}()
//}
//
//func (a *altstandard) RecvPrepareRemote(prepare *stdpaxosproto.Prepare, ret chan<- Message) {
//	inst, bal, requestor := prepare.Instance, prepare.Ballot, int32(prepare.PropID)
//	checkAndCreateInstanceState(&a.instanceState, inst)
//	abk := a.instanceState[inst]
//
//	if abk.status == COMMITTED {
//		a.handleMsgAlreadyCommitted(requestor, inst, ret)
//	}
//
//	if bal.GreaterThan(abk.curBal) {
//		abk.status = PREPARED
//		abk.curBal = bal
//		recordInstanceMetadata(abk, a.stableStore, a.durable)
//		fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
//	}
//
//	msg := &stdpaxosproto.PrepareReply{
//		Instance:   inst,
//		Req:        prepare.Ballot,
//		Cur:        a.instanceState[inst].curBal,
//		CurPhase:   a.getPhase(inst),
//		VBal:       a.instanceState[inst].vBal,
//		AcceptorId: a.meID,
//		WhoseCmd:   a.instanceState[inst].whoseCmds,
//		Command:    a.instanceState[inst].cmds,
//	}
//	toWhom := int32(prepare.PropID)
//	funcRet := func() uint8 { return a.prepareReplyRPC }
//	funcNeg := func() bool {
//		return !msg.Cur.Equal(msg.Req)
//	}
//	toRet := &protoMessage{giveToWhom(toWhom), funcRet, funcNeg, msg}
//	go func() { ret <- toRet }()
//}
//
//func (a *altstandard) getPhase(inst int32) stdpaxosproto.Phase {
//	var phase stdpaxosproto.Phase
//	if a.instanceState[inst].status == ACCEPTED {
//		phase = stdpaxosproto.ACCEPTANCE
//	} else if a.instanceState[inst].status == PREPARED {
//		phase = stdpaxosproto.PROMISE
//	} else {
//		panic("should not return phase when neither promised nor accepted")
//	}
//	return phase
//}
//
//func (a *altstandard) RecvAcceptRemote(accept *stdpaxosproto.Accept, ret chan<- Message) {
//	inst, bal, requestor := accept.Instance, accept.Ballot, int32(accept.PropID)
//	checkAndCreateInstanceState(&a.instanceState, inst)
//	abk := a.instanceState[inst]
//
//	if abk.status == COMMITTED {
//		a.handleMsgAlreadyCommitted(requestor, inst, ret)
//	}
//
//	if bal.GreaterThan(abk.curBal) || bal.Equal(abk.curBal) {
//		nonDurableConfAccept(accept, abk, a.stableStore, a.durable)
//		fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
//	}
//
//	go func(whoseCmds int32, cur stdpaxosproto.Ballot, phase stdpaxosproto.Phase) {
//		msg := &stdpaxosproto.AcceptReply{
//			Instance:   inst,
//			AcceptorId: a.meID,
//			WhoseCmd:   whoseCmds,
//			Req:        accept.Ballot,
//			Cur:        cur,
//			CurPhase:   phase,
//		}
//
//		ret <- protoMessage{
//			towhom:       giveToWhom(int32(accept.PropID)),
//			isnegative:   func() bool { return !msg.Cur.Equal(msg.Req) },
//			gettype:      func() uint8 { return a.acceptReplyRPC },
//			Serializable: msg,
//		}
//	}(abk.whoseCmds, abk.curBal, abk.getPhase())
//}
//
//func (a *altstandard) handleCommit(inst int32, ballot stdpaxosproto.Ballot, cmds []*state.Command, whoseCmds int32) {
//	checkAndCreateInstanceState(&a.instanceState, inst)
//	if a.instanceState[inst].status != COMMITTED {
//		if ballot.GreaterThan(a.instanceState[inst].vBal) {
//			a.instanceState[inst].status = COMMITTED
//			a.instanceState[inst].curBal = ballot
//			a.instanceState[inst].vBal = ballot
//			a.instanceState[inst].cmds = cmds
//			a.instanceState[inst].whoseCmds = whoseCmds
//			recordInstanceMetadata(a.instanceState[inst], a.stableStore, a.durable)
//			recordCommands(a.instanceState[inst].cmds, a.stableStore, a.durable)
//			//	fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
//		} else {
//			// if a ballot was chosen, then all proceeding ballots propose the same value
//			a.instanceState[inst].status = COMMITTED
//			a.instanceState[inst].curBal = ballot
//			a.instanceState[inst].vBal = ballot
//			recordInstanceMetadata(a.instanceState[inst], a.stableStore, a.durable)
//			// no need for fsync as if we lose data, old safe state is restored
//		}
//	}
//}
//
//func (a *altstandard) RecvCommitRemote(commit *stdpaxosproto.Commit) <-chan struct{} {
//	c := make(chan struct{}, 1)
//	a.handleCommit(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
//	go func() {
//		c <- struct{}{}
//		close(c)
//	}()
//	return c
//}
//
//func (a *altstandard) RecvCommitShortRemote(commit *stdpaxosproto.CommitShort) <-chan struct{} {
//	if a.instanceState[commit.Instance].cmds != nil || commit.GreaterThan(a.instanceState[commit.Instance].vBal) {
//		c := make(chan struct{}, 1)
//		a.handleCommit(commit.Instance, commit.Ballot, a.instanceState[commit.Instance].cmds, commit.WhoseCmd)
//		go func() {
//			c <- struct{}{}
//			close(c)
//		}()
//		return c
//	} else {
//		panic("Got a short commits msg but we don't have any record of the value")
//	}
//}
//
////type outgoingPromiseResponses struct {
////	//	done       chan bool
////	retMsgChan chan Message
////	stdpaxosproto.Ballot
////}
////
////type outgoingAcceptResponses struct {
////	//	done       chan bool
////	retMsgChan chan Message
////	stdpaxosproto.Ballot
////}
////
////type responsesAwaiting struct {
////	propsPrepResps   map[int32]*outgoingPromiseResponses
////	propsAcceptResps map[int32]*outgoingAcceptResponses
////}
//
////fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
////
////for inst, instStore := range store {
////	a.instanceState[inst].Lock()
////	for _, pid := range a.ProposerIDs {
////		if prep, pexists := instStore.propsPrepResps[pid]; pexists {
////			go func(toWhom int32, prepResp chan<- Message, msg *stdpaxosproto.PrepareReply) {
////				prepResp <- protoMessage{
////					giveToWhom(toWhom),
////					func() uint8 { return a.prepareReplyRPC },
////
////					msg,
////				}
////				close(prepResp)
////			}(pid, prep.retMsgChan, a.getPrepareReply(inst, prep.Ballot))
////			delete(instStore.propsPrepResps, pid)
////		} else if acc, aexists := instStore.propsAcceptResps[pid]; aexists {
////			go func(toWhom int32, accResp chan<- Message, msg *stdpaxosproto.AcceptReply) {
////				accResp <- protoMessage{
////					towhom:       giveToWhom(toWhom),
////					gettype:      func() uint8 { return a.acceptReplyRPC },
////					Serializable: msg,
////				}
////				close(accResp)
////			}(pid, acc.retMsgChan, a.getAcceptReply(inst, acc))
////			delete(instStore.propsAcceptResps, pid)
////		}
////	}
////	a.instanceState[inst].Unlock()
////}
//
//type altbetterBatcher struct {
//	awaitingResponses map[int32]*responsesAwaiting
//	instanceState     map[int32]*InstanceBookkeeping
//	promiseRequests   chan incomingPromiseRequests
//	acceptRequests    chan incomingAcceptRequests
//	commits           chan incomingCommitMsgs
//	commitShorts      chan incomingCommitShortMsgs
//}
//
//type altbetterBatching struct {
//	stableStore      stablestore.StableStore
//	durable          bool
//	emuatedWriteTime time.Duration
//	emulatedSS       bool
//	maxAcceptanceBatchWait     time.Duration
//
//	ProposerIDs                []int32
//	meID                       int32
//	batcher                    *altbetterBatcher
//	acceptReplyRPC             uint8
//	prepareReplyRPC            uint8
//	commitRPC                  uint8
//	commitShortRPC             uint8
//	maxInstance                int32
//	catchupOnProceedingCommits bool
//}
//
//func (a *altbetterBatching) RecvPrepareRemote(prepare *stdpaxosproto.Prepare) <-chan Message {
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
//func (a *altbetterBatching) RecvAcceptRemote(accept *stdpaxosproto.Accept) <-chan Message {
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
//func (a *altbetterBatching) RecvCommitRemote(commit *stdpaxosproto.Commit) <-chan struct{} {
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
//func (a *altbetterBatching) RecvCommitShortRemote(commit *stdpaxosproto.CommitShort) <-chan struct{} {
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
//func (a *altbetterBatching) checkAndUpdateMaxInstance(inst int32) {
//	if a.maxInstance < inst {
//		a.maxInstance = inst
//	}
//}
//
//func (a *altbetterBatching) batchPersister() {
//	timeout := time.NewTimer(a.maxAcceptanceBatchWait)
//	for {
//		select {
//		case inc := <-a.batcher.commitShorts:
//			cmtMsg, resp := inc.incomingMsg, inc.done
//			inst := cmtMsg.Instance
//			a.checkAndUpdateMaxInstance(inst)
//
//			instState := a.getInstState(inst)
//			if instState.status == COMMITTED {
//				close(resp)
//				break
//			}
//
//			if instState.cmds == nil {
//				panic("committing with no value!!!")
//			}
//			// store commit
//			instState.status = COMMITTED
//			instState.curBal = cmtMsg.Ballot
//			instState.vBal = cmtMsg.Ballot
//
//			instState.whoseCmds = cmtMsg.WhoseCmd
//
//			go func() {
//				resp <- struct{}{}
//				close(resp)
//			}()
//
//			break
//		case inc := <-a.batcher.commits:
//			// store all data
//			cmtMsg, resp := inc.incomingMsg, inc.done
//			inst := cmtMsg.Instance
//			a.checkAndUpdateMaxInstance(inst)
//			instState := a.getInstState(inst)
//			if instState.status == COMMITTED {
//				go func() {
//					close(resp)
//
//				}()
//				break
//			}
//
//			// store commit
//			instState.status = COMMITTED
//			instState.curBal = cmtMsg.Ballot
//			instState.vBal = cmtMsg.Ballot
//			instState.cmds = cmtMsg.Command
//			instState.whoseCmds = cmtMsg.WhoseCmd
//			go func() {
//				resp <- struct{}{}
//				close(resp)
//			}()
//			break
//		case inc := <-a.batcher.promiseRequests:
//			a.recvPromiseRequest(inc)
//			break
//		case inc := <-a.batcher.acceptRequests:
//			a.recvAcceptRequest(inc)
//			break
//		case <-timeout.C:
//			// return responses for max and not max
//			a.doBatch()
//			timeout.Reset(a.maxAcceptanceBatchWait)
//			break
//
//		}
//
//	}
//}
//
//func (a *altbetterBatching) doBatch() {
//
//	dlog.AgentPrintfN(a.meID, "Acceptor batch starting, now persisting received ballots")
//	for instToResp, _ := range a.batcher.awaitingResponses {
//		abk := a.batcher.instanceState[instToResp]
//		recordInstanceMetadata(abk, a.stableStore, a.durable)
//		if abk.cmds != nil {
//			recordCommands(abk.cmds, a.stableStore, a.durable)
//		}
//	}
//	fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
//
//	dlog.AgentPrintfN(a.meID, "Acceptor batch performed, now returning responses")
//	for instToResp, awaitingResps := range a.batcher.awaitingResponses {
//		abk := a.batcher.instanceState[instToResp]
//		returnPrepareAndAcceptResponses(instToResp, a.meID, awaitingResps, abk, a.prepareReplyRPC, a.acceptReplyRPC, a.commitRPC)
//	}
//
//	a.batcher.awaitingResponses = make(map[int32]*responsesAwaiting, 100)
//}
//
//func (a *altbetterBatching) recvAcceptRequest(inc incomingAcceptRequests) {
//	dlog.Println("Received accept request")
//	dlog.AgentPrintfN(a.meID, "Received accept request for instance %d from %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.PropID, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
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
//	instAwaitingResponses := a.getAwaitingResponses(inst)
//	if instState.curBal.GreaterThan(accMsg.Ballot) {
//		//check if most recent ballot from proposer to return preempt to
//		prevAcc := instAwaitingResponses.propsAcceptResps[requestor]
//		if prevAcc != nil {
//			if prevAcc.GreaterThan(accMsg.Ballot) {
//				close(respHand)
//				return
//			}
//		}
//
//		prevPrep := instAwaitingResponses.propsPrepResps[requestor]
//		if prevPrep != nil {
//			if prevPrep.GreaterThan(accMsg.Ballot) {
//				close(respHand)
//				return
//			}
//		}
//		closeAllPreviousResponseToProposer(instAwaitingResponses, requestor)
//		a.batcher.awaitingResponses[inst].propsAcceptResps[requestor] = &outgoingAcceptResponses{
//			retMsgChan: respHand,
//			Ballot:     accMsg.Ballot,
//		}
//		return
//	}
//
//	// store commit
//	instState.status = ACCEPTED
//	instState.curBal = accMsg.Ballot
//	instState.vBal = accMsg.Ballot
//	instState.cmds = accMsg.Command
//	instState.whoseCmds = accMsg.WhoseCmd
//
//	// close any previous awaiting responses and store this current one
//	instAwaitingResps := a.getAwaitingResponses(inst)
//	closeAllPreviousResponseToProposer(instAwaitingResps, requestor)
//	a.batcher.awaitingResponses[inst].propsAcceptResps[requestor] = &outgoingAcceptResponses{
//		retMsgChan: respHand,
//		Ballot:     accMsg.Ballot,
//	}
//}
//
//func (a *altbetterBatching) recvPromiseRequest(inc incomingPromiseRequests) {
//	dlog.AgentPrintfN(a.meID, "Received promise request for instance %d from %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.PropID, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
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
//	instAwaitingResponses := a.getAwaitingResponses(inst)
//	if instState.curBal.GreaterThan(prepMsg.Ballot) || (instState.curBal.Equal(prepMsg.Ballot) && instState.status == ACCEPTED) {
//		// check if most recent ballot from proposer to return preempt to
//		prevAcc := instAwaitingResponses.propsAcceptResps[requestor]
//		if prevAcc != nil {
//			if prevAcc.GreaterThan(prepMsg.Ballot) || prevAcc.Equal(prepMsg.Ballot) {
//				close(respHand)
//				return
//			}
//		}
//
//		prevPrep := instAwaitingResponses.propsPrepResps[requestor]
//		if prevPrep != nil {
//			if prevPrep.GreaterThan(prepMsg.Ballot) {
//				close(respHand)
//				return
//			}
//		}
//		closeAllPreviousResponseToProposer(a.batcher.awaitingResponses[inst], requestor)
//		a.batcher.awaitingResponses[inst].propsPrepResps[requestor] = &outgoingPromiseResponses{
//			retMsgChan: respHand,
//			Ballot:     prepMsg.Ballot,
//		}
//		return
//	}
//
//	// store commit
//	instState.status = PREPARED
//	instState.curBal = prepMsg.Ballot
//	//recordInstanceMetadata(instState, a.stableStore, a.durable)
//
//	// close any previous awaiting responses and store this current one
//	instAwaitingResps := a.getAwaitingResponses(inst)
//	closeAllPreviousResponseToProposer(instAwaitingResps, requestor)
//	instAwaitingResps.propsPrepResps[requestor] = &outgoingPromiseResponses{
//		retMsgChan: respHand,
//		Ballot:     prepMsg.Ballot,
//	}
//}
//
//func (a *altbetterBatching) handleMsgAlreadyCommitted(requestor int32, inst int32, respHand chan Message) {
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
//		iState, exists := a.batcher.instanceState[i]
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
//func (a *altbetterBatching) getInstState(inst int32) *InstanceBookkeeping {
//	_, exists := a.batcher.instanceState[inst]
//	if !exists {
//		a.batcher.instanceState[inst] = &InstanceBookkeeping{
//			status:    0,
//			cmds:      nil,
//			curBal:    stdpaxosproto.Ballot{-1, -1},
//			vBal:      stdpaxosproto.Ballot{-1, -1},
//			whoseCmds: -1,
//			Mutex:     sync.Mutex{},
//		}
//	}
//	return a.batcher.instanceState[inst]
//}
//
//func (a *altbetterBatching) getAwaitingResponses(inst int32) *responsesAwaiting {
//	_, awaitingRespsExists := a.batcher.awaitingResponses[inst]
//	if !awaitingRespsExists {
//		a.batcher.awaitingResponses[inst] = &responsesAwaiting{
//			propsPrepResps:   make(map[int32]*outgoingPromiseResponses),
//			propsAcceptResps: make(map[int32]*outgoingAcceptResponses),
//		}
//	}
//	return a.batcher.awaitingResponses[inst]
//}
//func altcloseAllPreviousResponseToProposer(instAwaitingResps *responsesAwaiting, proposer int32) {
//	prop, existsp := instAwaitingResps.propsPrepResps[proposer]
//	//go func() {
//	if existsp {
//		close(prop.retMsgChan)
//		delete(instAwaitingResps.propsPrepResps, proposer)
//	}
//	acc, existsa := instAwaitingResps.propsAcceptResps[proposer]
//	if existsa {
//		close(acc.retMsgChan)
//		delete(instAwaitingResps.propsAcceptResps, proposer)
//	}
//	//}()
//}
//
//func (a *altbetterBatching) getPhase(inst int32) stdpaxosproto.Phase {
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
//func altreturnResponsesAndResetAwaitingResponses(inst int32, myId int32, awaitingResps *responsesAwaiting, abk *InstanceBookkeeping, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8) {
//	dlog.AgentPrintfN(myId, "Acceptor %d returning batch of responses for instance %d", myId, inst)
//	if abk.status == COMMITTED {
//		dlog.AgentPrintfN(myId, "Acceptor %d returning no responses for instance %d as it has been committed in this batch", myId, inst)
//		for _, prepResp := range awaitingResps.propsPrepResps {
//			close(prepResp.retMsgChan)
//
//		}
//		for _, accResp := range awaitingResps.propsAcceptResps {
//			close(accResp.retMsgChan)
//		}
//		return
//	}
//
//	for pid, prepResp := range awaitingResps.propsPrepResps {
//		dlog.AgentPrintfN(myId, "Acceptor returning Prepare Reply to Replica %d for instance %d at current ballot %d.%d when requested %d.%d", pid,
//			inst, abk.curBal.Number, abk.curBal.PropID, prepResp.Number, prepResp.PropID)
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
//		resp, lpid := prepResp, pid
//		go func() {
//			resp.retMsgChan <- protoMessage{
//				towhom:       giveToWhom(lpid),
//				gettype:      func() uint8 { return prepareReplyRPC },
//				isnegative:   func() bool { return !prep.Cur.Equal(prep.Req) },
//				Serializable: prep,
//			}
//			close(resp.retMsgChan)
//		}()
//	}
//
//	for pid, accResp := range awaitingResps.propsAcceptResps {
//		dlog.AgentPrintfN(myId, "Acceptor returning Accept Reply to Replica %d for instance %d at current ballot %d.%d when requested %d.%d",
//			pid, inst, abk.curBal.Number, abk.curBal.PropID, accResp.Number, accResp.PropID)
//		acc := &stdpaxosproto.AcceptReply{
//			Instance:   inst,
//			AcceptorId: myId,
//			CurPhase:   abk.getPhase(),
//			Cur:        abk.curBal,
//			Req:        accResp.Ballot,
//			WhoseCmd:   abk.whoseCmds,
//		}
//		resp, lpid := accResp, pid
//		go func() {
//			resp.retMsgChan <- protoMessage{
//				towhom:       giveToWhom(lpid),
//				gettype:      func() uint8 { return acceptReplyRPC },
//				isnegative:   func() bool { return !acc.Cur.Equal(acc.Req) },
//				Serializable: acc,
//			}
//			close(resp.retMsgChan)
//		}()
//	}
//}
//
//func altreturnCommitsToAwaitingInstanceResponses(awaitingResps *responsesAwaiting, cmtMsg *stdpaxosproto.Commit, commitRPC uint8, meId int32) {
//	for pid, prepResp := range awaitingResps.propsPrepResps {
//		if pid == meId {
//			close(prepResp.retMsgChan)
//			continue
//		}
//		prepResp.retMsgChan <- protoMessage{
//			towhom:       giveToWhom(pid),
//			gettype:      func() uint8 { return commitRPC },
//			isnegative:   func() bool { return true },
//			Serializable: cmtMsg,
//		}
//		close(prepResp.retMsgChan)
//	}
//
//	for pid, accResp := range awaitingResps.propsAcceptResps {
//		if pid == meId {
//			close(accResp.retMsgChan)
//			continue
//		}
//		accResp.retMsgChan <- protoMessage{
//			towhom:       giveToWhom(pid),
//			gettype:      func() uint8 { return commitRPC },
//			isnegative:   func() bool { return true },
//			Serializable: cmtMsg,
//		}
//		close(accResp.retMsgChan)
//	}
//}
