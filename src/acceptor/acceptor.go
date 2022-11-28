package acceptor

import (
	"encoding/binary"
	"epaxos/dlog"
	"epaxos/stablestore"
	"epaxos/state"
	"epaxos/stdpaxosproto"
	"sync"
	"time"
)

type Acceptor interface {
	//RecvPrepareLocal(prepare *stdpaxosproto.Prepare) <-chan bool // bool
	RecvPrepareRemote(prepare *stdpaxosproto.Prepare) <-chan Message
	//	RecvAcceptLocal(accept *stdpaxosproto.Accept) <-chan bool
	RecvAcceptRemote(accept *stdpaxosproto.Accept) <-chan Message
	//	RecvCommitLocal(commit *stdpaxosproto.Commit) <-chan struct{}           // bool
	RecvCommitRemote(commit *stdpaxosproto.Commit) <-chan struct{} // bool
	//	RecvCommitShortLocal(short *stdpaxosproto.CommitShort) <-chan struct{}  // bool
	RecvCommitShortRemote(short *stdpaxosproto.CommitShort) <-chan struct{} // bool
}

func StandardAcceptorNew(file stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration, id int32, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8, commitShortRPC uint8, catchupOnProceedingCommits bool) *standard {
	return &standard{
		instanceState:    make(map[int32]*AcceptorBookkeeping),
		stableStore:      file,
		durable:          durable,
		emuatedWriteTime: emulatedWriteTime,
		emulatedSS:       emulatedSS,
		meID:             id,
		prepareReplyRPC:  prepareReplyRPC,
		acceptReplyRPC:   acceptReplyRPC,
		commitRPC:        commitRPC,
		commitShortRPC:   commitShortRPC,
	}
}

func BetterBatchingAcceptorNew(file stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration, id int32, maxBatchWait time.Duration, proposerIDs []int32, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8, commitShortRPC uint8, catchupOnProceedingCommits bool) *betterBatching {
	bat := &betterBatching{

		batcher: &batchingAcceptor{
			instanceState:              make(map[int32]*AcceptorBookkeeping, 100),
			awaitingPrepareReplies:     make(map[int32]map[int32]outgoingPromiseResponses),
			awaitingAcceptReplies:      make(map[int32]map[int32]outgoingAcceptResponses),
			promiseRequests:            make(chan incomingPromiseRequests, 1000),
			acceptRequests:             make(chan incomingAcceptRequests, 1000),
			commits:                    make(chan incomingCommitMsgs, 1000),
			commitShorts:               make(chan incomingCommitShortMsgs, 1000),
			prepareReplyRPC:            prepareReplyRPC,
			acceptReplyRPC:             acceptReplyRPC,
			commitRPC:                  commitRPC,
			commitShortRPC:             commitShortRPC,
			maxInstance:                -1,
			meID:                       id,
			catchupOnProceedingCommits: catchupOnProceedingCommits,
			stableStore:                file,
			durable:                    durable,
			emuatedWriteTime:           emulatedWriteTime,
			emulatedSS:                 emulatedSS,
			maxBatchWait:               maxBatchWait,
		},
		ProposerIDs: proposerIDs,
	}
	go bat.batchPersister()
	return bat
}

type InstanceStatus int

const (
	NOT_STARTED InstanceStatus = iota
	PREPARED
	ACCEPTED
	COMMITTED
)

type AcceptorBookkeeping struct {
	status    InstanceStatus
	cmds      []*state.Command
	curBal    stdpaxosproto.Ballot
	vBal      stdpaxosproto.Ballot
	whoseCmds int32
	//sync.Mutex
}

// append a log entry to stable storage
func recordInstanceMetadata(abk *AcceptorBookkeeping, stableStore stablestore.StableStore, durable bool) {
	if !durable {
		return
	}

	var b [8]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(abk.curBal.Number))
	binary.LittleEndian.PutUint32(b[4:8], uint32(abk.curBal.PropID))
	_, _ = stableStore.Write(b[:])
}

// write a sequence of commands to stable storage
func recordCommands(cmds []*state.Command, stableStore stablestore.StableStore, durable bool) {
	if !durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(stableStore)
		//io.Writer(stableStore)
	}
}

// fsync with the stable store
func fsync(stableStore stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration) {
	if !durable {
		return
	}

	if emulatedSS {
		t := time.NewTimer(emulatedWriteTime)
		<-t.C
		//		time.Sleep(emulatedWriteTime)
	} else {
		_ = stableStore.Sync()
	}
}

func checkAndCreateInstanceState(instanceState *map[int32]*AcceptorBookkeeping, inst int32) {
	if _, exists := (*instanceState)[inst]; !exists {
		(*instanceState)[inst] = &AcceptorBookkeeping{
			status:    NOT_STARTED,
			cmds:      nil,
			curBal:    stdpaxosproto.Ballot{Number: -1, PropID: -1},
			vBal:      stdpaxosproto.Ballot{Number: -1, PropID: -1},
			whoseCmds: -1,
		}
	}
}

func checkAndHandleCommitted(inst int32, state *AcceptorBookkeeping) (bool, *stdpaxosproto.Commit) {
	committed := state.status == COMMITTED
	if committed {
		return true, &stdpaxosproto.Commit{
			LeaderId:   int32(state.curBal.PropID),
			Instance:   inst,
			Ballot:     state.curBal,
			WhoseCmd:   state.whoseCmds,
			MoreToCome: 0,
			Command:    state.cmds,
		}
	} else {
		return false, nil
	}
}

type standard struct {
	instanceState              map[int32]*AcceptorBookkeeping
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
}

func giveToWhom(toWhom int32) func() int32 {
	return func() int32 {
		return toWhom
	}
}

func (a *standard) handleMsgAlreadyCommitted(requestor int32, inst int32, respHand chan Message) {
	//closeAllPreviousResponseToProposer(instAwaitingResponses, requestor)
	//	go func() {
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
			c := &stdpaxosproto.Commit{
				LeaderId:   int32(iState.vBal.PropID),
				Instance:   instI,
				Ballot:     iState.vBal,
				WhoseCmd:   iState.whoseCmds,
				MoreToCome: 0,
				Command:    iState.cmds}
			respHand <- protoMessage{
				towhom:     func() int32 { return requestor },
				gettype:    func() uint8 { return a.commitRPC },
				isnegative: func() bool { return true },
				UDPaxos:    c,
				//Serializable: c,
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(respHand)
	}()
	//	}()
}

func (a *standard) RecvPrepareRemote(prepare *stdpaxosproto.Prepare) <-chan Message {
	dlog.AgentPrintfN(a.meID, "Acceptor received prepare request for instance %d from %d at ballot %d.%d", prepare.Instance, prepare.PropID, prepare.Ballot.Number, prepare.Ballot.PropID)
	inst, bal, requestor := prepare.Instance, prepare.Ballot, int32(prepare.PropID)
	responseC := make(chan Message, 1)
	checkAndCreateInstanceState(&a.instanceState, inst)
	abk := a.instanceState[inst]

	if abk.status == COMMITTED {
		a.handleMsgAlreadyCommitted(requestor, inst, responseC)
		return responseC
	}

	if bal.GreaterThan(abk.curBal) {
		abk.status = PREPARED
		abk.curBal = bal
		recordInstanceMetadata(abk, a.stableStore, a.durable)
		fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
	}

	msg := a.getPrepareReply(prepare, inst)
	a.returnPrepareReply(msg, responseC)
	close(responseC)
	return responseC
}

func (a *standard) returnPrepareReply(msg *stdpaxosproto.PrepareReply, responseC chan<- Message) {
	toWhom := int32(msg.Req.PropID)
	funcRet := func() uint8 { return a.prepareReplyRPC }
	funcNeg := func() bool {
		return !msg.Cur.Equal(msg.Req)
	}
	toRet := &protoMessage{giveToWhom(toWhom), funcRet, funcNeg, msg}
	responseC <- toRet
}

func (a *standard) getPrepareReply(prepare *stdpaxosproto.Prepare, inst int32) *stdpaxosproto.PrepareReply {
	return &stdpaxosproto.PrepareReply{
		Instance:   inst,
		Req:        prepare.Ballot,
		Cur:        a.instanceState[inst].curBal,
		CurPhase:   a.getPhase(inst),
		VBal:       a.instanceState[inst].vBal,
		AcceptorId: a.meID,
		WhoseCmd:   a.instanceState[inst].whoseCmds,
		Command:    a.instanceState[inst].cmds,
	}
}

func (a *standard) getPhase(inst int32) stdpaxosproto.Phase {
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

func (a *standard) RecvAcceptRemote(accept *stdpaxosproto.Accept) <-chan Message {
	dlog.AgentPrintfN(a.meID, "Acceptor received accept request for instance %d from %d at ballot %d.%d", accept.Instance, accept.PropID, accept.Ballot.Number, accept.Ballot.PropID)
	inst, bal, requestor := accept.Instance, accept.Ballot, int32(accept.PropID)
	responseC := make(chan Message, 1)
	checkAndCreateInstanceState(&a.instanceState, inst)
	abk := a.instanceState[inst]

	if abk.status == COMMITTED {
		a.handleMsgAlreadyCommitted(requestor, inst, responseC)
		return responseC
	}

	if bal.GreaterThan(abk.curBal) || bal.Equal(abk.curBal) {
		nonDurableAccept(accept, abk, a.stableStore, a.durable)
		fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
	}
	acptReply := a.getAcceptReply(inst, accept)
	a.returnAcceptReply(acptReply, responseC)
	close(responseC)
	return responseC
}

func (a *standard) getAcceptReply(inst int32, accept *stdpaxosproto.Accept) *stdpaxosproto.AcceptReply {
	return &stdpaxosproto.AcceptReply{
		Instance:   inst,
		AcceptorId: a.meID,
		WhoseCmd:   a.instanceState[inst].whoseCmds,
		Req:        accept.Ballot,
		Cur:        a.instanceState[inst].curBal,
		CurPhase:   a.getPhase(inst),
	}
}

func (a *standard) returnAcceptReply(acceptReply *stdpaxosproto.AcceptReply, responseC chan<- Message) {
	responseC <- protoMessage{
		towhom:     giveToWhom(int32(acceptReply.Req.PropID)),
		isnegative: func() bool { return !acceptReply.Cur.Equal(acceptReply.Req) },
		gettype:    func() uint8 { return a.acceptReplyRPC },
		UDPaxos:    acceptReply,
	}
}

func nonDurableAccept(accept *stdpaxosproto.Accept, abk *AcceptorBookkeeping, stableStore stablestore.StableStore, durable bool) {
	abk.status = ACCEPTED
	abk.curBal = accept.Ballot
	abk.vBal = accept.Ballot
	abk.cmds = accept.Command
	abk.whoseCmds = accept.WhoseCmd
	recordInstanceMetadata(abk, stableStore, durable)
	recordCommands(abk.cmds, stableStore, durable)
}

func (a *standard) handleCommit(inst int32, ballot stdpaxosproto.Ballot, cmds []*state.Command, whoseCmds int32) {
	checkAndCreateInstanceState(&a.instanceState, inst)
	if a.instanceState[inst].status != COMMITTED {
		if ballot.GreaterThan(a.instanceState[inst].vBal) {
			a.instanceState[inst].status = COMMITTED
			a.instanceState[inst].curBal = ballot
			a.instanceState[inst].vBal = ballot
			a.instanceState[inst].cmds = cmds
			a.instanceState[inst].whoseCmds = whoseCmds
			recordInstanceMetadata(a.instanceState[inst], a.stableStore, a.durable)
			recordCommands(a.instanceState[inst].cmds, a.stableStore, a.durable)
			//	fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
		} else {
			// if a ballot was chosen, then all proceeding ballots propose the same value
			a.instanceState[inst].status = COMMITTED
			a.instanceState[inst].curBal = ballot
			a.instanceState[inst].vBal = ballot
			recordInstanceMetadata(a.instanceState[inst], a.stableStore, a.durable)
			// no need for fsync as if we lose data, old safe state is restored
		}
	}
}

func (a *standard) RecvCommitRemote(commit *stdpaxosproto.Commit) <-chan struct{} {
	c := make(chan struct{}, 1)
	a.handleCommit(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
	go func() {
		c <- struct{}{}
		close(c)
	}()
	return c
}

func (a *standard) RecvCommitShortRemote(commit *stdpaxosproto.CommitShort) <-chan struct{} {
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

type incomingCommits struct {
	done chan struct{}
	inst int32
}

type incomingCommitMsgs struct {
	done        chan struct{}
	incomingMsg *stdpaxosproto.Commit
}

type incomingCommitShortMsgs struct {
	done        chan struct{}
	incomingMsg *stdpaxosproto.CommitShort
}

type incomingPromiseRequests struct {
	retMsg      chan Message
	incomingMsg *stdpaxosproto.Prepare
}

type incomingAcceptRequests struct {
	retMsg      chan Message
	incomingMsg *stdpaxosproto.Accept
}

type outgoingPromiseResponses struct {
	//	done       chan bool
	retMsgChan chan Message
	stdpaxosproto.Ballot
}

type outgoingAcceptResponses struct {
	//	done       chan bool
	retMsgChan chan Message
	stdpaxosproto.Ballot
}

type batchingAcceptor struct {
	meID                   int32
	maxInstance            int32
	instanceState          map[int32]*AcceptorBookkeeping
	awaitingPrepareReplies map[int32]map[int32]outgoingPromiseResponses
	awaitingAcceptReplies  map[int32]map[int32]outgoingAcceptResponses
	promiseRequests        chan incomingPromiseRequests
	acceptRequests         chan incomingAcceptRequests
	commits                chan incomingCommitMsgs
	commitShorts           chan incomingCommitShortMsgs

	catchupOnProceedingCommits bool

	acceptReplyRPC  uint8
	prepareReplyRPC uint8
	commitRPC       uint8
	commitShortRPC  uint8

	stableStore      stablestore.StableStore
	durable          bool
	emuatedWriteTime time.Duration
	emulatedSS       bool
	maxBatchWait     time.Duration
}

type betterBatching struct {
	ProposerIDs []int32
	batcher     *batchingAcceptor
}

func (a *betterBatching) RecvPrepareRemote(prepare *stdpaxosproto.Prepare) <-chan Message {
	c := make(chan Message, 1)
	go func() {
		a.batcher.promiseRequests <- incomingPromiseRequests{
			retMsg:      c,
			incomingMsg: prepare,
		}
	}()
	return c
}

func (a *betterBatching) RecvAcceptRemote(accept *stdpaxosproto.Accept) <-chan Message {
	c := make(chan Message, 1)
	go func() {
		a.batcher.acceptRequests <- incomingAcceptRequests{
			retMsg:      c,
			incomingMsg: accept,
		}
	}()
	return c
}

func (a *betterBatching) RecvCommitRemote(commit *stdpaxosproto.Commit) <-chan struct{} {
	c := make(chan struct{}, 1)
	go func() {
		a.batcher.commits <- incomingCommitMsgs{
			done:        c,
			incomingMsg: commit,
		}
	}()
	return c
}

func (a *betterBatching) RecvCommitShortRemote(commit *stdpaxosproto.CommitShort) <-chan struct{} {
	c := make(chan struct{}, 1)
	go func() {
		a.batcher.commitShorts <- incomingCommitShortMsgs{
			done:        c,
			incomingMsg: commit,
		}
	}()
	return c
}

func (a *batchingAcceptor) checkAndUpdateMaxInstance(inst int32) {
	if a.maxInstance < inst {
		a.maxInstance = inst
	}
}

func (b *batchingAcceptor) handleCommitShort(inc incomingCommitShortMsgs) {
	cmtMsg, resp := inc.incomingMsg, inc.done
	inst := cmtMsg.Instance
	b.checkAndUpdateMaxInstance(inst)

	instState := b.getInstState(inst)
	if instState.status == COMMITTED {
		close(resp)
		return
	}

	if instState.cmds == nil {
		panic("committing with no value!!!")
	}
	// store commit
	instState.status = COMMITTED
	instState.curBal = cmtMsg.Ballot
	instState.vBal = cmtMsg.Ballot

	instState.whoseCmds = cmtMsg.WhoseCmd

	go func() {
		resp <- struct{}{}
		close(resp)
	}()
}

func (a *batchingAcceptor) handleCommit(inc incomingCommitMsgs) {
	// store all data
	cmtMsg, resp := inc.incomingMsg, inc.done
	inst := cmtMsg.Instance
	a.checkAndUpdateMaxInstance(inst)
	instState := a.getInstState(inst)
	if instState.status == COMMITTED {
		go func() {
			close(resp)
		}()
		return
	}

	// store commit
	instState.status = COMMITTED
	instState.curBal = cmtMsg.Ballot
	instState.vBal = cmtMsg.Ballot
	instState.cmds = cmtMsg.Command
	instState.whoseCmds = cmtMsg.WhoseCmd
	go func() {
		resp <- struct{}{}
		close(resp)
	}()
}

func (a *betterBatching) batchPersister() {
	timeout := time.NewTimer(a.batcher.maxBatchWait)
	for {
		select {
		case inc := <-a.batcher.commitShorts:
			a.batcher.handleCommitShort(inc)
			break
		case inc := <-a.batcher.commits:
			a.batcher.handleCommit(inc)
		case inc := <-a.batcher.promiseRequests:
			a.batcher.recvPromiseRequest(inc)
			break
		case inc := <-a.batcher.acceptRequests:
			a.batcher.recvAcceptRequest(inc)
			break
		case <-timeout.C:
			// return responses for max and not max
			a.batcher.doBatch()
			timeout.Reset(a.batcher.maxBatchWait)
			break

		}

	}
}

func (a *batchingAcceptor) doBatch() {
	dlog.AgentPrintfN(a.meID, "Acceptor batch starting. Now persisting received ballots.")
	for instToResp, _ := range a.awaitingPrepareReplies {
		abk := a.instanceState[instToResp]
		recordInstanceMetadata(abk, a.stableStore, a.durable)
		//if abk.cmds != nil {
		//	recordCommands(abk.cmds, a.stableStore, a.durable)
		//}
	}
	for instToResp, _ := range a.awaitingAcceptReplies {
		abk := a.instanceState[instToResp]
		recordInstanceMetadata(abk, a.stableStore, a.durable)
		if abk.cmds != nil {
			recordCommands(abk.cmds, a.stableStore, a.durable)
		}
	}

	fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
	dlog.AgentPrintfN(a.meID, "Acceptor batch performed. Now returning responses for %d instances.", len(a.awaitingPrepareReplies)+len(a.awaitingAcceptReplies))

	a.returnPrepareAndAcceptRepliesAndClear()
	dlog.AgentPrintfN(a.meID, "Acceptor done returning responses")
}

func (a *batchingAcceptor) returnPrepareAndAcceptRepliesAndClear() {
	a.returnPrepareRepliesAndClear()
	a.returnAcceptRepliesAndClear()
}

func (a *batchingAcceptor) returnAcceptRepliesAndClear() {
	for instToResp, awaitingResps := range a.awaitingAcceptReplies {
		abk := a.instanceState[instToResp]
		if abk.status == COMMITTED {
			dlog.AgentPrintfN(a.meID, "Acceptor %d returning no responses for instance %d as it has been committed in this batch", a.meID, instToResp)
			for _, prepResp := range awaitingResps {
				close(prepResp.retMsgChan)
			}
			continue
		}

		returnAcceptRepliesAwaiting(instToResp, a.meID, awaitingResps, abk, a.acceptReplyRPC)
	}

	a.awaitingAcceptReplies = make(map[int32]map[int32]outgoingAcceptResponses, 100)
}

func (a *batchingAcceptor) returnPrepareRepliesAndClear() {
	for instToResp, awaitingResps := range a.awaitingPrepareReplies {
		abk := a.instanceState[instToResp]
		if abk.status == COMMITTED {
			dlog.AgentPrintfN(a.meID, "Acceptor %d returning no responses for instance %d as it has been committed in this batch", a.meID, instToResp)
			for _, prepResp := range awaitingResps {
				close(prepResp.retMsgChan)
			}
			continue
		}
		returnPrepareRepliesAwaiting(instToResp, a.meID, awaitingResps, abk, a.prepareReplyRPC)
	}
	a.awaitingPrepareReplies = make(map[int32]map[int32]outgoingPromiseResponses, 100)
}

func (a *batchingAcceptor) getAwaitingPrepareReplies(inst int32) map[int32]outgoingPromiseResponses {
	_, awaitingRespsExists := a.awaitingPrepareReplies[inst]
	if !awaitingRespsExists {
		a.awaitingPrepareReplies[inst] = make(map[int32]outgoingPromiseResponses)
	}
	return a.awaitingPrepareReplies[inst]
}

func (a *batchingAcceptor) getAwaitingAcceptReplies(inst int32) map[int32]outgoingAcceptResponses {
	_, awaitingRespsExists := a.awaitingAcceptReplies[inst]
	if !awaitingRespsExists {
		a.awaitingAcceptReplies[inst] = make(map[int32]outgoingAcceptResponses)
	}
	return a.awaitingAcceptReplies[inst]
}

func (a *batchingAcceptor) recvAcceptRequest(inc incomingAcceptRequests) {
	dlog.AgentPrintfN(a.meID, "Acceptor received accept request for instance %d from %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.PropID, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
	accMsg, respHand, requestor := inc.incomingMsg, inc.retMsg, int32(inc.incomingMsg.PropID)
	inst := accMsg.Instance

	a.checkAndUpdateMaxInstance(inst)
	instState := a.getInstState(inst)

	if instState.status == COMMITTED {
		a.handleMsgAlreadyCommitted(requestor, inst, respHand)
		return
	}

	instAwaitingAcceptReplies := a.getAwaitingAcceptReplies(inst)
	instAwaitingPrepareReplies := a.getAwaitingPrepareReplies(inst)
	if instState.curBal.GreaterThan(accMsg.Ballot) {
		dlog.AgentPrintfN(a.meID, "Acceptor intending to preempt ballot in instance %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
		//check if most recent ballot from proposer to return preempt to
		prevAcc, wasPrevAcc := instAwaitingAcceptReplies[requestor]
		if wasPrevAcc {
			if prevAcc.GreaterThan(accMsg.Ballot) {
				close(respHand)
				return
			}
		}
		prevPrep, wasPrevPrep := instAwaitingPrepareReplies[requestor]
		if wasPrevPrep {
			if prevPrep.GreaterThan(accMsg.Ballot) {
				close(respHand)
				return
			}
		}
		closeAllPreviousResponseToProposer(instAwaitingPrepareReplies, instAwaitingAcceptReplies, requestor)
		instAwaitingAcceptReplies[requestor] = outgoingAcceptResponses{
			retMsgChan: respHand,
			Ballot:     accMsg.Ballot,
		}
		return
	}
	dlog.AgentPrintfN(a.meID, "Acceptor intending to accept ballot in instance %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)

	instState.status = ACCEPTED
	instState.curBal = accMsg.Ballot
	instState.vBal = accMsg.Ballot
	instState.cmds = accMsg.Command
	instState.whoseCmds = accMsg.WhoseCmd

	// close any previous awaiting responses and store this current one
	closeAllPreviousResponseToProposer(instAwaitingPrepareReplies, instAwaitingAcceptReplies, requestor)
	instAwaitingAcceptReplies[requestor] = outgoingAcceptResponses{
		retMsgChan: respHand,
		Ballot:     accMsg.Ballot,
	}
}

func (a *batchingAcceptor) recvPromiseRequest(inc incomingPromiseRequests) {
	dlog.AgentPrintfN(a.meID, "Acceptor received promise request for instance %d from %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.PropID, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
	prepMsg, respHand, requestor := inc.incomingMsg, inc.retMsg, int32(inc.incomingMsg.PropID)

	inst := prepMsg.Instance
	a.checkAndUpdateMaxInstance(inst)
	instState := a.getInstState(inst)

	if instState.status == COMMITTED {
		a.handleMsgAlreadyCommitted(requestor, inst, respHand)
		return
	}

	instAwaitingAcceptReplies := a.getAwaitingAcceptReplies(inst)
	instAwaitingPrepareReplies := a.getAwaitingPrepareReplies(inst)
	if instState.curBal.GreaterThan(prepMsg.Ballot) || (instState.curBal.Equal(prepMsg.Ballot) && instState.status == ACCEPTED) {
		dlog.AgentPrintfN(a.meID, "Acceptor intending to preempt ballot in instance %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)
		// check if most recent ballot from proposer to return preempt to
		prevAcc, wasPrevAcc := instAwaitingAcceptReplies[requestor]
		if wasPrevAcc {
			if prevAcc.GreaterThan(prepMsg.Ballot) || prevAcc.Equal(prepMsg.Ballot) {
				close(respHand)
				return
			}
		}

		prevPrep, wasPrevPrep := instAwaitingPrepareReplies[requestor]
		if wasPrevPrep {
			if prevPrep.GreaterThan(prepMsg.Ballot) {
				close(respHand)
				return
			}
		}
		closeAllPreviousResponseToProposer(instAwaitingPrepareReplies, instAwaitingAcceptReplies, requestor)
		instAwaitingPrepareReplies[requestor] = outgoingPromiseResponses{
			retMsgChan: respHand,
			Ballot:     prepMsg.Ballot,
		}
		return
	}
	dlog.AgentPrintfN(a.meID, "Acceptor intending to promise ballot in instance %d at ballot %d.%d", inc.incomingMsg.Instance, inc.incomingMsg.Ballot.Number, inc.incomingMsg.Ballot.PropID)

	instState.status = PREPARED
	instState.curBal = prepMsg.Ballot
	//recordInstanceMetadata(instState, a.stableStore, a.durable)

	// close any previous awaiting responses and store this current one
	closeAllPreviousResponseToProposer(instAwaitingPrepareReplies, instAwaitingAcceptReplies, requestor)
	instAwaitingPrepareReplies[requestor] = outgoingPromiseResponses{
		retMsgChan: respHand,
		Ballot:     prepMsg.Ballot,
	}
}

func (a *batchingAcceptor) handleMsgAlreadyCommitted(requestor int32, inst int32, respHand chan Message) {
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
				UDPaxos: &stdpaxosproto.Commit{
					LeaderId:   int32(iState.vBal.PropID),
					Instance:   instI,
					Ballot:     iState.vBal,
					WhoseCmd:   iState.whoseCmds,
					MoreToCome: 0,
					Command:    iState.cmds},
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(respHand)
	}()
}

func (a *batchingAcceptor) getInstState(inst int32) *AcceptorBookkeeping {
	_, exists := a.instanceState[inst]
	if !exists {
		a.instanceState[inst] = &AcceptorBookkeeping{
			status:    0,
			cmds:      nil,
			curBal:    stdpaxosproto.Ballot{-1, -1},
			vBal:      stdpaxosproto.Ballot{-1, -1},
			whoseCmds: -1,
			//Mutex:     sync.Mutex{},
		}
	}
	return a.instanceState[inst]
}

//func (a *batchingAcceptor) getAwaitingResponses(inst int32) *responsesAwaiting {
//	_, awaitingRespsExists := a.awaitingResponses[inst]
//	if !awaitingRespsExists {
//		a.awaitingResponses[inst] = &responsesAwaiting{
//			propsPrepResps:   make(map[int32]*outgoingPromiseResponses),
//			propsAcceptResps: make(map[int32]*outgoingAcceptResponses),
//		}
//	}
//	return a.awaitingResponses[inst]
//}

func closeAllPreviousResponseToProposer(awaitingPrepareReplies map[int32]outgoingPromiseResponses, awaitingAcceptReplies map[int32]outgoingAcceptResponses, proposer int32) {
	prop, existsp := awaitingPrepareReplies[proposer]
	//go func() {
	if existsp {
		close(prop.retMsgChan)
		delete(awaitingPrepareReplies, proposer)
	}
	acc, existsa := awaitingAcceptReplies[proposer]
	if existsa {
		close(acc.retMsgChan)
		delete(awaitingAcceptReplies, proposer)
	}
	//}()
}

func (a *betterBatching) getPhase(inst int32) stdpaxosproto.Phase {
	var phase stdpaxosproto.Phase
	if a.batcher.instanceState[inst].status == ACCEPTED {
		phase = stdpaxosproto.ACCEPTANCE
	} else if a.batcher.instanceState[inst].status == PREPARED {
		phase = stdpaxosproto.PROMISE
	} else {
		panic("should not return phase when neither promised nor accepted")
	}
	return phase
}

func (abk *AcceptorBookkeeping) getPhase() stdpaxosproto.Phase {
	var phase stdpaxosproto.Phase
	if abk.status == ACCEPTED {
		phase = stdpaxosproto.ACCEPTANCE
	} else if abk.status == PREPARED {
		phase = stdpaxosproto.PROMISE
	} else {
		panic("should not return phase when neither promised nor accepted")
	}
	return phase
}

//func returnPrepareAndAcceptResponses(inst int32, myId int32, awaitingPromiseResps map[int32]outgoingPromiseResponses, awaitingAcceptResps map[int32]outgoingAcceptResponses, abk *AcceptorBookkeeping, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8) {
//	dlog.AgentPrintfN(myId, "Acceptor %d returning batch of %d responses for instance %d", myId, len(awaitingPromiseResps)+len(awaitingAcceptResps), inst)
//	if abk.status == COMMITTED {
//		dlog.AgentPrintfN(myId, "Acceptor %d returning no responses for instance %d as it has been committed in this batch", myId, inst)
//		for _, prepResp := range awaitingPromiseResps {
//			close(prepResp.retMsgChan)
//
//		}
//		for _, accResp := range awaitingAcceptResps {
//			close(accResp.retMsgChan)
//		}
//		return
//	}
//
//	returnPrepareRepliesAwaiting(inst, myId, awaitingPromiseResps, abk, prepareReplyRPC)
//
//	returnAcceptRepliesAwaiting(inst, myId, awaitingAcceptResps, abk, acceptReplyRPC)
//}

func returnAcceptRepliesAwaiting(inst int32, myId int32, awaitingResps map[int32]outgoingAcceptResponses, abk *AcceptorBookkeeping, acceptReplyRPC uint8) {
	outOfOrderAccept := false
	for pid, accResp := range awaitingResps {

		if abk.curBal.GreaterThan(accResp.Ballot) {
			outOfOrderAccept = true
		}

		// if we accepted anything here but cur bal has changed, it is safe to respond positively to accept request
		//   -- we are telling proposers of the greater ballot of this acceptance
		curPhase := abk.getPhase()
		curBal := abk.curBal
		if abk.vBal.Equal(accResp.Ballot) {
			curPhase = stdpaxosproto.ACCEPTANCE
			curBal = abk.vBal
		}
		acc := &stdpaxosproto.AcceptReply{
			Instance:   inst,
			AcceptorId: myId,
			CurPhase:   curPhase,
			Cur:        curBal,
			Req:        accResp.Ballot,
			WhoseCmd:   abk.whoseCmds,
		}

		preempt := !acc.Cur.Equal(acc.Req)
		respTypeS := "acceptance"
		if preempt {
			respTypeS = "preempt"
		}
		dlog.AgentPrintfN(myId, "Acceptor returning Accept Reply (%s) to Replica %d for instance %d at current ballot %d.%d when requested %d.%d", respTypeS,
			pid, inst, abk.curBal.Number, abk.curBal.PropID, accResp.Number, accResp.PropID)

		resp, lpid := accResp, pid
		go func() {
			resp.retMsgChan <- protoMessage{
				towhom:     giveToWhom(lpid),
				gettype:    func() uint8 { return acceptReplyRPC },
				isnegative: func() bool { return preempt },
				UDPaxos:    acc,
			}
			close(resp.retMsgChan)
		}()
	}

	if outOfOrderAccept {
		dlog.AgentPrintfN(myId, "Acceptor %d has acknowledged ballots out of order in instance %d", myId, inst)
	}
}

func returnPrepareRepliesAwaiting(inst int32, myId int32, awaitingResps map[int32]outgoingPromiseResponses, abk *AcceptorBookkeeping, prepareReplyRPC uint8) {
	for pid, prepResp := range awaitingResps {
		prep := &stdpaxosproto.PrepareReply{
			Instance:   inst,
			Cur:        abk.curBal,
			CurPhase:   abk.getPhase(),
			Req:        prepResp.Ballot,
			VBal:       abk.vBal,
			AcceptorId: myId,
			WhoseCmd:   abk.whoseCmds,
			Command:    abk.cmds,
		}
		preempt := !prep.Cur.Equal(prep.Req)
		typeRespS := "promise"
		if preempt {
			typeRespS = "preempt"
		}
		dlog.AgentPrintfN(myId, "Acceptor returning Prepare Reply (%s) to Replica %d for instance %d at current ballot %d.%d when requested %d.%d", typeRespS, pid,
			inst, abk.curBal.Number, abk.curBal.PropID, prepResp.Number, prepResp.PropID)

		resp, lpid := prepResp, pid
		go func() {
			resp.retMsgChan <- protoMessage{
				towhom:     giveToWhom(lpid),
				gettype:    func() uint8 { return prepareReplyRPC },
				isnegative: func() bool { return preempt },
				UDPaxos:    prep,
			}
			close(resp.retMsgChan)
		}()
	}
}

//func returnCommitsToAwaitingInstanceResponses(awaitingResps *responsesAwaiting, cmtMsg *stdpaxosproto.Commit, commitRPC uint8, meId int32) {
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
