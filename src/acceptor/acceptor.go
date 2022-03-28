package acceptor

import (
	"dlog"
	"encoding/binary"
	"fastrpc"
	"stablestore"
	"state"
	"stdpaxosproto"
	"sync"

	//sync2 "fsync"
	"time"
)

type Response interface {
	ToWhom() int32
	GetType() uint8
	IsNegative() bool
	GetSerialisable() fastrpc.Serializable
	fastrpc.Serializable
}

type protoResponse struct {
	towhom     func() int32
	gettype    func() uint8
	isnegative func() bool
	//	getSerialisable func() fastrpc.Serializable
	fastrpc.Serializable
}

func (p protoResponse) GetSerialisable() fastrpc.Serializable {
	return p.Serializable
}

func (p protoResponse) ToWhom() int32 {
	return p.towhom()
}

func (p protoResponse) GetType() uint8 {
	return p.gettype()
}

func (p protoResponse) IsNegative() bool {
	return p.isnegative()
}

type Acceptor interface {
	//	RecvPrepareLocal(prepare *stdpaxosproto.Prepare) <-chan bool // bool
	RecvPrepareRemote(prepare *stdpaxosproto.Prepare) <-chan Response
	//	RecvAcceptLocal(accept *stdpaxosproto.Accept) <-chan bool
	RecvAcceptRemote(accept *stdpaxosproto.Accept) <-chan Response
	//	RecvCommitLocal(commit *stdpaxosproto.Commit) <-chan struct{}           // bool
	RecvCommitRemote(commit *stdpaxosproto.Commit) <-chan struct{} // bool
	//	RecvCommitShortLocal(short *stdpaxosproto.CommitShort) <-chan struct{}  // bool
	RecvCommitShortRemote(short *stdpaxosproto.CommitShort) <-chan struct{} // bool
}

func StandardAcceptorNew(file stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration, id int32, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8, commitShortRPC uint8) *standard {
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

func BatchingAcceptorNew(file stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration, id int32, maxBatchWait time.Duration, proposerIDs []int32, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8, commitShortRPC uint8) *batching {
	bat := &batching{
		instanceState:    make(map[int32]*AcceptorBookkeeping),
		stableStore:      file,
		durable:          durable,
		emuatedWriteTime: emulatedWriteTime,
		emulatedSS:       emulatedSS,
		maxBatchWait:     maxBatchWait,
		batcher: struct {
			//localPromiseRequests chan incomingLocalPromiseRequests
			promiseRequests chan incomingPromiseRequests
			//localAcceptRequests  chan incomingLocalAcceptRequests
			acceptRequests chan incomingAcceptRequests
			commits        chan incomingCommits
		}{
			//localPromiseRequests: make(chan incomingLocalPromiseRequests),
			promiseRequests: make(chan incomingPromiseRequests),
			//localAcceptRequests:  make(chan incomingLocalAcceptRequests),
			acceptRequests: make(chan incomingAcceptRequests),
			commits:        make(chan incomingCommits),
		},
		ProposerIDs:     proposerIDs,
		meID:            id,
		prepareReplyRPC: prepareReplyRPC,
		acceptReplyRPC:  acceptReplyRPC,
		commitRPC:       commitRPC,
		commitShortRPC:  commitShortRPC,
	}
	go bat.batchPersister()
	return bat
}

func BetterBatchingAcceptorNew(file stablestore.StableStore, durable bool, emulatedSS bool, emulatedWriteTime time.Duration, id int32, maxBatchWait time.Duration, proposerIDs []int32, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8, commitShortRPC uint8) *betterBatching {
	bat := &betterBatching{
		stableStore:      file,
		durable:          durable,
		emuatedWriteTime: emulatedWriteTime,
		emulatedSS:       emulatedSS,
		maxBatchWait:     maxBatchWait,
		batcher: &betterBatcher{
			awaitingResponses: make(map[int32]*responsesAwaiting, 100),
			instanceState:     make(map[int32]*AcceptorBookkeeping, 100),
			promiseRequests:   make(chan incomingPromiseRequests, 1000),
			acceptRequests:    make(chan incomingAcceptRequests, 1000),
			commits:           make(chan incomingCommitMsgs, 1000),
			commitShorts:      make(chan incomingCommitShortMsgs, 1000),
		},
		ProposerIDs:     proposerIDs,
		meID:            id,
		prepareReplyRPC: prepareReplyRPC,
		acceptReplyRPC:  acceptReplyRPC,
		commitRPC:       commitRPC,
		commitShortRPC:  commitShortRPC,
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
	sync.Mutex
}

//append a log entry to stable storage
func recordInstanceMetadata(abk *AcceptorBookkeeping, stableStore stablestore.StableStore, durable bool) {
	if !durable {
		return
	}

	var b [8]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(abk.curBal.Number))
	binary.LittleEndian.PutUint32(b[4:8], uint32(abk.curBal.PropID))
	_, _ = stableStore.Write(b[:])
}

//write a sequence of commands to stable storage
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

//fsync with the stable store
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
			status: NOT_STARTED,
			cmds:   nil,
			curBal: stdpaxosproto.Ballot{-1, -1},
			vBal:   stdpaxosproto.Ballot{-1, -1},
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
	instanceState    map[int32]*AcceptorBookkeeping
	stableStore      stablestore.StableStore
	durable          bool
	emuatedWriteTime time.Duration
	emulatedSS       bool
	meID             int32
	acceptReplyRPC   uint8
	prepareReplyRPC  uint8
	commitRPC        uint8
	commitShortRPC   uint8
}

func giveToWhom(toWhom int32) func() int32 {
	return func() int32 {
		return toWhom
	}
}

func (a *standard) RecvPrepareRemote(prepare *stdpaxosproto.Prepare) <-chan Response {
	inst, bal := prepare.Instance, prepare.Ballot
	cMsg := make(chan Response, 1)
	checkAndCreateInstanceState(&a.instanceState, inst)
	abk := a.instanceState[inst]

	committed, cmtMsg := checkAndHandleCommitted(inst, abk)

	var msg fastrpc.Serializable
	var funcNeg func() bool
	var toWhom int32
	var funcRet func() uint8
	if committed {
		msg = cmtMsg
		toWhom = int32(prepare.PropID)
		funcRet = func() uint8 { return a.commitRPC }
		funcNeg = func() bool { return true }
	} else {
		if bal.GreaterThan(abk.curBal) || bal.Equal(abk.curBal) {
			abk.status = PREPARED
			abk.curBal = bal
			recordInstanceMetadata(abk, a.stableStore, a.durable)
			fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)

		}
		msg = &stdpaxosproto.PrepareReply{
			Instance:   inst,
			Req:        prepare.Ballot,
			Cur:        a.instanceState[inst].curBal,
			CurPhase:   a.getPhase(inst),
			VBal:       a.instanceState[inst].vBal,
			AcceptorId: a.meID,
			WhoseCmd:   a.instanceState[inst].whoseCmds,
			Command:    a.instanceState[inst].cmds,
		}
		toWhom = int32(prepare.PropID)
		funcRet = func() uint8 { return a.prepareReplyRPC }
		funcNeg = func() bool { return a.instanceState[inst].curBal.GreaterThan(prepare.Ballot) }
	}
	toRet := &protoResponse{giveToWhom(toWhom), funcRet, funcNeg, msg}
	go func() {
		cMsg <- toRet
		close(cMsg)
	}()
	return cMsg
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

func (a *standard) RecvAcceptRemote(accept *stdpaxosproto.Accept) <-chan Response {
	inst, bal := accept.Instance, accept.Ballot
	cMsg := make(chan Response, 1)
	checkAndCreateInstanceState(&a.instanceState, inst)
	abk := a.instanceState[inst]

	committed, cmtMsg := checkAndHandleCommitted(inst, abk)

	var msg fastrpc.Serializable
	//	var toWhom int32
	var funcRet func() uint8
	if committed {
		msg = cmtMsg
		funcRet = func() uint8 { return a.commitRPC }
	} else {
		if bal.GreaterThan(abk.curBal) || bal.Equal(abk.curBal) {
			nonDurableAccept(accept, abk, a.stableStore, a.durable)
			fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
		}
		phase := stdpaxosproto.PROMISE
		if abk.status == ACCEPTED {
			phase = stdpaxosproto.ACCEPTANCE
		}
		msg = &stdpaxosproto.AcceptReply{
			Instance:   inst,
			AcceptorId: a.meID,
			WhoseCmd:   a.instanceState[inst].whoseCmds,
			Req:        accept.Ballot,
			Cur:        abk.curBal,
			CurPhase:   phase,
		}
		funcRet = func() uint8 { return a.acceptReplyRPC }
	}
	go func() {
		cMsg <- protoResponse{
			towhom:       giveToWhom(int32(accept.PropID)),
			gettype:      funcRet,
			Serializable: msg,
		}
		close(cMsg)
	}()
	return cMsg
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
			fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
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
	retMsg      chan Response
	incomingMsg *stdpaxosproto.Prepare
}

type incomingAcceptRequests struct {
	retMsg      chan Response
	incomingMsg *stdpaxosproto.Accept
}

type batching struct {
	instanceState    map[int32]*AcceptorBookkeeping
	stableStore      stablestore.StableStore
	durable          bool
	emuatedWriteTime time.Duration
	emulatedSS       bool
	maxBatchWait     time.Duration
	batcher          struct {
		promiseRequests chan incomingPromiseRequests
		acceptRequests  chan incomingAcceptRequests
		commits         chan incomingCommits
	}
	ProposerIDs []int32
	meID        int32

	acceptReplyRPC  uint8
	prepareReplyRPC uint8
	commitRPC       uint8
	commitShortRPC  uint8
}

func (a *batching) RecvPrepareRemote(prepare *stdpaxosproto.Prepare) <-chan Response {
	inst, bal := prepare.Instance, prepare.Ballot
	c := make(chan Response)
	checkAndCreateInstanceState(&a.instanceState, inst)
	abk := a.instanceState[inst]

	committed, cmtMsg := checkAndHandleCommitted(inst, abk)
	if committed {
		go func() {
			c <- protoResponse{
				giveToWhom(int32(prepare.PropID)),
				func() uint8 { return a.commitRPC },
				func() bool { return true },
				cmtMsg,
			}
			close(c)
		}()
	} else {
		if bal.GreaterThan(abk.curBal) || bal.Equal(abk.curBal) {
			abk.status = PREPARED
			abk.curBal = bal
			recordInstanceMetadata(abk, a.stableStore, a.durable)
		}
		go func() {
			a.batcher.promiseRequests <- incomingPromiseRequests{
				retMsg:      c,
				incomingMsg: prepare,
			}
		}()
	}
	return c
}

func (a *batching) RecvAcceptRemote(accept *stdpaxosproto.Accept) <-chan Response {
	inst, bal := accept.Instance, accept.Ballot
	c := make(chan Response)
	checkAndCreateInstanceState(&a.instanceState, inst)
	abk := a.instanceState[inst]

	committed, cmtMsg := checkAndHandleCommitted(inst, abk)

	if committed {
		go func() {
			c <- protoResponse{
				towhom:       giveToWhom(int32(accept.PropID)),
				gettype:      func() uint8 { return a.commitRPC },
				Serializable: cmtMsg,
			}
			close(c)
		}()
	} else {
		if bal.GreaterThan(abk.curBal) || bal.Equal(abk.curBal) {
			nonDurableAccept(accept, abk, a.stableStore, a.durable)
		}
		go func() {
			a.batcher.acceptRequests <- incomingAcceptRequests{
				retMsg:      c,
				incomingMsg: accept,
			}
		}()
	}
	return c
}

func (a *batching) handleCommit(inst int32, ballot stdpaxosproto.Ballot, cmds []*state.Command, whoseCmds int32) {
	if a.instanceState[inst].status != COMMITTED {
		if a.instanceState[inst].vBal.GreaterThan(ballot) { //ballot.GreaterThan(a.instanceState[inst].vBal) {
			a.instanceState[inst].status = COMMITTED
			a.instanceState[inst].curBal = ballot
			a.instanceState[inst].vBal = ballot
			a.instanceState[inst].cmds = cmds
			a.instanceState[inst].whoseCmds = whoseCmds
			recordInstanceMetadata(a.instanceState[inst], a.stableStore, a.durable)
			recordCommands(a.instanceState[inst].cmds, a.stableStore, a.durable)
		} else {
			// if a ballot was chosen, then all proceeding ballots propose the same value
			a.instanceState[inst].status = COMMITTED
			a.instanceState[inst].curBal = ballot
			a.instanceState[inst].vBal = ballot
			recordInstanceMetadata(a.instanceState[inst], a.stableStore, a.durable)
		}
	}
}

func (a *batching) RecvCommitRemote(commit *stdpaxosproto.Commit) <-chan struct{} {
	c := make(chan struct{})
	checkAndCreateInstanceState(&a.instanceState, commit.Instance)
	a.instanceState[commit.Instance].Lock()
	a.handleCommit(commit.Instance, commit.Ballot, commit.Command, commit.WhoseCmd)
	a.instanceState[commit.Instance].Unlock()
	go func() {
		a.batcher.commits <- incomingCommits{
			done: c,
			inst: commit.Instance,
		}
	}()
	return c
}

func (a *batching) RecvCommitShortRemote(commit *stdpaxosproto.CommitShort) <-chan struct{} {
	checkAndCreateInstanceState(&a.instanceState, commit.Instance)
	a.instanceState[commit.Instance].Lock()
	defer a.instanceState[commit.Instance].Unlock()
	safe := a.instanceState[commit.Instance].cmds != nil || commit.GreaterThan(a.instanceState[commit.Instance].vBal)
	if safe {
		c := make(chan struct{})
		a.handleCommit(commit.Instance, commit.Ballot, a.instanceState[commit.Instance].cmds, commit.WhoseCmd)
		go func() {
			a.batcher.commits <- incomingCommits{
				done: c,
				inst: commit.Instance,
			}
		}()
		return c
	} else {
		panic("Got a short commits msg but we don't have any record of the value")
	}
}

type outgoingPromiseResponses struct {
	//	done       chan bool
	retMsgChan chan Response
	stdpaxosproto.Ballot
}

type outgoingAcceptResponses struct {
	//	done       chan bool
	retMsgChan chan Response
	stdpaxosproto.Ballot
}

type responsesAwaiting struct {
	propsPrepResps   map[int32]*outgoingPromiseResponses
	propsAcceptResps map[int32]*outgoingAcceptResponses
}

func (a *batching) returnPrepareAndAcceptResponses(store map[int32]*responsesAwaiting) {
	//fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)
	//
	//for inst, instStore := range store {
	//	a.instanceState[inst].Lock()
	//	for _, pid := range a.ProposerIDs {
	//		if prep, pexists := instStore.propsPrepResps[pid]; pexists {
	//			go func(toWhom int32, prepResp chan<- Response, msg *stdpaxosproto.PrepareReply) {
	//				prepResp <- protoResponse{
	//					giveToWhom(toWhom),
	//					func() uint8 { return a.prepareReplyRPC },
	//
	//					msg,
	//				}
	//				close(prepResp)
	//			}(pid, prep.retMsgChan, a.getPrepareReply(inst, prep.Ballot))
	//			delete(instStore.propsPrepResps, pid)
	//		} else if acc, aexists := instStore.propsAcceptResps[pid]; aexists {
	//			go func(toWhom int32, accResp chan<- Response, msg *stdpaxosproto.AcceptReply) {
	//				accResp <- protoResponse{
	//					towhom:       giveToWhom(toWhom),
	//					gettype:      func() uint8 { return a.acceptReplyRPC },
	//					Serializable: msg,
	//				}
	//				close(accResp)
	//			}(pid, acc.retMsgChan, a.getAcceptReply(inst, acc))
	//			delete(instStore.propsAcceptResps, pid)
	//		}
	//	}
	//	a.instanceState[inst].Unlock()
	//}
}

func (a *batching) getPhase(inst int32) stdpaxosproto.Phase {
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

func (a *batching) getAcceptReply(inst int32, acc *outgoingAcceptResponses) *stdpaxosproto.AcceptReply {

	return &stdpaxosproto.AcceptReply{
		Instance:   inst,
		AcceptorId: a.meID,
		Cur:        a.instanceState[inst].curBal,
		CurPhase:   a.getPhase(inst),
		Req:        acc.Ballot,
		WhoseCmd:   a.instanceState[inst].whoseCmds,
	}
}

func (a *batching) getPrepareReply(inst int32, req stdpaxosproto.Ballot) *stdpaxosproto.PrepareReply {
	return &stdpaxosproto.PrepareReply{
		Instance:   inst,
		Cur:        a.instanceState[inst].curBal,
		Req:        req,
		CurPhase:   a.getPhase(inst),
		VBal:       a.instanceState[inst].vBal,
		AcceptorId: a.meID,
		WhoseCmd:   a.instanceState[inst].whoseCmds,
		Command:    a.instanceState[inst].cmds,
	}
}

func (a *batching) clearCurProposals(curProposals map[int32]*responsesAwaiting) {
	curProposals = make(map[int32]*responsesAwaiting)
}

func (a *batching) respondAndClearCurProposals(curProposals map[int32]*responsesAwaiting) {
	a.returnPrepareAndAcceptResponses(curProposals)
	a.clearCurProposals(curProposals)
}

func (a *batching) batchPersister() {
	curAwaiting := make(map[int32]*responsesAwaiting)

	timeout := time.NewTimer(a.maxBatchWait)
	for {
		select {
		case inc := <-a.batcher.commits:
			a.batcherRecvCommit(inc, curAwaiting)
			break
		case inc := <-a.batcher.promiseRequests:
			a.batcherUpdateProposerResponsePromise(inc, curAwaiting)
			break
		case inc := <-a.batcher.acceptRequests:
			// if new max replace old max and return false
			a.batcherUpdateProposerReponseAccept(inc, curAwaiting)
			break
		case <-timeout.C:
			// return responses for max and not max
			a.respondAndClearCurProposals(curAwaiting)
			timeout.Reset(a.maxBatchWait)
			break
		}
	}
}

func (a *batching) checkAndInitaliseCurProposals(curProposals map[int32]*AcceptorBookkeeping, inst int32) {
	if _, exists := curProposals[inst]; !exists {
		curProposals[inst] = &AcceptorBookkeeping{
			status:    NOT_STARTED,
			cmds:      nil,
			curBal:    stdpaxosproto.Ballot{-1, -1},
			vBal:      stdpaxosproto.Ballot{-1, -1},
			whoseCmds: -1,
		}
	}
}

func (a *batching) checkAndInitaliseCurProposalsForInst(curProposals map[int32]*responsesAwaiting, inst int32) {
	if _, exists := curProposals[inst]; !exists {
		curProposals[inst] = &responsesAwaiting{
			propsPrepResps:   make(map[int32]*outgoingPromiseResponses),
			propsAcceptResps: make(map[int32]*outgoingAcceptResponses),
		}
	}
}

func (a *batching) batcherUpdateProposerReponseAccept(inc incomingAcceptRequests, curProposals map[int32]*responsesAwaiting) {
	acc, msgChan := inc.incomingMsg, inc.retMsg
	a.checkAndInitaliseCurProposalsForInst(curProposals, acc.Instance)

	instProposals := curProposals[acc.Instance]
	if propAccept, exists := instProposals.propsAcceptResps[int32(acc.PropID)]; exists {
		if propAccept.Ballot.GreaterThan(acc.Ballot) {
			close(msgChan)
			return
		}
		close(propAccept.retMsgChan)
	}

	if propPrepare, exists := instProposals.propsPrepResps[int32(acc.PropID)]; exists {
		if propPrepare.GreaterThan(acc.Ballot) {
			close(msgChan)
			return
		}
		close(propPrepare.retMsgChan)
		delete(instProposals.propsPrepResps, int32(acc.PropID))
	}

	instProposals.propsAcceptResps[int32(acc.PropID)] = &outgoingAcceptResponses{
		retMsgChan: msgChan,
		Ballot:     acc.Ballot,
	}
}

func (a *batching) batcherUpdateProposerResponsePromise(inc incomingPromiseRequests, curProposals map[int32]*responsesAwaiting) {
	prep, msgChan := inc.incomingMsg, inc.retMsg
	a.checkAndInitaliseCurProposalsForInst(curProposals, prep.Instance)

	instProposals := curProposals[prep.Instance]
	// if there is a previous accept waiting, close it
	if propAccept, exists := instProposals.propsAcceptResps[int32(prep.PropID)]; exists {
		// if the current accept is newer close newly received one
		if propAccept.Ballot.GreaterThan(prep.Ballot) || propAccept.Ballot.Equal(prep.Ballot) {
			// accept take precedent if equal
			close(msgChan)
			return
		}
		close(propAccept.retMsgChan)
		delete(instProposals.propsAcceptResps, int32(prep.PropID))
	}

	if propPrepare, exists := instProposals.propsPrepResps[int32(prep.PropID)]; exists {
		// if the current prepare is newer close newly received one
		if propPrepare.Ballot.GreaterThan(prep.Ballot) {
			close(msgChan)
			return
		}
		// if there is a previous prepare waiting, close it
		close(propPrepare.retMsgChan)
		// don't need to delete as overwriting now
	}

	instProposals.propsPrepResps[int32(prep.PropID)] = &outgoingPromiseResponses{
		retMsgChan: msgChan,
		Ballot:     prep.Ballot,
	}
}

func (a *batching) batcherRecvCommit(inc incomingCommits, curProposals map[int32]*responsesAwaiting) {
	// must fsync here as previous proposals might not have been persisted yet
	a.checkAndInitaliseCurProposalsForInst(curProposals, inc.inst)
	for _, v := range curProposals[inc.inst].propsPrepResps {
		if v == nil {
			continue
		}
		close(v.retMsgChan)
	}
	for _, v := range curProposals[inc.inst].propsAcceptResps {
		if v == nil {
			continue
		}
		close(v.retMsgChan)
	}
	delete(curProposals, inc.inst)
	a.returnPrepareAndAcceptResponses(curProposals)
	go func() {
		inc.done <- struct{}{}
		close(inc.done)
	}()
}

type betterBatcher struct {
	awaitingResponses map[int32]*responsesAwaiting
	instanceState     map[int32]*AcceptorBookkeeping
	promiseRequests   chan incomingPromiseRequests
	acceptRequests    chan incomingAcceptRequests
	commits           chan incomingCommitMsgs
	commitShorts      chan incomingCommitShortMsgs
}

type betterBatching struct {
	stableStore      stablestore.StableStore
	durable          bool
	emuatedWriteTime time.Duration
	emulatedSS       bool
	maxBatchWait     time.Duration

	ProposerIDs     []int32
	meID            int32
	batcher         *betterBatcher
	acceptReplyRPC  uint8
	prepareReplyRPC uint8
	commitRPC       uint8
	commitShortRPC  uint8
}

func (a *betterBatching) RecvPrepareRemote(prepare *stdpaxosproto.Prepare) <-chan Response {
	c := make(chan Response, 1)
	go func() {
		a.batcher.promiseRequests <- incomingPromiseRequests{
			retMsg:      c,
			incomingMsg: prepare,
		}
	}()
	return c
}

func (a *betterBatching) RecvAcceptRemote(accept *stdpaxosproto.Accept) <-chan Response {
	c := make(chan Response, 1)
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

func (a *betterBatching) batchPersister() {
	timeout := time.NewTimer(a.maxBatchWait)
	for {
		select {
		case inc := <-a.batcher.commitShorts:
			cmtMsg, resp := inc.incomingMsg, inc.done
			inst := cmtMsg.Instance

			instState := a.getInstState(inst)
			if instState.status == COMMITTED {
				close(resp)
				break
			}

			if instState.cmds == nil {
				panic("committing with no value!!!")
			}
			// store commit
			instState.status = COMMITTED
			instState.curBal = cmtMsg.Ballot
			instState.vBal = cmtMsg.Ballot

			instState.whoseCmds = cmtMsg.WhoseCmd
			recordInstanceMetadata(instState, a.stableStore, a.durable)
			recordCommands(instState.cmds, a.stableStore, a.durable)
			fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)

			go func() {
				resp <- struct{}{}
				close(resp)
			}()

			for instToResp, awaitingResps := range a.batcher.awaitingResponses {
				//inst
				if instToResp == cmtMsg.Instance {
					// if we receive a commit assume that others in this instance have also reeived it... Later prepares will then be sent a commit in return
					for _, pid := range a.ProposerIDs {
						closeAllPreviousResponsesToProposer(awaitingResps, pid)
					}
				} else {
					abk := a.batcher.instanceState[instToResp]
					returnResponsesAndResetAwaitingResponses(instToResp, a.meID, awaitingResps, *abk, a.prepareReplyRPC, a.acceptReplyRPC, a.commitRPC)
				}
			}
			a.batcher.awaitingResponses = make(map[int32]*responsesAwaiting, 100)
			break
		case inc := <-a.batcher.commits:
			// store all data
			cmtMsg, resp := inc.incomingMsg, inc.done
			inst := cmtMsg.Instance
			instState := a.getInstState(inst)
			if instState.status == COMMITTED {
				close(resp)
				break
			}

			// store commit
			instState.status = COMMITTED
			instState.curBal = cmtMsg.Ballot
			instState.vBal = cmtMsg.Ballot
			instState.cmds = cmtMsg.Command
			instState.whoseCmds = cmtMsg.WhoseCmd
			recordInstanceMetadata(instState, a.stableStore, a.durable)
			recordCommands(instState.cmds, a.stableStore, a.durable)
			fsync(a.stableStore, a.durable, a.emulatedSS, a.emuatedWriteTime)

			go func() {
				resp <- struct{}{}
				close(resp)
			}()

			for instToResp, awaitingResps := range a.batcher.awaitingResponses {
				if instToResp == cmtMsg.Instance {
					for _, pid := range a.ProposerIDs {
						closeAllPreviousResponsesToProposer(awaitingResps, pid)

					}
					// go a.returnCommitsToAwaitingInstanceResponses(awaitingResps, cmtMsg)
				} else {
					abk := a.batcher.instanceState[instToResp]
					returnResponsesAndResetAwaitingResponses(instToResp, a.meID, awaitingResps, *abk, a.prepareReplyRPC, a.acceptReplyRPC, a.commitRPC)
				}
			}
			a.batcher.awaitingResponses = make(map[int32]*responsesAwaiting, 100)
			break
		case inc := <-a.batcher.promiseRequests:
			dlog.Println("Received promise request")
			prepMsg, respHand := inc.incomingMsg, inc.retMsg
			inst := prepMsg.Instance
			instState := a.getInstState(inst)
			if instState.status == COMMITTED {
				close(respHand)
				break
			}

			if instState.curBal.GreaterThan(prepMsg.Ballot) {
				close(respHand)
				break
			}
			if instState.curBal.Equal(prepMsg.Ballot) && instState.status == ACCEPTED {
				close(respHand)
				break
			}

			// store commit
			instState.status = PREPARED
			instState.curBal = prepMsg.Ballot
			recordInstanceMetadata(instState, a.stableStore, a.durable)

			// close any previous awaiting responses and store this current one
			instAwaitingResps := a.getAwaitingResponses(inst)
			closeAllPreviousResponsesToProposer(instAwaitingResps, int32(prepMsg.PropID))
			instAwaitingResps.propsPrepResps[int32(prepMsg.PropID)] = &outgoingPromiseResponses{
				retMsgChan: respHand,
				Ballot:     prepMsg.Ballot,
			}
			break
		case inc := <-a.batcher.acceptRequests:
			dlog.Println("Received accept request")
			accMsg, respHand := inc.incomingMsg, inc.retMsg
			inst := accMsg.Instance
			instState := a.getInstState(inst)
			if instState.status == COMMITTED {
				close(respHand)
				break
			}
			if instState.curBal.GreaterThan(accMsg.Ballot) {
				close(respHand)
				break
			}

			// store commit
			instState.status = ACCEPTED
			instState.curBal = accMsg.Ballot
			instState.vBal = accMsg.Ballot
			instState.cmds = accMsg.Command
			instState.whoseCmds = accMsg.WhoseCmd
			recordInstanceMetadata(instState, a.stableStore, a.durable)
			recordCommands(instState.cmds, a.stableStore, a.durable)

			// close any previous awaiting responses and store this current one
			instAwaitingResps := a.getAwaitingResponses(inst)
			closeAllPreviousResponsesToProposer(instAwaitingResps, int32(accMsg.PropID))
			instAwaitingResps.propsAcceptResps[int32(accMsg.PropID)] = &outgoingAcceptResponses{
				retMsgChan: respHand,
				Ballot:     accMsg.Ballot,
			}
			break
		case <-timeout.C:
			// return responses for max and not max
			for instToResp, awaitingResps := range a.batcher.awaitingResponses {
				abk := a.batcher.instanceState[instToResp]
				returnResponsesAndResetAwaitingResponses(instToResp, a.meID, awaitingResps, *abk, a.prepareReplyRPC, a.acceptReplyRPC, a.commitRPC)
			}
			a.batcher.awaitingResponses = make(map[int32]*responsesAwaiting, 100)
			timeout.Reset(a.maxBatchWait)
			break
		}
	}
}

func (a *betterBatching) getInstState(inst int32) *AcceptorBookkeeping {
	_, exists := a.batcher.instanceState[inst]
	if !exists {
		a.batcher.instanceState[inst] = &AcceptorBookkeeping{
			status:    0,
			cmds:      nil,
			curBal:    stdpaxosproto.Ballot{-1, -1},
			vBal:      stdpaxosproto.Ballot{-1, -1},
			whoseCmds: -1,
			Mutex:     sync.Mutex{},
		}
	}
	return a.batcher.instanceState[inst]
}

func (a *betterBatching) getAwaitingResponses(inst int32) *responsesAwaiting {
	_, awaitingRespsExists := a.batcher.awaitingResponses[inst]
	if !awaitingRespsExists {
		a.batcher.awaitingResponses[inst] = &responsesAwaiting{
			propsPrepResps:   make(map[int32]*outgoingPromiseResponses),
			propsAcceptResps: make(map[int32]*outgoingAcceptResponses),
		}
	}
	return a.batcher.awaitingResponses[inst]
}
func closeAllPreviousResponsesToProposer(instAwaitingResps *responsesAwaiting, proposer int32) {
	prop, existsp := instAwaitingResps.propsPrepResps[proposer]
	acc, existsa := instAwaitingResps.propsAcceptResps[proposer]
	go func() {
		if existsp {
			close(prop.retMsgChan)
		}
		if existsa {
			close(acc.retMsgChan)
		}
	}()
	delete(instAwaitingResps.propsPrepResps, proposer)
	delete(instAwaitingResps.propsAcceptResps, proposer)
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

func (abk AcceptorBookkeeping) getPhase() stdpaxosproto.Phase {
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

func returnResponsesAndResetAwaitingResponses(inst int32, myId int32, awaitingResps *responsesAwaiting, abk AcceptorBookkeeping, prepareReplyRPC uint8, acceptReplyRPC uint8, commitRPC uint8) {
	go func() {
		if abk.status == COMMITTED {
			cmtMsg := &stdpaxosproto.Commit{
				LeaderId:   int32(abk.vBal.PropID),
				Instance:   inst,
				Ballot:     abk.vBal,
				WhoseCmd:   abk.whoseCmds,
				MoreToCome: 0,
				Command:    abk.cmds,
			}
			returnCommitsToAwaitingInstanceResponses(awaitingResps, cmtMsg, commitRPC)
			return
		}

		for pid, prepResp := range awaitingResps.propsPrepResps {
			prepResp.retMsgChan <- protoResponse{
				towhom:     giveToWhom(pid),
				gettype:    func() uint8 { return prepareReplyRPC },
				isnegative: func() bool { return abk.curBal.GreaterThan(prepResp.Ballot) },
				Serializable: &stdpaxosproto.PrepareReply{
					Instance:   inst,
					Cur:        abk.curBal,
					CurPhase:   abk.getPhase(),
					Req:        prepResp.Ballot,
					VBal:       abk.vBal,
					AcceptorId: myId,
					WhoseCmd:   abk.whoseCmds,
					Command:    abk.cmds,
				},
			}
			close(prepResp.retMsgChan)
		}

		for pid, accResp := range awaitingResps.propsAcceptResps {
			accResp.retMsgChan <- protoResponse{
				towhom:     giveToWhom(pid),
				gettype:    func() uint8 { return acceptReplyRPC },
				isnegative: func() bool { return abk.curBal.GreaterThan(accResp.Ballot) },
				Serializable: &stdpaxosproto.AcceptReply{
					Instance:   inst,
					AcceptorId: myId,
					CurPhase:   abk.getPhase(),
					Cur:        abk.curBal,
					Req:        accResp.Ballot,
					WhoseCmd:   abk.whoseCmds,
				},
			}
			close(accResp.retMsgChan)
		}
	}()
}

func returnCommitsToAwaitingInstanceResponses(awaitingResps *responsesAwaiting, cmtMsg *stdpaxosproto.Commit, commitRPC uint8) {
	for pid, prepResp := range awaitingResps.propsPrepResps {
		prepResp.retMsgChan <- protoResponse{
			towhom:       giveToWhom(pid),
			gettype:      func() uint8 { return commitRPC },
			isnegative:   func() bool { return true },
			Serializable: cmtMsg,
		}
		close(prepResp.retMsgChan)
	}

	for pid, accResp := range awaitingResps.propsAcceptResps {
		accResp.retMsgChan <- protoResponse{
			towhom:       giveToWhom(pid),
			gettype:      func() uint8 { return commitRPC },
			isnegative:   func() bool { return true },
			Serializable: cmtMsg,
		}
		close(accResp.retMsgChan)
	}
}
