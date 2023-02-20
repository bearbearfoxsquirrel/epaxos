package proposer

import (
	"encoding/binary"
	"epaxos/batching"
	"epaxos/dlog"
	"epaxos/genericsmr"
	"epaxos/genericsmrproto"
	"epaxos/stablestore"
	"epaxos/state"
	_const "epaxos/twophase/const"
)

type Executor interface {
	Learnt(inst int32, cmds []*state.Command, whose int32)
	ProposedBatch(inst int32, b batching.ProposalBatch)
}

type EagerByExecExecutor struct {
	SimpleExecutor
	ExecOpenInstanceSignal
}

func (e *EagerByExecExecutor) Learnt(inst int32, cmds []*state.Command, whose int32) {
	e.SimpleExecutor.Learnt(inst, cmds, whose)
	e.ExecOpenInstanceSignal.CheckExec(e)
}

type SimpleExecutor struct {
	executedUpTo  int32
	clientBatches []batching.ProposalBatch
	cmds          [][]*state.Command
	learnt        []bool
	whose         []int32
	*genericsmr.Replica
	stablestore.StableStore
	meId   int32
	dreply bool
	state  *state.State
}

type ExecInformer interface {
	GetExecutedUpTo() int32
}

func (e *SimpleExecutor) GetExecutedUpTo() int32 {
	return e.executedUpTo
}

func GetNewExecutor(id int32, r *genericsmr.Replica, store stablestore.StableStore, dreply bool) SimpleExecutor {
	return SimpleExecutor{
		executedUpTo:  -1,
		clientBatches: make([]batching.ProposalBatch, _const.ISpaceLen),
		cmds:          make([][]*state.Command, _const.ISpaceLen),
		learnt:        make([]bool, _const.ISpaceLen),
		whose:         make([]int32, _const.ISpaceLen),
		Replica:       r,
		StableStore:   store,
		meId:          id,
		dreply:        dreply,
		state:         state.InitState(),
	}
}

func (ex *SimpleExecutor) ProposedBatch(inst int32, b batching.ProposalBatch) {
	if ex.clientBatches[inst] != nil {
		if b.GetUID() != ex.clientBatches[inst].GetUID() {
			panic("Should not propose multiple batches to a single instance")
		}
	}

	ex.clientBatches[inst] = b
}

func (ex *SimpleExecutor) Learnt(inst int32, cmds []*state.Command, whose int32) {
	if ex.learnt[inst] == true {
		return
	}

	ex.learnt[inst] = true
	ex.whose[inst] = whose
	ex.cmds[inst] = cmds

	if ex.clientBatches[inst] != nil && ex.whose[inst] != ex.meId {
		ex.clientBatches[inst] = nil
	}

	if ex.clientBatches[inst] != nil && !ex.dreply {
		ex.replyToNondurablyClients(inst)
	}
	ex.exec()
}

func (ex *SimpleExecutor) exec() {
	oldExecutedUpTo := ex.executedUpTo
	for ex.learnt[ex.executedUpTo+1] {
		crt := ex.executedUpTo + 1
		ex.executedUpTo += 1
		if ex.whose[crt] != ex.meId {
			dlog.AgentPrintfN(ex.meId, ExecutingFmt(crt, ex.whose[crt]))
		} else {
			dlog.AgentPrintfN(ex.meId, ExecutingBatchFmt(crt, ex.whose[crt], ex.clientBatches[crt]))
		}
		if ex.whose[crt] == -1 { // is NOOP
			continue
		}
		toExec := ex.cmds[crt]
		length := len(toExec)
		if ex.whose[crt] != ex.meId {
			for j := 0; j < length; j++ {
				dlog.Printf("Executing " + toExec[j].String())
				toExec[j].Execute(ex.state)
			}
			continue
		}
		for j := 0; j < length; j++ {
			dlog.Printf("Executing " + toExec[j].String())
			val := toExec[j].Execute(ex.state)
			if !ex.dreply {
				continue
			}
			ex.replyToClientOfCmd(crt, j, val)
		}
	}
	if ex.executedUpTo > oldExecutedUpTo {
		ex.recordExecutedUpTo()
	}
}

func (ex *SimpleExecutor) replyToClientOfCmd(inst int32, i int, value state.Value) {
	proposals := ex.clientBatches[inst].GetProposals()
	propreply := &genericsmrproto.ProposeReplyTS{
		1,
		proposals[i].CommandId,
		value,
		proposals[i].Timestamp}
	ex.Replica.ReplyProposeTS(propreply, proposals[i].Reply, proposals[i].Mutex)
}

func (ex *SimpleExecutor) recordExecutedUpTo() {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(ex.executedUpTo))
	ex.StableStore.WriteAt(b[:], 4)
}

func (ex *SimpleExecutor) replyToNondurablyClients(inst int32) {
	b := ex.clientBatches[inst]
	for i := 0; i < len(b.GetCmds()); i++ {
		ex.replyToClientOfCmd(inst, i, state.NIL())
	}
}
