package atlas

import (
	"epaxos/CommitExecutionComparator"
	"epaxos/dlog"
	"epaxos/epaxosproto"
	"epaxos/genericsmrproto"
	"epaxos/state"
	"sort"
	"time"
)

const (
	WHITE int8 = iota
	GRAY
	BLACK
)

type Exec struct {
	r     *Replica
	stack []*Instance //= make([]*Instance, 0, 100)
}

type ExecInst struct {
	*Instance
}

type SCComponent struct {
	nodes []*Instance
	color int8
}

func contains(s []int, elem int) bool {
	for _, e := range s {
		if e == elem {
			return true
		}
	}
	return false
}

func isOkayToAddToS(s []instanceId, executedUpTo []int32, check instanceId, checkDeps []instanceId) bool {
	for dep := range checkDeps {
		if inS(s, checkDeps[dep]) {
			continue // is in s
		}
		if executedUpTo[checkDeps[dep].replica] >= checkDeps[dep].instance { // is check deps executed
			continue // has been executed already
		}
		return false
	}
	return true
}

func inS(s []instanceId, check instanceId) bool {
	for _, id := range s {
		if id.replica != check.replica || id.instance != check.instance {
			continue
		}
		return true // is in s
	}
	return false
}

func (e *Exec) getDepList(id instanceId) []instanceId {
	inst := e.r.InstanceSpace[id.replica][id.instance]
	deps := inst.Deps
	ret := make([]instanceId, 0, e.r.N)
	for l := int32(0); l < int32(e.r.N); l++ {
		dep := instanceId{replica: l, instance: deps[l]}
		ret = append(ret, dep)
	}
	return ret
}

func smallestSlice(slices [][]instanceId) int {
	smallest := 0

	for i, slice := range slices {
		if len(slice) < len(slices[smallest]) {
			smallest = i
		}
	}

	return smallest
}

func (e *Exec) executeCommand(replica int32, instance int32) bool {
	if e.r.InstanceSpace[replica][instance] == nil {
		return false
	}
	inst := e.r.InstanceSpace[replica][instance]
	if inst.Status == epaxosproto.EXECUTED {
		return true
	}
	if inst.Status != epaxosproto.COMMITTED {
		dlog.Printf("Not committed instance %d.%d\n", replica, instance)
		return false
	}

	// for each command in
	//for {
	//each time a new command is committed
	// check every log for
	// the list of commands and deps that they can execute
	// compare them to each other for the min
	// if there is one then execute it

	//S[l] = make([]instanceId, 0, int32(e.r.N)*(commitedUpTo-executedUpTo))
	// every command that is committed and every dependency is committed
	//for l := int32(0); l < int32(e.r.N); l++ {
	//	commitedUpTo := e.r.CommittedUpTo[l]
	//	executedUpTo := e.r.ExecedUpTo[l]
	//	if commitedUpTo == executedUpTo {
	//		continue
	//	}
	//	//cur := int32(1)
	//	//for cont := true; !cont {
	//	//	considering := instanceId{l, executedUpTo + 1}
	//	//	//instance := e.r.InstanceSpace[considering.replica][considering.instance]
	//	//	deps := e.getDepList(considering)
	//	//	if !isOkayToAddToS(S[l], e.r.ExecedUpTo, considering, deps) {
	//	//		break
	//	//	}
	//	//	S[l] = append(S[l], considering)
	//	//	//cur += 1
	//	//}
	//}
	//sMin := smallestSlice(S)
	//if len(S[sMin]) == 0 {
	//	return false
	//}
	return e.findSCC(inst)
	//var S []instanceId = make([]instanceId, e.r.N)
	//sort.Slice(S, func(i, j int) bool {
	//	return S[i].replica < S[j].replica || (S[i].replica == S[j].replica && S[i].instance < S[j].instance)
	//})
	//
	//e.executeInstances(S)
	//}

	//return true
}

func (e *Exec) findSCC(root *Instance) bool {
	index := 1
	// find SCCs using Tarjan's algorithm
	e.stack = e.stack[0:0]
	ret := e.strongconnect(root, &index)

	// reset all indexes in the stack
	for j := 0; j < len(e.stack); j++ {
		e.stack[j].Exec.Index = 0
	}
	return ret
}

func (e *Exec) strongconnect(v *Instance, index *int) bool {
	v.Exec.Index = *index
	v.Exec.Lowlink = *index
	*index = *index + 1

	l := len(e.stack)
	if l == cap(e.stack) {
		newSlice := make([]*Instance, l, 2*l)
		copy(newSlice, e.stack)
		e.stack = newSlice
	}
	e.stack = e.stack[0 : l+1]
	e.stack[l] = v

	if v.Cmds == nil {
		dlog.Printf("Null instance! \n")
		return false
	}

	for q := int32(0); q < int32(e.r.N); q++ {
		inst := v.Deps[q]
		for i := e.r.ExecedUpTo[q] + 1; i <= inst; i++ {
			if e.r.InstanceSpace[q][i] == nil || e.r.InstanceSpace[q][i].Cmds == nil {
				dlog.Printf("Null instance %d.%d\n", q, i)
				return false
			}

			if e.r.InstanceSpace[q][i].Status == epaxosproto.EXECUTED {
				continue
			}

			for e.r.InstanceSpace[q][i].Status != epaxosproto.COMMITTED {
				dlog.Printf("Not committed instance %d.%d\n", q, i)
				return false
			}

			w := e.r.InstanceSpace[q][i]

			if w.Exec.Index == 0 {
				if !e.strongconnect(w, index) {
					return false
				}
				if w.Exec.Lowlink < v.Exec.Lowlink {
					v.Exec.Lowlink = w.Exec.Lowlink
				}
			} else if e.inStack(w) {
				if w.Exec.Index < v.Exec.Lowlink {
					v.Exec.Lowlink = w.Exec.Index
				}
			}
		}
	}

	if v.Exec.Lowlink == v.Exec.Index {
		//found SCC
		//list := e.stack[l:]
		list := e.stack[l:]

		//execute commands in the increasing order of the Seq field
		sort.Sort(nodeArray(list))
		for _, w := range list {
			for idx := 0; idx < len(w.Cmds); idx++ {
				shouldRespond := e.r.Dreply && w.lb != nil && w.lb.clientProposals != nil
				dlog.Printf("Executing "+w.Cmds[idx].String()+" at %d.%d with (deps=%d, scc_size=%d, shouldRespond=%t)\n", w.Exec.id.replica, w.Exec.id.instance, w.Deps, len(list), shouldRespond)

				if w.Cmds[idx].Op == state.NONE {
					// nothing to do

				} else if shouldRespond {
					dlog.Println("Executed value being sent to clients")

					w.Cmds[idx].Execute(e.r.State)

					e.r.ReplyProposeTS(
						&genericsmrproto.ProposeReplyTS{
							TRUE,
							w.lb.clientProposals[idx].CommandId,
							w.lb.clientProposals[idx].Command.V,
							w.lb.clientProposals[idx].Timestamp},
						w.lb.clientProposals[idx].Reply,
						w.lb.clientProposals[idx].Mutex)

				} else if w.Cmds[idx].Op == state.PUT {
					w.Cmds[idx].Execute(e.r.State)
				}

			}
			w.Status = epaxosproto.EXECUTED
			e.doRecord(w.Exec.id.replica, w.Exec.id.instance)

		}
		e.stack = e.stack[0:l]
	}
	return true
}

func (e *Exec) inStack(w *Instance) bool {
	for _, u := range e.stack {
		if w == u {
			return true
		}
	}
	return false
}

func (e *Exec) executeInstances(insts []instanceId) {
	for _, instID := range insts {
		w := e.r.InstanceSpace[instID.replica][instID.instance]
		for idx := 0; idx < len(w.Cmds); idx++ {
			shouldRespond := e.r.Dreply && w.lb != nil && w.lb.clientProposals != nil
			dlog.Printf("Executing "+w.Cmds[idx].String()+" at %d.%d with (seq=%d, deps=%d, shouldRespond=%t)\n", instID.replica, instID.instance, w.Deps, shouldRespond)

			if w.Cmds[idx].Op == state.NONE {
				// nothing to do

			} else if shouldRespond {
				dlog.Println("Executed value being sent to clients")

				w.Cmds[idx].Execute(e.r.State)

				e.r.ReplyProposeTS(
					&genericsmrproto.ProposeReplyTS{
						TRUE,
						w.lb.clientProposals[idx].CommandId,
						w.lb.clientProposals[idx].Command.V,
						w.lb.clientProposals[idx].Timestamp},
					w.lb.clientProposals[idx].Reply,
					w.lb.clientProposals[idx].Mutex)

			} else if w.Cmds[idx].Op == state.PUT {
				w.Cmds[idx].Execute(e.r.State)
			}
		}
		w.Status = epaxosproto.EXECUTED
		e.doRecord(instID.replica, instID.instance)
	}
}

type nodeArray []*Instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].Exec.id.replica < na[j].Exec.id.replica || (na[i].Exec.id.replica == na[j].Exec.id.replica && na[i].Exec.id.instance < na[j].Exec.id.instance) // na[i].Seq < na[j].Seq || (na[i].Seq == na[j].Seq && na[i].id.replica < na[j].id.replica) || (na[i].Seq == na[j].Seq && na[i].id.replica == na[j].id.replica && na[i].proposeTime < na[j].proposeTime)
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}

func (e *Exec) doRecord(log int32, seq int32) {
	if e.r.cmpCommitAndExec {
		id := CommitExecutionComparator.InstanceID{Log: log, Seq: seq}
		now := time.Now()
		if e.r.sepExecThread {
			e.r.commitExecMutex.Lock()
			e.r.commitExecComp.RecordExecution(id, now)
			//e.r.execLatsToRec <- ToRecord{id, now}
			e.r.commitExecMutex.Unlock()
		} else {
			e.r.commitExecComp.RecordExecution(id, now)
		}
	}
}
