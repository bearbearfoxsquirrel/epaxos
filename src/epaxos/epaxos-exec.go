package epaxos

import (
	//    "state"
	"dlog"
	"epaxosproto"
	"genericsmrproto"
	"sort"
	"state"
)

const (
	WHITE int8 = iota
	GRAY
	BLACK
)

type Exec struct {
	r *Replica
}

type SCComponent struct {
	nodes []*Instance
	color int8
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

	if !e.findSCC(inst) {
		return false
	}

	return true
}

var stack []*Instance = make([]*Instance, 0, 100)

func (e *Exec) findSCC(root *Instance) bool {
	index := 1
	// find SCCs using Tarjan's algorithm
	stack = stack[0:0]
	ret := e.strongconnect(root, &index)
	// reset all indexes in the stack
	for j := 0; j < len(stack); j++ {
		stack[j].Index = 0
	}
	return ret
}

func (e *Exec) strongconnect(v *Instance, index *int) bool {
	v.Index = *index
	v.Lowlink = *index
	*index = *index + 1

	l := len(stack)
	if l == cap(stack) {
		newSlice := make([]*Instance, l, 2*l)
		copy(newSlice, stack)
		stack = newSlice
	}
	stack = stack[0 : l+1]
	stack[l] = v

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

			if e.r.transconf {
				for _, alpha := range v.Cmds {
					for _, beta := range e.r.InstanceSpace[q][i].Cmds {
						if !state.Conflict(&alpha, &beta) {
							continue
						}
					}
				}
			}

			if e.r.InstanceSpace[q][i].Status == epaxosproto.EXECUTED {
				continue
			}

			for e.r.InstanceSpace[q][i].Status != epaxosproto.COMMITTED {
				dlog.Printf("Not committed instance %d.%d\n", q, i)
				return false
			}

			w := e.r.InstanceSpace[q][i]

			if w.Index == 0 {
				if !e.strongconnect(w, index) {
					return false
				}
				if w.Lowlink < v.Lowlink {
					v.Lowlink = w.Lowlink
				}
			} else if e.inStack(w) {
				if w.Index < v.Lowlink {
					v.Lowlink = w.Index
				}
			}
		}
	}

	if v.Lowlink == v.Index {
		//found SCC
		list := stack[l:]

		//execute commands in the increasing order of the Seq field
		sort.Sort(nodeArray(list))
		for _, w := range list {
			for idx := 0; idx < len(w.Cmds); idx++ {
				shouldRespond := e.r.Dreply && w.lb != nil && w.lb.clientProposals != nil
				dlog.Printf("Executing "+w.Cmds[idx].String()+" at %d.%d with (seq=%d, deps=%d, scc_size=%d, shouldRespond=%t)\n", w.id.replica, w.id.instance, w.Seq, w.Deps, len(list), shouldRespond)
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
		}
		stack = stack[0:l]
	}

	return true
}

func (e *Exec) inStack(w *Instance) bool {
	for _, u := range stack {
		if w == u {
			return true
		}
	}
	return false
}

type nodeArray []*Instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].Seq < na[j].Seq || (na[i].Seq == na[j].Seq && na[i].id.replica < na[j].id.replica) || (na[i].Seq == na[j].Seq && na[i].id.replica == na[j].id.replica && na[i].proposeTime < na[j].proposeTime)
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}

/*



const (
	WHITE int8 = iota
	GRAY
	BLACK
)

type Exec struct {
	r *Replica
}

type SCComponent struct {
	nodes []*Instance
	color int8
}

var overallRoot *Instance
var overallRep int32
var overallInst int32

func (e *Exec) executeCommand(replica int32, instance int32) bool {
	if e.r.InstanceSpace[replica][instance] == nil {
		return false
	}
	inst := e.r.InstanceSpace[replica][instance]
	if inst.Status == epaxosproto.EXECUTED {
		return true
	}
	if inst.Status != epaxosproto.COMMITTED {
		return false
	}

	overallRep = replica
	overallInst = instance
	overallRoot = inst

	if !e.findSCC(inst) {
		return false
	}

	return true
}

var stack []*Instance = make([]*Instance, 0, 100)

func (e *Exec) findSCC(root *Instance) bool {
	index := 1
	//find SCCs using Tarjan's algorithm
	stack = stack[0:0]
	return e.strongconnect(root, &index)
}

func (e *Exec) strongconnect(v *Instance, index *int) bool {
	v.Index = *index
	v.Lowlink = *index
	*index = *index + 1

	l := len(stack)
	if l == cap(stack) {
		newSlice := make([]*Instance, l, 2*l)
		copy(newSlice, stack)
		stack = newSlice
	}
	stack = stack[0 : l+1]
	stack[l] = v

	for q := int32(0); q < int32(e.r.N); q++ {
		inst := v.Deps[q]
		for i := e.r.ExecedUpTo[q] + 1; i <= inst; i++ {
			for e.r.InstanceSpace[q][i] == nil || e.r.InstanceSpace[q][i].Cmds == nil || v.Cmds == nil {
				// Sarah update: in the original code this was time.Sleep(1000 * 1000)
				return false
			}

			w := e.r.InstanceSpace[q][i]

			if w.Status == epaxosproto.EXECUTED {
				continue
			}

			// Instances that don't conflict can be skipped
			conflict := false
			for ci, _ := range v.Cmds {
				for di, _ := range w.Cmds {
					if state.Conflict(&v.Cmds[ci], &w.Cmds[di]) {
						conflict = true
					}
				}
			}
			if !conflict {
				continue
			}

			// Don't need to wait for reads
			allReads := true
			for _, cmd := range w.Cmds {
				if cmd.Op != state.GET {
					allReads = false
				}
			}
			if allReads {
				continue
			}

			// Livelock fix: any instance that has a high seq and the root
			// as a dependency will necessarily execute after it. (As will
			// any of its dependencies the root wouldn't already know about.)
			if// infiniteFix bool?
			(w.Seq > overallRoot.Seq || (overallRep < q && w.Seq == overallRoot.Seq)) &&
				w.Deps[overallRep] >= overallInst {
				break
			}

			for e.r.InstanceSpace[q][i].Status != epaxosproto.COMMITTED {
				// Sarah update: in the original code this was time.Sleep(1000 * 1000)
				return false
			}

			if w.Index == 0 {
				//e.strongconnect(w, index)
				if !e.strongconnect(w, index) {
					for j := l; j < len(stack); j++ {
						stack[j].Index = 0
					}
					stack = stack[0:l]
					return false
				}
				if w.Lowlink < v.Lowlink {
					v.Lowlink = w.Lowlink
				}
			} else { //if e.inStack(w)  //<- probably unnecessary condition, saves a linear search
				if w.Index < v.Lowlink {
					v.Lowlink = w.Index
				}
			}
		}
	}

	if v.Lowlink == v.Index {
		//found SCC
		list := stack[l:len(stack)]

		//execute commands in the increasing order of the Seq field
		sort.Sort(nodeArray(list))
		for _, w := range list {
			for w.Cmds == nil {
				// Sarah update: in the original code this was time.Sleep(1000 * 1000)
				return false
			}
			for idx := 0; idx < len(w.Cmds); idx++ {
				w.Cmds[idx].Execute(e.r.State)
				if w.lb != nil && w.lb.clientProposals != nil {

					e.r.ReplyProposeTS(
						&genericsmrproto.ProposeReplyTS{
							TRUE,
							w.lb.clientProposals[idx].CommandId,
							w.lb.clientProposals[idx].Command.V,
							w.lb.clientProposals[idx].Timestamp},
						w.lb.clientProposals[idx].Reply,
						w.lb.clientProposals[idx].Mutex)
				}
			}
			w.Status = epaxosproto.EXECUTED
		}
		stack = stack[0:l]
	}
	return true
}

func (e *Exec) inStack(w *Instance) bool {
	for _, u := range stack {
		if w == u {
			return true
		}
	}
	return false
}

type nodeArray []*Instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].Seq < na[j].Seq
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}
*/
