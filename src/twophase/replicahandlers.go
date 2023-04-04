package twophase

import (
	"epaxos/acceptor"
	"epaxos/dlog"
	"epaxos/fastrpc"
	"epaxos/genericsmr"
	"epaxos/stdpaxosproto"
	"epaxos/twophase/learner"
)

type PrepareResponsesRPC struct {
	PrepareReply uint8
	Commit       uint8
}

func acceptorSyncHandlePrepareLocal(id int32, acc acceptor.Acceptor, prepare *stdpaxosproto.Prepare, rpc PrepareResponsesRPC) *stdpaxosproto.PrepareReply {
	c := acc.RecvPrepareRemote(prepare)
	msg := <-c
	if msg.GetType() != rpc.PrepareReply {
		panic("Not got a prepare reply back")
	}
	if msg.IsNegative() {
		return nil
		//panic("Not got a promise back")
	}
	return msg.GetSerialisable().(*stdpaxosproto.PrepareReply)
}

func acceptorHandlePrepareLocal(id int32, acc acceptor.Acceptor, learner learner.Learner, replica *genericsmr.Replica, prepare *stdpaxosproto.Prepare, rpc PrepareResponsesRPC, promiseChan chan<- fastrpc.Serializable) {
	if learnerCheckChosen(learner, prepare.Instance, prepare.Ballot, "Prepare", int32(prepare.PropID), rpc.Commit, replica, id) {
		return
	}
	c := acc.RecvPrepareRemote(prepare)
	go func() {
		for msg := range c {
			if msg.ToWhom() != id {
				if replica.UDP {
					replica.SendUDPMsg(msg.ToWhom(), msg.GetType(), msg.GetUDPaxos(), false)
				} else {
					replica.SendMsg(msg.ToWhom(), msg.GetType(), msg.GetSerialisable())
				}
				continue
			}
			preply := msg.GetSerialisable().(*stdpaxosproto.PrepareReply)
			if msg.IsNegative() {
				continue
			}
			if msg.GetType() == rpc.PrepareReply {
				isPreemptStr := isPreemptOrPromise(preply)
				dlog.AgentPrintfN(id, "Sending Prepare Reply (%s) to Replica %d for instance %d with current ballot %d.%d and value ballot %d.%d and whose commands %d in response to a Prepare in instance %d at ballot %d.%d",
					isPreemptStr, prepare.PropID, preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd, prepare.Instance, prepare.Number, prepare.PropID)
			}
			promiseChan <- preply
		}
	}()

}

func isPreemptOrPromise(preply *stdpaxosproto.PrepareReply) string {
	isPreempt := preply.Cur.GreaterThan(preply.Req)
	isPreemptStr := "Preempt"
	if !isPreempt {
		isPreemptStr = "Promise"
	}
	return isPreemptStr
}

func learnerCheckChosen(l learner.Learner, inst int32, atmt stdpaxosproto.Ballot, atmtMsg string, who int32, cmtRPC uint8, replica *genericsmr.Replica, meID int32) bool {
	if l.IsChosen(inst) && l.HasLearntValue(inst) {
		if who == meID {
			return true
		}
		b, v, whose := l.GetChosen(inst)
		dlog.AgentPrintfN(meID, "Sending Commit to Replica %d for instance %d at ballot %d.%d with whose commands %d in response to a %s in instance %d at ballot %d.%d",
			who, inst, b.Number, b.PropID, whose, atmtMsg, inst, atmt.Number, atmt.PropID)
		cmt := &stdpaxosproto.Commit{
			LeaderId:   int32(b.PropID),
			Instance:   inst,
			Ballot:     b,
			WhoseCmd:   whose,
			MoreToCome: 0,
			Command:    v,
		}
		if replica.UDP {
			replica.SendUDPMsg(who, cmtRPC, cmt, false)
		} else {
			replica.SendMsg(who, cmtRPC, cmt)
		}
		return true
	}
	return false
}

func handlePrepareResponsesFromAcceptor(resp <-chan acceptor.Message, rpc PrepareResponsesRPC, prepare *stdpaxosproto.Prepare, id int32, replica *genericsmr.Replica, noPreempt bool) {
	for msg := range resp {
		if msg.GetType() != rpc.PrepareReply {
			continue
		}
		if msg.IsNegative() && noPreempt {
			continue
		}
		preply := msg.GetSerialisable().(*stdpaxosproto.PrepareReply)
		isPreemptStr := isPreemptOrPromise(preply)
		dlog.AgentPrintfN(id, "Sending Prepare Reply (%s) to Replica %d for instance %d with current ballot %d.%d and value ballot %d.%d and whose commands %d in response to a Prepare in instance %d at ballot %d.%d",
			isPreemptStr, prepare.PropID, preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd, prepare.Instance, prepare.Number, prepare.PropID)
		if msg.ToWhom() == id {
			continue
		}
		if replica.UDP {
			replica.SendUDPMsg(msg.ToWhom(), msg.GetType(), msg.GetUDPaxos(), false)
		} else {
			replica.SendMsg(msg.ToWhom(), msg.GetType(), msg.GetSerialisable())
		}
	}
}

func acceptorHandlePrepareFromRemote(id int32, l learner.Learner, acc acceptor.Acceptor, prepare *stdpaxosproto.Prepare, rpc PrepareResponsesRPC, replica *genericsmr.Replica, noPreempt bool) {
	//s := time.Now()
	if learnerCheckChosen(l, prepare.Instance, prepare.Ballot, "Prepare", int32(prepare.PropID), rpc.Commit, replica, id) {
		return
	}

	resp := acc.RecvPrepareRemote(prepare)
	//dlog.AgentPrintfN(id, "It took %d µs for acceptor to handle prepare", time.Now().Sub(s).Microseconds())
	go func() {
		handlePrepareResponsesFromAcceptor(resp, rpc, prepare, id, replica, noPreempt)
		//dlog.AgentPrintfN(id, "It took %d µs for acceptor to handle prepare and return message", time.Now().Sub(s).Microseconds())
	}()
}

func acceptorSyncHandlePrepare(id int32, l learner.Learner, acc acceptor.Acceptor, prepare *stdpaxosproto.Prepare, rpc PrepareResponsesRPC, replica *genericsmr.Replica, bcastPrepare bool) {
	if learnerCheckChosen(l, prepare.Instance, prepare.Ballot, "Prepare", int32(prepare.PropID), rpc.Commit, replica, id) {
		return
	}

	resp := acc.RecvPrepareRemote(prepare)
	handlePrepareResponsesFromAcceptor(resp, rpc, prepare, id, replica, bcastPrepare)
}

type AcceptResponsesRPC struct {
	AcceptReply uint8
	Commit      uint8
}

func acceptorHandleAcceptLocal(id int32, accptr acceptor.Acceptor, accept *stdpaxosproto.Accept, rpc AcceptResponsesRPC, replica *genericsmr.Replica, bcastAcceptance bool, acceptanceChan chan<- fastrpc.Serializable) {
	c := accptr.RecvAcceptRemote(accept)
	go func() {
		msg := <-c
		if msg.GetType() != rpc.AcceptReply {
			return
		}
		if msg.IsNegative() {
			return
			//	panic("Not got an acceptance back")
		}
		areply := msg.GetSerialisable().(*stdpaxosproto.AcceptReply)
		if bcastAcceptance {
			dlog.AgentPrintfN(id, "Sending Acceptance of instance %d with current ballot %d.%d and whose commands %d to all replicas", accept.Instance, accept.Ballot.Number, accept.Ballot.PropID, accept.WhoseCmd)
			if replica.UDP {
				bcastAcceptanceUDP(replica, areply, rpc.AcceptReply, id)
			} else {
				bcastAcceptanceTCP(replica, areply, rpc.AcceptReply, id)
			}
		}
		acceptanceChan <- msg.GetSerialisable().(*stdpaxosproto.AcceptReply)
	}()
}

func isPreemptOrAccept(areply *stdpaxosproto.AcceptReply) string {
	isPreempt := areply.Cur.GreaterThan(areply.Req)
	isPreemptStr := "Preempt"
	if !isPreempt {
		isPreemptStr = "Accept"
	}
	return isPreemptStr
}

func acceptorHandleAcceptResponse(responseC <-chan acceptor.Message, rpc AcceptResponsesRPC, id int32, bcastPrepare bool, replica *genericsmr.Replica, acceptanceChan chan<- fastrpc.Serializable, accept *stdpaxosproto.Accept, bcastAcceptDisklessNOOP bool, bcastAcceptance bool) {
	for resp := range responseC {
		if resp.ToWhom() == id {
			continue
		}
		if resp.GetType() != rpc.AcceptReply {
			continue
		}
		if resp.IsNegative() && bcastPrepare {
			continue
		}
		areply := resp.GetSerialisable().(*stdpaxosproto.AcceptReply)
		isPreemptStr := isPreemptOrAccept(areply)
		dlog.AgentPrintfN(id, "Sending Accept Reply (%s) to Replica %d for instance %d with current ballot %d.%d and whose commands %d in response to a Accept in instance %d at ballot %d.%d",
			isPreemptStr, accept.PropID, areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.WhoseCmd, accept.Instance, accept.Number, accept.PropID)

		if resp.IsNegative() {
			if replica.UDP {
				replica.SendUDPMsg(resp.ToWhom(), resp.GetType(), areply, true)
			} else {
				replica.SendMsg(resp.ToWhom(), resp.GetType(), areply)
			}
			continue
		}

		if bcastAcceptance {
			dlog.AgentPrintfN(id, "Sending Acceptance for instance %d with current ballot %d.%d and whose commands %d to all replicas", accept.Instance, accept.Ballot.Number, accept.Ballot.PropID, accept.WhoseCmd)
			if replica.UDP {
				bcastAcceptanceUDP(replica, areply, rpc.AcceptReply, id)
			} else {
				bcastAcceptanceTCP(replica, areply, rpc.AcceptReply, id)
			}
			acceptanceChan <- areply
			continue
		}

		if replica.UDP {
			replica.SendUDPMsg(resp.ToWhom(), resp.GetType(), areply, true)
		} else {
			replica.SendMsg(resp.ToWhom(), resp.GetType(), areply)
		}
		continue
	}
}

func bcastAcceptanceUDP(replica *genericsmr.Replica, areply *stdpaxosproto.AcceptReply, rpcCode uint8, id int32) {
	for i := 0; i < replica.N; i++ {
		if i == int(id) {
			continue
		}
		replica.SendUDPMsg(int32(i), rpcCode, areply, true)
	}
}

func bcastAcceptanceTCP(replica *genericsmr.Replica, areply *stdpaxosproto.AcceptReply, rpcCode uint8, id int32) {
	replica.Mutex.Lock()
	for i := 0; i < replica.N; i++ {
		if i == int(id) {
			continue
		}
		replica.SendMsgUNSAFE(int32(i), rpcCode, areply)
	}
	replica.Mutex.Unlock()
}

func acceptorHandleAccept(id int32, l learner.Learner, acc acceptor.Acceptor, accept *stdpaxosproto.Accept, rpc AcceptResponsesRPC, replica *genericsmr.Replica, bcastAcceptance bool, acceptanceChan chan<- fastrpc.Serializable, bcastPrepare bool, bcastAcceptDisklessNOOP bool) {
	if learnerCheckChosen(l, accept.Instance, accept.Ballot, "Accept", int32(accept.PropID), rpc.Commit, replica, id) {
		return
	}
	dlog.AgentPrintfN(id, "Acceptor handing Accept from Replica %d in instance %d at ballot %d.%d as it can form a quorum", accept.PropID, accept.Instance, accept.Number, accept.PropID)
	responseC := acc.RecvAcceptRemote(accept)
	go func() {
		acceptorHandleAcceptResponse(responseC, rpc, id, bcastPrepare, replica, acceptanceChan, accept, bcastAcceptDisklessNOOP, bcastAcceptance)
	}()
}
