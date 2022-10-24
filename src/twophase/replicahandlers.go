package twophase

import (
	"epaxos/acceptor"
	"epaxos/dlog"
	"epaxos/fastrpc"
	"epaxos/genericsmr"
	"epaxos/stdpaxosproto"
)

type PrepareResponsesRPC struct {
	prepareReply uint8
	commit       uint8
}

func acceptorSyncHandlePrepareLocal(id int32, acc acceptor.Acceptor, prepare *stdpaxosproto.Prepare, rpc PrepareResponsesRPC) *stdpaxosproto.PrepareReply {
	c := acc.RecvPrepareRemote(prepare)
	msg := <-c
	if msg.GetType() != rpc.prepareReply {
		panic("Not got a prepare reply back")
	}
	if msg.IsNegative() {
		panic("Not got a promise back")
	}
	return msg.GetSerialisable().(*stdpaxosproto.PrepareReply)
}

func acceptorSyncHandleAcceptLocal(id int32, accptr acceptor.Acceptor, accept *stdpaxosproto.Accept, rpc AcceptResponsesRPC, replica *genericsmr.Replica, bcastAcceptance bool) *stdpaxosproto.AcceptReply {
	c := accptr.RecvAcceptRemote(accept)
	msg := <-c
	if msg.GetType() != rpc.acceptReply {
		panic("Not got an accept reply back")
	}
	if msg.IsNegative() {
		panic("Not got an acceptance back")
	}

	if bcastAcceptance || accept.LeaderId == -3 {
		dlog.AgentPrintfN(id, "Sending Acceptance of instance %d with current ballot %d.%d and whose commands %d to all replicas", accept.Instance, accept.Ballot.Number, accept.Ballot.PropID, accept.WhoseCmd)
		//go func() {
		replica.Mutex.Lock()
		for i := 0; i < replica.N; i++ {
			if i == int(id) {
				continue
			}
			replica.SendMsgUNSAFE(int32(i), msg.GetType(), msg.GetSerialisable())
		}
		replica.Mutex.Unlock()
		//}()
	}
	return msg.GetSerialisable().(*stdpaxosproto.AcceptReply)
}

func acceptorHandlePrepareLocal(id int32, acc acceptor.Acceptor, replica *genericsmr.Replica, prepare *stdpaxosproto.Prepare, rpc PrepareResponsesRPC, promiseChan chan<- fastrpc.Serializable) {
	c := acc.RecvPrepareRemote(prepare)
	go func() {
		for msg := range c {
			if msg.ToWhom() != id {
				replica.SendMsg(msg.ToWhom(), msg.GetType(), msg.GetSerialisable())
				continue
			}
			preply := msg.GetSerialisable().(*stdpaxosproto.PrepareReply)
			if msg.IsNegative() {
				continue
			}
			if msg.GetType() == rpc.prepareReply {
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

func acceptorHandlePrepare(id int32, acc acceptor.Acceptor, prepare *stdpaxosproto.Prepare, rpc PrepareResponsesRPC, isAccMsgFilter bool, msgFilter chan<- *messageFilterComm, replica *genericsmr.Replica, bcastPrepare bool) {
	resp := acc.RecvPrepareRemote(prepare)
	go func() {
		for msg := range resp {
			if msg.GetType() == rpc.commit {
				cmt := msg.GetSerialisable().(*stdpaxosproto.Commit)
				dlog.AgentPrintfN(id, "Sending Commit to Replica %d for instance %d at ballot %d.%d with whose commands %d in response to a Prepare in instance %d at ballot %d.%d", prepare.PropID, cmt.Instance, cmt.Number, cmt.PropID, cmt.WhoseCmd, prepare.Instance, prepare.Number, prepare.PropID)
			}

			if msg.GetType() == rpc.prepareReply {
				preply := msg.GetSerialisable().(*stdpaxosproto.PrepareReply)
				isPreemptStr := isPreemptOrPromise(preply)
				if msg.IsNegative() && bcastPrepare {
					continue
				}
				dlog.AgentPrintfN(id, "Sending Prepare Reply (%s) to Replica %d for instance %d with current ballot %d.%d and value ballot %d.%d and whose commands %d in response to a Prepare in instance %d at ballot %d.%d",
					isPreemptStr, prepare.PropID, preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd, prepare.Instance, prepare.Number, prepare.PropID)
			}
			if msg.ToWhom() == id {
				continue
			}
			replica.SendMsg(msg.ToWhom(), msg.GetType(), msg.GetSerialisable())
		}
	}()
}

type AcceptResponsesRPC struct {
	acceptReply uint8
	commit      uint8
}

func acceptorHandleAcceptLocal(id int32, accptr acceptor.Acceptor, accept *stdpaxosproto.Accept, rpc AcceptResponsesRPC, acceptanceChan chan<- fastrpc.Serializable, replica *genericsmr.Replica, bcastAcceptance bool) {
	c := accptr.RecvAcceptRemote(accept)
	go func() {
		for msg := range c {
			if msg.ToWhom() != id {
				replica.SendMsg(msg.ToWhom(), msg.GetType(), msg.GetSerialisable())
				continue
			}

			acc := msg.GetSerialisable().(*stdpaxosproto.AcceptReply)
			if msg.GetType() == rpc.acceptReply {
				areply := msg.GetSerialisable().(*stdpaxosproto.AcceptReply)
				isPreemptStr := isPreemptOrAccept(areply)
				dlog.AgentPrintfN(id, "Sending Accept Reply (%s) to Replica %d for instance %d with current ballot %d.%d and whose commands %d in response to a Accept in instance %d at ballot %d.%d",
					isPreemptStr, accept.PropID, areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
			}
			if msg.IsNegative() {
				continue
			}
			if !bcastAcceptance && accept.LeaderId != -3 {
				acceptanceChan <- acc
				continue
			}
			// send acceptances to everyone
			dlog.AgentPrintfN(id, "Sending Acceptance of instance %d with current ballot %d.%d and whose commands %d to all replicas", accept.Instance, accept.Ballot.Number, accept.Ballot.PropID, accept.WhoseCmd)
			replica.Mutex.Lock()
			for i := 0; i < replica.N; i++ {
				if i == int(id) {
					continue
				}
				replica.SendMsgUNSAFE(int32(i), msg.GetType(), msg.GetSerialisable())
			}
			replica.Mutex.Unlock()
			acceptanceChan <- acc
		}
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

func acceptorHandleAccept(id int32, acc acceptor.Acceptor, accept *stdpaxosproto.Accept, rpc AcceptResponsesRPC, isAccMsgFilter bool, msgFilter chan<- *messageFilterComm, replica *genericsmr.Replica, bcastAcceptance bool, acceptanceChan chan<- fastrpc.Serializable, bcastPrepare bool) {
	dlog.AgentPrintfN(id, "Acceptor handing Accept from Replica %d in instance %d at ballot %d.%d as it can form a quorum", accept.PropID, accept.Instance, accept.Number, accept.PropID)
	responseC := acc.RecvAcceptRemote(accept)
	go func() {
		for resp := range responseC {
			if resp.ToWhom() == id {
				continue
			}

			if resp.GetType() == rpc.acceptReply {
				if resp.IsNegative() && bcastPrepare {
					continue
				}
				areply := resp.GetSerialisable().(*stdpaxosproto.AcceptReply)
				isPreemptStr := isPreemptOrAccept(areply)
				dlog.AgentPrintfN(id, "Sending Accept Reply (%s) to Replica %d for instance %d with current ballot %d.%d and whose commands %d in response to a Accept in instance %d at ballot %d.%d",
					isPreemptStr, accept.PropID, areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
			}

			if resp.IsNegative() {
				//if bcastPrepare {
				//continue
				//}
				replica.SendMsg(resp.ToWhom(), resp.GetType(), resp.GetSerialisable())
				continue
			}

			replica.Mutex.Lock()
			if !bcastAcceptance && accept.LeaderId != -3 { // if acceptance broadcast
				replica.SendMsgUNSAFE(resp.ToWhom(), resp.GetType(), resp.GetSerialisable())
				replica.Mutex.Unlock()
				continue
			}
			dlog.AgentPrintfN(id, "Sending Acceptance of instance %d with current ballot %d.%d and whose commands %d to all replicas", accept.Instance, accept.Ballot.Number, accept.Ballot.PropID, accept.WhoseCmd)
			for i := 0; i < replica.N; i++ {
				if i == int(id) {
					continue
				}
				replica.SendMsgUNSAFE(int32(i), resp.GetType(), resp.GetSerialisable())
			}
			replica.Mutex.Unlock()
			acceptanceChan <- resp.GetSerialisable()
		}
	}()
}
