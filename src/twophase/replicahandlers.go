package twophase

import (
	"acceptor"
	"dlog"
	"fastrpc"
	"genericsmr"
	"stdpaxosproto"
)

type PrepareResponsesRPC struct {
	prepareReply uint8
	commit       uint8
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
				dlog.AgentPrintfN(id, "Returning Prepare Reply (%s) to Replica %d for instance %d with current ballot %d.%d and value ballot %d.%d and whose commands %d in response to a Prepare in instance %d at ballot %d.%d",
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
			//if isAccMsgFilter {
			//	if msg.IsNegative() {
			//		c := make(chan bool, 1)
			//		msgFilter <- &messageFilterComm{
			//			inst: prepare.Instance,
			//			ret:  c,
			//		}
			//		if yes := <-c; yes {
			//			if msg.GetType() == rpc.commit {
			//				cmt := msg.GetSerialisable().(*stdpaxosproto.Commit)
			//				dlog.AgentPrintfN(id, "Filtered Commit to Replica %d for instance %d at ballot %d.%d with whose commands %d in response to a Prepare in instance %d at ballot %d.%d", prepare.PropID, cmt.Instance, cmt.Number, cmt.PropID, cmt.WhoseCmd, prepare.Instance, prepare.Number, prepare.PropID)
			//			}
			//
			//			if msg.GetType() == rpc.prepareReply {
			//				preply := msg.GetSerialisable().(*stdpaxosproto.PrepareReply)
			//				isPreemptStr := isPreemptOrPromise(preply)
			//				dlog.AgentPrintfN(id, "Filtered Prepare Reply (%s) to Replica %d for instance %d with current ballot %d.%d and value ballot %d.%d and whose commands %d in response to a Prepare in instance %d at ballot %d.%d",
			//					isPreemptStr, prepare.PropID, preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd, prepare.Instance, preply.Req.Number, preply.Req.PropID)
			//			}
			//			continue
			//		}
			//	}
			//}

			if msg.GetType() == rpc.commit {
				cmt := msg.GetSerialisable().(*stdpaxosproto.Commit)
				dlog.AgentPrintfN(id, "Returning Commit to Replica %d for instance %d at ballot %d.%d with whose commands %d in response to a Prepare in instance %d at ballot %d.%d", prepare.PropID, cmt.Instance, cmt.Number, cmt.PropID, cmt.WhoseCmd, prepare.Instance, prepare.Number, prepare.PropID)
			}

			if msg.GetType() == rpc.prepareReply {
				preply := msg.GetSerialisable().(*stdpaxosproto.PrepareReply)
				isPreemptStr := isPreemptOrPromise(preply)
				if msg.IsNegative() && bcastPrepare {
					continue
				}
				dlog.AgentPrintfN(id, "Returning Prepare Reply (%s) to Replica %d for instance %d with current ballot %d.%d and value ballot %d.%d and whose commands %d in response to a Prepare in instance %d at ballot %d.%d",
					isPreemptStr, prepare.PropID, preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd, prepare.Instance, prepare.Number, prepare.PropID)
			}
			if msg.ToWhom() == id {
				continue
			}
			replica.SendMsg(msg.ToWhom(), msg.GetType(), msg)
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
				dlog.AgentPrintfN(id, "Returning Accept Reply (%s) to Replica %d for instance %d with current ballot %d.%d and whose commands %d in response to a Accept in instance %d at ballot %d.%d",
					isPreemptStr, accept.PropID, areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
			}
			if msg.IsNegative() {
				continue
			}
			//go func() { acceptanceChan <- acc }()

			if !bcastAcceptance { // send acceptances to everyone
				acceptanceChan <- acc
				continue
			}
			dlog.AgentPrintfN(id, "Broadcasting Acceptance of %d with current ballot %d.%d and whose commands %d to all replicas", accept.Instance, accept.Ballot.Number, accept.Ballot.PropID, accept.WhoseCmd)
			for i := 0; i < replica.N; i++ {
				if i == int(id) {
					continue
				}
				replica.SendMsg(int32(i), msg.GetType(), msg)
			}
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
			//if isAccMsgFilter {
			//	if resp.GetType() == rpc.commit {
			//		cmt := resp.GetSerialisable().(*stdpaxosproto.Commit)
			//		dlog.AgentPrintfN(id, "Returning Commit to Replica %d for instance %d at ballot %d.%d with whose commands %d in response to a Accept in instance %d at ballot %d.%d", accept.PropID, cmt.Instance, cmt.Number, cmt.PropID, cmt.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
			//	}
			//
			//	if resp.GetType() == rpc.acceptReply {
			//		areply := resp.GetSerialisable().(*stdpaxosproto.AcceptReply)
			//		isPreemptStr := isPreemptOrAccept(areply)
			//		dlog.AgentPrintfN(id, "Returning Accept Reply (%d) to Replica %d for instance %d with current ballot %d.%d and whose commands %d in response to a Accept in instance %d at ballot %d.%d",
			//			isPreemptStr, accept.PropID, areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
			//	}
			//
			//	if resp.IsNegative() {
			//		c := make(chan bool, 1)
			//		msgFilter <- &messageFilterComm{
			//			inst: accept.Instance,
			//			ret:  c,
			//		}
			//		if yes := <-c; yes {
			//			if resp.GetType() == rpc.commit {
			//				cmt := resp.GetSerialisable().(*stdpaxosproto.Commit)
			//				dlog.AgentPrintfN(id, "Filtered Commit to Replica %d for instance %d at ballot %d.%d with whose commands %d in response to a Accept in instance %d at ballot %d.%d", accept.PropID, cmt.Instance, cmt.Number, cmt.PropID, cmt.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
			//			}
			//
			//			if resp.GetType() == rpc.acceptReply {
			//				preply := resp.GetSerialisable().(*stdpaxosproto.PrepareReply)
			//				isPreemptStr := isPreemptOrPromise(preply)
			//				dlog.AgentPrintfN(id, "Filtered Accept Reply (%s) to Replica %d for instance %d with current ballot %d.%d and value ballot %d.%d and whose commands %d in response to a Accept in instance %d at ballot %d.%d",
			//					isPreemptStr, accept.PropID, preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
			//			}
			//			return
			//		}
			//		replica.SendMsg(resp.ToWhom(), resp.GetType(), resp)
			//		continue
			//	}
			//}

			if resp.GetType() == rpc.acceptReply {
				areply := resp.GetSerialisable().(*stdpaxosproto.AcceptReply)
				isPreemptStr := isPreemptOrAccept(areply)
				dlog.AgentPrintfN(id, "Returning Accept Reply (%s) to Replica %d for instance %d with current ballot %d.%d and whose commands %d in response to a Accept in instance %d at ballot %d.%d",
					isPreemptStr, accept.PropID, areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
			}
			if resp.IsNegative() {
				if resp.IsNegative() && resp.GetType() == rpc.acceptReply && bcastPrepare {
					continue
				}
				replica.SendMsg(resp.ToWhom(), resp.GetType(), resp)
				continue
			}

			//go func() {}()
			if !bcastAcceptance { // if acceptance broadcast
				acceptanceChan <- resp.GetSerialisable()
				continue
			}
			dlog.AgentPrintfN(id, "Broadcasting Acceptance of %d with current ballot %d.%d and whose commands %d to all replicas", accept.Instance, accept.Ballot.Number, accept.Ballot.PropID, accept.WhoseCmd)
			for i := 0; i < replica.N; i++ {
				if i == int(id) {
					continue
				}
				replica.SendMsg(int32(i), resp.GetType(), resp)
			}
			acceptanceChan <- resp.GetSerialisable()
		}
	}()
}

// todo replace bcasts etc. with on chosen closures? What's the performance penalty of that?
