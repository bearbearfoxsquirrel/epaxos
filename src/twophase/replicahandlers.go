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

//

func acceptorHandlePrepareLocal(id int32, acc acceptor.Acceptor, prepare *stdpaxosproto.Prepare, rpc PrepareResponsesRPC, promiseChan chan<- fastrpc.Serializable) {
	resp := acc.RecvPrepareRemote(prepare)
	go func(prepMsg *stdpaxosproto.Prepare, c <-chan acceptor.Message) {
		msg, msgokie := <-c
		if !msgokie {
			return
		}
		if msg.GetType() != rpc.prepareReply {
			return
		}

		prepResp := msg.GetSerialisable().(*stdpaxosproto.PrepareReply)
		if prepResp.Cur.GreaterThan(prepMsg.Ballot) {
			dlog.AgentPrintfN(id, "Preempt on Prepare by our Acceptor (Replica %d) on instance %d at round %d.%d with current round %d.%d with whose commands %d", id, prepResp.Instance, prepResp.Req.Number, prepResp.Req.PropID, prepResp.Cur.Number, prepResp.Cur.PropID, prepResp.WhoseCmd)
			return
		}
		dlog.AgentPrintfN(id, "Promise by our Acceptor (Replica %d) on instance %d at round %d.%d with whose commands %d", id, prepResp.Instance, prepResp.Cur.Number, prepResp.Cur.PropID, prepResp.WhoseCmd)
		promiseChan <- prepResp
	}(prepare, resp)
}

func isPreemptOrPromise(preply *stdpaxosproto.PrepareReply) string {
	isPreempt := preply.Cur.GreaterThan(preply.Req)
	isPreemptStr := "Preempt"
	if !isPreempt {
		isPreemptStr = "Promise"
	}
	return isPreemptStr
}

func acceptorHandlePrepare(id int32, acc acceptor.Acceptor, prepare *stdpaxosproto.Prepare, rpc PrepareResponsesRPC, isAccMsgFilter bool, msgFilter chan<- *messageFilterComm, replica *genericsmr.Replica) {
	resp := acc.RecvPrepareRemote(prepare)
	go func(response <-chan acceptor.Message) {
		for msg := range response {
			if isAccMsgFilter {
				if msg.IsNegative() {
					c := make(chan bool, 1)
					msgFilter <- &messageFilterComm{
						inst: prepare.Instance,
						ret:  c,
					}
					if yes := <-c; yes {
						if msg.GetType() == rpc.commit {
							cmt := msg.GetSerialisable().(*stdpaxosproto.Commit)
							dlog.AgentPrintfN(id, "Filtered Commit to Replica %d for instance %d at ballot %d.%d with whose commands %d in response to a Prepare in instance %d at ballot %d.%d", prepare.PropID, cmt.Instance, cmt.Number, cmt.PropID, cmt.WhoseCmd, prepare.Instance, prepare.Number, prepare.PropID)
						}

						if msg.GetType() == rpc.prepareReply {
							preply := msg.GetSerialisable().(*stdpaxosproto.PrepareReply)
							isPreemptStr := isPreemptOrPromise(preply)
							dlog.AgentPrintfN(id, "Filtered Prepare Reply (%s) to Replica %d for instance %d with current ballot %d.%d and value ballot %d.%d and whose commands %d in response to a Prepare in instance %d at ballot %d.%d",
								isPreemptStr, prepare.PropID, preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd, prepare.Instance, preply.Req.Number, preply.Req.PropID)
						}
						continue
					}
				}
			}
			if msg.GetType() == rpc.commit {
				cmt := msg.GetSerialisable().(*stdpaxosproto.Commit)
				dlog.AgentPrintfN(id, "Returning Commit to Replica %d for instance %d at ballot %d.%d with whose commands %d in response to a Prepare in instance %d at ballot %d.%d", prepare.PropID, cmt.Instance, cmt.Number, cmt.PropID, cmt.WhoseCmd, prepare.Instance, prepare.Number, prepare.PropID)
			}

			if msg.GetType() == rpc.prepareReply {
				preply := msg.GetSerialisable().(*stdpaxosproto.PrepareReply)
				isPreemptStr := isPreemptOrPromise(preply)
				dlog.AgentPrintfN(id, "Returning Prepare Reply (%s) to Replica %d for instance %d with current ballot %d.%d and value ballot %d.%d and whose commands %d in response to a Prepare in instance %d at ballot %d.%d",
					isPreemptStr, prepare.PropID, preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd, prepare.Instance, prepare.Number, prepare.PropID)
			}
			//responseHandler(msg)
			replica.SendMsg(msg.ToWhom(), msg.GetType(), msg)

		}
	}(resp)
}

type AcceptResponsesRPC struct {
	acceptReply uint8
	commit      uint8
}

func acceptorHandleAcceptLocal(id int32, accptr acceptor.Acceptor, accept *stdpaxosproto.Accept, rpc AcceptResponsesRPC, acceptanceChan chan<- fastrpc.Serializable, replica *genericsmr.Replica, bcastAcceptance bool) {
	c := accptr.RecvAcceptRemote(accept)
	go func(c <-chan acceptor.Message, acptMsg *stdpaxosproto.Accept) {
		if msg, msgokie := <-c; msgokie {
			if msg.GetType() != rpc.acceptReply {
				return
			}
			acc := msg.GetSerialisable().(*stdpaxosproto.AcceptReply)
			if acc.Cur.GreaterThan(acc.Req) {
				dlog.AgentPrintfN(id, "Preempt by our Acceptor (Replica %d) on Accept in instance %d at round %d.%d with whose commands %d", id, acc.Instance, acc.Cur.Number, acc.Cur.PropID, acc.WhoseCmd)
				return
			}
			dlog.AgentPrintfN(id, "Acceptance by our Acceptor (Replica %d) on instance %d at round %d.%d with whose commands %d", id, acc.Instance, acc.Cur.Number, acc.Cur.PropID, acc.WhoseCmd)
			acceptanceChan <- acc

			if bcastAcceptance && !msg.IsNegative() { // send acceptances to everyone
				for i := 0; i < replica.N; i++ {
					if i == int(id) {
						continue
					}
					replica.SendMsg(int32(i), msg.GetType(), msg)
				}
			}
		}
	}(c, accept)
}

func isPreemptOrAccept(areply *stdpaxosproto.AcceptReply) string {
	isPreempt := areply.Cur.GreaterThan(areply.Req)
	isPreemptStr := "Preempt"
	if !isPreempt {
		isPreemptStr = "Accept"
	}
	return isPreemptStr
}

func acceptorHandleAccept(id int32, acc acceptor.Acceptor, accept *stdpaxosproto.Accept, rpc AcceptResponsesRPC, isAccMsgFilter bool, msgFilter chan<- *messageFilterComm, replica *genericsmr.Replica, bcastAcceptance bool, acceptanceChan chan<- fastrpc.Serializable) {
	dlog.AgentPrintfN(id, "Acceptor handing Accept from Replica %d in instance %d at ballot %d.%d as it can form a quorum", accept.PropID, accept.Instance, accept.Number, accept.PropID)
	responseC := acc.RecvAcceptRemote(accept)
	go func(responseC <-chan acceptor.Message) {
		for resp := range responseC {
			if isAccMsgFilter {
				if resp.GetType() == rpc.commit {
					cmt := resp.GetSerialisable().(*stdpaxosproto.Commit)
					dlog.AgentPrintfN(id, "Returning Commit to Replica %d for instance %d at ballot %d.%d with whose commands %d in response to a Accept in instance %d at ballot %d.%d", accept.PropID, cmt.Instance, cmt.Number, cmt.PropID, cmt.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
				}

				if resp.GetType() == rpc.acceptReply {
					areply := resp.GetSerialisable().(*stdpaxosproto.AcceptReply)
					isPreemptStr := isPreemptOrAccept(areply)
					dlog.AgentPrintfN(id, "Returning Accept Reply (%d) to Replica %d for instance %d with current ballot %d.%d and whose commands %d in response to a Accept in instance %d at ballot %d.%d",
						isPreemptStr, accept.PropID, areply.Instance, areply.Cur.Number, areply.Cur.PropID, areply.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
				}

				if resp.IsNegative() {
					c := make(chan bool, 1)
					msgFilter <- &messageFilterComm{
						inst: accept.Instance,
						ret:  c,
					}
					if yes := <-c; yes {
						if resp.GetType() == rpc.commit {
							cmt := resp.GetSerialisable().(*stdpaxosproto.Commit)
							dlog.AgentPrintfN(id, "Filtered Commit to Replica %d for instance %d at ballot %d.%d with whose commands %d in response to a Accept in instance %d at ballot %d.%d", accept.PropID, cmt.Instance, cmt.Number, cmt.PropID, cmt.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
						}

						if resp.GetType() == rpc.acceptReply {
							preply := resp.GetSerialisable().(*stdpaxosproto.PrepareReply)
							isPreemptStr := isPreemptOrPromise(preply)
							dlog.AgentPrintfN(id, "Filtered Accept Reply (%s) to Replica %d for instance %d with current ballot %d.%d and value ballot %d.%d and whose commands %d in response to a Accept in instance %d at ballot %d.%d",
								isPreemptStr, accept.PropID, preply.Instance, preply.Cur.Number, preply.Cur.PropID, preply.VBal.Number, preply.VBal.PropID, preply.WhoseCmd, accept.Instance, accept.Number, accept.PropID)
						}
						return
					}
					replica.SendMsg(resp.ToWhom(), resp.GetType(), resp)
					continue
				}
			}
			if bcastAcceptance && !resp.IsNegative() { // if acceptance broadcast
				for i := 0; i < replica.N; i++ {
					if i == int(id) {
						acceptanceChan <- resp.GetSerialisable()
						continue
					}
					replica.SendMsg(int32(i), resp.GetType(), resp)
				}
			} else {
				replica.SendMsg(resp.ToWhom(), resp.GetType(), resp)

				if resp.GetType() == rpc.acceptReply && !resp.IsNegative() {
					acceptanceChan <- resp.GetSerialisable()
				}
			}
		}
	}(responseC)
}

// todo replace bcasts etc. with on chosen closures? What's the performance penalty of that?
