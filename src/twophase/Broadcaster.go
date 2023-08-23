package twophase

import (
	"epaxos/dlog"
	"epaxos/fastrpc"
	"epaxos/genericsmr"
	"epaxos/state"
	"epaxos/stdpaxosproto"
	"epaxos/twophase/proposer"
	"math/rand"
)

type ListSender interface {
	BcastTo(group []int32, rcp uint8, msg fastrpc.UDPaxos)
}

type UnreliableUDPListSender struct {
	*genericsmr.Replica
}

func (u UnreliableUDPListSender) BcastTo(group []int32, rcp uint8, msg fastrpc.UDPaxos) {
	u.BcastUDPMsg(group, rcp, msg, false)
}

type ReliableUDPListSender struct {
	*genericsmr.Replica
}

func (r ReliableUDPListSender) BcastTo(group []int32, rcp uint8, msg fastrpc.UDPaxos) {
	r.BcastUDPMsg(group, rcp, msg, true)
}

type TCPListSender struct {
	*genericsmr.Replica
}

func (u *TCPListSender) BcastTo(group []int32, rcp uint8, msg fastrpc.UDPaxos) {
	u.Replica.SendToGroup(group, rcp, msg)
}

type PrepareBroadcaster interface {
	Bcast(instance int32, ballot stdpaxosproto.Ballot)
}

type PrepareSelfSender interface {
	Send(prepare *stdpaxosproto.Prepare)
}

type AcceptSelfSender interface {
	Send(accept *stdpaxosproto.Accept)
}

type AsyncPrepareSender struct {
	prepareChan chan fastrpc.Serializable
}

func (a *AsyncPrepareSender) Send(prepare *stdpaxosproto.Prepare) {
	go func() { a.prepareChan <- prepare }()
}

type SyncPrepareSender struct {
	id int32
	r  *Replica
}

func (s *SyncPrepareSender) Send(prepare *stdpaxosproto.Prepare) {
	promise := acceptorSyncHandlePrepareLocal(s.id, s.r.Acceptor, prepare, s.r.PrepareResponsesRPC)
	if promise == nil {
		return
	}
	s.r.HandlePromise(promise)
}

// too lazy to do right now
//type SyncAcceptSender struct {
//	id int32
//	r  *Replica
//}
//
//func (s *SyncAcceptSender) Send(accept *stdpaxosproto.Accept) {
//
//}

type AsyncAcceptSender struct {
	acceptChan chan fastrpc.Serializable
}

func (a *AsyncAcceptSender) Send(accept *stdpaxosproto.Accept) {
	go func() { a.acceptChan <- accept }()
}

type ValueBroadcaster interface {
	BcastAccept(instance int32, ballot stdpaxosproto.Ballot, whosecmds int32, cmds []*state.Command)
	BcastCommit(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32)
}

func getPrepare(instance int32, ballot stdpaxosproto.Ballot) stdpaxosproto.Prepare {
	return stdpaxosproto.Prepare{
		LeaderId: int32(ballot.PropID),
		Instance: instance,
		Ballot:   ballot,
	}
}

func getCommitShort(id int32, instance int32, ballot stdpaxosproto.Ballot, whose int32) stdpaxosproto.CommitShort {
	return stdpaxosproto.CommitShort{
		LeaderId: id,
		Instance: instance,
		Ballot:   ballot,
		Count:    0,
		WhoseCmd: whose,
	}
}

func getCommit(id int32, instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) stdpaxosproto.Commit {
	return stdpaxosproto.Commit{
		LeaderId:   id,
		Instance:   instance,
		Ballot:     ballot,
		WhoseCmd:   whose,
		MoreToCome: 0,
		Command:    command,
	}
}

func getActiveAccept(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) stdpaxosproto.Accept {
	return stdpaxosproto.Accept{
		LeaderId: -1,
		Instance: instance,
		Ballot:   ballot,
		WhoseCmd: whose,
		Command:  command,
	}
}

func getPassiveAccept(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) stdpaxosproto.Accept {
	return stdpaxosproto.Accept{
		LeaderId: -1,
		Instance: instance,
		Ballot:   ballot,
		WhoseCmd: whose,
		Command:  command,
	}
}

// tcp
// udp
// bcast prepare all
// bcast prepare to group
// bcast prepare to qrm

// bcast quickly to all //accept diskless noop --
// bcast accept qrm with disk
// bcast

type PrepareAllFromWriteAheadReplica struct {
	n   int32
	id  int32
	rpc uint8
	proposer.AcceptorQrmInfo
	PrepareSelfSender
	ListSender
	thrifty bool
	f       int32
}

func NewPrepareAllFromWriteAheadReplica(n int32, id int32, rpc uint8, f int32, thrifty bool, acceptorQrmInfo proposer.AcceptorQrmInfo, prepareSelfSender PrepareSelfSender, listSender ListSender) *PrepareAllFromWriteAheadReplica {
	return &PrepareAllFromWriteAheadReplica{n: n, id: id, rpc: rpc, AcceptorQrmInfo: acceptorQrmInfo, PrepareSelfSender: prepareSelfSender, ListSender: listSender, thrifty: thrifty, f: f}
}

func (b *PrepareAllFromWriteAheadReplica) Bcast(instance int32, ballot stdpaxosproto.Ballot) {
	args := getPrepare(instance, ballot)
	sentTo := make([]int32, 0, b.n)
	sendMsgs := make([]int32, 0, b.n)
	inGroup := false
	g := b.AcceptorQrmInfo.GetGroup(instance)
	for _, i := range g {
		if i == b.id {
			inGroup = true
			break
		}
	}
	if b.thrifty {
		rand.Shuffle(len(g), func(i, j int) {
			tmp := g[i]
			g[i] = g[j]
			g[j] = tmp
		})
	}
	for _, i := range g {
		if i == b.id {
			continue
		}
		sentTo = append(sentTo, i)
		sendMsgs = append(sendMsgs, i)
		if b.thrifty && int32(len(sentTo)) >= b.f {
			break
		}
	}
	if inGroup {
		b.PrepareSelfSender.Send(&args)
		sentTo = append(sentTo, b.id)
	}
	b.ListSender.BcastTo(sendMsgs, b.rpc, &args)
	dlog.AgentPrintfN(b.id, "Broadcasting Prepare for instance %d at ballot %d.%d to replicas %v", args.Instance, args.Number, args.PropID, sentTo)
}

type BcastFastLearning struct {
	acceptRPC      uint8
	commitShortRPC uint8
	id             int32
	n              int32
	aids           []int32
	proposer.AcceptorQrmInfo
	SendQrmSize
	ListSender // ReliableListSender
	AcceptSelfSender
}

func NewBcastFastLearning(acceptRPC uint8, commitShortRPC uint8, id int32, n int32, aids []int32, acceptorQrmInfo proposer.AcceptorQrmInfo, sendQrmSize SendQrmSize, listSender ListSender, acceptSelfSender AcceptSelfSender) *BcastFastLearning {
	return &BcastFastLearning{acceptRPC: acceptRPC, commitShortRPC: commitShortRPC, id: id, n: n, aids: aids, AcceptorQrmInfo: acceptorQrmInfo, SendQrmSize: sendQrmSize, ListSender: listSender, AcceptSelfSender: acceptSelfSender}
}

type SendQrmSize interface {
	GetBcastNum() int
}

type Thrifty struct {
	f int
}

func (t *Thrifty) GetBcastNum() int {
	return t.f + 1
}

type NonThrifty struct {
	f int
}

func (n *NonThrifty) GetBcastNum() int {
	return 2*n.f + 1
}

func (a *BcastFastLearning) BcastAccept(instance int32, ballot stdpaxosproto.Ballot, whosecmds int32, cmds []*state.Command) {
	// -1 = ack, -2 = passive observe, -3 = diskless accept
	pa := getActiveAccept(instance, ballot, cmds, whosecmds)
	paPassive := getPassiveAccept(instance, ballot, cmds, whosecmds)
	sendC := a.SendQrmSize.GetBcastNum()
	sentTo := make([]int32, 0, a.n)
	sendMsgsActive := make([]int32, 0, a.n)
	sendMsgsPassive := make([]int32, 0, a.n)
	acceptorGroup := a.AcceptorQrmInfo.GetGroup(instance)
	rand.Shuffle(len(acceptorGroup), func(i, j int) {
		tmp := acceptorGroup[i]
		acceptorGroup[i] = acceptorGroup[j]
		acceptorGroup[j] = tmp
	})
	selfInGroup := false

	// figure out first if we are in group (to prefer self)
	for _, peer := range acceptorGroup {
		if peer == a.id {
			selfInGroup = true
			sentTo = append(sentTo, a.id)
			break
		}
	}

	// get active group
	for _, peer := range acceptorGroup {
		if peer == a.id {
			continue
		}
		if len(sentTo) >= sendC {
			sentTo = append(sentTo, peer)
			sendMsgsPassive = append(sendMsgsActive, peer)
			continue
		}
		sendMsgsActive = append(sendMsgsActive, peer)
		sentTo = append(sentTo, peer)
	}

	// send to those not in group
	for peer := int32(0); peer < a.n; peer++ {
		if a.AcceptorQrmInfo.IsInGroup(instance, peer) {
			continue
		}
		sendMsgsPassive = append(sendMsgsPassive, peer)
		sentTo = append(sentTo, peer)
	}

	a.ListSender.BcastTo(sendMsgsActive, a.acceptRPC, &pa)
	a.ListSender.BcastTo(sendMsgsPassive, a.acceptRPC, &paPassive)
	if selfInGroup {
		a.AcceptSelfSender.Send(&pa)
	}
	dlog.AgentPrintfN(a.id, "Broadcasting Accept for instance %d with whose commands %d at ballot %d.%d to Replicas %v (active acceptors %v)", pa.Instance, pa.WhoseCmd, pa.Number, pa.PropID, sentTo, sendMsgsActive)
}

func (a *BcastFastLearning) BcastCommit(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) {
	argsShort := getCommitShort(a.id, instance, ballot, whose)
	a.ListSender.BcastTo(a.aids, a.commitShortRPC, &argsShort)
	dlog.AgentPrintfN(a.id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d", instance, argsShort.WhoseCmd, argsShort.Number, argsShort.PropID)
}

type QuorumCounter interface {
	HasAcked(q int32, instance int32, ballot stdpaxosproto.Ballot) bool
	GetAckersGroup(instance int32, ballot stdpaxosproto.Ballot) []int32
	GetConsensusGroup(instance int32, ballot stdpaxosproto.Ballot) []int32
}

type BcastSlowLearning struct {
	acceptRPC      uint8
	commitShortRPC uint8
	commitPRC      uint8
	id             int32
	n              int32
	aids           []int32
	SendQrmSize
	ListSender
	AcceptSelfSender
	QuorumCounter
}

func NewBcastSlowLearning(acceptRPC uint8, commitShortRPC uint8, commitPRC uint8, id int32, n int32, aids []int32, sendQrmSize SendQrmSize, listSender ListSender, acceptSelfSender AcceptSelfSender, quorumCounter QuorumCounter) *BcastSlowLearning {
	return &BcastSlowLearning{acceptRPC: acceptRPC, commitShortRPC: commitShortRPC, commitPRC: commitPRC, id: id, n: n, aids: aids, SendQrmSize: sendQrmSize, ListSender: listSender, AcceptSelfSender: acceptSelfSender, QuorumCounter: quorumCounter}
}

func (b *BcastSlowLearning) BcastAccept(instance int32, ballot stdpaxosproto.Ballot, whosecmds int32, cmds []*state.Command) {
	pa := getActiveAccept(instance, ballot, cmds, whosecmds)
	sendC := b.SendQrmSize.GetBcastNum()
	if state.CommandsEqual(state.DisklessNOOPP(), cmds) {
		pa.LeaderId = -3
	}
	sentTo := make([]int32, 0, b.n)
	acceptorGroup := b.QuorumCounter.GetConsensusGroup(instance, ballot)
	rand.Shuffle(len(acceptorGroup), func(i, j int) {
		tmp := acceptorGroup[i]
		acceptorGroup[i] = acceptorGroup[j]
		acceptorGroup[j] = tmp
	})
	selfInGroup := false
	for _, peer := range acceptorGroup {
		if peer == b.id {
			selfInGroup = true
			sentTo = append(sentTo, b.id)
			break
		}
	}
	for _, peer := range acceptorGroup {
		if peer == b.id {
			continue
		}
		if len(sentTo) >= sendC {
			break
		}
		sentTo = append(sentTo, peer)
	}
	if !selfInGroup {
		b.ListSender.BcastTo(sentTo, b.acceptRPC, &pa)
		dlog.AgentPrintfN(b.id, "Broadcasting Accept for instance %d with whose commands %d at ballot %d.%d to Replicas %v", pa.Instance, pa.WhoseCmd, pa.Number, pa.PropID, sentTo)
		return
	}
	b.AcceptSelfSender.Send(&pa)
	b.ListSender.BcastTo(sentTo[1:], b.acceptRPC, &pa)
	dlog.AgentPrintfN(b.id, "Broadcasting Accept for instance %d with whose commands %d at ballot %d.%d to Replicas %v", pa.Instance, pa.WhoseCmd, pa.Number, pa.PropID, sentTo)
}

func (b *BcastSlowLearning) BcastCommit(instance int32, ballot stdpaxosproto.Ballot, command []*state.Command, whose int32) {
	pc := getCommit(b.id, instance, ballot, command, whose)
	pcs := getCommitShort(b.id, instance, ballot, whose)
	sList := make([]int32, 0, b.id)
	list := make([]int32, 0, b.n)
	for q := int32(0); q < b.n; q++ {
		if q == b.id {
			continue
		}

		inQrm := b.QuorumCounter.HasAcked(q, instance, ballot)
		//inQrm := r.instanceSpace[instance].Qrms[lwcproto.ConfigBal{Config: -1, Ballot: ballot}].HasAcknowledged(q)
		if inQrm {
			sList = append(sList, q)
		} else {
			list = append(list, q)
		}
	}
	b.ListSender.BcastTo(list, b.commitPRC, &pc)
	b.ListSender.BcastTo(sList, b.commitShortRPC, &pcs)
	dlog.AgentPrintfN(b.id, "Broadcasting Commit for instance %d learnt with whose commands %d, at ballot %d.%d (short list %v, long list %v)", instance, pcs.WhoseCmd, pcs.Number, pcs.PropID, sList, list)

}
