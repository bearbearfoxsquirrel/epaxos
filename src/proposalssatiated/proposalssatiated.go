package proposalssatiated

//
//import (
//	"acceptor"
//	"fastrpc"
//	"genericsmr"
//	"lwcproto"
//	"proposer"
//)
//
//type ProposerRanks struct {
//	pid int32
//	rank lwcproto.ConfigBal
//}
//
//type ProposalsSatiated struct {
//	Instance int32
//	MaxBallot lwcproto.ConfigBal
//	Proposers []ProposerRanks
//}
//
//type InstanceRecord struct {
//	proposers map[int32]lwcproto.ConfigBal
//	proposersArr []int32
//	maxProposalRecv lwcproto.ConfigBal
//}
//
//type Tracker struct {
//	rpc uint8
//	channel chan fastrpc.Serializable
//	bcastOnSatiated bool
//	instances map[int32]*InstanceRecord
//	//broadcaster
//	*genericsmr.Replica
//	threshold int
//	allProposers []int32
//	proposer.Proposer
//	acceptor.Acceptor
//}
//
//func TrackerNew(replica *genericsmr.Replica, thresthold int, bcastOnSatiated bool) *Tracker {
//	channel := make(chan fastrpc.Serializable)
//	t := Tracker{
//		rpc:             replica.RegisterRPC(new(ProposalsSatiated), channel),
//		channel:         channel,
//		bcastOnSatiated: bcastOnSatiated,
//		instances:       make(map[int32]*InstanceRecord),
//		Replica:         replica,
//		threshold:       thresthold,
//	}
//	return &t
//}
//
//func (t *Tracker) satiated(inst int32) bool {
//	return len(t.instances[inst].proposers) >= t.threshold
//}
//
//type ProposalSatiatedCode int32
//
//const (
//	IGNORED ProposalSatiatedCode = iota // Not part of group
//	ACKED // Part of group
//)
//
//
//// if doesn't exist
//// 	create
//// check if satiated
//
//func (t *Tracker) AcceptorHandlePrepareReceived(inst int32, pid int32, bal lwcproto.ConfigBal) ProposalSatiatedCode {
//	// check if exists, else create new
//	if _, exists := t.instances[inst]; !exists {
//		t.instances[inst] = &InstanceRecord{
//			proposers: make(map[int32]lwcproto.ConfigBal),
//			proposersArr: make([]int32, 0, t.threshold),
//		}
//	}
//
//	if t.satiated(inst) {
//		if _, existsP := t.instances[inst].proposers[pid]; existsP {
//			// a proposer who was in the satiating group is following up
//			// let them through as a fault might have occured
//			// should never have to reset even after >F faults because we have F+1 proposers in the group
//			t.checkAndUpdateMaxProposalReceived(inst, bal)
//			return ACKED
//		} else {
//			t.sendSatiatedMsg(pid, inst)
//			return IGNORED
//		}
//	} else {
//		if _, existsP := t.instances[inst].proposers[pid]; !existsP {
//			t.instances[inst].proposers[pid] = bal
//			//		t.instances[inst].proposersArr = append(t.instances[inst].proposersArr, pid)
//		}
//		t.checkAndUpdateMaxProposalReceived(inst, bal)
//		t.checkAndCloseOnInstance(inst)
//		return ACKED
//	}
//}
//
//func (t *Tracker) HandleSatiated(satiated ProposalsSatiated) {
//	// if it is relevant
//		// if we are not in the proposer group
//			// consider the instance closed. We not long should make proposals to it
//			// add all relevant info to it too
//		// else ignore
//	// ignore
//
//}
//
//func (t *Tracker) ProposerShouldTackleInstance(inst int32) bool {
//	// if is satiated and we are not part of it
//}
//
//func (t *Tracker) checkAndUpdateMaxProposalReceived(inst int32, bal lwcproto.ConfigBal) {
//	if bal.GreaterThan(t.instances[inst].maxProposalRecv) {
//		t.instances[inst].maxProposalRecv = bal
//	}
//}
//
//func (t *Tracker) getProposers(inst int32) []ProposerRanks {
//	proposersLen := len(t.instances[inst].proposers)
//	proposers := make([]ProposerRanks, proposersLen)
//	for i := int32(0); i < int32(t.N); i++ {
//		if bal, e := t.instances[inst].proposers[i]; e {
//			proposers[i] = ProposerRanks{i,bal}
//		}
//	}
//	return proposers
//}
//
//
//// upon satiated, broadcast out satiated to everyone and then return a promise on the max proposal
//func (t *Tracker) checkAndCloseOnInstance(inst int32) {
//	proposers := t.getProposers(inst)
//
//	if t.satiated(inst) && t.bcastOnSatiated {
//		msg := ProposalsSatiated{
//			Instance:  inst,
//			MaxBallot: t.instances[inst].maxProposalRecv,
//			Proposers: proposers,
//		}
//		for _, p := range t.allProposers {
//			if _, existsP := t.instances[inst].proposers[p]; existsP{
//				continue
//			} else {
//				t.SendMsg(p, t.rpc, &msg)
//			}
//		}
//		t.Acceptor.PromiseAndReturnMsg(t.instances[inst].maxProposalRecv)
//	}
//}
//
//func (t *Tracker) sendSatiatedMsg(pid int32, inst int32) {
//	msg := ProposalsSatiated{
//		Instance:  inst,
//		MaxBallot: t.instances[inst].maxProposalRecv,
//		Proposers: t.instances[inst].proposersArr,
//	}
//	t.Replica.SendMsg(pid, t.rpc, &msg)
//}
////func (t *ProposalsSatiatedTracker) IsSatiated(inst int32) bool {
////
////}
////
////func (t *ProposalsSatiatedTracker) IsInGroup(inst int32, pid int32) bool {
////
////}
//
////func (t *ProposalsSatiatedTracker) Reset(inst int32) {
////	// should not be needed
////	delete(t.instances, inst)
////}
//
//
//// cannot remember once an instance is closed so need to handle this situation elsewere
//// e.g. guard around closed
//func (t *Tracker) Close(inst int32) {
//	delete(t.instances, inst)
//}
//
