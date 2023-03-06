package proposer

import (
	"epaxos/dlog"
	"epaxos/instanceagentmapper"
	"epaxos/lwcproto"
	"epaxos/quorumsystem"
)

type InstanceQuormaliser interface {
	ProposerInstanceQuorumaliser
	LearnerQuorumaliser
	AcceptorQrmInfo
}

//type Quorumable interface {
//	BeginQrm(map[int32]map[stdpaxosproto.Ballot]A)
//}

type ProposerInstanceQuorumaliser interface {
	StartPromiseQuorumOnCurBal(pbk *PBK, inst int32)
}

type LearnerQuorumaliser interface {
	TrackProposalAcceptance(pbk *PBK, inst int32, bal lwcproto.ConfigBal)
}

type AcceptorQrmInfo interface {
	IsInGroup(inst int32, aid int32) bool
	GetGroup(inst int32) []int32
}

// //////////////////////////////
// STANDARD
// /////////////////////////////
type Standard struct {
	quorumsystem.SynodQuorumSystemConstructor
	Aids []int32
	MyID int32
}

func (qrmliser *Standard) IsInGroup(inst int32, aid int32) bool {
	return true
}

func (qrmliser *Standard) StartPromiseQuorumOnCurBal(pbk *PBK, inst int32) {
	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(qrmliser.Aids)
	pbk.Qrms[pbk.PropCurBal] = quorumaliser
	pbk.Qrms[pbk.PropCurBal].StartPromiseQuorum()
}

func (qrmliser *Standard) TrackProposalAcceptance(pbk *PBK, inst int32, bal lwcproto.ConfigBal) {
	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(qrmliser.Aids)
	pbk.Qrms[bal] = quorumaliser
	quorumaliser.StartAcceptanceQuorum()
}

func (qrmliser *Standard) GetGroup(inst int32) []int32 {
	return qrmliser.Aids
}

//////////////////////////////////
// MINIMAL
/////////////////////////////////

//type MinimalSpecific struct {
//	acceptorGroups [][]int32
//	quorumsystem.SynodQuorumSystemConstructor
//	GroupGetter
//}
//
//func (q *MinimalSpecific) Add(Id int32) {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (q *MinimalSpecific) Reached() bool {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (q *MinimalSpecific) Acknowledged(i int32) bool {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (q *MinimalSpecific) IsInGroup(inst int32, aid int32) bool {
//	gi := q.gForInst(inst)
//	g := q.acceptorGroups[gi]
//	for i := 0; i < len(g); i++ {
//		if g[i] == aid {
//			return true
//		}
//	}
//	return false
//}
//
//func (q *MinimalSpecific) gForInst(inst int32) int32 {
//	return inst % int32(len(q.acceptorGroups))
//}
//
//func (q *MinimalSpecific) StartPromiseQuorumOnCurBal(pbk *PBK, inst int32) {
//	quorumaliser := q.SynodQuorumSystemConstructor.Construct(q.acceptorGroups[q.gForInst(inst)])
//	pbk.Qrms[pbk.PropCurBal] = quorumaliser
//	pbk.Qrms[pbk.PropCurBal].StartPromiseQuorum()
//}

type Minimal struct {
	AcceptorMapper instanceagentmapper.InstanceAgentMapper
	MapperCache    map[int32][]int32
	quorumsystem.SynodQuorumSystemConstructor
	MyID int32
}

func (qrmliser *Minimal) StartPromiseQuorumOnCurBal(pbk *PBK, inst int32) {
	//make quorum
	if _, exists := qrmliser.MapperCache[inst]; !exists {
		qrmliser.MapperCache[inst] = qrmliser.AcceptorMapper.GetGroup(inst)
	}
	group := qrmliser.MapperCache[inst]
	dlog.AgentPrintfN(qrmliser.MyID, "Minimal acceptor group for instance %d is %v", inst, group)
	//log.Println("group for instance", inst, ":", group)
	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(group)
	pbk.Qrms[pbk.PropCurBal] = quorumaliser
	pbk.Qrms[pbk.PropCurBal].StartPromiseQuorum()
}

func (qrmliser *Minimal) TrackProposalAcceptance(pbk *PBK, inst int32, bal lwcproto.ConfigBal) {
	//make quorum
	if _, exists := qrmliser.MapperCache[inst]; !exists {
		qrmliser.MapperCache[inst] = qrmliser.AcceptorMapper.GetGroup(inst)
	}
	group := qrmliser.MapperCache[inst]
	dlog.AgentPrintfN(qrmliser.MyID, "Minimal acceptor group for instance %d is %v", inst, group)

	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(group)
	pbk.Qrms[bal] = quorumaliser
	quorumaliser.StartAcceptanceQuorum()
}

func (qrmliser *Minimal) IsInGroup(inst int32, aid int32) bool {
	if _, exists := qrmliser.MapperCache[inst]; !exists {
		qrmliser.MapperCache[inst] = qrmliser.AcceptorMapper.GetGroup(inst)
	}

	for _, aidInQrm := range qrmliser.MapperCache[inst] {
		if aid == aidInQrm {
			return true
		}
	}

	return false
}

func (qrmliser *Minimal) GetGroup(inst int32) []int32 {
	if _, exists := qrmliser.MapperCache[inst]; !exists {
		qrmliser.MapperCache[inst] = qrmliser.AcceptorMapper.GetGroup(inst)
	}
	return qrmliser.MapperCache[inst]
}

type StaticMapped struct {
	AcceptorMapper instanceagentmapper.InstanceAgentMapper
	//MapperCache    map[int32][]int32
	quorumsystem.SynodQuorumSystemConstructor
	MyID int32
}

func (qrl StaticMapped) StartPromiseQuorumOnCurBal(pbk *PBK, inst int32) {
	group := qrl.GetGroup(inst)
	dlog.AgentPrintfN(qrl.MyID, "Acceptor group for instance %d is %v", inst, group)
	quorumaliser := qrl.SynodQuorumSystemConstructor.Construct(group)
	pbk.Qrms[pbk.PropCurBal] = quorumaliser
	pbk.Qrms[pbk.PropCurBal].StartPromiseQuorum()
}

func (qrl StaticMapped) TrackProposalAcceptance(pbk *PBK, inst int32, bal lwcproto.ConfigBal) {
	group := qrl.GetGroup(inst)
	dlog.AgentPrintfN(qrl.MyID, "Acceptor group for instance %d is %v", inst, group)
	quorumaliser := qrl.SynodQuorumSystemConstructor.Construct(group)
	pbk.Qrms[pbk.PropCurBal] = quorumaliser
	pbk.Qrms[pbk.PropCurBal].StartAcceptanceQuorum()
}

func (qrl StaticMapped) IsInGroup(inst int32, aid int32) bool {
	for _, aidInQrm := range qrl.GetGroup(inst) {
		if aid == aidInQrm {
			return true
		}
	}
	return false
}

func (qrl StaticMapped) GetGroup(inst int32) []int32 {
	return qrl.AcceptorMapper.GetGroup(inst)

}
