package proposalmanager

import (
	"dlog"
	"instanceagentmapper"
	"lwcproto"
	"quorumsystem"
)

type Quormaliser interface {
	ProposerQuorumaliser
	LearnerQuorumaliser
	AcceptorQrmInfo
}

type ProposerQuorumaliser interface {
	StartPromiseQuorumOnCurBal(pbk *ProposingBookkeeping, inst int32)
}

type LearnerQuorumaliser interface {
	TrackProposalAcceptance(pbk *ProposingBookkeeping, inst int32, bal lwcproto.ConfigBal)
}

type AcceptorQrmInfo interface {
	IsInQrm(inst int32, aid int32) bool
	GetQrm(inst int32) []int
}

////////////////////////////////
// STANDARD
///////////////////////////////
type Standard struct {
	quorumsystem.SynodQuorumSystemConstructor
	Aids []int
	MyID int32
}

func (qrmliser *Standard) IsInQrm(inst int32, aid int32) bool {
	return true
}

func (qrmliser *Standard) StartPromiseQuorumOnCurBal(pbk *ProposingBookkeeping, inst int32) {
	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(qrmliser.Aids)
	pbk.Qrms[pbk.PropCurBal] = quorumaliser
	pbk.Qrms[pbk.PropCurBal].StartPromiseQuorum()
}

func (qrmliser *Standard) TrackProposalAcceptance(pbk *ProposingBookkeeping, inst int32, bal lwcproto.ConfigBal) {
	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(qrmliser.Aids)
	pbk.Qrms[bal] = quorumaliser
	quorumaliser.StartAcceptanceQuorum()
}

func (qrmliser *Standard) GetQrm(inst int32) []int {
	return qrmliser.Aids
}

//////////////////////////////////
// MINIMAL
/////////////////////////////////
type Minimal struct {
	AcceptorMapper instanceagentmapper.InstanceAgentMapper
	MapperCache    map[int32][]int
	quorumsystem.SynodQuorumSystemConstructor
	MyID int32
}

func (qrmliser *Minimal) StartPromiseQuorumOnCurBal(pbk *ProposingBookkeeping, inst int32) {
	//make quorum
	if _, exists := qrmliser.MapperCache[inst]; !exists {
		qrmliser.MapperCache[inst] = qrmliser.AcceptorMapper.GetGroup(int(inst))
	}
	group := qrmliser.MapperCache[inst]
	dlog.AgentPrintfN(qrmliser.MyID, "Minimal acceptor group for instance %d is %v", inst, group)
	//log.Println("group for instance", inst, ":", group)
	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(group)
	pbk.Qrms[pbk.PropCurBal] = quorumaliser
	pbk.Qrms[pbk.PropCurBal].StartPromiseQuorum()
}

func (qrmliser *Minimal) TrackProposalAcceptance(pbk *ProposingBookkeeping, inst int32, bal lwcproto.ConfigBal) {
	//make quorum
	if _, exists := qrmliser.MapperCache[inst]; !exists {
		qrmliser.MapperCache[inst] = qrmliser.AcceptorMapper.GetGroup(int(inst))
	}
	group := qrmliser.MapperCache[inst]
	dlog.AgentPrintfN(qrmliser.MyID, "Minimal acceptor group for instance %d is %v", inst, group)

	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(group)
	pbk.Qrms[bal] = quorumaliser
	quorumaliser.StartAcceptanceQuorum()
}

func (qrmliser *Minimal) IsInQrm(inst int32, aid int32) bool {
	if _, exists := qrmliser.MapperCache[inst]; !exists {
		qrmliser.MapperCache[inst] = qrmliser.AcceptorMapper.GetGroup(int(inst))
	}

	for _, aidInQrm := range qrmliser.MapperCache[inst] {
		if int(aid) == aidInQrm {
			return true
		}
	}

	return false
}

func (qrmliser *Minimal) GetQrm(inst int32) []int {
	if _, exists := qrmliser.MapperCache[inst]; !exists {
		qrmliser.MapperCache[inst] = qrmliser.AcceptorMapper.GetGroup(int(inst))
	}
	return qrmliser.MapperCache[inst]
}
