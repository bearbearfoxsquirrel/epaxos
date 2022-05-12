package twophase

import (
	"dlog"
	"instanceagentmapper"
	"quorumsystem"
	"stdpaxosproto"
)

type Quormaliser interface {
	ProposerQuorumaliser
	LearnerQuorumaliser
	AcceptorQrmInfo
}

type ProposerQuorumaliser interface {
	startPromiseQuorumOnCurBal(pbk *ProposingBookkeeping, inst int32)
}

type LearnerQuorumaliser interface {
	trackProposalAcceptance(pbk *ProposingBookkeeping, inst int32, bal stdpaxosproto.Ballot)
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

func (qrmliser *Standard) startPromiseQuorumOnCurBal(pbk *ProposingBookkeeping, inst int32) {
	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(qrmliser.Aids)
	pbk.qrms[pbk.propCurBal] = quorumaliser
	pbk.qrms[pbk.propCurBal].StartPromiseQuorum()
}

func (qrmliser *Standard) trackProposalAcceptance(pbk *ProposingBookkeeping, inst int32, bal stdpaxosproto.Ballot) {
	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(qrmliser.Aids)
	pbk.qrms[bal] = quorumaliser
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

func (qrmliser *Minimal) startPromiseQuorumOnCurBal(pbk *ProposingBookkeeping, inst int32) {
	//make quorum
	if _, exists := qrmliser.MapperCache[inst]; !exists {
		qrmliser.MapperCache[inst] = qrmliser.AcceptorMapper.GetGroup(int(inst))
	}
	group := qrmliser.MapperCache[inst]
	dlog.AgentPrintfN(qrmliser.MyID, "Minimal acceptor group for instance %d is %v", inst, group)
	//log.Println("group for instance", inst, ":", group)
	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(group)
	pbk.qrms[pbk.propCurBal] = quorumaliser
	pbk.qrms[pbk.propCurBal].StartPromiseQuorum()
}

func (qrmliser *Minimal) trackProposalAcceptance(pbk *ProposingBookkeeping, inst int32, bal stdpaxosproto.Ballot) {
	//make quorum
	if _, exists := qrmliser.MapperCache[inst]; !exists {
		qrmliser.MapperCache[inst] = qrmliser.AcceptorMapper.GetGroup(int(inst))
	}
	group := qrmliser.MapperCache[inst]
	dlog.AgentPrintfN(qrmliser.MyID, "Minimal acceptor group for instance %d is %v", inst, group)

	quorumaliser := qrmliser.SynodQuorumSystemConstructor.Construct(group)
	pbk.qrms[bal] = quorumaliser
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
