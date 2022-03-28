package twophase

import (
	"instanceagentmapper"
	"quorumsystem"
	"state"
	"stdpaxosproto"
)

type ProposingBookkeeping struct {
	status        ProposerStatus
	proposalInfos map[stdpaxosproto.Ballot]quorumsystem.SynodQuorumSystem
	//proposalInfos      map[stdpaxosproto.Ballot]*QuorumInfo
	proposeValueBal stdpaxosproto.Ballot // highest proposeValueBal at which a command was accepted
	whoseCmds       int32
	cmds            []*state.Command     // the accepted command
	propCurBal      stdpaxosproto.Ballot // highest this replica proposeValueBal tried so far
	clientProposals proposalBatch
	maxKnownBal     stdpaxosproto.Ballot
}

type ProposalManager interface {
	StartNewProposal(replica ConfigRoundProposable, inst int32)
	trackProposalAcceptance(replica ConfigRoundProposable, inst int32, bal stdpaxosproto.Ballot)
	getBalloter() Balloter
	IsInQrm(inst int32, aid int32) bool
	//Close(inst int32)
}

type ReducedQuorumProposalInitiator struct {
	AcceptorMapper instanceagentmapper.InstanceAcceptorMapper
	MapperCache    map[int32][]int
	quorumsystem.SynodQuorumSystemConstructor
	Balloter
}

type NormalQuorumProposalInitiator struct {
	quorumsystem.SynodQuorumSystemConstructor
	Balloter
	Aids []int
}

func beginPreparingBallot(pbk *ProposingBookkeeping, balloter Balloter) {
	pbk.status = PREPARING
	nextBal := balloter.getNextProposingBal(pbk.maxKnownBal.Number)
	nextBallot := nextBal
	pbk.propCurBal = nextBallot
	pbk.maxKnownBal = nextBal
}

type ConfigRoundProposable interface {
	GetPBK(inst int32) *ProposingBookkeeping
}

//
//func (r *ELPReplica) GetPBK(inst int32) *ProposingBookkeeping {
//	return r.instanceSpace[inst].pbk
//}

func (proposalConstructor *NormalQuorumProposalInitiator) IsInQrm(inst int32, aid int32) bool {
	return true
}

func (proposalConstructor *NormalQuorumProposalInitiator) StartNewProposal(r ConfigRoundProposable, inst int32) {
	pbk := r.GetPBK(inst)
	beginPreparingBallot(pbk, proposalConstructor.Balloter)
	quorumaliser := proposalConstructor.SynodQuorumSystemConstructor.Construct(proposalConstructor.Aids)
	pbk.proposalInfos[pbk.propCurBal] = quorumaliser
	pbk.proposalInfos[pbk.propCurBal].StartPromiseQuorum()
}

func (proposalConstructor *NormalQuorumProposalInitiator) trackProposalAcceptance(r ConfigRoundProposable, inst int32, bal stdpaxosproto.Ballot) {
	pbk := r.GetPBK(inst)
	quorumaliser := proposalConstructor.SynodQuorumSystemConstructor.Construct(proposalConstructor.Aids)
	pbk.proposalInfos[bal] = quorumaliser
	quorumaliser.StartAcceptanceQuorum()
}

func (proposalConstructor *NormalQuorumProposalInitiator) getBalloter() Balloter {
	return proposalConstructor.Balloter
}

func (proposalConstructor *ReducedQuorumProposalInitiator) StartNewProposal(r ConfigRoundProposable, inst int32) {
	pbk := r.GetPBK(inst)
	beginPreparingBallot(pbk, proposalConstructor.Balloter)
	//make quorum
	if _, exists := proposalConstructor.MapperCache[inst]; !exists {
		proposalConstructor.MapperCache[inst] = proposalConstructor.AcceptorMapper.GetGroup(int(inst))
	}
	group := proposalConstructor.MapperCache[inst]
	//log.Println("group for instance", inst, ":", group)
	quorumaliser := proposalConstructor.SynodQuorumSystemConstructor.Construct(group)
	pbk.proposalInfos[pbk.propCurBal] = quorumaliser
	pbk.proposalInfos[pbk.propCurBal].StartPromiseQuorum()
}

func (proposalConstructor *ReducedQuorumProposalInitiator) trackProposalAcceptance(r ConfigRoundProposable, inst int32, bal stdpaxosproto.Ballot) {
	pbk := r.GetPBK(inst)
	//make quorum
	if _, exists := proposalConstructor.MapperCache[inst]; !exists {
		proposalConstructor.MapperCache[inst] = proposalConstructor.AcceptorMapper.GetGroup(int(inst))
	}
	group := proposalConstructor.MapperCache[inst]

	quorumaliser := proposalConstructor.SynodQuorumSystemConstructor.Construct(group)
	pbk.proposalInfos[bal] = quorumaliser
	quorumaliser.StartAcceptanceQuorum()
}

func (proposalConstructor *ReducedQuorumProposalInitiator) getBalloter() Balloter {
	return proposalConstructor.Balloter
}

func (proposalConstructor *ReducedQuorumProposalInitiator) IsInQrm(inst int32, aid int32) bool {
	if _, exists := proposalConstructor.MapperCache[inst]; !exists {
		proposalConstructor.MapperCache[inst] = proposalConstructor.AcceptorMapper.GetGroup(int(inst))
	}

	for _, aidInQrm := range proposalConstructor.MapperCache[inst] {
		if int(aid) == aidInQrm {
			return true
		}
	}

	return false
}
