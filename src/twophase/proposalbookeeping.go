package twophase

import (
	"genericsmr"
	"instanceacceptormapper"
	"quorumsystem"
	"state"
	"stdpaxosproto"
)

//
//type BallotlotProposingBookkeeping struct {
//	status ProposerStatus
//	ProposalTracking
//	//BallotlotTracker
//	ValueTracker
//	maxAccepted stdpaxosproto.Ballot
//	maxKnown    stdpaxosproto.Ballot
//}

type ProposingBookkeeping struct {
	status        ProposerStatus
	proposalInfos map[stdpaxosproto.Ballot]quorumsystem.SynodQuorumSystem
	//proposalInfos      map[stdpaxosproto.Ballot]*QuorumInfo
	maxAcceptedBal  stdpaxosproto.Ballot // highest maxAcceptedBal at which a command was accepted
	whoseCmds       int32
	cmds            []state.Command      // the accepted command
	propCurBal      stdpaxosproto.Ballot // highest this replica maxAcceptedBal tried so far
	clientProposals []*genericsmr.Propose
	maxKnownBal     stdpaxosproto.Ballot
}

type ProposalManager interface {
	beginNewProposal(replica ConfigRoundProposable, inst int32)
	trackProposalAcceptance(replica ConfigRoundProposable, inst int32, bal stdpaxosproto.Ballot)
	getBalloter() Balloter
}

type ReducedQuorumProposalInitiator struct {
	AcceptorMapper instanceacceptormapper.InstanceAcceptorMapper
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

func (proposalConstructor *NormalQuorumProposalInitiator) beginNewProposal(r ConfigRoundProposable, inst int32) {
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

func (proposalConstructor *ReducedQuorumProposalInitiator) beginNewProposal(r ConfigRoundProposable, inst int32) {
	pbk := r.GetPBK(inst)
	beginPreparingBallot(pbk, proposalConstructor.Balloter)
	//make quorum
	group := proposalConstructor.AcceptorMapper.GetGroup(int(inst))
	//log.Println("group for instance", inst, ":", group)
	quorumaliser := proposalConstructor.SynodQuorumSystemConstructor.Construct(group)
	pbk.proposalInfos[pbk.propCurBal] = quorumaliser
	pbk.proposalInfos[pbk.propCurBal].StartPromiseQuorum()
}

func (proposalConstructor *ReducedQuorumProposalInitiator) trackProposalAcceptance(r ConfigRoundProposable, inst int32, bal stdpaxosproto.Ballot) {
	pbk := r.GetPBK(inst)
	//make quorum
	group := proposalConstructor.AcceptorMapper.GetGroup(int(inst))
	quorumaliser := proposalConstructor.SynodQuorumSystemConstructor.Construct(group)
	pbk.proposalInfos[bal] = quorumaliser
	quorumaliser.StartAcceptanceQuorum()
}

func (proposalConstructor *ReducedQuorumProposalInitiator) getBalloter() Balloter {
	return proposalConstructor.Balloter
}
