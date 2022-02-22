package configtwophase

import (
	"genericsmr"
	"instanceacceptormapper"
	"lwcproto"
	"quorumsystem"
	"state"
)

//
//type ConfigBallotProposingBookkeeping struct {
//	status ProposerStatus
//	ProposalTracking
//	//ConfigBallotTracker
//	ValueTracker
//	maxAccepted lwcproto.ConfigBal
//	maxKnown    lwcproto.ConfigBal
//}

type ProposingBookkeeping struct {
	status        ProposerStatus
	proposalInfos map[lwcproto.ConfigBal]quorumsystem.SynodQuorumSystem
	//proposalInfos      map[lwcproto.ConfigBal]*QuorumInfo
	maxAcceptedConfBal lwcproto.ConfigBal // highest maxAcceptedConfBal at which a command was accepted
	whoseCmds          int32
	cmds               []state.Command    // the accepted command
	propCurConfBal     lwcproto.ConfigBal // highest this replica maxAcceptedConfBal tried so far
	clientProposals    []*genericsmr.Propose
	maxKnownBal        lwcproto.Ballot
}

type ProposalManager interface {
	beginNewProposal(replica ConfigRoundProposable, inst int32, conf int32)
	trackProposalAcceptance(replica ConfigRoundProposable, inst int32, bal lwcproto.ConfigBal)
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

func beginPreparingConfBal(pbk *ProposingBookkeeping, balloter Balloter, conf int32) {
	pbk.status = PREPARING
	nextBal := balloter.getNextProposingBal(pbk.maxKnownBal.Number)
	nextConfBal := lwcproto.ConfigBal{conf, nextBal}
	pbk.propCurConfBal = nextConfBal
	pbk.maxKnownBal = nextBal
}

type ConfigRoundProposable interface {
	GetPBK(inst int32) *ProposingBookkeeping
}

//
//func (r *ELPReplica) GetPBK(inst int32) *ProposingBookkeeping {
//	return r.instanceSpace[inst].pbk
//}

func (proposalConstructor *NormalQuorumProposalInitiator) beginNewProposal(r ConfigRoundProposable, inst int32, conf int32) {
	pbk := r.GetPBK(inst)
	beginPreparingConfBal(pbk, proposalConstructor.Balloter, conf)
	quorumaliser := proposalConstructor.SynodQuorumSystemConstructor.Construct(proposalConstructor.Aids)
	pbk.proposalInfos[pbk.propCurConfBal] = quorumaliser
	pbk.proposalInfos[pbk.propCurConfBal].StartPromiseQuorum()
}

func (proposalConstructor *NormalQuorumProposalInitiator) trackProposalAcceptance(r ConfigRoundProposable, inst int32, bal lwcproto.ConfigBal) {
	pbk := r.GetPBK(inst)
	quorumaliser := proposalConstructor.SynodQuorumSystemConstructor.Construct(proposalConstructor.Aids)
	pbk.proposalInfos[bal] = quorumaliser
	quorumaliser.StartAcceptanceQuorum()
}

func (propsalCOnstructor *NormalQuorumProposalInitiator) getBalloter() Balloter {
	return propsalCOnstructor.Balloter
}

func (proposalConstructor *ReducedQuorumProposalInitiator) beginNewProposal(r ConfigRoundProposable, inst int32, conf int32) {
	pbk := r.GetPBK(inst)
	beginPreparingConfBal(pbk, proposalConstructor.Balloter, conf)
	//make quorum
	group := proposalConstructor.AcceptorMapper.GetGroup(int(inst))
	//log.Println("group for instance", inst, ":", group)
	quorumaliser := proposalConstructor.SynodQuorumSystemConstructor.Construct(group)
	pbk.proposalInfos[pbk.propCurConfBal] = quorumaliser
	pbk.proposalInfos[pbk.propCurConfBal].StartPromiseQuorum()
}

func (proposalConstructor *ReducedQuorumProposalInitiator) trackProposalAcceptance(r ConfigRoundProposable, inst int32, bal lwcproto.ConfigBal) {
	pbk := r.GetPBK(inst)
	//make quorum
	group := proposalConstructor.AcceptorMapper.GetGroup(int(inst))
	quorumaliser := proposalConstructor.SynodQuorumSystemConstructor.Construct(group)
	pbk.proposalInfos[bal] = quorumaliser
	quorumaliser.StartAcceptanceQuorum()
}

func (inititor *ReducedQuorumProposalInitiator) getBalloter() Balloter {
	return inititor.Balloter
}
