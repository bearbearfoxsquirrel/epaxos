package twophase

import (
	"batching"
	"quorumsystem"
	"state"
	"stdpaxosproto"
)

type Proposal interface {
	GreaterThan(proposal Proposal) bool
	LessThan(proposal Proposal) bool
	Equal(proposal Proposal) bool
}

type ProposalWriter interface {
	Write()
}

type ProposalBookkeeping interface {
	GetCurrentProposal() Proposal
	SetCurrentProposal(proposal Proposal)
	GetCurrentValue() (Proposal, *[]*state.Command, int)
	SetCurrentValue(proposal Proposal, commands *[]*state.Command, whoseCmds int32)
	// todo identify main interface for proposer/learner
	//SetBackingOff()
	//SetReadyToPropose()
	//SetNowProposing()
	//SetClosed()
}

type ProposingBookkeeping struct {
	status          ProposerStatus
	qrms            map[stdpaxosproto.Ballot]quorumsystem.SynodQuorumSystem
	proposeValueBal stdpaxosproto.Ballot // highest proposeValueBal at which a command was accepted
	whoseCmds       int32
	cmds            []*state.Command     // the accepted command
	propCurBal      stdpaxosproto.Ballot // highest this replica proposeValueBal tried so far
	clientProposals batching.ProposalBatch
	maxKnownBal     stdpaxosproto.Ballot
}

func (pbk *ProposingBookkeeping) popBatch() batching.ProposalBatch {
	b := pbk.clientProposals
	pbk.clientProposals = nil
	return b
}

func (pbk *ProposingBookkeeping) putBatch(bat batching.ProposalBatch) {
	pbk.clientProposals = bat
}

func (pbk *ProposingBookkeeping) getBatch() batching.ProposalBatch {
	return pbk.clientProposals
}

func getEmptyInstance() *ProposingBookkeeping {
	return &ProposingBookkeeping{
		status:          NOT_BEGUN,
		qrms:            make(map[stdpaxosproto.Ballot]quorumsystem.SynodQuorumSystem),
		proposeValueBal: stdpaxosproto.Ballot{Number: -1, PropID: -1},
		whoseCmds:       -1,
		cmds:            nil,
		propCurBal:      stdpaxosproto.Ballot{Number: -1, PropID: -1},
		clientProposals: nil,
		maxKnownBal:     stdpaxosproto.Ballot{Number: -1, PropID: -1},
	}
}

func (pbk *ProposingBookkeeping) setNowProposing() {
	pbk.status = PROPOSING
	pbk.proposeValueBal = pbk.propCurBal
}

func (pbk *ProposingBookkeeping) setProposal(whoseCmds int32, bal stdpaxosproto.Ballot, val []*state.Command) {
	pbk.whoseCmds = whoseCmds
	pbk.proposeValueBal = bal
	pbk.cmds = val
}
