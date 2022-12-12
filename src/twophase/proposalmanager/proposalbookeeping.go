package proposalmanager

import (
	"epaxos/batching"
	"epaxos/lwcproto"
	"epaxos/quorumsystem"
	"epaxos/state"
	"epaxos/stdpaxosproto"
)

type ProposerStatus int

const (
	NOT_BEGUN ProposerStatus = iota
	BACKING_OFF
	PREPARING
	READY_TO_PROPOSE
	PROPOSING
	CLOSED
)

type ProposerBK interface {
	//GetQrm()
	//GetStatus()
	//GetProposeValue() (ballot, []*state.Command, whose)
}

type PBK struct {
	Status          ProposerStatus
	Qrms            map[lwcproto.ConfigBal]quorumsystem.SynodQuorumSystem
	ProposeValueBal lwcproto.ConfigBal // highest ProposeValueBal at which a command was accepted
	WhoseCmds       int32
	Cmds            []*state.Command   // the accepted command
	PropCurBal      lwcproto.ConfigBal // highest this replica ProposeValueBal tried so far
	ClientProposals batching.ProposalBatch
	MaxKnownBal     lwcproto.ConfigBal
}

func (pbk *PBK) popBatch() batching.ProposalBatch {
	b := pbk.ClientProposals
	pbk.ClientProposals = nil
	return b
}

func (pbk *PBK) PutBatch(bat batching.ProposalBatch) {
	pbk.ClientProposals = bat
}

func (pbk *PBK) getBatch() batching.ProposalBatch {
	return pbk.ClientProposals
}

func GetEmptyInstance() *PBK {
	return &PBK{
		Status:          NOT_BEGUN,
		Qrms:            make(map[lwcproto.ConfigBal]quorumsystem.SynodQuorumSystem),
		ProposeValueBal: lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}},
		WhoseCmds:       -1,
		Cmds:            nil,
		PropCurBal:      lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}},
		ClientProposals: nil,
		MaxKnownBal:     lwcproto.ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}},
	}
}

func (pbk *PBK) SetNowProposing() {
	pbk.Status = PROPOSING
	pbk.ProposeValueBal = pbk.PropCurBal
}

func (pbk *PBK) setProposal(whoseCmds int32, bal lwcproto.ConfigBal, val []*state.Command) {
	pbk.WhoseCmds = whoseCmds
	pbk.ProposeValueBal = bal
	pbk.Cmds = val
}

func (pbk *PBK) isProposingClientValue() bool {
	return pbk.Status == PROPOSING && pbk.ClientProposals != nil
}
