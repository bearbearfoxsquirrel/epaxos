package configproposalmanager

//
//import (
//	"batching"
//	"lwcproto"
//	"quorumsystem"
//	"state"
//	"stdpaxosproto"
//)
//
//type ProposerStatus int
//
//const (
//	NOT_BEGUN ProposerStatus = iota
//	BACKING_OFF
//	PREPARING
//	READY_TO_PROPOSE
//	PROPOSING
//	CLOSED
//)
//
//type ProposingBookkeeping struct {
//	Status          ProposerStatus
//	Qrms            map[lwcproto.ConfigBal]quorumsystem.SynodQuorumSystem
//	ProposeValueBal lwcproto.ConfigBal // highest ProposeValueBal at which a command was accepted
//	WhoseCmds       int32
//	Cmds            []*state.Command     // the accepted command
//	PropCurBal      lwcproto.ConfigBal // highest this replica ProposeValueBal tried so far
//	ClientProposals batching.ProposalBatch
//	MaxKnownBal     lwcproto.ConfigBal
//}
//
//
//func (pbk *ProposingBookkeeping) popBatch() batching.ProposalBatch {
//	b := pbk.ClientProposals
//	pbk.ClientProposals = nil
//	return b
//}
//
//func (pbk *ProposingBookkeeping) PutBatch(bat batching.ProposalBatch) {
//	pbk.ClientProposals = bat
//}
//
//func (pbk *ProposingBookkeeping) getBatch() batching.ProposalBatch {
//	return pbk.ClientProposals
//}
//
//func GetEmptyInstance() *ProposingBookkeeping {
//	return &ProposingBookkeeping{
//		Status:          NOT_BEGUN,
//		Qrms:            make(map[lwcproto.ConfigBal]quorumsystem.SynodQuorumSystem),
//		ProposeValueBal: lwcproto.ConfigBal{Number: -1, PropID: -1},
//		WhoseCmds:       -1,
//		Cmds:            nil,
//		PropCurBal:      lwcproto.ConfigBal{Number: -1, PropID: -1},
//		ClientProposals: nil,
//		MaxKnownBal:     lwcproto.ConfigBal{Number: -1, PropID: -1},
//	}
//}
//
//func (pbk *ProposingBookkeeping) SetNowProposing() {
//	pbk.Status = PROPOSING
//	pbk.ProposeValueBal = pbk.PropCurBal
//}
//
//func (pbk *ProposingBookkeeping) setProposal(whoseCmds int32, bal stdpaxosproto.Ballot, val []*state.Command) {
//	pbk.WhoseCmds = whoseCmds
//	pbk.ProposeValueBal = bal
//	pbk.Cmds = val
//}
//
//func (pbk *ProposingBookkeeping) isProposingClientValue() bool {
//	return pbk.Status == PROPOSING && pbk.ClientProposals != nil
//}
//
//type ConfigProposingBookkeeping map[int32]*ProposingBookkeeping
//
//type CPBK struct {
//	Status          ProposerStatus
//	Qrms            map[lwcproto.ConfigBal]quorumsystem.SynodQuorumSystem
//	ProposeValueBal lwcproto.ConfigBal // highest ProposeValueBal at which a command was accepted
//	WhoseCmds       int32
//	Cmds            []*state.Command   // the accepted command
//	PropCurBal      lwcproto.ConfigBal // highest this replica ProposeValueBal tried so far
//	ClientProposals batching.ProposalBatch
//	MaxKnownBal     lwcproto.ConfigBal
//}
//
