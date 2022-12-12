package acceptor

import (
	"epaxos/stablestore"
	"epaxos/state"
	"epaxos/stdpaxosproto"
	"time"
)

type instanceProposalBookkeeping struct {
	cmds      []*state.Command
	vBal      stdpaxosproto.Ballot
	whoseCmds int32
}

type batchedInstanceBookkeeping struct {
	status        InstanceStatus
	curBal        stdpaxosproto.Ballot
	instProposals map[int32]instanceProposalBookkeeping
}

type ClassedInstancesAcceptor struct {
	instanceClassLen           int32
	instanceState              map[int32]*batchedInstanceBookkeeping
	stableStore                stablestore.StableStore
	durable                    bool
	emuatedWriteTime           time.Duration
	emulatedSS                 bool
	meID                       int32
	acceptReplyRPC             uint8
	prepareReplyRPC            uint8
	commitRPC                  uint8
	commitShortRPC             uint8
	catchupOnProceedingCommits bool
	maxInstance                int32
}
