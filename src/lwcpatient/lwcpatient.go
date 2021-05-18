package lwcpatient

/*
import (
	"dlog"
	"fastrpc"
	"genericsmr"
	"log"
	"lwcproto"
	"time"
)


func NewReplicaPatient(id int, peerAddrList []string, thrifty bool, exec bool, lread bool, dreply bool, durable bool, batchWait int, f int, crtConfig int32, storageLoc string, maxOpenInstances int32, minBackoff int32, maxInitBackoff int32, maxBackoff int32, noopwait int32, alwaysNoop bool, factor float64) *Replica {
	retryInstances := make(chan RetryInfo, maxOpenInstances*10000)
	r := &Replica{
		Replica:          genericsmr.NewReplica(id, peerAddrList, thrifty, exec, lread, dreply, f, storageLoc),
		configChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		commitShortChan:  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		prepareReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		acceptReplyChan:  make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		//	instancesToRecover:  nil,
		prepareRPC:          0,
		acceptRPC:           0,
		commitRPC:           0,
		commitShortRPC:      0,
		prepareReplyRPC:     0,
		acceptReplyRPC:      0,
		instanceSpace:       make([]*Instance, 15*1024*1024),
		crtInstance:         -1, //get from storage
		crtConfig:           crtConfig,
		Shutdown:            false,
		counter:             0,
		flush:               true,
		executedUpTo:        -1, //get from storage
		batchWait:           batchWait,
		maxBalInc:           100,
		maxBeganInstances:   maxOpenInstances,
		beginNew:            make(chan struct{}, maxOpenInstances),
		crtOpenedInstances:  make([]int32, maxOpenInstances),
		proposableInstances: make(chan ProposalInfo, MAXPROPOSABLEINST),
		valuesToPropose:     make(chan ProposalTuples, 50000),
		noopInstance:        make(chan ProposalInfo, maxOpenInstances*1000),
		noopWaitUs:          noopwait,
		retryInstance:       retryInstances,
		BackoffManager:      NewBackoffManager(minBackoff, maxInitBackoff, maxBackoff, &retryInstances, factor),
		alwaysNoop:          alwaysNoop,
	}

	r.Durable = durable

	r.prepareRPC = r.RegisterRPC(new(lwcproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(lwcproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(lwcproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(lwcproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(lwcproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(lwcproto.AcceptReply), r.acceptReplyChan)

	//r.submitNewConfig()
	go r.runPatiently()

	return r
}


func (r *Replica) runPatiently() {
	r.ConnectToPeers()
	r.RandomisePeerOrder()
	if r.Exec {
		go r.executeCommands()
	}
	fastClockChan = make(chan bool, 1)
	//Enabled fast clock when batching
	if r.BatchingEnabled() {
		go r.fastClock()
	}

	for i := 0; i < int(r.maxBeganInstances); i++ {
		log.Println("sending begin new")
		r.beginNew <- struct{}{}
		r.crtOpenedInstances[i] = -1
	}

	onOffProposeChan := r.ProposeChan
	//if r.crtConfig > 1 {
	// send to all
	//}
	go r.WaitForClientConnections()

	newPeerOrderTimer := time.NewTimer(1 * time.Second)

	for !r.Shutdown {

		select {
		case <-newPeerOrderTimer.C:
			r.RandomisePeerOrder()
			dlog.Println("Recomputing closest peers")
			newPeerOrderTimer.Reset(100 * time.Millisecond)
			break
		case propose := <-onOffProposeChan:
			//got a Propose from a clien
			for i := 0; i < int(r.maxBeganInstances); i++ {
				r.beginNextInstance()
			}
			r.handlePropose(propose)
			if r.BatchingEnabled() {
				onOffProposeChan = nil
			}
			break
		case next := <-r.retryInstance:
			r.checkAndRetry(next)
			break
		case <-fastClockChan:
			//activate new proposals channel
			onOffProposeChan = r.ProposeChan
			break
		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*lwcproto.Prepare)
			//got a Prepare message
			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
			r.handlePrepare(prepare)
			break
		case acceptS := <-r.acceptChan:
			accept := acceptS.(*lwcproto.Accept)
			//got an Accept message
			dlog.Printf("Received Accept Request from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
			r.handleAccept(accept)
			// if new or equal backoff again
			break
		case commitS := <-r.commitChan:
			commit := commitS.(*lwcproto.Commit)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommit(commit)
			break
		case commitS := <-r.commitShortChan:
			commit := commitS.(*lwcproto.CommitShort)
			//got a Commit message
			dlog.Printf("Received short Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommitShort(commit)
			break
		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*lwcproto.PrepareReply)
			//got a Prepare reply
			dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
			r.handlePrepareReply(prepareReply)
			break
		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*lwcproto.AcceptReply)
			//got an Accept reply
			dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
			r.handleAcceptReply(acceptReply)
			break
		}
	}
}


func (r *Replica) handlePrepareReplyPatiently(preply *lwcproto.PrepareReply) {
	inst := r.instanceSpace[preply.Instance]
	pbk := r.instanceSpace[preply.Instance].pbk

	if inst.abk.status == COMMITTED {
		dlog.Println("Inst already known to be chosen")
		return
	}

	valWhatDone := r.proposerCheckAndHandleAcceptedValue(preply.Instance, preply.AcceptorId, preply.VConfigBal, preply.Command)
	// early learning
	if valWhatDone == CHOSEN {
		dlog.Printf("Preparing instance recognised as chosen (instance %d), returning commit \n", preply.Instance)
		if preply.ConfigBal.IsZeroVal() {
			panic("Why we commit zero ballot???")
		}
		r.acceptorCommit(preply.Instance, preply.ConfigBal, preply.Command)
		r.proposerCloseCommit(preply.Instance, preply.ConfigBal)
		r.bcastCommit(preply.Instance, preply.ConfigBal, preply.Command)
		return
	}

	if pbk.propCurConfBal.GreaterThan(preply.ConfigBal) || pbk.status != PREPARING {
		dlog.Printf("Message in late \n")
		return
	}

	if r.proposerCheckAndHandlePreempt(preply.Instance, preply.ConfigBal) {
		//pbk.nacks++
		//	if pbk.nacks+1 > r.N>>1 { // no qrm
		// we could send promise also through a prepare reply to the owner of the preempting confbal
		dlog.Printf("Another active proposer using config-ballot %d.%d.%d greater than mine\n", preply.ConfigBal)
		if preply.ConfigBal.Config > r.crtConfig {
			r.recordNewConfig(preply.ConfigBal.Config)
			r.sync()
		}
		return
	}

	qrm := pbk.proposalInfos[pbk.propCurConfBal]
	qrm.quorumAdd(preply.AcceptorId)
	if inst.abk.curBal.GreaterThan(pbk.propCurConfBal.Ballot) {
		panic("somehow acceptor has moved on but proposer hasn't")
	}
	if int(qrm.quorumCount()+1) >= r.Replica.ReadQuorumSize() {
		if pbk.status == CLOSED || inst.abk.status == COMMITTED {
			panic("oh nnooooooooo")
		}

		pbk.status = READY_TO_PROPOSE

		dlog.Println("Can now propose in instance", preply.Instance)
		qrm.quorumClear()
		qrm.qrmType = ACCEPTANCE

		if inst.pbk.maxAcceptedConfBal.IsZeroVal() {
			select {
			case cmds := <-r.valuesToPropose:
				pbk.cmds = cmds.cmd
				pbk.clientProposals = cmds.proposal
				break
			default:
				pbk.cmds = state.NOOP()
			}
		}
		pbk.status = PROPOSING
		// if we reorder bcast and recording - the acknowledger of the request of acceptance can count a qrm of 2 and quick learn
		r.acceptorAcceptOnConfBal(preply.Instance, preply.ConfigBal, inst.pbk.cmds)
		r.proposerCheckAndHandleAcceptedValue(preply.Instance, r.Id, preply.ConfigBal, inst.pbk.cmds)
		r.bcastAccept(preply.Instance)
	}
}




*/
