package main

import (
	"epaxos"
	"flag"
	"fmt"
	"genericsmr"
	"gpaxos"
	"io/ioutil"
	"log"
	"masterproto"
	"math/rand"
	"mencius"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"paxos"
	"runnable"
	"runtime/pprof"
	"time"
	"twophase"
)

var portnum *int = flag.Int("port", 7070, "Port # to listen on. Defaults to 7070")
var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost.")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
var myAddr *string = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")

var doMencius *bool = flag.Bool("m", false, "Use Mencius as the replication protocol. Defaults to false.")
var doGpaxos *bool = flag.Bool("g", false, "Use Generalized Paxos as the replication protocol. Defaults to false.")
var doEpaxos *bool = flag.Bool("e", false, "Use EPaxos as the replication protocol. Defaults to false.")
var doLWCSpec *bool = flag.Bool("ls", false, "Use Less Writey Consensus as the replication protocol with Speculative proposlas. Defaults to false.")

var doLWCGlobalSpec *bool = flag.Bool("lgs", false, "Use Less Writey Consensus as the replication protocol with global Speculative proposlas. Defaults to false.")

var doLWCPatient *bool = flag.Bool("lp", false, "Use Less Writey Consensus as the replication protocol with patient proposlas. Defaults to false.")
var doSTDSpec *bool = flag.Bool("ss", false, "Use Standard Paxos Consensus as the replication protocol with Speculative proposals. Defaults to false.")
var doSTDGlobalSpec *bool = flag.Bool("sgs", false, "Use Standard Paxos Consensus as the replication protocol with Speculative proposals. Defaults to false.")
var doSTDPatient *bool = flag.Bool("sp", false, "Use Standard Paxos Consensus as the replication protocol with Patient proposals. Defaults to false.")

var doELP *bool = flag.Bool("elp", false, "Use ELP algorithm")
var doLessWriteyNonEager *bool = flag.Bool("lw", false, "Use less writey algorithm")
var dostdEager *bool = flag.Bool("ebl", false, "Use eager 2 phase paxos algorithm")
var doBaselineTwoPhase *bool = flag.Bool("bl2p", false, "Use ELP algorithm")

var crtConfig = flag.Int("config", 1, "Current config in LWC")
var maxOInstances = flag.Int("oi", 1, "Max number of open instances in leaderless LWC")
var minBackoff = flag.Int("minbackoff", 5000, "Minimum backoff for a proposing replica that been preempted")
var maxInitBackoff = flag.Int("maxibackoff", 0, "Maximum initial backoff for a proposing replica that been preempted (default 110% min)")
var maxBackoff = flag.Int("maxbackoff", 100000000, "Maximum backoff for a proposing replica that been preempted")
var noopWait = flag.Int("noopwait", 0, "Wait time in microseconds before proposing no-op")
var alwaysNoop *bool = flag.Bool("alwaysnoop", false, "Always submit noops if there is no command awaiting execution?")
var factor *float64 = flag.Float64("factorbackoff", 0.5, "Factor for backoff")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var thrifty = flag.Bool("thrifty", false, "Use only as many messages as strictly required for inter-replica communication.")
var exec = flag.Bool("exec", false, "Execute commands.")
var lread = flag.Bool("lread", false, "Execute locally read command.")
var dreply = flag.Bool("dreply", false, "Reply to client only after command has been executed.")
var beacon = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")
var maxfailures = flag.Int("f", -1, "maximum number of maxfailures; default is a minority, ignored by other protocols than Paxos.")
var durable = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")
var batchWait = flag.Int("batchwait", 0, "Milliseconds to wait before sending a batch. If set to 0, batching is disabled. Defaults to 0.")
var transitiveConflicts = flag.Bool("transitiveconf", false, "Conflict relation is transitive.")
var storageParentDir = flag.String("storageparentdir", "./", "The parent directory of the stable storage file. Defaults to ./")
var quiet *bool = flag.Bool("quiet", false, "Log nothing?")
var fastLearn *bool = flag.Bool("flearn", false, "Learn quickly when f=1")
var whoCrash *int = flag.Int("whocrash", -1, "Who will crash in this run (-1 means no one)")
var whenCrashSeconds *int = flag.Int("whencrash", -1, "When will they crash after beginning to execute (seconds)")
var howLongCrashSeconds *int = flag.Int("howlongcrash", -1, "When will they restart after crashing (seconds)")
var catchupBatchSize *int = flag.Int("catchupbatch", 200, "How many instances will replicas respond with in catch up requests after recovery")

var timeoutus *int = flag.Int("timeoutus", 100000, "timeoutus for retrying a phase of paxos (microseconds) - default is 100000 us")

var initProposalWaitUs = flag.Int("initproposalwaitus", 0, "How long to wait before trying to propose after acquring promise qrm")

var emulatedSS *bool = flag.Bool("emulatedss", false, "emulated stable storage")
var emulatedWriteTimeNs *int = flag.Int("emulatedwritetimens", 0, "emulated stable storage write time in nanoseconds")
var minBatchSize *int = flag.Int("minbatch", 1, "Minimum batch size for speculative Paxos (only for LWP currently)")
var group1Size *int = flag.Int("group1size", -1, "group 1 size (-1 = thrify)")
var flushCommit *bool = flag.Bool("flushcommit", true, "flush commits to buffer")
var softExp *bool = flag.Bool("softExp", false, "flush commits to buffer")

var doStats *bool = flag.Bool("dostats", false, "record server stats")
var statsLoc *string = flag.String("statsloc", "./", "parent location where to store server stats")

var nothreadexec *bool = flag.Bool("nothreadexec", false, "optional turning off of execution in a separate thread of epaxos")

var catchUpFallenBehind *bool = flag.Bool("catchupfallenbehind", false, "catch up those who send messages for instances fallen behind")

var deadTime *int = flag.Int("deadtime", 60000, "time to take replica out of quorum (default 60 seconds)")
var maxBatchSizeBytes *int = flag.Int("maxbatchsize", 100000, "max vals held in a proposal")

var skipwaitms *int = flag.Int("skipwaitms", 350, "ms to wait before mencius skips")
var maxoutstandingskips *int = flag.Int("maxoskips", 300, "max outstanding skips")
var constBackoff *bool = flag.Bool("cbackoff", false, "Maintain a constant backoff")
var requeueOnPreempt *bool = flag.Bool("requeuepreempt", false, "Requeue a client proposal as soon as it is preempted (even if it might later be chosen in that instance)")
var reducePropConfs *bool = flag.Bool("reducepropconfs", false, "Reduce proposer conflicts in speculative proposals")

var bcastAcceptance *bool = flag.Bool("bcastacc", false, "In lwp broadcast acceptance")

var batch *bool = flag.Bool("batch", false, "turns on if batch wait > 0 also")

var reducedQrmSize *bool = flag.Bool("reducedqrmsize", false, "sets qrms to the minimum f+1 size (2f+1 groups of acceptors)")
var gridQrms *bool = flag.Bool("gridqrms", false, "Use grid quorums")

var sendFastestQrm *bool = flag.Bool("sendfastestqrm", false, "Send to fastest thought qrm")

var tsStatsFilename *string = flag.String("tsstatsfilename", "", "Name for timeseries stats file")
var instStatsFilename *string = flag.String("inststatsfilename", "", "Name for instance stats file")
var proposalStatsFilename *string = flag.String("proposalstatsfilename", "", "Name for proposal stats file")
var logFilename *string = flag.String("logfilename", "", "Name for log file")

var sendProposerState *bool = flag.Bool("sendproposerstate", false, "Proposers periodically send their current state to each other")
var proactivepreempt *bool = flag.Bool("proactivepreempt", false, "Upon being preempted, the proposer prepares on the preempting ballot")
var batchingAcceptor *bool = flag.Bool("batchingacceptor", false, "Acceptor batches responses and disk writes")
var accMaxBatchWaitMs *int = flag.Int("accmaxbatchwaitms", 5, "Max time in ms the acceptor waits to batch responses. Otherwise, commits and local events trigger syncing and responding. Subject to change")

var minimalAcceptorNegatives *bool = flag.Bool("minimalaccnegatives", false, "Only the minimal number (at most F+1) of acceptors will respond negatively in each quorum")
var timeBasedBallots *bool = flag.Bool("tbal", false, "The maximum ballot available to proposers is dictated by the time since they last chose a ballot")
var sendPreparesAllAcceptors *bool = flag.Bool("bcastprep", false, "if using minimal quorums, send prepares to all acceptors - passive observation")
var minimalProposers *bool = flag.Bool("minimalproposers", false, "When a proposer receives F+1 proposals to greater than theirs they stop proposing to that instance")
var mappedProposers *bool = flag.Bool("mappedproposers", false, "F+1 proposers are statically mapped to instances")
var dynamicMappedProposers *bool = flag.Bool("dmapprops", false, "Dynamically map proposers to instances - bounded by n and f+1")
var mappedProposersNum *int = flag.Int("mappropsnum", 1, "How many proposers are mapped statically to each instance")
var instsToOpenPerBatch *int = flag.Int("blinstsopenperbatch", 1, "How many instances to open per batch")
var rateLimitEagerOpenInsts *bool = flag.Bool("ratelimiteager", false, "Should eager instance pipeline be rate limited to noop time?")
var batchFlush *bool = flag.Bool("batchflush", false, "Should messages be flushed as a batch")
var batchFlushWait *int = flag.Int("batchflushwait", -1, "How long to wait before flushing writers")

var patientProposals *bool = flag.Bool("patprops", false, "Use patient proposals to minimise preempted accept messages")
var prewriteAcc *bool = flag.Bool("pwa", false, "Use Prewriting acceptor to reduce writes in phase 1")
var eagerFwInduction *bool = flag.Bool("fwi", false, "Use forward induction for eager promise quorums")
var q1 *bool = flag.Bool("q1", false, "Which queueing system to use (0 or 1)")
var bcastCommit *bool = flag.Bool("bcastc", false, "bcast commits when using bcast accept")
var nopreempt *bool = flag.Bool("np", false, "don't send preempt messages")
var id *int = flag.Int("id", -1, "id of the replica")

var pam *bool = flag.Bool("pam", false, "Do proposer acceptor mapping from file")
var pamloc *string = flag.String("pamloc", "./pam.json", "Location of the pam file")

func main() {

	flag.Parse()

	rand.Seed(time.Now().UnixNano() ^ int64(os.Getpid()))

	if *maxInitBackoff == 0 {
		*maxInitBackoff = int(float64(*minBackoff) * 1.5)
	}

	//	runtime.mg
	if *quiet == true {
		log.SetOutput(ioutil.Discard)
	}

	if (*dostdEager || *doBaselineTwoPhase) && *logFilename != "" {
		file, _ := os.Create(*statsLoc + fmt.Sprintf("/%s", *logFilename))
		log.SetOutput(file)
	}

	if *doMencius && *thrifty {
		log.Fatal("incompatble options -m -thrifty")
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	log.Printf("Server starting on port %d\n", *portnum)
	if *id == -1 {
		panic("invalid peer id")
	}
	replicaId, nodeList, isLeader := registerWithMaster(fmt.Sprintf("%s:%d", *masterAddr, *masterPort), int32(*id))

	if *doEpaxos || *doMencius || *doGpaxos || *maxfailures == -1 {
		*maxfailures = (len(nodeList) - 1) / 2
	}

	log.Printf("Tolerating %d max. failures\n", *maxfailures)

	whenCrash := time.Duration(*whenCrashSeconds) * time.Second
	howLongCrash := time.Duration(*howLongCrashSeconds) * time.Second

	//TODO give parent dir to all replica types

	//initalProposalWait := time.Duration(*initProposalWaitUs) * time.Microsecond

	emulatedWriteTime := time.Nanosecond * time.Duration(*emulatedWriteTimeNs)
	timeout := time.Microsecond * time.Duration(*timeoutus)

	smrReplica := genericsmr.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *maxfailures, *storageParentDir, int32(*deadTime), *batchFlush, time.Duration(*batchFlushWait)*time.Microsecond)
	var runnable runnable.Runnable
	if *doEpaxos {
		log.Println("Starting Egalitarian Paxos replica...")
		rep := epaxos.NewReplica(smrReplica, replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *beacon, *durable, *batchWait, *transitiveConflicts, *maxfailures, *storageParentDir, *fastLearn, *emulatedSS, emulatedWriteTime, *doStats, *statsLoc, !*nothreadexec, int32(*deadTime), *sendFastestQrm)
		rpc.Register(rep)
		runnable = rep
	} else if *doMencius {
		log.Println("Starting Mencius replica...")
		rep := mencius.NewReplica(smrReplica, replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *maxfailures, *storageParentDir, *emulatedSS, emulatedWriteTime, int32(*deadTime), *batchWait, *skipwaitms, *maxoutstandingskips, *batch)
		rpc.Register(rep)
		runnable = rep
	} else if *doGpaxos {
		log.Println("Starting Generalized Paxos replica...")
		rep := gpaxos.NewReplica(smrReplica, replicaId, nodeList, isLeader, *thrifty, *exec, *lread, *dreply, *maxfailures, int32(*deadTime))
		rpc.Register(rep)

		runnable = rep
	} else if *doLWCSpec {
		log.Println("Starting LWC replica...")
		//rep := lwcspeculative.NewReplica(smrReplica, replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait), *alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, time.Duration(*initProposalWaitUs)*time.Microsecond, *emulatedSS, emulatedWriteTime, int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *doStats, *statsLoc, *catchUpFallenBehind, int32(*deadTime), *maxBatchSizeBytes, *constBackoff, *requeueOnPreempt, *reducePropConfs, *bcastAcceptance, int32(*minBatchSize))
		//runnable = rep
		//rpc.Register(rep)
	} else if *doLWCGlobalSpec {
		log.Println("Starting LWC replica...")
		//rep := lwcglobalspec.NewReplica(smrReplica, replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait), *alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, time.Duration(*initProposalWaitUs)*time.Microsecond, *emulatedSS, emulatedWriteTime, int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *doStats, int32(*deadTime))
		//rpc.Register(rep)
		//runnable = rep
	} else if *doLWCPatient {
		log.Println("Starting LWC replica...")
		//rep := lwcpatient.NewReplica(smrReplica, replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait), *alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, *emulatedSS, emulatedWriteTime, int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *catchUpFallenBehind, int32(*deadTime), *maxBatchSizeBytes, *constBackoff, *requeueOnPreempt)
		//rpc.Register(rep)
		//runnable = rep
	} else if *doSTDSpec {
		log.Println("Starting Standard Paxos (speculative) replica...")
		//rep := stdpaxosspeculative.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait), *alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, time.Duration(*initProposalWaitUs)*time.Microsecond, *emulatedSS, emulatedWriteTime, int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *catchUpFallenBehind, int32(*deadTime), *maxBatchSizeBytes, *constBackoff, *requeueOnPreempt, *reducePropConfs)
		//rpc.Register(rep)
		//runnable = rep
	} else if *doSTDGlobalSpec {
		log.Println("Starting Standard Paxos (speculative) replica...")
		//rep := stdpaxosglobalspec.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait), *alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, time.Duration(*initProposalWaitUs)*time.Microsecond, *emulatedSS, emulatedWriteTime, int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, int32(*deadTime))
		//rpc.Register(rep)
		//runnable = rep
	} else if *doSTDPatient {
		log.Println("Starting LWC replica...")
	} else if *doELP || *doLessWriteyNonEager {

		//var qrm quorumsystem.SynodQuorumSystemConstructor
		//
		//qrm = &quorumsystem.SynodCountingQuorumSystemConstructor{
		//	F:                0,
		//	Replica:          smrReplica,
		//	Thrifty:          *thrifty,
		//	BroadcastFastest: *sendFastestQrm,
		//}
		//if *gridQrms {
		//	qrm = &quorumsystem.SynodGridQuorumSystemConstructor{
		//		F:                *maxfailures,
		//		Replica:          smrReplica,
		//		Thrifty:          *thrifty,
		//		BroadcastFastest: *sendFastestQrm,
		//	}
		//}
		//
		//aids := make([]int, len(nodeList))
		//for i, _ := range aids {
		//	aids[i] = i
		//}
		//
		//balloter := configtwophase.Balloter{
		//	PropID: int32(replicaId),
		//	N:      int32(smrReplica.N),
		//	MaxInc: 10000,
		//	//DoTimeBasedBallots: timebasedBallots
		//}
		//var initialtor configtwophase.ProposalManager
		//initialtor = &configtwophase.NormalQuorumProposalInitiator{
		//	SynodQuorumSystemConstructor: qrm,
		//	Balloter:                     balloter,
		//	Aids:                         aids,
		//}
		//
		//if *reducedQrmSize {
		//	var mapper instanceagentmapper.InstanceAgentMapper
		//	if *gridQrms {
		//		mapper = &instanceagentmapper.InstanceAcceptorGridMapper{
		//			Acceptors: aids,
		//			F:         *maxfailures,
		//			N:         len(nodeList),
		//		}
		//	} else {
		//		mapper = &instanceagentmapper.InstanceAcceptorSetMapper{
		//			Acceptors: aids,
		//			F:         *maxfailures,
		//			N:         len(nodeList),
		//		}
		//	}
		//
		//	initialtor = &configtwophase.ReducedQuorumProposalInitiator{
		//		AcceptorMapper:               mapper,
		//		SynodQuorumSystemConstructor: qrm,
		//		Balloter:                     balloter,
		//	}
		//}
		//if *doELP {
		//	rep := configtwophase.NewElpReplica(smrReplica, replicaId, *durable, *batchWait, *storageParentDir,
		//		int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait),
		//		*alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, initalProposalWait, *emulatedSS, emulatedWriteTime,
		//		int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *doStats, *statsLoc, *catchUpFallenBehind,
		//		*maxBatchSizeBytes, *constBackoff, *requeueOnPreempt, *reducePropConfs, *bcastAcceptance, int32(*minBatchSize), initialtor, *tsStatsFilename, *instStatsFilename)
		//	runnable = rep
		//	rpc.Register(rep)
		//} else {
		//	rep := configtwophase.NewLwsReplica(initialtor, smrReplica, replicaId, *durable, *batchWait, *storageParentDir,
		//		int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait),
		//		*alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, *emulatedSS, emulatedWriteTime,
		//		int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *doStats, *statsLoc, *catchUpFallenBehind,
		//		*maxBatchSizeBytes, *constBackoff, *requeueOnPreempt, *tsStatsFilename, *instStatsFilename)
		//	runnable = rep
		//	rpc.Register(rep)
		//}
	} else if *dostdEager || *doBaselineTwoPhase {

		//aids := make([]int, len(nodeList))
		//for i, _ := range aids {
		//	aids[i] = i
		//}

		acceptorMaxBatchWait := time.Duration(*accMaxBatchWaitMs) * time.Millisecond

		rep := twophase.NewBaselineTwoPhaseReplica(replicaId, smrReplica, *durable, *batchWait, *storageParentDir,
			int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait),
			*alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, *emulatedSS, emulatedWriteTime,
			int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *doStats, *statsLoc,
			*catchUpFallenBehind, int32(*deadTime), *maxBatchSizeBytes, *constBackoff, *requeueOnPreempt,
			*tsStatsFilename, *instStatsFilename, *proposalStatsFilename, *sendProposerState,
			*proactivepreempt, *batchingAcceptor, acceptorMaxBatchWait, *sendPreparesAllAcceptors, *minimalProposers,
			*timeBasedBallots, *mappedProposers, *dynamicMappedProposers, *bcastAcceptance,
			int32(*mappedProposersNum), int32(*instsToOpenPerBatch), *dostdEager, *sendFastestQrm, *gridQrms, *reducedQrmSize,
			*minimalAcceptorNegatives, *prewriteAcc, *patientProposals, *sendFastestQrm, *eagerFwInduction, *q1, *bcastCommit, *nopreempt,
			*pam, *pamloc)
		runnable = rep
		rpc.Register(rep)
		//}
	} else {
		log.Println("Starting classic Paxos replica...")
		rep := paxos.NewReplica(smrReplica, replicaId, nodeList, isLeader, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, *storageParentDir, *emulatedSS, emulatedWriteTime, int32(*deadTime), *sendFastestQrm)
		rpc.Register(rep)
		runnable = rep
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill)
	go catchKill(interrupt, runnable)
	rpc.HandleHTTP()
	//listen for RPC on a different port (8070 by default)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.Serve(l, nil)
}

func registerWithMaster(masterAddr string, id int32) (int, []string, bool) {
	args := &masterproto.RegisterArgs{*myAddr, *portnum, id}
	var reply masterproto.RegisterReply

	for done := false; !done; {
		log.Printf("connecting to: %v", masterAddr)
		mcli, err := rpc.DialHTTP("tcp", masterAddr)
		if err == nil {
			err = mcli.Call("Master.Register", args, &reply)
			if err == nil && reply.Ready == true {
				done = true
				break
			}
		}
		if err != nil {
			log.Printf("%v", err)
		}
		time.Sleep(1e9)
	}

	return reply.ReplicaId, reply.NodeList, reply.IsLeader
}

func catchKill(interrupt chan os.Signal, runnable runnable.Runnable) {
	<-interrupt
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	fmt.Println("Caught signal")
	if runnable != nil {
		runnable.CloseUp()
	}
	os.Exit(0)
}
