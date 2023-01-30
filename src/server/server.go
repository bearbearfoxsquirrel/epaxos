package main

import (
	"epaxos/epaxos"
	"epaxos/genericsmr"
	"epaxos/gpaxos"
	"epaxos/masterproto"
	"epaxos/mencius"
	"epaxos/paxos"
	"epaxos/runnable"
	"epaxos/twophase"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"
)

var portnum = flag.Int("port", 7070, "Port # to listen on. Defaults to 7070")
var masterAddr = flag.String("maddr", "", "Master address. Defaults to localhost.")
var masterPort = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
var myAddr = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")

var doMencius = flag.Bool("m", false, "Use Mencius as the replication protocol. Defaults to false.")
var doGpaxos = flag.Bool("g", false, "Use Generalized Paxos as the replication protocol. Defaults to false.")
var doEpaxos = flag.Bool("e", false, "Use EPaxos as the replication protocol. Defaults to false.")
var doLWCSpec = flag.Bool("ls", false, "Use Less Writey Consensus as the replication protocol with Speculative proposlas. Defaults to false.")

var doLWCGlobalSpec = flag.Bool("lgs", false, "Use Less Writey Consensus as the replication protocol with global Speculative proposlas. Defaults to false.")

var doLWCPatient = flag.Bool("lp", false, "Use Less Writey Consensus as the replication protocol with patient proposlas. Defaults to false.")
var doSTDSpec = flag.Bool("ss", false, "Use Standard Paxos Consensus as the replication protocol with Speculative proposals. Defaults to false.")
var doSTDGlobalSpec = flag.Bool("sgs", false, "Use Standard Paxos Consensus as the replication protocol with Speculative proposals. Defaults to false.")

var crtConfig = flag.Int("config", 1, "Current config in LWC")
var maxOInstances = flag.Int("oi", 1, "Max number of open instances in leaderless LWC")
var minBackoff = flag.Int("minbackoff", 5000, "Minimum backoff for a proposing replica that been preempted")
var maxInitBackoff = flag.Int("maxibackoff", 0, "Maximum initial backoff for a proposing replica that been preempted (default 110% min)")
var maxBackoff = flag.Int("maxbackoff", 100000000, "Maximum backoff for a proposing replica that been preempted")
var noopWait = flag.Int("noopwait", 0, "Wait time in microseconds before proposing no-op")
var alwaysNoop = flag.Bool("alwaysnoop", false, "Always submit noops if there is no command awaiting execution?")
var factor = flag.Float64("factorbackoff", 0.5, "Factor for backoff")
var procs = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
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
var quiet = flag.Bool("quiet", false, "Log nothing?")
var fastLearn = flag.Bool("flearn", false, "Learn quickly when f=1")
var whoCrash = flag.Int("whocrash", -1, "Who will crash in this run (-1 means no one)")
var whenCrashSeconds = flag.Int("whencrash", -1, "When will they crash after beginning to execute (seconds)")
var howLongCrashSeconds = flag.Int("howlongcrash", -1, "When will they restart after crashing (seconds)")
var catchupBatchSize = flag.Int("catchupbatch", 200, "How many instances will replicas respond with in catch up requests after recovery")

var timeoutus = flag.Int("timeoutus", 100000, "timeoutus for retrying a phase of paxos (microseconds) - default is 100000 us")

var initProposalWaitUs = flag.Int("initproposalwaitus", 0, "How long to wait before trying to propose after acquring promise qrm")

var emulatedSS = flag.Bool("emulatedss", false, "emulated stable storage")
var emulatedWriteTimeNs = flag.Int("emulatedwritetimens", 0, "emulated stable storage write time in nanoseconds")
var minBatchSize = flag.Int("minbatch", 1, "Minimum batch size for speculative Paxos (only for LWP currently)")
var group1Size = flag.Int("group1size", -1, "group 1 size (-1 = thrify)")
var flushCommit = flag.Bool("flushcommit", true, "flush commits to buffer")
var softExp = flag.Bool("softExp", false, "flush commits to buffer")

var doStats = flag.Bool("dostats", false, "record server stats")
var statsLoc = flag.String("statsloc", "./", "parent location where to store server stats")

var nothreadexec = flag.Bool("nothreadexec", false, "optional turning off of execution in a separate thread of epaxos")

var catchUpFallenBehind = flag.Bool("catchupfallenbehind", false, "catch up those who send messages for instances fallen behind")

var deadTime = flag.Int("deadtime", 60000, "time to take replica out of quorum (default 60 seconds)")
var maxBatchSizeBytes = flag.Int("maxbatchsize", 100000, "max vals held in a proposal")

var skipwaitms = flag.Int("skipwaitms", 350, "ms to wait before mencius skips")
var maxoutstandingskips = flag.Int("maxoskips", 300, "max outstanding skips")
var constBackoff = flag.Bool("cbackoff", false, "Maintain a constant backoff")
var requeueOnPreempt = flag.Bool("requeuepreempt", false, "Requeue a client proposal as soon as it is preempted (even if it might later be chosen in that instance)")
var reducePropConfs = flag.Bool("reducepropconfs", false, "Reduce proposer conflicts in speculative proposals")

var bcastAcceptance = flag.Bool("bcastacc", false, "In lwp broadcast acceptance")
var bcastAcceptDisklessNOOP = flag.Bool("bcastaccdl", false, "In lwp broadcast acceptance of diskless noops")

var batch = flag.Bool("batch", false, "turns on if batch wait > 0 also")

var reducedQrmSize = flag.Bool("reducedqrmsize", false, "sets qrms to the minimum f+1 size (2f+1 groups of acceptors)")
var gridQrms = flag.Bool("gridqrms", false, "Use grid quorums")

var sendFastestQrm = flag.Bool("sendfastestqrm", false, "Send to fastest thought qrm")

var tsStatsFilename = flag.String("tsstatsfilename", "", "Name for timeseries stats file")
var instStatsFilename = flag.String("inststatsfilename", "", "Name for instance stats file")
var proposalStatsFilename = flag.String("proposalstatsfilename", "", "Name for proposal stats file")
var logFilename = flag.String("logfilename", "", "Name for log file")

var sendProposerState = flag.Bool("sendproposerstate", false, "Proposers periodically send their current state to each other")
var proactivepreempt = flag.Bool("proactivepreempt", false, "Upon being preempted, the proposer prepares on the preempting ballot")
var batchingAcceptor = flag.Bool("bata", false, "Acceptor batches responses and disk writes")
var accMaxBatchWaitMs = flag.Int("accmaxbatchwaitms", 5, "Max time in ms the acceptor waits to batch responses. Otherwise, commits and local events trigger syncing and responding. Subject to change")

var minimalAcceptorNegatives = flag.Bool("minimalaccnegatives", false, "Only the minimal number (at most F+1) of acceptors will respond negatively in each quorum")
var timeBasedBallots = flag.Bool("tbal", false, "The maximum ballot available to proposers is dictated by the time since they last chose a ballot")
var sendPreparesAllAcceptors = flag.Bool("bcastprep", false, "if using minimal quorums, send prepares to all acceptors - passive observation")
var minimalProposers = flag.Bool("minimalproposers", false, "When a proposer receives F+1 proposals to greater than theirs they stop proposing to that instance")
var mappedProposers = flag.Bool("mappedproposers", false, "F+1 proposers are statically mapped to instances")
var dynamicMappedProposers = flag.Bool("dmapprops", false, "Dynamically map proposers to instances - bounded by n and f+1")
var mappedProposersNum = flag.Int("mappropsnum", 1, "How many proposers are mapped statically to each instance")
var instsToOpenPerBatch = flag.Int("blinstsopenperbatch", 1, "How many instances to open per batch")
var rateLimitEagerOpenInsts = flag.Bool("ratelimiteager", false, "Should eager instance pipeline be rate limited to noop time?")
var batchFlush = flag.Bool("batchflush", false, "Should messages be flushed as a batch")
var batchFlushWait = flag.Int("batchflushwait", -1, "How long to wait before flushing writers")

var patientProposals = flag.Bool("patprops", false, "Use patient proposals to minimise preempted accept messages")
var prepwrittenpromises = flag.Bool("pwa", false, "Use Prewriting acceptor to reduce writes in phase 1")
var q1 = flag.Bool("q1", false, "Which queueing system to use (0 or 1)")
var bcastCommit = flag.Bool("bcastc", false, "bcast commits when using bcast accept")
var nopreempt = flag.Bool("np", false, "don't send preempt messages")
var id = flag.Int("id", -1, "id of the replica")

var pam = flag.Bool("pam", false, "Do proposer acceptor mapping from file")
var pamloc = flag.String("pamloc", "./pam.json", "Location of the pam file")
var syncacceptor = flag.Bool("synca", false, "2 Phase acceptor synchronously prepares and accepts")
var disklessnoops = flag.Bool("disklessnoops", false, "Use diskless noops")
var foceDisklessnoops = flag.Bool("fdisklessnoops", false, "Force diskless noops")

var eagerFwInduction = flag.Bool("fwi", false, "Use forward induction for eager promise quorums")
var doChosenFWI = flag.Bool("cfwi", false, "Do forward induction in response to chosen")
var doValueFWI = flag.Bool("vfwi", false, "Do forward induction in response to value proposed")
var doLatePropsFWI = flag.Bool("lfwi", false, "Do forward induction in response to late proposals")
var forwardingInstances = flag.Int("fwinsts", 0, "How pipelined instances to induce forwards")
var inductiveConfs = flag.Bool("ic", false, "Use inductive conflicts in baseline 2 phase algorithm")
var doEager = flag.Bool("eager", false, "Use eager 2 phase paxos algorithm")
var doBaselineTwoPhase = flag.Bool("bl2p", false, "Use baseline (non eager algorithm)")
var eagerByExec = flag.Bool("ebe", false, "Do eager by execute and crt instance gap")
var eagerByExecFac = flag.Float64("ebef", 1, "What factor to use for eager by exec gap (how big the gap between executed and max opened instance should be)")

var udp = flag.Bool("udp", false, "Use UDP (only available in two phase)")

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

	if (*doEager || *doBaselineTwoPhase) && *logFilename != "" {
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

	emulatedWriteTime := time.Nanosecond * time.Duration(*emulatedWriteTimeNs)
	timeout := time.Microsecond * time.Duration(*timeoutus)

	smrReplica := genericsmr.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *maxfailures, *storageParentDir, int32(*deadTime), *batchFlush, time.Duration(*batchFlushWait)*time.Microsecond, *udp)
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
	} else if *doEager || *doBaselineTwoPhase || *eagerByExec {
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
			int32(*mappedProposersNum), int32(*instsToOpenPerBatch), *doEager, *sendFastestQrm, *gridQrms, *reducedQrmSize,
			*minimalAcceptorNegatives, *prepwrittenpromises, *patientProposals, *sendFastestQrm, *eagerFwInduction, *doChosenFWI, *doValueFWI, *doLatePropsFWI,
			int32(*forwardingInstances), *q1, *bcastCommit, *nopreempt, *pam, *pamloc, *syncacceptor, *disklessnoops, *foceDisklessnoops,
			*eagerByExec, *bcastAcceptDisklessNOOP, float32(*eagerByExecFac), *inductiveConfs)
		runnable = rep
		rpc.Register(rep)
		//}
	} else {
		log.Println("Starting classic Paxos replica...")
		rep := paxos.NewReplica(smrReplica, replicaId, nodeList, isLeader, *thrifty, *exec, *lread, *dreply, *durable,
			*batchWait, *maxfailures, *storageParentDir, *emulatedSS, emulatedWriteTime, int32(*deadTime), *sendFastestQrm,
			*reducedQrmSize, *mappedProposers, int32(*mappedProposersNum), *pam, *pamloc)
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
