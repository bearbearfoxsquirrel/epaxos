package main

import (
	"epaxos"
	"flag"
	"fmt"
	"gpaxos"
	"io/ioutil"
	"log"
	"lwcglobalspec"
	"lwcpatient"
	"lwcspeculative"
	"masterproto"
	"mencius"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"paxos"
	"runtime/pprof"
	"stdpaxosglobalspec"
	"stdpaxospatient"
	"stdpaxosspeculative"
	"time"
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

var crtConfig = flag.Int("config", 1, "Current config in LWC")
var maxOInstances = flag.Int("oi", 50, "Max number of open instances in leaderless LWC")
var minBackoff = flag.Int("minbackoff", 5000, "Minimum backoff for a proposing replica that been preempted")
var maxInitBackoff = flag.Int("maxibackoff", 0, "Maximum initial backoff for a proposing replica that been preempted (default 110% min)")
var maxBackoff = flag.Int("maxbackoff", 1000000, "Maximum backoff for a proposing replica that been preempted")
var noopWait = flag.Int("noopwait", 10000, "Wait time in microseconds before proposing no-op")
var alwaysNoop *bool = flag.Bool("alwaysnoop", false, "Always submit noops if there is no command awaiting execution?")
var factor *float64 = flag.Float64("factorbackoff", 0.5, "Factor for backoff")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var thrifty = flag.Bool("thrifty", false, "Use only as many messages as strictly required for inter-replica communication.")
var exec = flag.Bool("exec", false, "Execute commands.")
var lread = flag.Bool("lread", false, "Execute locally read command.")
var dreply = flag.Bool("dreply", false, "Reply to client only after command has been executed.")
var beacon = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")
var maxfailures = flag.Int("maxfailures", -1, "maximum number of maxfailures; default is a minority, ignored by other protocols than Paxos.")
var durable = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")
var batchWait = flag.Int("batchwait", 0, "Milliseconds to wait before sending a batch. If set to 0, batching is disabled. Defaults to 0.")
var transitiveConflicts = flag.Bool("transitiveconf", true, "Conflict relation is transitive.")
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

var group1Size *int = flag.Int("group1size", -1, "group 1 size (-1 = thrify)")
var flushCommit *bool = flag.Bool("flushcommit", true, "flush commits to buffer")
var softExp *bool = flag.Bool("softExp", false, "flush commits to buffer")

var cmpCmtExec *bool = flag.Bool("cmtexeccmp", false, "record comparision of commit instances vs executed instances")
var cmpCmtExecLoc *string = flag.String("commitExecCmpLoc", "", "file where to store commit exec deltas")

var nothreadexec *bool = flag.Bool("nothreadexec", false, "optional turning off of execution in a separate thread of epaxos")

var catchUpFallenBehind *bool = flag.Bool("catchupfallenbehind", false, "catch up those who send messages for instances fallen behind")

var deadTime *int = flag.Int("deadtime", 5000, "time to take replica out of quorum (default 5 second)")
var batchsize *int = flag.Int("batchsize", 1024, "max vals held in a proposal")

var skipwaitms *int = flag.Int("skipwaitms", 350, "ms to wait before mencius skips")
var maxoutstandingskips *int = flag.Int("maxoskips", 300, "max outstanding skips")
var constBackoff *bool = flag.Bool("cbackoff", false, "Maintain a constant backoff")
var requeueOnPreempt *bool = flag.Bool("requeuepreempt", false, "Requeue a client proposal as soon as it is preempted (even if it might later be chosen in that instance)")
var reducePropConfs *bool = flag.Bool("reducepropconfs", false, "Reduce proposer conflicts in speculative proposals")

var bcastAcceptance *bool = flag.Bool("bcastacceptance", false, "In lwp broadcast acceptance")

var batch *bool = flag.Bool("batch", false, "turns on if batch wait > 0 also")

//var randomisedExpBackoff *bool = flag.Bool("rexpbackoff", false, "Use a randomised exponential backoff")

func main() {
	flag.Parse()

	if *maxInitBackoff == 0 {
		*maxInitBackoff = int(float64(*minBackoff) * 1.2)
	}

	//runtime.GOMAXPROCS(*procs)
	//	runtime.mg
	if *quiet == true {
		log.SetOutput(ioutil.Discard)
	}

	//if *doMencius && *thrifty {
	//	log.Fatal("incompatble options -m -thrifty")
	//}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt)
		go catchKill(interrupt)
	}

	log.Printf("Server starting on port %d\n", *portnum)

	replicaId, nodeList, isLeader := registerWithMaster(fmt.Sprintf("%s:%d", *masterAddr, *masterPort))

	if *doEpaxos || *doMencius || *doGpaxos || *maxfailures == -1 {
		*maxfailures = (len(nodeList) - 1) / 2
	}

	log.Printf("Tolerating %d max. failures\n", *maxfailures)

	whenCrash := time.Duration(*whenCrashSeconds) * time.Second
	howLongCrash := time.Duration(*howLongCrashSeconds) * time.Second

	//TODO give parent dir to all replica types

	emulatedWriteTime := time.Nanosecond * time.Duration(*emulatedWriteTimeNs)
	timeout := time.Microsecond * time.Duration(*timeoutus)
	if *doEpaxos {
		log.Println("Starting Egalitarian Paxos replica...")
		rep := epaxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *beacon, *durable, *batchWait, *transitiveConflicts, *maxfailures, *storageParentDir, *fastLearn, *emulatedSS, emulatedWriteTime, *cmpCmtExec, *cmpCmtExecLoc, !*nothreadexec, int32(*deadTime))
		rpc.Register(rep)
	} else if *doMencius {
		log.Println("Starting Mencius replica...")
		rep := mencius.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *maxfailures, *storageParentDir, *emulatedSS, emulatedWriteTime, int32(*deadTime), *batchWait, *skipwaitms, *maxoutstandingskips, *batch)
		rpc.Register(rep)
	} else if *doGpaxos {
		log.Println("Starting Generalized Paxos replica...")
		rep := gpaxos.NewReplica(replicaId, nodeList, isLeader, *thrifty, *exec, *lread, *dreply, *maxfailures, int32(*deadTime))
		rpc.Register(rep)

	} else if *doLWCSpec {
		log.Println("Starting LWC replica...")
		rep := lwcspeculative.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait), *alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, time.Duration(*initProposalWaitUs)*time.Microsecond, *emulatedSS, emulatedWriteTime, int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *cmpCmtExec, *cmpCmtExecLoc, *catchUpFallenBehind, int32(*deadTime), *batchsize, *constBackoff, *requeueOnPreempt, *reducePropConfs, *bcastAcceptance)
		rpc.Register(rep)
	} else if *doLWCGlobalSpec {
		log.Println("Starting LWC replica...")
		rep := lwcglobalspec.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait), *alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, time.Duration(*initProposalWaitUs)*time.Microsecond, *emulatedSS, emulatedWriteTime, int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *cmpCmtExec, int32(*deadTime))
		rpc.Register(rep)
	} else if *doLWCPatient {
		log.Println("Starting LWC replica...")
		rep := lwcpatient.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait), *alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, *emulatedSS, emulatedWriteTime, int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *catchUpFallenBehind, int32(*deadTime), *batchsize, *constBackoff, *requeueOnPreempt)
		rpc.Register(rep)
	} else if *doSTDSpec {
		log.Println("Starting Standard Paxos (speculative) replica...")
		rep := stdpaxosspeculative.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait), *alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, time.Duration(*initProposalWaitUs)*time.Microsecond, *emulatedSS, emulatedWriteTime, int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *catchUpFallenBehind, int32(*deadTime), *batchsize, *constBackoff, *requeueOnPreempt, *reducePropConfs)
		rpc.Register(rep)
	} else if *doSTDGlobalSpec {
		log.Println("Starting Standard Paxos (speculative) replica...")
		rep := stdpaxosglobalspec.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait), *alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, time.Duration(*initProposalWaitUs)*time.Microsecond, *emulatedSS, emulatedWriteTime, int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, int32(*deadTime))
		rpc.Register(rep)
	} else if *doSTDPatient {
		log.Println("Starting LWC replica...")
		rep := stdpaxospatient.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait), *alwaysNoop, *factor, int32(*whoCrash), whenCrash, howLongCrash, *emulatedSS, emulatedWriteTime, int32(*catchupBatchSize), timeout, *group1Size, *flushCommit, *softExp, *catchUpFallenBehind, int32(*deadTime), *batchsize, *constBackoff, *requeueOnPreempt)
		rpc.Register(rep)
	} else {
		log.Println("Starting classic Paxos replica...")
		rep := paxos.NewReplica(replicaId, nodeList, isLeader, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, *storageParentDir, *emulatedSS, emulatedWriteTime, int32(*deadTime))
		rpc.Register(rep)
	}

	rpc.HandleHTTP()
	//listen for RPC on a different port (8070 by default)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.Serve(l, nil)

}

func registerWithMaster(masterAddr string) (int, []string, bool) {
	args := &masterproto.RegisterArgs{*myAddr, *portnum}
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

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	fmt.Println("Caught signal")
	os.Exit(0)
}
