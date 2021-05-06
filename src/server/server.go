package main

import (
	"epaxos"
	"flag"
	"fmt"
	"gpaxos"
	"io/ioutil"
	"log"
	"lwc"
	"masterproto"
	"mencius"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"paxos"
	"runtime"
	"runtime/pprof"
	"time"
)

var portnum *int = flag.Int("port", 7070, "Port # to listen on. Defaults to 7070")
var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost.")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
var myAddr *string = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")
var doMencius *bool = flag.Bool("m", false, "Use Mencius as the replication protocol. Defaults to false.")
var doGpaxos *bool = flag.Bool("g", false, "Use Generalized Paxos as the replication protocol. Defaults to false.")
var doEpaxos *bool = flag.Bool("e", false, "Use EPaxos as the replication protocol. Defaults to false.")
var doLWC *bool = flag.Bool("l", false, "Use Less Writey Consensus as the replication protocol. Defaults to false.")
var crtConfig = flag.Int("config", 1, "Current config in LWC")
var maxOInstances = flag.Int("oi", 50, "Max number of open instances in leaderless LWC")
var minBackoff = flag.Int("minbackoff", 500000, "Minimum backoff for a proposing replica that been preempted")
var maxInitBackoff = flag.Int("maxibackoff", 900000, "Maximum initial backoff for a proposing replica that been preempted")
var maxBackoff = flag.Int("maxbackoff", 10000000, "Maximum backoff for a proposing replica that been preempted")
var noopWait = flag.Int("noopwait", 10000, "Wait time in microseconds before proposing no-op")

var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var thrifty = flag.Bool("thrifty", false, "Use only as many messages as strictly required for inter-replica communication.")
var exec = flag.Bool("exec", true, "Execute commands.")
var lread = flag.Bool("lread", false, "Execute locally read command.")
var dreply = flag.Bool("dreply", true, "Reply to client only after command has been executed.")
var beacon = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")
var maxfailures = flag.Int("maxfailures", -1, "maximum number of maxfailures; default is a minority, ignored by other protocols than Paxos.")
var durable = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")
var batchWait = flag.Int("batchwait", 0, "Milliseconds to wait before sending a batch. If set to 0, batching is disabled. Defaults to 0.")
var transitiveConflicts = flag.Bool("transitiveconf", true, "Conflict relation is transitive.")
var storageParentDir = flag.String("storageparentdir", "./", "The parent directory of the stable storage file. Defaults to ./")
var quiet *bool = flag.Bool("quiet", false, "Log nothing?")

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	if *quiet == true {
		log.SetOutput(ioutil.Discard)
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

	//TODO give parent dir to all replica types
	if *doEpaxos {
		log.Println("Starting Egalitarian Paxos replica...")
		rep := epaxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *beacon, *durable, *batchWait, *transitiveConflicts, *maxfailures, *storageParentDir)
		rpc.Register(rep)
	} else if *doMencius {
		log.Println("Starting Mencius replica...")
		rep := mencius.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *maxfailures)
		rpc.Register(rep)
	} else if *doGpaxos {
		log.Println("Starting Generalized Paxos replica...")
		rep := gpaxos.NewReplica(replicaId, nodeList, isLeader, *thrifty, *exec, *lread, *dreply, *maxfailures)
		rpc.Register(rep)

	} else if *doLWC {
		log.Println("Starting LWC replica...")
		rep := lwc.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures, int32(*crtConfig), *storageParentDir, int32(*maxOInstances), int32(*minBackoff), int32(*maxInitBackoff), int32(*maxBackoff), int32(*noopWait))
		rpc.Register(rep)
	} else {
		log.Println("Starting classic Paxos replica...")
		rep := paxos.NewReplica(replicaId, nodeList, isLeader, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures)
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
