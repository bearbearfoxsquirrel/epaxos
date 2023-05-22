package main

import (
	"epaxos/bindings"
	"epaxos/dlog"
	"epaxos/genericsmrproto"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"
)

var clientId int64 = *flag.Int64("id", -1, "the id of the client. Default is RFC 4122 nodeID.")
var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port. ")
var outstanding *int = flag.Int("q", 1000, "Total number of requests. ")
var writes *int = flag.Int("w", 100, "Percentage of updates (writes). ")
var psize *int = flag.Int("psize", 100, "Payload size for writes.")
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). ")
var fast *bool = flag.Bool("f", false, "Fast Paxos: send message directly to all replicas. ")
var localReads *bool = flag.Bool("l", false, "Execute reads at the closest (local) replica. ")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. ")
var conflicts *int = flag.Int("c", 0, "Number of conflicts that should occur (chance of hitting a hot key)")
var verbose *bool = flag.Bool("v", false, "verbose mode. ")
var scan *bool = flag.Bool("s", false, "replace read with short scan (100 elements)")
var connectReplica *int = flag.Int("connectreplica", -1, "Must state which replica to send requests to")
var latencyOutput = flag.String("lato", "", "Must state where resultant latencies will be written")
var sampleRateMs = flag.Int("samerate", 1000, "how often to sample timeseries data (ms)")
var outputTimeseriesToFile *bool = flag.Bool("timeseriestofile", false, "output the timeseries benchmark to a file")
var timeseriesFile *string = flag.String("timeseriesfile", "", "where to store timeseries file")
var forceFlush *bool = flag.Bool("ff", false, "Force flush log file output")

type ClientValue struct {
	uid   int32 //not great but its only for testing. Only need uid for local client
	key   int64
	value []byte
}

type TimeseriesStats struct {
	minLatency        int64
	maxLatency        int64
	avgLatency        int64
	deliveredRequests int64
	deliveredBytes    int64
	file              *os.File
	forceFlush        bool
}

func NewTimeseriesStates(storeToFile bool, loc string, forceFlush bool) TimeseriesStats {
	timeseriesStat := TimeseriesStats{
		minLatency:        math.MaxInt64,
		maxLatency:        0,
		avgLatency:        0,
		deliveredRequests: 0,
		deliveredBytes:    0,
	}
	if storeToFile {
		timeseriesStat.file, _ = os.Create(loc)
		timeseriesStat.forceFlush = forceFlush
	}
	return timeseriesStat
}

func (timeseriesStats TimeseriesStats) String() string {
	var mbps float64 = (float64(timeseriesStats.deliveredBytes) * 8.) / (1024. * 1024.)
	minLat := timeseriesStats.minLatency
	if timeseriesStats.minLatency == math.MaxInt64 {
		minLat = 0
	}
	return fmt.Sprintf("%d value/sec, %.2f Mbps, latency min %d us max %d us avg %d us",
		timeseriesStats.deliveredRequests, mbps, minLat,
		timeseriesStats.maxLatency, timeseriesStats.avgLatency)
}

func (stats *TimeseriesStats) update(deliveredBytes int64, latency time.Duration) {
	stats.deliveredRequests++
	stats.deliveredBytes += deliveredBytes
	stats.avgLatency = latency.Microseconds() - stats.avgLatency/stats.deliveredRequests
	if latency.Microseconds() > stats.maxLatency {
		stats.maxLatency = latency.Microseconds()
	}
	if latency.Microseconds() < stats.minLatency {
		stats.minLatency = latency.Microseconds()
	}
}

func (stats *TimeseriesStats) reset() {
	stats.minLatency = math.MaxInt64
	stats.maxLatency = 0
	stats.avgLatency = 0
	stats.deliveredRequests = 0
	stats.deliveredBytes = 0
}

func (stats *TimeseriesStats) close() {
	if stats.file == nil {
		return
	}
	stats.file.Sync()
	stats.file.Close()
}

type LatencyRecorder struct {
	outputFile    *os.File
	stopRecording chan struct{}
	forceFlush    bool
}

func (latencyRecorder *LatencyRecorder) close() {
	if latencyRecorder.outputFile == nil {
		return
	}
	latencyRecorder.outputFile.Sync()
	latencyRecorder.outputFile.Close()
}

func (latencyRecorder *LatencyRecorder) record(latencyMicroseconds int64) {
	_, err := latencyRecorder.outputFile.WriteString(time.Now().Format("2006/01/02 15:04:05") + " " + fmt.Sprintf("%d\n", latencyMicroseconds))
	if err != nil {
		dlog.Println("Error writing value")
		return
	}
	if latencyRecorder.forceFlush {
		latencyRecorder.outputFile.Sync()
	}
}

func NewLatencyRecorder(outputFileLoc string, forceFlush bool) LatencyRecorder {
	file, err := os.Create(outputFileLoc)
	if err != nil {
		panic("Cannot open latency recording output file at location")
	}
	recorder := LatencyRecorder{
		outputFile:    file,
		stopRecording: make(chan struct{}),
		forceFlush:    forceFlush,
	}

	return recorder
}

type ClientBenchmarker struct {
	timeseriesStats      TimeseriesStats
	valueSubmissionTimes map[int32]time.Time
	latencyRecorder      LatencyRecorder
	clientID             int64
}

func newBenchmarker(clientID int64, recordedLatenciesPath string, storeTimeseriesToFile bool, timeSeriesFileLoc string, forceFlush bool) ClientBenchmarker {
	benchmarker := ClientBenchmarker{
		timeseriesStats:      NewTimeseriesStates(storeTimeseriesToFile, timeSeriesFileLoc, forceFlush),
		valueSubmissionTimes: make(map[int32]time.Time),
		latencyRecorder:      NewLatencyRecorder(recordedLatenciesPath, forceFlush),
		clientID:             clientID,
	}
	return benchmarker
}

func (benchmarker *ClientBenchmarker) stop() {
	benchmarker.latencyRecorder.close()
	benchmarker.timeseriesStats.close()
}

func (benchmarker *ClientBenchmarker) reset() {
	benchmarker.valueSubmissionTimes = make(map[int32]time.Time)
	benchmarker.timeseriesStats.reset()
}

func (benchmarker *ClientBenchmarker) register(value ClientValue) bool {
	if _, exists := benchmarker.valueSubmissionTimes[value.uid]; exists {
		return false
	}
	benchmarker.valueSubmissionTimes[value.uid] = time.Now()
	return true
}

func (benchmarker *ClientBenchmarker) close(value ClientValue) bool {
	valSubmittedAt, exists := benchmarker.valueSubmissionTimes[value.uid]
	if !exists {
		return false
	}
	now := time.Now()
	lat := now.Sub(valSubmittedAt)
	benchmarker.latencyRecorder.record(lat.Microseconds())
	benchmarker.timeseriesStats.update(int64(len(value.value)), lat)
	delete(benchmarker.valueSubmissionTimes, value.uid)
	return true
}

func (benchmarker *ClientBenchmarker) timeseriesStep() {
	if benchmarker.timeseriesStats.file != nil {
		benchmarker.timeseriesStats.file.WriteString(time.Now().Format("2006/01/02 15:04:05") + " " + benchmarker.timeseriesStats.String() + "\n")
		if benchmarker.timeseriesStats.forceFlush {
			benchmarker.timeseriesStats.file.Sync()
		}
	} else {
		log.Println(benchmarker.timeseriesStats.String())
	}
	benchmarker.timeseriesStats.reset()
}

func generateAndBeginBenchmarkingValue(benchmarker ClientBenchmarker, valSize int, maxOutstanding int, lastUID int32) ClientValue {
	if len(benchmarker.valueSubmissionTimes) == maxOutstanding {
		panic("too many added to client outstadning values")
	}
	registered := false
	wValue := make([]byte, valSize)
	rand.Read(wValue)
	var key int64

	if *conflicts <= 0 {
		key = int64(*connectReplica)
	} else {
		k := rand.Int31n(100)
		if k < int32(*conflicts) {
			key = int64(-1)
		} else {
			key = int64(*connectReplica)
		}
		//key = int64(rand.Int31() % int32(*conflicts+1))
	}
	val := ClientValue{
		uid: lastUID + 1,
		//uid:   rand.Int31(),
		key:   key,
		value: wValue,
	}
	for !registered {
		registered = benchmarker.register(val)
	}
	return val
}

func benchmarkValue(proxy *bindings.Parameters, value ClientValue) {
	proxy.Write(value.uid, value.key, value.value)
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*procs)
	rand.Seed(time.Now().UnixNano() * int64(os.Getpid()))

	if clientId == -1 {
		clientId = int64(uuid.New().ID())
	}

	var proxy *bindings.Parameters
	proxyMutex := sync.Mutex{}
	for {
		proxy = bindings.NewParameters(*masterAddr, *masterPort, *verbose, *noLeader, *fast, *localReads, *connectReplica)
		err := proxy.Connect()
		if err == nil {
			break
		}
		proxy.Disconnect()
	}

	benchmarker := newBenchmarker(clientId, *latencyOutput, *outputTimeseriesToFile, *timeseriesFile, *forceFlush)
	valueDone := make(chan ClientValue, *outstanding)

	// unmarshall loop
	go func() {
		proxyMutex.Lock()
		replicaReader := proxy.GetListener()
		proxyMutex.Unlock()
		for {
			rep := new(genericsmrproto.ProposeReplyTS)
			if err := rep.Unmarshal(replicaReader); err == nil {
				if rep.OK == uint8(1) {
					valueDone <- ClientValue{
						uid:   rep.CommandId,
						key:   int64(rep.CommandId),
						value: rep.Value,
					}
				}
			} else {
				err = errors.New("Failed to receive a response.")
				os.Exit(1)
				//todo move to main loop
				//proxyMutex.Lock()
				//proxy.Connect()
				//replicaReader = proxy.GetListener()
				//benchmarker.reset()
				//beginBenchmarkingValues(benchmarker, proxy, *outstanding, lastUID)
				//proxyMutex.Unlock()
			}
		}
	}()
	lastUID := beginBenchmarkingValues(benchmarker, proxy, *outstanding, -1)
	statsTimer := time.NewTimer(time.Duration(*sampleRateMs) * time.Millisecond)
	shutdown := false
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill)
	for !shutdown {
		select {
		case <-interrupt:
			benchmarker.stop()
			shutdown = true
			break
		case <-statsTimer.C:
			benchmarker.timeseriesStep()
			statsTimer = time.NewTimer(time.Duration(*sampleRateMs) * time.Millisecond)
			break
		case value := <-valueDone:
			// todo don't send just handle in here
			done := benchmarker.close(value)
			if !done {
				//	panic("returned value already done or never started")
			} else {
				newValue := generateAndBeginBenchmarkingValue(benchmarker, *psize, *outstanding, lastUID)
				lastUID = newValue.uid
				proxyMutex.Lock()
				benchmarkValue(proxy, newValue)
				proxyMutex.Unlock()
			}
			break
		}
	}
}

func beginBenchmarkingValues(benchmarker ClientBenchmarker, proxy *bindings.Parameters, outstanding int, fromUID int32) int32 {
	for i := 0; i < outstanding; i++ {
		value := generateAndBeginBenchmarkingValue(benchmarker, *psize, outstanding, fromUID)
		proxy.Write(value.uid, value.key, value.value)
		fromUID = value.uid
	}
	return fromUID
}
