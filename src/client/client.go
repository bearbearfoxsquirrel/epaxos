package main

import (
	"bindings"
	"dlog"
	"errors"
	"flag"
	"fmt"
	"genericsmrproto"
	"github.com/google/uuid"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
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
var conflicts *int = flag.Int("c", 1, "Num keys to conflict on. Defaults to 1")
var verbose *bool = flag.Bool("v", false, "verbose mode. ")
var scan *bool = flag.Bool("s", false, "replace read with short scan (100 elements)")
var connectReplica *int = flag.Int("connectreplica", -1, "Must state which replica to send requests to")
var latencyOutput = flag.String("lato", "", "Must state where resultant latencies will be written")
var settleInTime = flag.Int("settletime", 60, "Number of seconds to allow before recording latency")
var numLatenciesRecording = flag.Int("numlatencies", -1, "Number of latencies to record")
var timeLatenciesRecording = flag.Int("timerecordlatsecs", -1, "How long to record latencies for")

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
}

func NewTimeseriesStates() TimeseriesStats {
	return TimeseriesStats{
		minLatency:        math.MaxInt64,
		maxLatency:        0,
		avgLatency:        0,
		deliveredRequests: 0,
		deliveredBytes:    0,
	}
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

type LatencyRecorder struct {
	outputFile             *os.File
	totalLatenciesToRecord int
	beginRecording         chan bool
	shouldRecord           bool
	numLatenciesLeft       int
	timeLatenciesRecording time.Duration
	stopRecording          chan struct{}
}

func (latencyRecorder *LatencyRecorder) record(latencyMicroseconds int64) {
	select {
	case <-latencyRecorder.beginRecording:
		latencyRecorder.shouldRecord = true
		go func() {
			timer := time.NewTimer(latencyRecorder.timeLatenciesRecording)
			<-timer.C
			latencyRecorder.stopRecording <- struct{}{}
		}()
		break
	case <-latencyRecorder.stopRecording:
		latencyRecorder.shouldRecord = false
		break
	default:
		break
	}

	if latencyRecorder.shouldRecord && (latencyRecorder.numLatenciesLeft > 0 || latencyRecorder.totalLatenciesToRecord == -1) {
		_, err := latencyRecorder.outputFile.WriteString(fmt.Sprintf("%d\n", latencyMicroseconds))
		if err != nil {
			dlog.Println("Error writing value")
			return
		}
		latencyRecorder.numLatenciesLeft--

	}
}

func NewLatencyRecorder(outputFileLoc string, settleTime int, numLatenciesToRecord int, timeLatenciesRecording time.Duration) LatencyRecorder {
	file, err := os.Create(outputFileLoc)
	if err != nil {
		panic("Cannot open latency recording output file at location")
	}

	recorder := LatencyRecorder{
		outputFile:             file,
		totalLatenciesToRecord: numLatenciesToRecord,
		numLatenciesLeft:       numLatenciesToRecord,
		timeLatenciesRecording: timeLatenciesRecording,
		shouldRecord:           false,
		beginRecording:         make(chan bool),
		stopRecording:          make(chan struct{}),
	}

	timer := time.NewTimer(time.Duration(settleTime) * time.Second)

	go func() {
		<-timer.C
		recorder.beginRecording <- true
	}()

	return recorder
}

type ClientBenchmarker struct {
	timeseriesStates     TimeseriesStats
	valueSubmissionTimes map[int32]time.Time
	latencyRecorder      LatencyRecorder
	clientID             int64
}

func newBenchmarker(clientID int64, numLatenciesToRecord int, settleTime int, recordedLatenciesPath string, timeLatenciesRecording time.Duration) ClientBenchmarker {
	benchmarker := ClientBenchmarker{
		timeseriesStates:     NewTimeseriesStates(),
		valueSubmissionTimes: make(map[int32]time.Time),
		latencyRecorder:      NewLatencyRecorder(recordedLatenciesPath, settleTime, numLatenciesToRecord, timeLatenciesRecording),
		clientID:             clientID,
	}

	return benchmarker
}

func (benchmarker *ClientBenchmarker) reset() {
	benchmarker.valueSubmissionTimes = make(map[int32]time.Time)
	benchmarker.timeseriesStates.reset()
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
	benchmarker.timeseriesStates.update(int64(len(value.value)), lat)
	delete(benchmarker.valueSubmissionTimes, value.uid)
	return true
}

func (benchmarker *ClientBenchmarker) timeseriesStep() {
	log.Println(benchmarker.timeseriesStates.String())
	benchmarker.timeseriesStates.reset()
}

func generateAndBeginBenchmarkingValue(benchmarker ClientBenchmarker, valSize int, maxOutstanding int) ClientValue {
	if len(benchmarker.valueSubmissionTimes) == maxOutstanding {
		panic("too many added to client outstadning values")
	}
	registered := false
	wValue := make([]byte, valSize)
	rand.Read(wValue)
	val := ClientValue{
		uid:   rand.Int31(),
		key:   rand.Int63() % int64(*conflicts+1),
		value: wValue,
	}
	for !registered {
		registered = benchmarker.register(val)
	}
	return val
}

func benchmarkValue(proxy *bindings.Parameters, value ClientValue) {
	proxy.Write(int32(value.uid), value.key, value.value)
}

func main() {

	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	rand.Seed(time.Now().UnixNano())

	//if *conflicts > 100 {
	//	log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	//}

	var proxy *bindings.Parameters
	for {
		proxy = bindings.NewParameters(*masterAddr, *masterPort, *verbose, *noLeader, *fast, *localReads, *connectReplica)
		err := proxy.Connect()
		if err == nil {
			break
		}
		proxy.Disconnect()
	}

	if clientId == -1 {
		clientId = int64(uuid.New().ID())
	}

	benchmarker := newBenchmarker(clientId, *numLatenciesRecording, *settleInTime, *latencyOutput, time.Second*time.Duration(*timeLatenciesRecording))

	valueDone := make(chan ClientValue, *outstanding)

	go func() {
		replicaReader := proxy.GetListener()
		for {

			rep := new(genericsmrproto.ProposeReplyTS)
			if err := rep.Unmarshal(replicaReader); err == nil {
				if rep.OK == uint8(1) {
					valueDone <- ClientValue{
						uid:   rep.CommandId,
						key:   int64(rand.Int31() % int32(*conflicts+1)),
						value: rep.Value,
					}
				} else {
					err = errors.New("Failed to receive a response.")
					replicaReader = proxy.GetListener()
					benchmarker.reset()
					beginBenchmarkingValues(benchmarker, proxy, *outstanding)
				}
			}
		}
	}()

	beginBenchmarkingValues(benchmarker, proxy, *outstanding)

	shouldStats := make(chan bool)
	statsTimer := time.NewTimer(time.Duration(1) * time.Second)
	go func() {
		<-statsTimer.C
		shouldStats <- true
	}()

	// set up listener chan

	shutdown := false
	for !shutdown {
		select {
		case <-shouldStats:
			benchmarker.timeseriesStep()
			statsTimer = time.NewTimer(time.Second)
			go func() {
				<-statsTimer.C
				shouldStats <- true
			}()
			break
		case value := <-valueDone:
			done := benchmarker.close(value)
			if !done {
				//	panic("returned value already done or never started")
			} else {
				newValue := generateAndBeginBenchmarkingValue(benchmarker, *psize, *outstanding)

				benchmarkValue(proxy, newValue)
			}
			//case value <-listener:
			break
			//default:
		}
	}

}

func beginBenchmarkingValues(benchmarker ClientBenchmarker, proxy *bindings.Parameters, outstanding int) {
	for i := 0; i < outstanding; i++ {
		value := generateAndBeginBenchmarkingValue(benchmarker, *psize, outstanding)
		proxy.Write(value.uid, value.key, value.value)
	}
}
