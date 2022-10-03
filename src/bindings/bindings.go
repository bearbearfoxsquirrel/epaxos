package bindings

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"epaxos/genericsmrproto"
	"epaxos/masterproto"
	"epaxos/state"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const TRUE = uint8(1)
const TIMEOUT = 3 * time.Second
const MAX_ATTEMPTS = 3

type Parameters struct {
	masterAddr     string
	masterPort     int
	verbose        bool
	localReads     bool
	closestReplica int
	Leader         int
	leaderless     bool
	isFast         bool
	n              int
	replicaLists   []string
	servers        []net.Conn
	readers        []*bufio.Reader
	writers        []*bufio.Writer
	receiveMutex   sync.Mutex
	writeMutex     sync.Mutex
	id             int32
	retries        int32
}

func NewParameters(masterAddr string, masterPort int, verbose bool, leaderless bool, fast bool, localReads bool, connectReplica int) *Parameters {
	return &Parameters{
		masterAddr,
		masterPort,
		verbose,
		localReads,
		connectReplica,
		0,
		leaderless,
		fast,
		0,
		nil,
		nil,
		nil,
		nil,
		sync.Mutex{},
		sync.Mutex{},
		0,
		10}
}

func (b *Parameters) Connect() error {
	var err error

	log.Printf("Dialing master...\n")
	var master *rpc.Client
	master = b.MasterDial()

	log.Printf("Getting replica list from master...\n")
	var replyRL *masterproto.GetReplicaListReply

	// loop until the call succeeds
	// (or too many attempts)
	for i, done := 0, false; !done; i++ {
		replyRL = new(masterproto.GetReplicaListReply)
		err = Call(master, "Master.GetReplicaList", new(masterproto.GetReplicaListArgs), replyRL)
		if err == nil && replyRL.Ready {
			done = true
		} else if i == MAX_ATTEMPTS {
			// if too many attempts, connect again
			master.Close()
			return errors.New("Too many call attempts!")
		}
	}

	// get closest replica
	//err = b.FindClosestReplica(replyRL)
	b.replicaLists = replyRL.ReplicaList

	//	if err != nil {
	//		return err
	//}
	//b.closestReplica =
	log.Printf("node list %v, closest (alive) = %v", b.replicaLists, b.closestReplica)

	// init some parameters
	b.n = len(b.replicaLists)
	b.servers = make([]net.Conn, b.n)
	b.readers = make([]*bufio.Reader, b.n)
	b.writers = make([]*bufio.Writer, b.n)

	// get list of nodes to connect to
	var toConnect []int
	toConnect = append(toConnect, b.closestReplica)

	if !b.leaderless {
		log.Printf("Getting leader from master...\n")
		var replyL *masterproto.GetLeaderReply

		for i, done := 0, false; !done; i++ {
			replyL = new(masterproto.GetLeaderReply)
			err = Call(master, "Master.GetLeader", new(masterproto.GetLeaderArgs), replyL)
			if err == nil {
				done = true
			} else if i == MAX_ATTEMPTS {
				// if too many attempts, connect again
				master.Close()
				return errors.New("Too many call attempts!")
			}
		}

		b.Leader = replyL.LeaderId
		if b.closestReplica != b.Leader {
			toConnect = append(toConnect, b.Leader)
		}
		log.Printf("The Leader is replica %d\n", b.Leader)
	}

	for _, i := range toConnect {
		log.Println("Connection to ", i, " -> ", b.replicaLists[i])
		b.servers[i] = Dial(b.replicaLists[i], false)
		b.readers[i] = bufio.NewReader(b.servers[i])
		b.writers[i] = bufio.NewWriter(b.servers[i])
	}

	log.Println("Connected")

	return nil
}

func Dial(addr string, connect bool) net.Conn {
	var conn net.Conn
	var err error
	var resp *http.Response
	var done bool

	for done = false; !done; {
		conn, err = net.DialTimeout("tcp", addr, TIMEOUT)

		if err == nil {
			if connect {
				// connect if no error
				io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")
				resp, err = http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
				if err == nil && resp != nil && resp.Status == "200 Connected to Go RPC" {
					done = true
				}
			} else {
				done = true
			}
		}

		if !done {
			// if not done yet, try again
			log.Println("Connection error with ", addr, ": ", err)
			if conn != nil {
				conn.Close()
			}
		}
	}

	return conn
}

func (b *Parameters) MasterDial() *rpc.Client {
	var master *rpc.Client
	var addr string
	var conn net.Conn

	addr = fmt.Sprintf("%s:%d", b.masterAddr, b.masterPort)
	conn = Dial(addr, true)
	master = rpc.NewClient(conn)

	return master
}

func Call(cli *rpc.Client, method string, args interface{}, reply interface{}) error {
	c := make(chan error, 1)
	go func() { c <- cli.Call(method, args, reply) }()
	select {
	case err := <-c:
		if err != nil {
			log.Printf("Error in RPC: " + method)
		}
		return err

	case <-time.After(TIMEOUT):
		log.Printf("RPC timeout: " + method)
		return errors.New("RPC timeout")
	}
}

func (b *Parameters) FindClosestReplica(replyRL *masterproto.GetReplicaListReply) error {
	// save replica list and closest
	b.replicaLists = replyRL.ReplicaList

	log.Printf("Pinging all replicas...\n")

	minLatency := math.MaxFloat64
	for i := 0; i < len(b.replicaLists); i++ {
		if !replyRL.AliveList[i] {
			continue
		}
		addr := strings.Split(string(b.replicaLists[i]), ":")[0]
		if addr == "" {
			addr = "127.0.0.1"
		}
		out, err := exec.Command("ping", addr, "-c 3", "-q").Output()
		if err == nil {
			// parse ping output
			latency, _ := strconv.ParseFloat(strings.Split(string(out), "/")[4], 64)
			log.Printf("%v -> %v", i, latency)

			// save if closest replica
			if minLatency > latency {
				b.closestReplica = i
				minLatency = latency
			}
		} else {
			log.Printf("cannot ping " + b.replicaLists[i])
			return err
		}
	}

	return nil
}

func (b *Parameters) Disconnect() {
	for _, server := range b.servers {
		if server != nil {
			server.Close()
		}
	}
	log.Printf("Disconnected")
}

// not idempotent in case of a failure
func (b *Parameters) Write(id int32, key int64, value []byte) {
	//b.id++
	args := genericsmrproto.Propose{id, state.Command{state.PUT, state.Key(key), value}, 0}
	//args.CommandId = id //b.id

	if b.verbose {
		log.Println(args.Command.String())
	}

	b.execute(args)
}

func (b *Parameters) Read(id int32, key int64) {
	//b.id++
	args := genericsmrproto.Propose{id, state.Command{state.PUT, 0, state.NIL()}, 0}
	args.CommandId = id //b.id
	args.Command.K = state.Key(key)
	args.Command.Op = state.GET

	if b.verbose {
		log.Println(args.Command.String())
	}

	b.execute(args)
}

func (b *Parameters) Scan(key int64, count int64, id int32) {
	//b.id++
	args := genericsmrproto.Propose{id, state.Command{state.PUT, 0, state.NIL()}, 0}
	args.CommandId = id //b.id
	args.Command.K = state.Key(key)
	args.Command.V = make([]byte, 8)
	binary.LittleEndian.PutUint64(args.Command.V, uint64(count))
	args.Command.Op = state.SCAN

	if b.verbose {
		log.Println(args.Command.String())
	}

	b.execute(args)
}

func (b *Parameters) Stats() string {
	b.writers[b.closestReplica].WriteByte(genericsmrproto.STATS)
	b.writers[b.closestReplica].Flush()
	arr := make([]byte, 1000)
	b.readers[b.closestReplica].Read(arr)
	return string(bytes.Trim(arr, "\x00"))
}

// internals

func (b *Parameters) Submit(args genericsmrproto.Propose) {
	if b.isFast {
		log.Fatal("NYI")
	}

	err := errors.New("")
	//value := state.NIL()

	for err != nil {

		submitter := b.Leader
		if b.leaderless {
			submitter = b.closestReplica
		}

		if !b.isFast {
			//b.writeMutex.Lock()
			b.writers[submitter].WriteByte(genericsmrproto.PROPOSE)
			args.Marshal(b.writers[submitter])
			b.writers[submitter].Flush()
			//b.writeMutex.Unlock()
		} else {
			//send to everyone
			for rep := 0; rep < b.n; rep++ {
				b.writeMutex.Lock()
				b.writers[rep].WriteByte(genericsmrproto.PROPOSE)
				args.Marshal(b.writers[rep])
				b.writers[rep].Flush()
				b.writeMutex.Unlock()
			}
		}

		if b.verbose {
			log.Println("Sent to ", submitter)
		}
	}
}

/*
	func (b *Parameters) ListenForResponses() state.Value {
		for i := 0; i < b.n; i++ {
			if b.servers[i] != nil {
				value, err := b.waitReplies(i)
				if err != nil {

					log.Println("Error: ", err)

					for err != nil && b.retries > 0 {
						b.retries--
						b.Disconnect()
						log.Println("Reconnecting ...")
						time.Sleep(TIMEOUT) // must be inline with the closest quorum re-computation
						err = b.Connect()
					}

					if err != nil && b.retries == 0 {
						log.Fatal("Cannot recover.")
					}

				}
				return value
			}
		}
	}
*/
func (b *Parameters) execute(args genericsmrproto.Propose) {
	if b.isFast {
		log.Fatal("NYI")
	}

	//err := errors.New("")
	//value := state.NIL()

	//	for err != nil {

	submitter := b.Leader
	if b.leaderless {
		submitter = b.closestReplica
	}

	if !b.isFast {
		//b.writeMutex.Lock()
		b.writers[submitter].WriteByte(genericsmrproto.PROPOSE)
		args.Marshal(b.writers[submitter])
		b.writers[submitter].Flush()
		//b.writeMutex.Unlock()
	} else {
		//send to everyone
		for rep := 0; rep < b.n; rep++ {
			b.writeMutex.Lock()
			b.writers[rep].WriteByte(genericsmrproto.PROPOSE)
			args.Marshal(b.writers[rep])
			b.writers[rep].Flush()
			b.writeMutex.Unlock()
		}
	}

	if b.verbose {
		log.Println("Sent to ", submitter)
	}
	//	}
	/*	value, err = b.waitReplies(submitter)
			if err != nil {

				log.Println("Error: ", err)

					for err != nil && b.retries > 0 {
						b.retries--
						b.Disconnect()
						log.Println("Reconnecting ...")
						time.Sleep(TIMEOUT) // must be inline with the closest quorum re-computation
						err = b.Connect()
					}

					if err != nil && b.retries == 0 {
						log.Fatal("Cannot recover.")
					}

				}

		}

		if b.verbose {
			log.Println("Returning: ", value.String())
		}

		return value*/
}

func (b *Parameters) waitReplies(submitter int) (state.Value, error) {
	var err error
	ret := state.NIL()
	rep := new(genericsmrproto.ProposeReplyTS)
	//b.receiveMutex.Lock()
	if err = rep.Unmarshal(b.readers[submitter]); err == nil {
		if rep.OK == TRUE {
			//	log.Println("Got response OK")
			//	log.Print(rep.CommandId, rep.Value)
			ret = rep.Value
		} else {
			err = errors.New("Failed to receive a response.")
		}

	}
	//	b.receiveMutex.Unlock()
	return ret, err
}

func (b *Parameters) GetListener() *bufio.Reader {
	return b.readers[b.closestReplica]
}
