package quorumsystem

import (
	"dlog"
	"fastrpc"
	"genericsmr"
	"log"
	"math"
	"math/rand"
	"quorum"
)

type SynodQuorumSystemConstructor interface {
	Construct(acceptors []int) SynodQuorumSystem
	//Accept(augment Augmentor)
}

type SynodCountingQuorumSystemConstructor struct {
	F       int
	Thrifty bool
	*genericsmr.Replica
	//broadcastStrat BroadcastStrat
}

func (constructor *SynodCountingQuorumSystemConstructor) Construct(acc []int) SynodQuorumSystem {
	if len(acc) < 2*constructor.F+1 {
		panic("Acceptor group is too small to tolerate this number of failures")
	}
	return &CountingQuorumSynodQuorumSystem{
		p1size:  len(acc) - constructor.F,
		p2size:  constructor.F + 1,
		thrifty: constructor.Thrifty,
		Replica: constructor.Replica,
	}
}

type SynodGridQuorumSystemConstructor struct {
	F       int
	Thrifty bool
	*genericsmr.Replica
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func (constructor *SynodGridQuorumSystemConstructor) Construct(acc []int) SynodQuorumSystem {
	groupN := len(acc)
	sqrt := int(math.Ceil(math.Sqrt(float64(len(acc)))))
	if sqrt < constructor.F+1 {
		panic("Acceptor group is too small to tolerate this number of failures")
	}
	cols := make([][]int, sqrt)
	rows := make([][]int, constructor.F+1)

	j := 0
	k := 0

	for i := 0; i < len(cols); i++ {
		j = min(k+constructor.F+1, groupN)
		a := acc[k:j]
		k = j
		log.Println(a)
		cols[i] = a
	}

	rem := groupN
	for i := 0; i < len(rows); i++ {
		rowLen := min(sqrt, rem)
		row := make([]int, rowLen)
		for l := 0; l < rowLen; l++ {
			row[l] = cols[l][i]
		}
		rem -= rowLen
		log.Println(row)
		rows[i] = row
	}
	return &GridQuorumSynodQuorumSystem{
		cols:    cols,
		rows:    rows,
		all:     acc,
		thrifty: constructor.Thrifty,
		Replica: constructor.Replica,
	}
}

type Phase int

const (
	PROMISE Phase = iota
	ACCEPTANCE
	CHOSEN
)

type SynodQuorumSystem interface {
	StartPromiseQuorum()
	StartAcceptanceQuorum()
	QuorumReached() bool
	//	GetPhase() Phase
	//  GetResponders() []int
	AddToQuorum(int)
	HasAcknowledged(int) bool
	Broadcast(code uint8, msg fastrpc.Serializable)
	// INSTANCECHOSEN(CHOSEN MSG)

	//	RetryBroadcast(code uint8, msg fastrpc.Serializable)
	//	BeConstructed(constructor QuorumSystemConstructor)
}

type CountingQuorumSynodQuorumSystem struct {
	p1size  int
	p2size  int
	crtQrm  quorum.CountingQuorumTally
	thrifty bool
	allAids []int
	*genericsmr.Replica
	Phase
	crtQrmSize    int
	bcastAttempts int
}

func (qrmSys *CountingQuorumSynodQuorumSystem) QuorumReached() bool {
	return qrmSys.crtQrm.Reached()
}

func (qrmSys *CountingQuorumSynodQuorumSystem) AddToQuorum(i int) {
	qrmSys.crtQrm.Add(i)
}

func (qrmSys *CountingQuorumSynodQuorumSystem) HasAcknowledged(i int) bool {
	return qrmSys.crtQrm.Acknowledged(i)
}

//
//func (qrmSys *CountingQuorumSynodQuorumSystem) GetResponders() []int {
//	return qrmSys.crtQrm.
//}

func (qrmSys *CountingQuorumSynodQuorumSystem) StartPromiseQuorum() {
	qrmSys.crtQrm = quorum.CountingQuorumTally{
		Threshold:      qrmSys.p1size,
		ResponseHolder: quorum.ResponseHolder{make(map[int]struct{}), make(map[int]struct{})},
	}
	qrmSys.crtQrmSize = qrmSys.p1size
	qrmSys.Phase = PROMISE
}

func (qrmSys *CountingQuorumSynodQuorumSystem) StartAcceptanceQuorum() {
	qrmSys.crtQrm = quorum.CountingQuorumTally{
		Threshold:      qrmSys.p2size,
		ResponseHolder: quorum.ResponseHolder{make(map[int]struct{}), make(map[int]struct{})},
	}
	qrmSys.crtQrmSize = qrmSys.p2size
	qrmSys.Phase = ACCEPTANCE
}

func (qrmSys *CountingQuorumSynodQuorumSystem) Broadcast(code uint8, msg fastrpc.Serializable) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()

	qrmSys.Replica.CalculateAlive()
	if qrmSys.thrifty && qrmSys.bcastAttempts < 2 {
		groupSize := qrmSys.crtQrmSize
		numSent := 0
		// find out which quorums are alive

		rand.Shuffle(len(qrmSys.allAids), func(i, j int) {
			qrmSys.allAids[i], qrmSys.allAids[j] = qrmSys.allAids[j], qrmSys.allAids[i]
		})
		for _, a := range qrmSys.allAids {
			if qrmSys.Alive[a] {
				qrmSys.Replica.SendMsg(int32(a), code, msg)
				numSent++
			} else if int32(a) == qrmSys.Replica.Id {
				//	qrmSys.crtQrm.Add(a)
				numSent++
			}
			if numSent == groupSize {
				return // we managed to send to a quorum so can return
			}
		} // if fail to find a quorum, just fall back on sending to all
		qrmSys.bcastAttempts++
	}
	// send to all
	sendToAll(qrmSys.allAids, qrmSys.Replica, code, msg)
}

type GridQuorumSynodQuorumSystem struct {
	cols    [][]int
	rows    [][]int
	all     []int
	crtQrm  quorum.SpecificQuorumTally
	thrifty bool
	*genericsmr.Replica
	Phase
	bcastAttempts int
}

func (qrmSys *GridQuorumSynodQuorumSystem) QuorumReached() bool {
	return qrmSys.crtQrm.Reached()
}

func (qrmSys *GridQuorumSynodQuorumSystem) AddToQuorum(i int) {
	qrmSys.crtQrm.Add(i)
}

func (qrmSys *GridQuorumSynodQuorumSystem) HasAcknowledged(i int) bool {
	return qrmSys.crtQrm.Acknowledged(i)
}

func (qrmSys *GridQuorumSynodQuorumSystem) StartPromiseQuorum() {
	qrmSys.crtQrm = quorum.SpecificQuorumTally{
		Qrms:           qrmSys.rows,
		ResponseHolder: quorum.ResponseHolder{make(map[int]struct{}), make(map[int]struct{})},
	}
	qrmSys.Phase = PROMISE
}

func (qrmSys *GridQuorumSynodQuorumSystem) StartAcceptanceQuorum() {
	qrmSys.crtQrm = quorum.SpecificQuorumTally{
		Qrms:           qrmSys.cols,
		ResponseHolder: quorum.ResponseHolder{make(map[int]struct{}), make(map[int]struct{})},
	}
	qrmSys.Phase = ACCEPTANCE
}

func (qrmSys *GridQuorumSynodQuorumSystem) Broadcast(code uint8, msg fastrpc.Serializable) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()

	possibleQrms := qrmSys.rows
	if qrmSys.Phase == ACCEPTANCE {
		log.Println("Acceptance broadcast")
		possibleQrms = qrmSys.cols
	}

	qrmSys.Replica.CalculateAlive()
	if qrmSys.thrifty && qrmSys.bcastAttempts < 2 {
		// find out which quorums are alive
		livingQuorums := make([]int, 0, len(possibleQrms))
		for i, qrm := range possibleQrms {
			for j := 0; j < len(qrm); j++ {
				if !qrmSys.Replica.Alive[qrm[j]] && int32(qrm[j]) != qrmSys.Replica.Id { // we will always appear dead
					break
				}
			}
			livingQuorums = append(livingQuorums, i)
		}

		if len(livingQuorums) > 0 {
			// choose a random qrm that is alive
			qrmSelected := rand.Intn(len(livingQuorums))

			for _, a := range possibleQrms[livingQuorums[qrmSelected]] {
				// don't need to send to self, just need to record our acknowledgement
				if a32 := int32(a); qrmSys.Replica.Id != a32 {
					log.Println("Sending to %d", a32)
					qrmSys.Replica.SendMsg(a32, code, msg)
				} // else {
				//	qrmSys.crtQrm.Add(a)
				//}
			}
			return
		}
		qrmSys.bcastAttempts++
		// fall back on sending to all
	}
	// send to allAids
	sendToAll(qrmSys.all, qrmSys.Replica, code, msg)
}

func sendToAll(all []int, replica *genericsmr.Replica, code uint8, msg fastrpc.Serializable) {
	// send to allAids
	replica.CalculateAlive()
	for _, aid := range all {
		if replica.Alive[aid] && int32(aid) != replica.Id {
			replica.SendMsg(int32(aid), code, msg)
		}
	}
}
