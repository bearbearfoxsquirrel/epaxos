package quorumsystem

import (
	"dlog"
	"fastrpc"
	"genericsmr"
	"math"
	"quorum"
)

//todo figure out when to reset bcast attempts, new phase or never?

type SynodQuorumSystemConstructor interface {
	Construct(acceptors []int) SynodQuorumSystem
	//Accept(augment Augmentor)
}

type SynodCountingQuorumSystemConstructor struct {
	F       int
	Thrifty bool
	*genericsmr.Replica
	BroadcastFastest bool
	//broadcastStrat BroadcastStrat
}

func (constructor *SynodCountingQuorumSystemConstructor) Construct(acc []int) SynodQuorumSystem {
	amIInQrm := false
	if len(acc) == constructor.N {
		amIInQrm = true
	} else {
		for _, aid := range acc {
			if aid == int(constructor.Id) {
				amIInQrm = true
				break
			}
		}
	}
	if len(acc) < 2*constructor.F+1 {
		panic("Acceptor group is too small to tolerate this number of failures")
	}
	return &CountingQuorumSynodQuorumSystem{
		p1size:           len(acc) - constructor.F,
		p2size:           constructor.F + 1,
		thrifty:          constructor.Thrifty,
		Replica:          constructor.Replica,
		possibleAids:     acc,
		broadcastFastest: constructor.BroadcastFastest,
		amIInQrm:         amIInQrm,
	}
}

type SynodGridQuorumSystemConstructor struct {
	F                int
	Thrifty          bool
	BroadcastFastest bool
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
		//log.Println(a)
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
		//log.Println(row)
		rows[i] = row
	}
	all := make([]int, len(acc))
	for i, aid := range acc {
		all[i] = aid
	}
	return &GridQuorumSynodQuorumSystem{
		cols:             cols,
		rows:             rows,
		all:              all,
		thrifty:          constructor.Thrifty,
		Replica:          constructor.Replica,
		broadcastFastest: constructor.BroadcastFastest,
	}
}

type SynodTringleGridQuorumSystemConstructor struct {
	F       int
	Thrifty bool
	*genericsmr.Replica
}

//
//// in this construction excess acceptors are simply discounted
//func (constructor *SynodTringleGridQuorumSystemConstructor) Construct(acc []int) SynodQuorumSystem {
//	sqrt := int(math.Ceil(math.Sqrt(float64(len(acc)))))
//	if sqrt < constructor.F+1 {
//		panic("Acceptor group is too small to tolerate this number of failures")
//	}
//	cols := make([][]int, sqrt)
//	rows := make([][]int, sqrt)
//
//	j := 0
//	k := 0
//	colLen := sqrt + 1
//	for i := 0; i < len(cols); i++ {
//		j = k + colLen
//		a := acc[k:j]
//		k = j
//		//log.Println(a)
//		cols[i] = a
//		colLen--
//	}
//
//	for i := 0; i < len(rows); i++ {
//		rowLen := i + 1
//		row := make([]int, rowLen)
//		for l := 0; l < rowLen; l++ {
//			row[l] = cols[l][i-l]
//		}
//	}
//
//	return &GridQuorumSynodQuorumSystem{
//		cols: cols,
//		rows: rows,
//		//all:     acc,
//		thrifty: constructor.Thrifty,
//		Replica: constructor.Replica,
//	}
//}

type Phase int

const (
	PROMISE Phase = iota
	ACCEPTANCE
)

type SynodQuorumSystem interface {
	StartPromiseQuorum()
	StartAcceptanceQuorum()
	QuorumReached() bool
	//	GetPhase() Phase
	//  GetResponders() []int
	AddToQuorum(int)
	HasAcknowledged(int) bool
	Broadcast(code uint8, msg fastrpc.Serializable) []int
	// INSTANCECHOSEN(CHOSEN MSG)

	//	RetryBroadcast(code uint8, msg fastrpc.Serializable)
	//	BeConstructed(constructor QuorumSystemConstructor)
}

type CountingQuorumSynodQuorumSystem struct {
	p1size       int
	p2size       int
	crtQrm       quorum.CountingQuorumTally
	thrifty      bool
	possibleAids []int
	*genericsmr.Replica
	Phase
	bcastAttempts    int
	broadcastFastest bool
	amIInQrm         bool
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
	qrmSys.Phase = PROMISE
}

func (qrmSys *CountingQuorumSynodQuorumSystem) StartAcceptanceQuorum() {
	qrmSys.crtQrm = quorum.CountingQuorumTally{
		Threshold:      qrmSys.p2size,
		ResponseHolder: quorum.ResponseHolder{make(map[int]struct{}), make(map[int]struct{})},
	}
	qrmSys.Phase = ACCEPTANCE
}

func (qrmSys *CountingQuorumSynodQuorumSystem) Broadcast(code uint8, msg fastrpc.Serializable) []int {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()
	sendSize := qrmSys.crtQrm.Threshold
	sentTo := make([]int, 0, qrmSys.crtQrm.Threshold)
	if qrmSys.amIInQrm {
		sendSize--
		sentTo = append(sentTo, int(qrmSys.Id))
	}
	if qrmSys.thrifty { //&& qrmSys.bcastAttempts < 2 {
		var peerList []int32
		if qrmSys.broadcastFastest {
			peerList = qrmSys.Replica.GetLatencyPeerOrder()
		} else {
			peerList = qrmSys.Replica.GetAliveRandomPeerOrder()
		}

		for _, a := range peerList {
			numSent := 0
			for _, pa := range qrmSys.possibleAids {
				if int32(pa) == a && int32(pa) != qrmSys.Id {
					qrmSys.Replica.SendMsg(a, code, msg)
					//	log.Println("sent to ", a, "I am ", qrmSys.Id)
					sentTo = append(sentTo, int(a))
					numSent++
					break
				}

			}
			if numSent == sendSize {
				qrmSys.bcastAttempts++
				return sentTo // we managed to send to a quorum so can return
			}
		} // if fail to find a quorum, just fall back on sending to all
		qrmSys.bcastAttempts++
	}
	// send to all
	//log.Println("sending to all")
	sendToAll(qrmSys.possibleAids, qrmSys.Replica, code, msg)
	qrmSys.Replica.Mutex.Lock()
	for _, i := range qrmSys.possibleAids {
		if !qrmSys.Alive[i] {
			continue
		}
		sentTo = append(sentTo, i)
	}
	qrmSys.Replica.Mutex.Unlock()
	return sentTo
}

type GridQuorumSynodQuorumSystem struct {
	cols    [][]int
	rows    [][]int
	all     []int
	crtQrm  quorum.SpecificQuorumTally
	thrifty bool
	*genericsmr.Replica
	Phase
	bcastAttempts    int
	broadcastFastest bool
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

func (qrmSys *GridQuorumSynodQuorumSystem) Broadcast(code uint8, msg fastrpc.Serializable) []int {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()

	possibleQrms := qrmSys.rows
	if qrmSys.Phase == ACCEPTANCE {
		possibleQrms = qrmSys.cols
	}

	sentTo := make([]int, 0, qrmSys.Replica.N)
	if qrmSys.thrifty { //qrmSys.bcastAttempts < 2 {
		var selectedQrm []int
		if qrmSys.broadcastFastest {
			selectedQrm = qrmSys.Replica.FromCandidatesSelectBestLatency(possibleQrms)
		} else {
			selectedQrm = qrmSys.Replica.FromCandidatesSelectRandom(possibleQrms)
		}
		if len(selectedQrm) > 0 {
			for _, aid := range selectedQrm {
				sentTo = append(sentTo, aid)
				if aid == int(qrmSys.Replica.Id) {
					continue
				}
				qrmSys.Replica.SendMsg(int32(aid), code, msg)
			}
			qrmSys.bcastAttempts++
			return sentTo
		}
		// fall back on sending to all
	}
	qrmSys.bcastAttempts++
	// send to possibleAids
	sendToAll(qrmSys.all, qrmSys.Replica, code, msg)
	qrmSys.Mutex.Lock()
	for _, i := range qrmSys.all {
		if !qrmSys.Alive[i] {
			continue
		}
		sentTo = append(sentTo, i)
	}
	qrmSys.Mutex.Unlock()
	return sentTo
}

func sendToAll(all []int, replica *genericsmr.Replica, code uint8, msg fastrpc.Serializable) {
	// send to possibleAids
	replica.CalculateAlive()
	//replica.Mutex.Lock()
	for _, aid := range all {
		if int32(aid) != replica.Id {
			replica.SendMsg(int32(aid), code, msg)
		}
	}
	//replica.Mutex.Unlock()
}
