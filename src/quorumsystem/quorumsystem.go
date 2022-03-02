package quorumsystem

import (
	"dlog"
	"fastrpc"
	"genericsmr"
	"math"
	"math/rand"
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
	if len(acc) < 2*constructor.F+1 {
		panic("Acceptor group is too small to tolerate this number of failures")
	}
	return &CountingQuorumSynodQuorumSystem{
		p1size:           len(acc) - constructor.F,
		p2size:           constructor.F + 1,
		thrifty:          constructor.Thrifty,
		Replica:          constructor.Replica,
		allAids:          acc,
		broadcastFastest: constructor.BroadcastFastest,
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
	return &GridQuorumSynodQuorumSystem{
		cols: cols,
		rows: rows,
		//	all:     acc,
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

// in this construction excess acceptors are simply discounted
func (constructor *SynodTringleGridQuorumSystemConstructor) Construct(acc []int) SynodQuorumSystem {
	sqrt := int(math.Ceil(math.Sqrt(float64(len(acc)))))
	if sqrt < constructor.F+1 {
		panic("Acceptor group is too small to tolerate this number of failures")
	}
	cols := make([][]int, sqrt)
	rows := make([][]int, sqrt)

	j := 0
	k := 0
	colLen := sqrt + 1
	for i := 0; i < len(cols); i++ {
		j = k + colLen
		a := acc[k:j]
		k = j
		//log.Println(a)
		cols[i] = a
		colLen--
	}

	for i := 0; i < len(rows); i++ {
		rowLen := i + 1
		row := make([]int, rowLen)
		for l := 0; l < rowLen; l++ {
			row[l] = cols[l][i-l]
		}
	}

	return &GridQuorumSynodQuorumSystem{
		cols: cols,
		rows: rows,
		//all:     acc,
		thrifty: constructor.Thrifty,
		Replica: constructor.Replica,
	}
}

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
	p1size  int
	p2size  int
	crtQrm  quorum.CountingQuorumTally
	thrifty bool
	allAids []int
	*genericsmr.Replica
	Phase
	crtQrmSize       int
	bcastAttempts    int
	broadcastFastest bool
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

func (qrmSys *CountingQuorumSynodQuorumSystem) Broadcast(code uint8, msg fastrpc.Serializable) []int {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()

	qrmSys.Replica.CalculateAlive()
	sentTo := make([]int, 0, qrmSys.Replica.N-1)

	if qrmSys.thrifty { //&& qrmSys.bcastAttempts < 2 {
		groupSize := qrmSys.crtQrmSize - 1
		numSent := 0
		// find out which quorums are alive

		if qrmSys.broadcastFastest {
			qrmSys.Replica.SortPeerOrderByLatency()
		} else {
			rand.Shuffle(len(qrmSys.PreferredPeerOrder), func(i, j int) {
				qrmSys.PreferredPeerOrder[i], qrmSys.PreferredPeerOrder[j] = qrmSys.PreferredPeerOrder[j], qrmSys.PreferredPeerOrder[i]
			})
		}

		for _, a := range qrmSys.PreferredPeerOrder {
			if qrmSys.Alive[a] {
				qrmSys.Replica.SendMsg(int32(a), code, msg)
				sentTo = append(sentTo, int(a))
				numSent++
			}
			if numSent == groupSize {
				qrmSys.bcastAttempts++
				return sentTo // we managed to send to a quorum so can return
			}
		} // if fail to find a quorum, just fall back on sending to all
		qrmSys.bcastAttempts++
	}
	// send to all
	println("sending to all")
	sendToAll(qrmSys.PreferredPeerOrder, qrmSys.Replica, code, msg)
	for i := 0; i < qrmSys.N; i++ {
		if i == int(qrmSys.Id) || !qrmSys.Alive[i] {
			continue
		}
		sentTo = append(sentTo, i)
	}
	return sentTo
}

type GridQuorumSynodQuorumSystem struct {
	cols [][]int
	rows [][]int
	//all     []int
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

	sentTo := make([]int, 0, qrmSys.Replica.N-1)

	if qrmSys.Phase == ACCEPTANCE {
		//log.Println("Acceptance broadcast")
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
			qrmSelected := -1
			// choose a random qrm that is alive
			if qrmSys.broadcastFastest {
				bestQrm := -1
				bestScore := int32(math.MaxInt32)
				for qrm := range livingQuorums {
					crtScore := int32(0)
					for acc := range possibleQrms[qrm] {
						crtScore += qrmSys.Replica.ReplicasLatenciesOrders[acc]
					}
					if crtScore < bestScore {
						bestScore = crtScore
						bestQrm = qrm
					}
				}
				qrmSelected = bestQrm
			} else {
				qrmSelected = rand.Intn(len(livingQuorums))
			}

			for _, a := range possibleQrms[livingQuorums[qrmSelected]] {
				// don't need to send to self, just need to record our acknowledgement
				if a32 := int32(a); qrmSys.Replica.Id != a32 {
					//log.Println("Sending to %d", a32)
					qrmSys.Replica.SendMsg(a32, code, msg)
					sentTo = append(sentTo, a)

				}
			}
			return sentTo
		}
		qrmSys.bcastAttempts++
		// fall back on sending to all
	}
	// send to allAids
	sendToAll(qrmSys.PreferredPeerOrder, qrmSys.Replica, code, msg)
	for i := 0; i < qrmSys.N; i++ {
		if i == int(qrmSys.Id) || !qrmSys.Alive[i] {
			continue
		}
		sentTo = append(sentTo, i)
	}
	return sentTo
}

func sendToAll(all []int32, replica *genericsmr.Replica, code uint8, msg fastrpc.Serializable) {
	// send to allAids
	replica.CalculateAlive()
	for _, aid := range all {
		if replica.Alive[aid] && int32(aid) != replica.Id {
			replica.SendMsg(int32(aid), code, msg)
		}
	}
}

//type BetterGridQuorumSynodQuorumSystem struct {
//	cols    [][]int
//	rows    [][]int
//	all     []int
//	crtQrm  quorum.SpecificQuorumTally
//	thrifty bool
//	*genericsmr.Replica
//	Phase
//	bcastAttempts int
//
//	whichRowPromised int
//}
//
//func (qrmSys *BetterGridQuorumSynodQuorumSystem) QuorumReached() bool {
//	return qrmSys.crtQrm.Reached()
//}
//
//func (qrmSys *BetterGridQuorumSynodQuorumSystem) AddToQuorum(i int) {
//	qrmSys.crtQrm.Add(i)
//}
//
//func (qrmSys *BetterGridQuorumSynodQuorumSystem) HasAcknowledged(i int) bool {
//	return qrmSys.crtQrm.Acknowledged(i)
//}
//
//func (qrmSys *BetterGridQuorumSynodQuorumSystem) StartPromiseQuorum() {
//	qrmSys.crtQrm = quorum.SpecificQuorumTally{
//		Qrms:           qrmSys.rows,
//		ResponseHolder: quorum.ResponseHolder{make(map[int]struct{}), make(map[int]struct{})},
//	}
//	qrmSys.Phase = PROMISE
//}
//
//func (qrmSys *BetterGridQuorumSynodQuorumSystem) StartAcceptanceQuorum() {
//	qrmSys.crtQrm = quorum.SpecificQuorumTally{
//		Qrms:           qrmSys.cols,
//		ResponseHolder: quorum.ResponseHolder{make(map[int]struct{}), make(map[int]struct{})},
//	}
//	qrmSys.Phase = ACCEPTANCE
//}
//
//func (qrmSys *BetterGridQuorumSynodQuorumSystem) Broadcast(code uint8, msg fastrpc.Serializable) {
//	defer func() {
//		if err := recover(); err != nil {
//			dlog.Println("Prepare bcast failed:", err)
//		}
//	}()
//
//	possibleQrms := qrmSys.rows
//	if qrmSys.Phase == ACCEPTANCE {
//		//log.Println("Acceptance broadcast")
//		possibleQrms = qrmSys.cols
//	}
//
//	qrmSys.Replica.CalculateAlive()
//	if qrmSys.thrifty && qrmSys.bcastAttempts < 2 {
//		// find out which quorums are alive
//		livingQuorums := make([]int, 0, len(possibleQrms))
//		for i, qrm := range possibleQrms {
//			for j := 0; j < len(qrm); j++ {
//				if !qrmSys.Replica.Alive[qrm[j]] && int32(qrm[j]) != qrmSys.Replica.Id { // we will always appear dead
//					break
//				}
//			}
//			livingQuorums = append(livingQuorums, i)
//		}
//
//		if len(livingQuorums) > 0 {
//			// choose a random qrm that is alive
//			qrmSelected := rand.Intn(len(livingQuorums))
//
//			for _, a := range possibleQrms[livingQuorums[qrmSelected]] {
//				// don't need to send to self, just need to record our acknowledgement
//				if a32 := int32(a); qrmSys.Replica.Id != a32 {
//					//log.Println("Sending to %d", a32)
//					qrmSys.Replica.SendMsg(a32, code, msg)
//				} // else {
//				//	qrmSys.crtQrm.Add(a)
//				//}
//			}
//			return
//		}
//		qrmSys.bcastAttempts++
//		// fall back on sending to all
//	}
//	// send to allAids
//	sendToAll(qrmSys.all, qrmSys.Replica, code, msg)
//}
