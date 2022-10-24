package proposalmanager

import (
	"epaxos/dlog"
	"epaxos/lwcproto"
	"epaxos/stdpaxosproto"
	"fmt"
	"math"
	"math/rand"
	"time"
)

type RetryInfo struct {
	Inst           int32
	AttemptedBal   lwcproto.ConfigBal
	PreempterBal   lwcproto.ConfigBal
	PreempterAt    stdpaxosproto.Phase
	Prev           int32
	TimesPreempted int32
}

type BackoffInfo struct {
	minBackoff     int32
	maxInitBackoff int32
	maxBackoff     int32
	constBackoff   bool
}

type BackoffManager struct {
	currentBackoffs map[int32]RetryInfo
	BackoffInfo
	sig     chan RetryInfo
	factor  float64
	softFac bool
	cancel  map[int32]chan struct{}
}

func BackoffManagerNew(minBO, maxInitBO, maxBO int32, signalChan chan RetryInfo, factor float64, softFac bool, constBackoff bool) *BackoffManager {
	if minBO > maxInitBO {
		panic(fmt.Sprintf("minbackoff %d, maxinitbackoff %d, incorrectly set up", minBO, maxInitBO))
	}

	return &BackoffManager{
		currentBackoffs: make(map[int32]RetryInfo),
		BackoffInfo: BackoffInfo{
			minBackoff:     minBO,
			maxInitBackoff: maxInitBO,
			maxBackoff:     maxBO,
			constBackoff:   constBackoff,
		},
		sig:     signalChan,
		cancel:  make(map[int32]chan struct{}),
		factor:  factor,
		softFac: softFac,
	}
}

// range specification, note that min <= max
type IntRange struct {
	min, max int
}

// get next random value within the interval including min and max
func (ir *IntRange) NextRandom(r *rand.Rand) int {
	return r.Intn(ir.max-ir.min+1) + ir.min
}

func (bm *BackoffManager) ShouldBackoff(inst int32, preempter lwcproto.ConfigBal, preempterPhase stdpaxosproto.Phase) bool {
	curBackoffInfo, exists := bm.currentBackoffs[inst]
	if !exists {
		return true
	} else if preempter.GreaterThan(curBackoffInfo.PreempterBal) || (preempter.Equal(curBackoffInfo.PreempterBal) && preempterPhase > curBackoffInfo.PreempterAt) {
		return true
	} else {
		return false
	}
}
func (bm *BackoffManager) CheckAndHandleBackoff(inst int32, attemptedBal lwcproto.ConfigBal, preempter lwcproto.ConfigBal, prempterPhase stdpaxosproto.Phase) (bool, int32) {
	// if we give this a pointer to the timer we could stop the previous backoff before it gets pinged
	curBackoffInfo, exists := bm.currentBackoffs[inst]

	if !bm.ShouldBackoff(inst, preempter, prempterPhase) {
		dlog.Println("Ignoring backoff request as already backing off instance for this conf-bal or a greater one")
		return false, -1
	}

	var preemptNum int32 = 0
	if exists {
		preemptNum = curBackoffInfo.TimesPreempted + 1
	}

	var next int32
	if !bm.constBackoff {
		t := bm.minBackoff * int32(math.Pow(2, float64(preemptNum)))
		if t < 0 {
			t = math.MaxInt32
		}
		next = bm.minBackoff + rand.Int31n(t)
	}

	if bm.constBackoff {
		next = bm.minBackoff
	}

	if next > bm.maxBackoff {
		next = bm.maxBackoff
	}

	if next < 0 {
		panic("can't have negative backoff")
	}
	dlog.Printf("Beginning backoff of %d us for instance %d on conf-bal %d.%d (attempt %d)", next, inst, attemptedBal.Number, attemptedBal.PropID, preemptNum)
	info := RetryInfo{
		Inst:           inst,
		AttemptedBal:   attemptedBal,
		PreempterBal:   preempter,
		PreempterAt:    prempterPhase,
		Prev:           next,
		TimesPreempted: preemptNum,
	}
	bm.currentBackoffs[inst] = info
	if _, e := bm.cancel[inst]; e {
		bm.cancel[inst] <- struct{}{}
	}
	cancel := make(chan struct{})
	bm.cancel[inst] = cancel
	go func() {
		end := time.NewTimer(time.Duration(next) * time.Microsecond)
		select {
		case <-cancel:
			break
		case <-end.C:
			bm.sig <- info
			//<-cancel
			break
		}
	}()

	return true, next
}

func (bm *BackoffManager) StillRelevant(backoff RetryInfo) bool {
	curBackoff, exists := bm.currentBackoffs[backoff.Inst]

	if !exists {
		dlog.Printf("backoff has no record")
		return false
	} else {
		stillRelevant := backoff == curBackoff //DecideRetry update so that Inst also has bal backed off
		dlog.Println("Backoff of instance ", backoff.Inst, "is relevant? ", stillRelevant)
		return stillRelevant
	}
}

//func (bm *BackoffManager) ClearBackoff(inst int32) {
//	delete(bm.currentBackoffs, inst)
//}

func (bm *BackoffManager) StopBackoffs(inst int32) {
	if _, e := bm.currentBackoffs[inst]; !e {
		return
	}
	bm.cancel[inst] <- struct{}{}
	delete(bm.currentBackoffs, inst)
	delete(bm.cancel, inst)
}

// estimate time for a proposer to receive promise from you and all other acceptors in group (depends on thrifty,
// estimate time for the proposer to send to the acceptor group
// time to hear learn (either from the proposer, or from the acceptor group -- depends on bcast commit)
