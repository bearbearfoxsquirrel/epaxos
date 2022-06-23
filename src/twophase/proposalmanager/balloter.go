package proposalmanager

import (
	"lwcproto"
	"math"
	"math/rand"
	"stdpaxosproto"
	"time"
)

type Balloter struct {
	PropID, N, MaxInc          int32
	TimeSinceValueLastSelected time.Time
	DoTimeBasedBallot          bool
}

//func (balloter *Balloter) Learn(bat batching.ProposalBatch) {
//	TODO implement me
//panic("implement me")
//}

func (balloter *Balloter) UpdateValueSelected() {
	balloter.TimeSinceValueLastSelected = time.Now()
}

func (balloter *Balloter) GetNextProposingBal(config int32, maxPrevRoundNum int32) lwcproto.ConfigBal {
	mini := ((maxPrevRoundNum/balloter.MaxInc)+1)*balloter.MaxInc + balloter.N
	var max int32
	zero := time.Time{}
	max = mini + balloter.MaxInc

	var next int32
	if balloter.TimeSinceValueLastSelected != zero && balloter.DoTimeBasedBallot {
		timeDif := time.Now().Sub(balloter.TimeSinceValueLastSelected)

		diff := timeDif.Milliseconds()
		if timeDif.Milliseconds() > 10000 { // cap of 10 seconds
			diff = 10000
		}
		test := 1.0 - math.Exp(float64(-0.001)*float64(diff)) //int64(math.Pow(math.E, mini-timeDif.Microseconds()))
		//log.Println(test)
		slope := float64(1.0 * (max - mini) / (1 - 0))
		next = int32(float64(mini) + slope*(test-0))
		//next = mini + round(slope*(int32(timeDif.Microseconds())-0))
		//log.Println("ballot", next)
		//max = mini + balloter.MaxInc + int32(timeDif.Microseconds()/10)

	} else {
		next = int32(math.Floor(float64(rand.Int31n(max-mini) + mini))) //rand.Int31()*max - mini + 1 + mini

	}

	if balloter.PropID < 0 || next < mini {
		panic("bad round num")
	}
	return lwcproto.ConfigBal{Config: config, Ballot: stdpaxosproto.Ballot{next - balloter.PropID, int16(balloter.PropID)}}

}

func (ballot *Balloter) GetAttemptNumber(rnd int32) int {
	return int(rnd / ballot.MaxInc)
}
