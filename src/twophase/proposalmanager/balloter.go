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

func (balloter *Balloter) UpdateProposalChosen() {
	balloter.TimeSinceValueLastSelected = time.Now()
}

func (balloter *Balloter) GetNextProposingBal(config int32, maxPrevRoundNum int32) lwcproto.ConfigBal {
	mini := ((maxPrevRoundNum / balloter.MaxInc) + 1) * balloter.MaxInc
	var max int32
	max = mini + balloter.MaxInc
	zero := time.Time{}
	if balloter.TimeSinceValueLastSelected != zero && balloter.DoTimeBasedBallot {
		timeDif := time.Now().Sub(balloter.TimeSinceValueLastSelected)
		diff := timeDif.Microseconds()
		if timeDif.Microseconds() > 1e+7 { // cap of 10 seconds
			diff = 1e+7
		}
		jr := int32(10000) //within milliseconds just randomise
		next := mapper(float64(diff), 0, 1e+7, mini, max-jr)
		if next < mini {
			panic("too low bal")
		}
		if next > max {
			panic("too high bal")
		}

		j := int32(math.Floor(float64(rand.Int31n(jr))))

		return lwcproto.ConfigBal{
			Config: config,
			Ballot: stdpaxosproto.Ballot{next + j, int16(balloter.PropID)},
		}
	}
	next := int32(math.Floor(float64(rand.Int31n(max-mini) + mini))) //rand.Int31()*max - mini + 1 + mini
	if balloter.PropID < 0 || next < mini {
		panic("bad round num")
	}
	return lwcproto.ConfigBal{Config: config, Ballot: stdpaxosproto.Ballot{next, int16(balloter.PropID)}}
}

func (ballot *Balloter) GetAttemptNumber(rnd int32) int {
	return int(rnd / ballot.MaxInc)
}
