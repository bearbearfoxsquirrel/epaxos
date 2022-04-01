package configtwophase

import (
	"lwcproto"
	"math"
	"math/rand"
	"time"
)

type Balloter struct {
	PropID, N, MaxInc          int32
	timeSinceValueLastSelected time.Time
}

func (balloter *Balloter) UpdateValueSelected() {
	balloter.timeSinceValueLastSelected = time.Now()
}

func (balloter *Balloter) getNextProposingBal(maxPrevRoundNum int32) lwcproto.Ballot {
	mini := ((maxPrevRoundNum/balloter.MaxInc)+1)*balloter.MaxInc + balloter.N
	var max int32
	zero := time.Time{}
	if balloter.timeSinceValueLastSelected != zero {
		timeDif := time.Now().Sub(balloter.timeSinceValueLastSelected)
		max = mini + balloter.MaxInc + int32(timeDif.Microseconds()/10)
	} else {
		max = mini + balloter.MaxInc
	}

	//next := int32(math.Floor(rand.Float64()*float64(max-mini+1) + float64(mini)))
	next := int32(math.Floor(float64(rand.Int31()*(max-mini+1) + mini)))
	if balloter.PropID < 0 || next < mini {
		panic("bad round num")
	}
	//log.Println()
	return lwcproto.Ballot{next - balloter.PropID, int16(balloter.PropID)}

}

func (ballot *Balloter) GetAttemptNumber(rnd int32) int {
	return 1
}
