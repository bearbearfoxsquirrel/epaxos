package configtwophase

import (
	"math"
	"math/rand"
	"stdpaxosproto"
	"time"
)

type Balloter struct {
	PropID, N, MaxInc          int32
	timeSinceValueLastSelected time.Time
}

func (balloter *Balloter) UpdateValueSelected() {
	balloter.timeSinceValueLastSelected = time.Now()
}

func (balloter *Balloter) getNextProposingBal(maxPrevRoundNum int32) stdpaxosproto.Ballot {
	mini := ((maxPrevRoundNum/balloter.MaxInc)+1)*balloter.MaxInc + balloter.N
	var max int32
	zero := time.Time{}
	if balloter.timeSinceValueLastSelected != zero {
		timeDif := time.Now().Sub(balloter.timeSinceValueLastSelected)
		max = mini + balloter.MaxInc + int32(timeDif.Microseconds()/10)
	} else {
		max = mini + balloter.MaxInc
	}

	next := int32(math.Floor(rand.Float64()*float64(max-mini+1) + float64(mini)))
	if balloter.PropID == -1 || next == -1 {
		panic("bad round num")
	}
	return stdpaxosproto.Ballot{next - balloter.PropID, int16(balloter.PropID)}

}

func (ballot *Balloter) GetAttemptNumber(rnd int32) int {
	return 1
}
