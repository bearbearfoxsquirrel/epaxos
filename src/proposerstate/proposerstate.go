package proposerstate

import (
	"fastrpc"
	"genericsmr"
	"proposer"
	"time"
)

type Stateiser struct {
	rpc         uint8
	Recv        chan fastrpc.Serializable
	crtInstance int32
	*genericsmr.Replica
	stateFrequency time.Duration
	proposer.Proposer
	T *time.Timer
}

func StateiserNew(replica *genericsmr.Replica, proposer proposer.Proposer, freq time.Duration) Stateiser {
	var channel chan fastrpc.Serializable = make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE)
	rpc := replica.RegisterRPC(new(State), channel)
	return Stateiser{
		rpc:            rpc,
		Recv:           channel,
		crtInstance:    0,
		Replica:        replica,
		stateFrequency: freq,
		Proposer:       proposer,
		T:              new(time.Timer),
	}
}

func (state *Stateiser) SetCurrentInstance(inst int32) {
	state.crtInstance = inst
}

func (state *Stateiser) GetCurrentInstance() int32 {
	return state.crtInstance
}

func (state *Stateiser) BcastCurrentState() {
	for i := int32(0); i < int32(state.N); i++ {
		if i == state.Id {
			continue
		}
		msg := State{
			ProposerID:      state.Id,
			CurrentInstance: state.crtInstance,
		}
		state.SendMsg(i, state.rpc, &msg)
	}
}

func (state *Stateiser) BeginTimer() {
	if state.T == nil {
		state.T = time.NewTimer(state.stateFrequency)
	} else {
		state.T.Reset(state.stateFrequency)
	}
}

//func (state *Stateiser) StateLoop() {
//	timer := time.NewTimer(state.stateFrequency)
//	for state.Shutdown {
//		select {
//		case <-timer.C:
//			state.BcastCurrentState()
//			timer.Reset(state.stateFrequency)
//		case msg := <-state.Recv:
//			state.handleState(msg)
//		}
//	}
//}

func (state *Stateiser) HandleState(stateMsg *State, proposer proposer.Proposer) {
	proposer.IncrementCurrentInstanceTo(stateMsg.CurrentInstance)
}
