package proposalmanager

import (
	"epaxos/fastrpc"
	"epaxos/genericsmr"
	"io"
	"time"
)

type FailureEstimator struct {
	*genericsmr.Replica
	othersLatencies [][]time.Duration
	latenciesMsgs   chan fastrpc.Serializable
	latenciesMsgRPC uint8
}

type LatenciesMsg struct {
	Aid       int32
	Latencies []time.Duration
}

func (l LatenciesMsg) Marshal(writer io.Writer) {
	//TODO implement me
	panic("implement me")
}

func (l LatenciesMsg) Unmarshal(reader io.Reader) error {
	//TODO implement me
	panic("implement me")
}

func (l LatenciesMsg) New() fastrpc.Serializable {
	//TODO implement me
	panic("implement me")
}

func FailureEstimatorNew(replica *genericsmr.Replica) *FailureEstimator {

	est := &FailureEstimator{
		Replica:         replica,
		othersLatencies: make([][]time.Duration, replica.N),
		latenciesMsgs:   make(chan fastrpc.Serializable, 100),
	}

	for i := range est.othersLatencies {
		est.othersLatencies[i] = make([]time.Duration, replica.N)
	}
	est.latenciesMsgRPC = replica.RegisterRPC(new(LatenciesMsg), est.latenciesMsgs)
	return est
}

func (est *FailureEstimator) GetChan() chan fastrpc.Serializable {
	return est.latenciesMsgs
}

func (est *FailureEstimator) SendLatencies() {
	for i := int32(0); i < int32(est.N); i++ {
		if i == est.Id {
			continue
		}
		msg := &LatenciesMsg{Aid: est.Id, Latencies: est.Replica.GetPeerLatencies()}

		est.Replica.SendMsg(i, est.latenciesMsgRPC, msg)
	}
}
