package proposer

type Proposer interface {
	IncrementCurrentInstanceTo(inst int32)
	RequestPromise(inst int32)
	RequestAcceptance(inst int32)
	Close(inst int32)
}
