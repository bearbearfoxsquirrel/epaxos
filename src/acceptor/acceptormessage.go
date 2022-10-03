package acceptor

import "epaxos/fastrpc"

type Message interface {
	ToWhom() int32
	GetType() uint8
	IsNegative() bool
	GetSerialisable() fastrpc.Serializable
	fastrpc.Serializable
}

type protoMessage struct {
	towhom     func() int32
	gettype    func() uint8
	isnegative func() bool
	fastrpc.Serializable
}

func (p protoMessage) GetSerialisable() fastrpc.Serializable {
	return p.Serializable
}

func (p protoMessage) ToWhom() int32 {
	return p.towhom()
}

func (p protoMessage) GetType() uint8 {
	return p.gettype()
}

func (p protoMessage) IsNegative() bool {
	return p.isnegative()
}
