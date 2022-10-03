package lwcproto

import (
	"epaxos/state"
	"epaxos/stdpaxosproto"
)

type Configuration interface {
	GetConfiguration() Configuration
	//	GetOrdering() Ordering
}

type Round interface {
	GetRound() Round
	//	GetOrdering() Ordering
}

type ConfigurationRound interface {
	GetConfiguration() ConfigurationRound
	GetRound() Round
	// GetOrdering() Ordering
}

//
//type Orderable interface {
//	CompareIsLessThan(Orderable) IsLessThanOrdering
//	CompareIsEqual() Ordering
//	CompareIsGreaterThan() Ordering
//}
//
//
//
//type Proposal interface { // caller should know which type to give
//	Serialise()   []byte
//	GetHashable() interface{}
//}

type ConfigBal struct {
	Config int32
	stdpaxosproto.Ballot
}

func GetNilConfigBal() ConfigBal {
	return ConfigBal{Config: -1, Ballot: stdpaxosproto.Ballot{Number: -1, PropID: -1}}
}

//
//func (cb *ConfigBal) GetHash() interface{} {
//	return cb
//}
//
//type ProposalOrdering interface {
//	Apply() bool
//}
//
//type ConfBalLessThan struct {
//	a, b ConfigBal
//}
//
//func (lt *ConfBalLessThan) Apply() bool {
//	x := ConfBalOrdering{
//		a: lt.a,
//		b: lt.b,
//	}
//	return x.IsLessThan()
//}
//
//func (cb * ConfigBal) CompareIsLessThan(orderable Orderable) Ordering {
//	return &ConfBalOrdering{a, b}
//}

//type Ordering interface {
//	IsLessThan() bool
//	IsGreaterThan() bool
//	IsEqual() bool
//}
//
//type BalOrdering struct {
//	a, b Ballot
//}
//
//// a is less than b
//func (c BalOrdering) IsLessThan() bool {
//	if c.a.Number < c.b.Number {
//		return true
//	} else {
//		if c.a.PropID < c.b.PropID {
//			return true
//		} else {
//			return false
//		}
//	}
//}
//
//// a is greater than b
//func (c BalOrdering) IsGreaterThan() bool {
//	if c.a.Number > c.b.Number {
//		return true
//	} else {
//		if c.a.PropID > c.b.PropID {
//			return true
//		} else {
//			return false
//		}
//	}
//}
//
//func (c *BalOrdering) IsEqual() bool {
//	return c.a.Number == c.b.Number && c.a.PropID == c.b.PropID
//}
//
//type ConfBalOrdering struct {
//	a, b ConfigBal
//}
//
//func (c *ConfBalOrdering) IsGreaterThan() bool {
//	if c.a.Config > c.b.Config {
//		return true
//	} else {
//		balGreaterThan := BalOrdering{c.a.Ballot, c.b.Ballot}.IsGreaterThan()
//		if c.a.Config == c.b.Config && balGreaterThan {
//			return true
//		} else {
//			return false
//		}
//	}
//}
//
//func (c *ConfBalOrdering) IsLessThan() bool {
//	if c.a.Config < c.b.Config {
//		return true
//	} else {
//		balLessThan := BalOrdering{c.a.Ballot, c.b.Ballot}.IsLessThan()
//		if c.a.Config == c.b.Config && balLessThan {
//			return true
//		} else {
//			return false
//		}
//	}
//}
//
//func (c *ConfBalOrdering) IsEqual() bool {
//	balOrdering := BalOrdering{c.a.Ballot, c.b.Ballot}
//	return c.a.Config == c.b.Config && balOrdering.IsEqual()
//}

//type Orderable interface {
//	order(Orderable)
//}
//
//type Rnd struct {
//	Rnd int
//}
//
//type Orderingstrategies interface {
//	isLessThan(a, b Orderable) bool
//	isEqual(a, b Orderable) bool
//}
//
//
//
//func (*Rnd) isLessThan(rnd Rnd) {
//
//}
//
//
//type ConfigRnd struct {
//	Conf int
//	Rnd int
//}
//
//type RndOderingStrategy struct {
//}
//
//func (ordering *RndOderingStrategy) isLessThan(a, b Orderable) bool {
//	return //a < b
//}
//
//type ConfigOrderingStrategy struct {
//	RndOrderingStrategy
//}
//
//// a is less than b
//func (ordering *ConfigOrderingStrategy) isLessThan(a, b ConfigRnd) {
//
//}
//
//func (rnd *ConfigRnd) isLessThan(configRnd ConfigRnd) {
//	rnd.
//}

//type serialisable interface {
//	accept(serialiser Serialisiser)
//}

func (confBal ConfigBal) IsZero() bool {
	zero := ConfigBal{
		Config: -1,
		Ballot: stdpaxosproto.Ballot{-1, -1},
	}
	return confBal.Equal(zero)
}

func (configBal ConfigBal) GreaterThan(cmp ConfigBal) bool {
	return configBal.Config > cmp.Config || (configBal.Config == cmp.Config && configBal.Ballot.GreaterThan(cmp.Ballot))
}

func (configBal ConfigBal) Equal(cmp ConfigBal) bool {
	return configBal.Config == cmp.Config && configBal.Ballot.Equal(cmp.Ballot)
}

type Prepare struct {
	LeaderId int32
	Instance int32
	ConfigBal
}
type PrepareReply struct {
	Instance   int32
	Req        ConfigBal
	Cur        ConfigBal
	CurPhase   stdpaxosproto.Phase
	VBal       ConfigBal
	AcceptorId int32
	WhoseCmd   int32
	Command    []*state.Command
}

type Accept struct {
	LeaderId int32
	Instance int32
	ConfigBal
	WhoseCmd int32
	Command  []*state.Command
}

type AcceptReply struct {
	Instance   int32
	AcceptorId int32
	Cur        ConfigBal
	Req        ConfigBal
	CurPhase   stdpaxosproto.Phase
	WhoseCmd   int32
}

type Commit struct {
	LeaderId int32
	Instance int32
	ConfigBal
	WhoseCmd   int32
	MoreToCome int32
	Command    []*state.Command
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	ConfigBal
	Count    int32
	WhoseCmd int32
}
