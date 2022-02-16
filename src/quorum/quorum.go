package quorum

type QuorumTally interface {
	Add(id int)
	Remove()
	Clear()
	Reached() bool
	//	GetAcknowlegers() []int
	Acknowledged(int) bool
	//getWhoCanFormQuorums() [][]int
}

type CountingQuorumTally struct {
	ResponseHolder
	Threshold int
}

func (qrm *CountingQuorumTally) Remove() {
	panic("implement me")
}

func (qrm *CountingQuorumTally) Clear() {
	qrm.ResponseHolder.clear()
}

func (qrm *CountingQuorumTally) Add(aid int) {
	qrm.ResponseHolder.addAck(aid)
}

func (qrm *CountingQuorumTally) Reached() bool {
	return len(qrm.getAcks()) >= qrm.Threshold
}

//
//func (qrm *CountingQuorumTally) GetAcknowledgers() []int {
//	return
//}

func (qrm *CountingQuorumTally) Acknowledged(aid int) bool {
	if _, exists := qrm.getAcks()[aid]; exists {
		return true
	} else {
		return false
	}
}

type SpecificQuorumTally struct {
	ResponseHolder
	Qrms [][]int
}

func (qrm *SpecificQuorumTally) Remove() {
	panic("implement me")
}

func (qrm *SpecificQuorumTally) Add(id int) {
	qrm.ResponseHolder.addAck(id)
}

func (qrm *SpecificQuorumTally) Clear() {
	qrm.ResponseHolder.clear()
}

func (qrm *SpecificQuorumTally) Reached() bool {
	acks := qrm.getAcks()

	if len(acks) < len(qrm.Qrms) {
		return false
	}

	for _, thres := range qrm.Qrms {
		for _, neededAid := range thres {
			if _, exists := acks[neededAid]; !exists {
				break
			}
		}
		return true // acks form a qrm
	}

	return false
}

func (qrm *SpecificQuorumTally) Acknowledged(aid int) bool {
	if _, exists := qrm.getAcks()[aid]; exists {
		return true
	} else {
		return false
	}
}

//
//type GridQuorum struct {
//	ResponseHolder
//	rows []int
//	columns []int
//}
//
//func (qrm *GridQuorum) Add(id int) {
//	qrm.ResponseHolder.addAck(id)
//}
//
//func (qrm *GridQuorum) Clear() {
//	qrm.ResponseHolder.Clear()
//}
//
//func (qrm *GridQuorum) Reached() bool {
//	Acks := qrm.getAcks()
//
//	//	if len(Acks) < qrm.rowLen() && len(Acks) < qrm.colLen() {
//	//		return false
//	//	}
//
//	for _, thres := range qrm.rows {
//		if reflect.DeepEqual(Acks, thres) {return true}
//	}
//
//	for _, thres := range qrm.columns {
//		if reflect.DeepEqual(Acks, thres) {return true}
//	}
//	return false
//}
//
//func (qrm *GridQuorum) rowLen() int {
//	return len(qrm.columns)
//}
//
//func (qrm *GridQuorum) colLen() int {
//	return len(qrm.rows)
//}

// get quorum

// quorum Reached

// quorum Clear

// quorum Add

// promise quorum

// acceptance quorum
