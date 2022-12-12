package quorum

type QuorumTally interface {
	Add(id int32)
	Reached() bool
	Acknowledged(int32) bool
	CanFormQuorum(int32) bool
	//GetAckersGroup() []int32
}

func (qrm *SpecificQuorumTally) CanFormQuorum(aid int32) bool {
	l32 := int32(len(qrm.Can))
	for i := int32(0); i < l32; i++ {
		if aid == i {
			return true
		}
	}
	return false
}

//func (qrm *SpecificQuorumTally) Get() {
//	qr
//}

func (qrm *CountingQuorumTally) CanFormQuorum(aid int32) bool {
	l32 := int32(len(qrm.Can))
	for i := int32(0); i < l32; i++ {
		if aid == i {
			return true
		}
	}
	return false
}

type CountingQuorumTally struct {
	ResponseHolder
	Threshold int
	Can       []int32
}

func (qrm *CountingQuorumTally) Add(aid int32) {
	qrm.ResponseHolder.addAck(aid)
}

func (qrm *CountingQuorumTally) Reached() bool {
	return len(qrm.getAcks()) >= qrm.Threshold
}

func (qrm *CountingQuorumTally) Acknowledged(aid int32) bool {
	if _, exists := qrm.getAcks()[aid]; exists {
		return true
	} else {
		return false
	}
}

type SpecificQuorumTally struct {
	ResponseHolder
	Qrms [][]int32
	Can  []int32
}

func (qrm *SpecificQuorumTally) Add(id int32) {
	qrm.ResponseHolder.addAck(id)
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

func (qrm *SpecificQuorumTally) Acknowledged(aid int32) bool {
	if _, exists := qrm.getAcks()[aid]; exists {
		return true
	} else {
		return false
	}
}
