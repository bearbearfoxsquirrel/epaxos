package quorum

type ResponseHolder struct {
	Nacks map[int]struct{}
	Acks  map[int]struct{}
}

func (qrm *ResponseHolder) clear() {
	qrm.Nacks = make(map[int]struct{})
	qrm.Acks = make(map[int]struct{})
}

func (qrm *ResponseHolder) addAck(id int) {
	qrm.Acks[id] = struct{}{}
}

func (qrm *ResponseHolder) getAcks() map[int]struct{} {
	return qrm.Acks
}
