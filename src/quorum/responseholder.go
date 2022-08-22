package quorum

type ResponseHolder struct {
	Nacks map[int32]struct{}
	Acks  map[int32]struct{}
}

func (qrm *ResponseHolder) clear() {
	qrm.Nacks = make(map[int32]struct{})
	qrm.Acks = make(map[int32]struct{})
}

func (qrm *ResponseHolder) addAck(id int32) {
	qrm.Acks[id] = struct{}{}
}

func (qrm *ResponseHolder) getAcks() map[int32]struct{} {
	return qrm.Acks
}

func GetResponseHolder() ResponseHolder {
	return ResponseHolder{Nacks: make(map[int32]struct{}), Acks: make(map[int32]struct{})}
}
