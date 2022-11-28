package fastrpc

import (
	"encoding/binary"
	"io"
	"net"
	"time"
)

type Serializable interface {
	Marshal(io.Writer)
	Unmarshal(io.Reader) error
	New() Serializable
}

const MAXDATAGRAMLEN = 65507
const TIBSLLEN = 25

type UDPaxos interface {
	WriteDatagrams(code uint8, requireAck bool, writer *net.UDPConn, addr *net.UDPAddr, b *[65507]byte) []MSGRReceipt
	FromStrippedDatagrams(c CollectedM) error
	NewUDP() UDPaxos
	Serializable
}

type TIB struct {
	//20 bytes
	T  uint8
	I  int32
	BB int32
	BP int32
}

type MSGRReceipt struct {
	TIB
	Seq  int32
	Last int32
	Ack  bool
}

// map of seqs to byte
// MSGRReceipt has been stripped
type CollectedM struct {
	//MLen     map[int32]int
	Messages   map[int32][]byte
	Last       int32
	LastRecvAt time.Time
}

func DecodeTIBSL(b *[MAXDATAGRAMLEN]byte) MSGRReceipt {
	t := b[0]
	i := int32(binary.BigEndian.Uint32(b[4:8]))
	bb := int32(binary.BigEndian.Uint32(b[8:12]))
	bp := int32(binary.BigEndian.Uint32(b[12:16]))
	s := int32(binary.BigEndian.Uint32(b[16:20]))
	l := int32(binary.BigEndian.Uint32(b[20:24]))
	ack := false
	if b[24] == 1 {
		ack = true
	}
	return MSGRReceipt{TIB{t, i, bb, bp}, s, l, ack}
}

func DecodeTIBSLFromSlice(b []byte) MSGRReceipt {
	t := b[0]
	i := int32(binary.BigEndian.Uint32(b[4:8]))
	bb := int32(binary.BigEndian.Uint32(b[8:12]))
	bp := int32(binary.BigEndian.Uint32(b[12:16]))
	s := int32(binary.BigEndian.Uint32(b[16:20]))
	l := int32(binary.BigEndian.Uint32(b[20:24]))
	ack := false
	if b[24] == 1 {
		ack = true
	}
	return MSGRReceipt{TIB{t, i, bb, bp}, s, l, ack}
}

func EncodeTIBSL(t uint8, i int32, bb int32, bp int32, s int32, l int32, ack bool, b []byte) []byte {
	b[0] = t
	//binary.BigEndian.PutUint32(b[0:4], uint32(t))
	binary.BigEndian.PutUint32(b[4:8], uint32(i))
	binary.BigEndian.PutUint32(b[8:12], uint32(bb))
	binary.BigEndian.PutUint32(b[12:16], uint32(bp))
	binary.BigEndian.PutUint32(b[16:20], uint32(s))
	binary.BigEndian.PutUint32(b[20:24], uint32(l))
	if ack == true {
		b[24] = 1
	} else {
		b[24] = 0
	}
	return b
}

func EncodeTIBSLFromStruct(tibsl MSGRReceipt, b []byte) []byte {
	b[0] = tibsl.T
	binary.BigEndian.PutUint32(b[4:8], uint32(tibsl.I))
	binary.BigEndian.PutUint32(b[8:12], uint32(tibsl.BB))
	binary.BigEndian.PutUint32(b[12:16], uint32(tibsl.BP))
	binary.BigEndian.PutUint32(b[16:20], uint32(tibsl.Seq))
	binary.BigEndian.PutUint32(b[20:24], uint32(tibsl.Last))
	if tibsl.Ack == true {
		b[24] = 1
	} else {
		b[24] = 0
	}
	return b
}
