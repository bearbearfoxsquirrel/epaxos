package proposalssatiated

//
//import (
//	"bufio"
//	"encoding/binary"
//	"fastrpc"
//	"io"
//)
//
//type byteReader interface {
//	io.Reader
//	ReadByte() (c byte, err error)
//}
//
//func (t *ProposalsSatiated) New() fastrpc.Serializable {
//	return new(ProposalsSatiated)
//}
//
//func (t *ProposalsSatiated) Unmarshal(rr io.Reader) error {
//	var wire byteReader
//	var ok bool
//	if wire, ok = rr.(byteReader); !ok {
//		wire = bufio.NewReader(rr)
//	}
//
//	var b [16]byte
//	var bs []byte
//	bs = b[:16]
//
//	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
//		return err
//	}
//	t.Instance = int32(binary.LittleEndian.Uint32(bs[0:4]))
//	t.MaxBallot.Config = int32(binary.LittleEndian.Uint32(bs[4:8]))
//	t.MaxBallot.Number = int32(binary.LittleEndian.Uint32(bs[8:12]))
//	t.MaxBallot.PropID = int16(binary.LittleEndian.Uint16(bs[12:14]))
//
//	alen1, err := binary.ReadVarint(wire)
//	if err != nil {
//		return err
//	}
//	t.Proposers = make([]int32, alen1)
//	bs = b[:4]
//	for i := int64(0); i < alen1; i++ {
//		if _, err = io.ReadFull(wire, bs); err != nil {
//			return err
//		}
//		t.Proposers[i] = int32(binary.LittleEndian.Uint32(bs))
//	}
//
//	return nil
//}
//
//func (t *ProposalsSatiated) Marshal(wire io.Writer) {
//	var b [14]byte
//	var bs []byte
//	bs = b[:14]
//	binary.LittleEndian.PutUint32(bs[0:4], uint32(t.Instance))
//	binary.LittleEndian.PutUint32(bs[4:8], uint32(t.MaxBallot.Config))
//	binary.LittleEndian.PutUint32(bs[8:12], uint32(t.MaxBallot.Number))
//	binary.LittleEndian.PutUint16(bs[12:14], uint16(t.MaxBallot.PropID))
//	wire.Write(bs)
//	bs = b[:]
//	alen1 := int64(len(t.Proposers))
//	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
//		wire.Write(b[0:wlen])
//	}
//	bs = b[:4]
//	for i := int64(0); i < alen1; i++ {
//		binary.LittleEndian.PutUint32(bs, uint32(t.Proposers[i]))
//		wire.Write(bs)
//	}
//}
//
