package acceptor

import (
	"bufio"
	"epaxos/fastrpc"
	"epaxos/stdpaxosproto"
	"io"
)

type PromiseLease struct {
	From           int32 // inclusive
	MaxBalPromises stdpaxosproto.Ballot
}

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (p PromiseLease) Marshal(wire io.Writer) {
	var b [14]byte
	var bs []byte
	bs = b[:14]
	tmp32 := p.From
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	//tmp32 = p.From
	//bs[4] = byte(tmp32)
	//bs[5] = byte(tmp32 >> 8)
	//bs[6] = byte(tmp32 >> 16)
	//bs[7] = byte(tmp32 >> 24)

	tmp32 = p.MaxBalPromises.Number
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp16 := p.MaxBalPromises.PropID
	bs[12] = byte(tmp16)
	bs[13] = byte(tmp16 >> 8)
	wire.Write(bs)
}

func (p PromiseLease) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [18]byte
	var bs []byte
	bs = b[:18]
	if _, err := io.ReadAtLeast(wire, bs, 18); err != nil {
		return err
	}
	p.From = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	//p.Len = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	p.MaxBalPromises.Number = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	p.MaxBalPromises.PropID = int16((uint16(bs[12]) | (uint16(bs[13]) << 8)))
	return nil
}

func (p *PromiseLease) New() fastrpc.Serializable {
	panic("implement me")
}
