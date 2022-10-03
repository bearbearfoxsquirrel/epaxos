package proposerstate

import (
	"encoding/binary"
	"epaxos/fastrpc"
	"io"
)

func (t *State) New() fastrpc.Serializable {
	return new(State)
}

func (t *State) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]

	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.ProposerID = int32(binary.LittleEndian.Uint16(bs[0:4]))      //int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.CurrentInstance = int32(binary.LittleEndian.Uint32(bs[4:8])) //int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	return nil
}

func (t *State) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	binary.LittleEndian.PutUint32(bs[0:4], uint32(t.ProposerID))
	binary.LittleEndian.PutUint32(bs[4:8], uint32(t.CurrentInstance))
	//tmp32 := t.ProposerID
	//bs[0] = byte(tmp32)
	//bs[1] = byte(tmp32 >> 8)
	//bs[2] = byte(tmp32 >> 16)
	//bs[3] = byte(tmp32 >> 24)

	//tmp32 = t.CurrentInstance
	//bs[4] = byte(tmp32)
	//bs[5] = byte(tmp32 >> 8)
	//bs[6] = byte(tmp32 >> 16)
	//bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
}
