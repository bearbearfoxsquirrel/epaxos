package stdpaxosproto

import (
	"bufio"
	"encoding/binary"
	"epaxos/fastrpc"
	"epaxos/state"
	"io"
	"sync"
)

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (t *Prepare) New() fastrpc.Serializable {
	return new(Prepare)
}
func (t *Prepare) BinarySize() (nbytes int, sizeKnown bool) {
	return 14, true
}

type PrepareCache struct {
	mu    sync.Mutex
	cache []*Prepare
}

func NewPrepareCache() *PrepareCache {
	c := &PrepareCache{}
	c.cache = make([]*Prepare, 0)
	return c
}

func (p *PrepareCache) Get() *Prepare {
	var t *Prepare
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Prepare{}
	}
	return t
}
func (p *PrepareCache) Put(t *Prepare) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Prepare) Marshal(wire io.Writer) {
	var b [14]byte
	var bs []byte
	bs = b[:14]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.Ballot.Number
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp16 := t.Ballot.PropID
	bs[12] = byte(tmp16)
	bs[13] = byte(tmp16 >> 8)
	wire.Write(bs)
}

func (t *Prepare) Unmarshal(wire io.Reader) error {
	var b [14]byte
	var bs []byte
	bs = b[:14]
	if _, err := io.ReadAtLeast(wire, bs, 14); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Ballot.Number = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.Ballot.PropID = int16((uint16(bs[12]) | (uint16(bs[13]) << 8)))
	return nil
}

func (t *PrepareReply) New() fastrpc.Serializable {
	return new(PrepareReply)
}
func (t *PrepareReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type PrepareReplyCache struct {
	mu    sync.Mutex
	cache []*PrepareReply
}

func NewPrepareReplyCache() *PrepareReplyCache {
	c := &PrepareReplyCache{}
	c.cache = make([]*PrepareReply, 0)
	return c
}

func (p *PrepareReplyCache) Get() *PrepareReply {
	var t *PrepareReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &PrepareReply{}
	}
	return t
}
func (p *PrepareReplyCache) Put(t *PrepareReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *PrepareReply) Marshal(wire io.Writer) {
	var b [34]byte
	var bs []byte
	bs = b[:34]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)

	tmp32 = t.Cur.Number
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp16 := t.Cur.PropID
	bs[8] = byte(tmp16)
	bs[9] = byte(tmp16 >> 8)

	tmp32 = t.CurPhase.int32()
	bs[10] = byte(tmp32)
	bs[11] = byte(tmp32 >> 8)
	bs[12] = byte(tmp32 >> 16)
	bs[13] = byte(tmp32 >> 24)

	tmp32 = t.Req.Number
	bs[14] = byte(tmp32)
	bs[15] = byte(tmp32 >> 8)
	bs[16] = byte(tmp32 >> 16)
	bs[17] = byte(tmp32 >> 24)
	tmp16 = t.Req.PropID
	bs[18] = byte(tmp16)
	bs[19] = byte(tmp16 >> 8)

	tmp32 = t.VBal.Number
	bs[20] = byte(tmp32)
	bs[21] = byte(tmp32 >> 8)
	bs[22] = byte(tmp32 >> 16)
	bs[23] = byte(tmp32 >> 24)
	tmp16 = t.VBal.PropID
	bs[24] = byte(tmp16)
	bs[25] = byte(tmp16 >> 8)

	tmp32 = t.AcceptorId
	bs[26] = byte(tmp32)
	bs[27] = byte(tmp32 >> 8)
	bs[28] = byte(tmp32 >> 16)
	bs[29] = byte(tmp32 >> 24)

	tmp32 = t.WhoseCmd
	bs[30] = byte(tmp32)
	bs[31] = byte(tmp32 >> 8)
	bs[32] = byte(tmp32 >> 16)
	bs[33] = byte(tmp32 >> 24)

	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Marshal(wire)
	}
}

func (t *PrepareReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [34]byte
	var bs []byte
	bs = b[:34]
	if _, err := io.ReadAtLeast(wire, bs, 34); err != nil {
		return err
	}
	t.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))

	t.Cur.Number = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Cur.PropID = int16((uint16(bs[8]) | (uint16(bs[9]) << 8)))

	t.CurPhase = Phase((uint32(bs[10]) | (uint32(bs[11]) << 8) | (uint32(bs[12]) << 16) | (uint32(bs[13]) << 24)))

	t.Req.Number = int32((uint32(bs[14]) | (uint32(bs[15]) << 8) | (uint32(bs[16]) << 16) | (uint32(bs[17]) << 24)))
	t.Req.PropID = int16((uint16(bs[18]) | (uint16(bs[19]) << 8)))

	t.VBal.Number = int32((uint32(bs[20]) | (uint32(bs[21]) << 8) | (uint32(bs[22]) << 16) | (uint32(bs[23]) << 24)))
	t.VBal.PropID = int16((uint16(bs[24]) | (uint16(bs[25]) << 8)))

	t.AcceptorId = int32((uint32(bs[26]) | (uint32(bs[27]) << 8) | (uint32(bs[28]) << 16) | (uint32(bs[29]) << 24)))

	t.WhoseCmd = int32((uint32(bs[30]) | (uint32(bs[31]) << 8) | (uint32(bs[32]) << 16) | (uint32(bs[33]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Command = make([]*state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Command[i] = &state.Command{}
		t.Command[i].Unmarshal(wire)
	}
	return nil
}

func (t *Accept) New() fastrpc.Serializable {
	return new(Accept)
}
func (t *Accept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type AcceptCache struct {
	mu    sync.Mutex
	cache []*Accept
}

func NewAcceptCache() *AcceptCache {
	c := &AcceptCache{}
	c.cache = make([]*Accept, 0)
	return c
}

func (p *AcceptCache) Get() *Accept {
	var t *Accept
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Accept{}
	}
	return t
}
func (p *AcceptCache) Put(t *Accept) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Accept) Marshal(wire io.Writer) {
	var b [18]byte
	var bs []byte
	bs = b[:18]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)

	tmp32 = t.Number
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp16 := t.PropID
	bs[12] = byte(tmp16)
	bs[13] = byte(tmp16 >> 8)

	tmp32 = t.WhoseCmd
	bs[14] = byte(tmp32)
	bs[15] = byte(tmp32 >> 8)
	bs[16] = byte(tmp32 >> 16)
	bs[17] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Marshal(wire)
	}
}

func (t *Accept) Unmarshal(rr io.Reader) error {
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
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Number = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.PropID = int16((uint16(bs[12]) | (uint16(bs[13]) << 8)))

	t.WhoseCmd = int32((uint32(bs[14]) | (uint32(bs[15]) << 8) | (uint32(bs[16]) << 16) | (uint32(bs[17]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Command = make([]*state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Command[i] = &state.Command{}
		t.Command[i].Unmarshal(wire)
	}
	return nil
}

func (t *AcceptReply) New() fastrpc.Serializable {
	return new(AcceptReply)
}
func (t *AcceptReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 9, true
}

type AcceptReplyCache struct {
	mu    sync.Mutex
	cache []*AcceptReply
}

func NewAcceptReplyCache() *AcceptReplyCache {
	c := &AcceptReplyCache{}
	c.cache = make([]*AcceptReply, 0)
	return c
}

func (p *AcceptReplyCache) Get() *AcceptReply {
	var t *AcceptReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &AcceptReply{}
	}
	return t
}
func (p *AcceptReplyCache) Put(t *AcceptReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *AcceptReply) Marshal(wire io.Writer) {
	var b [28]byte
	var bs []byte
	bs = b[:28]
	tmp32 := t.Instance
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)

	tmp32 = t.AcceptorId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)

	tmp32 = t.Cur.Number
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp16 := t.Cur.PropID
	bs[12] = byte(tmp16)
	bs[13] = byte(tmp16 >> 8)

	tmp32 = t.CurPhase.int32()
	bs[14] = byte(tmp32)
	bs[15] = byte(tmp32 >> 8)
	bs[16] = byte(tmp32 >> 16)
	bs[17] = byte(tmp32 >> 24)

	tmp32 = t.Req.Number
	bs[18] = byte(tmp32)
	bs[19] = byte(tmp32 >> 8)
	bs[20] = byte(tmp32 >> 16)
	bs[21] = byte(tmp32 >> 24)
	tmp16 = t.Req.PropID
	bs[22] = byte(tmp16)
	bs[23] = byte(tmp16 >> 8)

	tmp32 = t.WhoseCmd
	bs[24] = byte(tmp32)
	bs[25] = byte(tmp32 >> 8)
	bs[26] = byte(tmp32 >> 16)
	bs[27] = byte(tmp32 >> 24)

	wire.Write(bs)
}

func (t *AcceptReply) Unmarshal(wire io.Reader) error {
	var b [28]byte
	var bs []byte
	bs = b[:28]
	if _, err := io.ReadAtLeast(wire, bs, 28); err != nil {
		return err
	}
	t.Instance = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))

	t.AcceptorId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))

	t.Cur.Number = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.Cur.PropID = int16((uint16(bs[12]) | (uint16(bs[13]) << 8)))

	t.CurPhase = Phase((uint32(bs[14]) | (uint32(bs[15]) << 8) | (uint32(bs[16]) << 16) | (uint32(bs[17]) << 24)))

	t.Req.Number = int32((uint32(bs[18]) | (uint32(bs[19]) << 8) | (uint32(bs[20]) << 16) | (uint32(bs[21]) << 24)))
	t.Req.PropID = int16((uint16(bs[22]) | (uint16(bs[23]) << 8)))

	t.WhoseCmd = int32((uint32(bs[24]) | (uint32(bs[25]) << 8) | (uint32(bs[26]) << 16) | (uint32(bs[27]) << 24)))
	return nil
}

func (t *Commit) New() fastrpc.Serializable {
	return new(Commit)
}
func (t *Commit) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type CommitCache struct {
	mu    sync.Mutex
	cache []*Commit
}

func NewCommitCache() *CommitCache {
	c := &CommitCache{}
	c.cache = make([]*Commit, 0)
	return c
}

func (p *CommitCache) Get() *Commit {
	var t *Commit
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Commit{}
	}
	return t
}
func (p *CommitCache) Put(t *Commit) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Commit) Marshal(wire io.Writer) {
	var b [22]byte
	var bs []byte
	bs = b[:22]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)

	tmp32 = t.Number
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp16 := t.PropID
	bs[12] = byte(tmp16)
	bs[13] = byte(tmp16 >> 8)

	tmp32 = t.WhoseCmd
	bs[14] = byte(tmp32)
	bs[15] = byte(tmp32 >> 8)
	bs[16] = byte(tmp32 >> 16)
	bs[17] = byte(tmp32 >> 24)

	tmp32 = t.MoreToCome
	bs[18] = byte(tmp32)
	bs[19] = byte(tmp32 >> 8)
	bs[20] = byte(tmp32 >> 16)
	bs[21] = byte(tmp32 >> 24)

	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Marshal(wire)
	}
}

func (t *Commit) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [22]byte
	var bs []byte
	bs = b[:22]
	if _, err := io.ReadAtLeast(wire, bs, 22); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Number = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.PropID = int16((uint16(bs[12]) | (uint16(bs[13]) << 8)))

	t.WhoseCmd = int32((uint32(bs[14]) | (uint32(bs[15]) << 8) | (uint32(bs[16]) << 16) | (uint32(bs[17]) << 24)))

	t.MoreToCome = int32((uint32(bs[18]) | (uint32(bs[19]) << 8) | (uint32(bs[20]) << 16) | (uint32(bs[21]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Command = make([]*state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Command[i] = &state.Command{}
		t.Command[i].Unmarshal(wire)
	}
	return nil
}

func (t *CommitShort) New() fastrpc.Serializable {
	return new(CommitShort)
}
func (t *CommitShort) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

type CommitShortCache struct {
	mu    sync.Mutex
	cache []*CommitShort
}

func NewCommitShortCache() *CommitShortCache {
	c := &CommitShortCache{}
	c.cache = make([]*CommitShort, 0)
	return c
}

func (p *CommitShortCache) Get() *CommitShort {
	var t *CommitShort
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &CommitShort{}
	}
	return t
}
func (p *CommitShortCache) Put(t *CommitShort) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *CommitShort) Marshal(wire io.Writer) {
	var b [22]byte
	var bs []byte
	bs = b[:22]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Instance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)

	tmp32 = t.Number
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp16 := t.PropID
	bs[12] = byte(tmp16)
	bs[13] = byte(tmp16 >> 8)

	tmp32 = t.Count
	bs[14] = byte(tmp32)
	bs[15] = byte(tmp32 >> 8)
	bs[16] = byte(tmp32 >> 16)
	bs[17] = byte(tmp32 >> 24)

	tmp32 = t.WhoseCmd
	bs[18] = byte(tmp32)
	bs[19] = byte(tmp32 >> 8)
	bs[20] = byte(tmp32 >> 16)
	bs[21] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *CommitShort) Unmarshal(wire io.Reader) error {
	var b [22]byte
	var bs []byte
	bs = b[:22]

	if _, err := io.ReadAtLeast(wire, bs, 22); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Number = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.PropID = int16((uint16(bs[12]) | (uint16(bs[13]) << 8)))
	t.Count = int32((uint32(bs[14]) | (uint32(bs[15]) << 8) | (uint32(bs[16]) << 16) | (uint32(bs[17]) << 24)))

	t.WhoseCmd = int32((uint32(bs[18]) | (uint32(bs[19]) << 8) | (uint32(bs[20]) << 16) | (uint32(bs[21]) << 24)))
	return nil
}
