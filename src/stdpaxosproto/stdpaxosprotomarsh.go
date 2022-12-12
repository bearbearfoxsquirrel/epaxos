package stdpaxosproto

import (
	"bufio"
	"encoding/binary"
	"epaxos/fastrpc"
	"epaxos/state"
	"io"
	"net"
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

// DATAGRAM MESSAGING

func (t *Prepare) FromStrippedDatagrams(c fastrpc.CollectedM) error {
	bs := c.Messages[0]
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Ballot.Number = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.Ballot.PropID = int16((uint16(bs[12]) | (uint16(bs[13]) << 8)))
	if int16(t.LeaderId) != t.Ballot.PropID {
		panic("bad")
	}
	return nil
}

func (t *Prepare) BcastDatagrams(code uint8, requireAcks bool, bcastList []int32, writers []*net.UDPConn, addr []*net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	bs := t.ToBuffer(code, requireAcks, b)
	bcastToGroup(bcastList, writers, bs)
	return t.getMsgReceipt(code, requireAcks)
}

func (t *Prepare) WriteDatagrams(code uint8, requireAck bool, writer *net.UDPConn, addr *net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	bs := t.ToBuffer(code, requireAck, b)
	writer.Write(bs)
	return t.getMsgReceipt(code, requireAck)
}

func (t *Prepare) getMsgReceipt(code uint8, requireAcks bool) fastrpc.MSGReceipt {
	return fastrpc.MSGReceipt{
		TIB: fastrpc.TIB{
			T:  code,
			I:  t.Instance,
			BB: t.Number,
			BP: int32(t.PropID),
		},
		Seq:  0,
		Last: 0,
		Ack:  requireAcks,
	}
}

func (t *Prepare) ToBuffer(code uint8, requireAck bool, b *[65507]byte) []byte {
	s := 14 + fastrpc.TIBSLLEN
	var bs []byte
	bs = b[:]
	bs = fastrpc.EncodeTIBSL(code, t.Instance, t.Ballot.Number, int32(t.Ballot.PropID), 0, 0, requireAck, bs)
	bs = b[fastrpc.TIBSLLEN:]
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
	bs = b[:s]
	return bs
}

func (t *Prepare) NewUDP() fastrpc.UDPaxos {
	return new(Prepare)
}

func bcastF(bcastList []int32, writers []*net.UDPConn) func(bs []byte) {
	return func(bs []byte) {
		for _, writer := range bcastList {
			writers[writer].Write(bs)
		}
	}
}

func sendF(writer *net.UDPConn) func(bs []byte) {
	return func(bs []byte) {
		writer.Write(bs)
	}
}

func bcastToGroup(bcastList []int32, writers []*net.UDPConn, bs []byte) {
	for _, writer := range bcastList {
		writers[writer].Write(bs)
	}
}

func (t *PrepareReply) BcastDatagrams(code uint8, requireAcks bool, bcastList []int32, writers []*net.UDPConn, addr []*net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	s := 34 + fastrpc.TIBSLLEN
	bs := t.ToBuffer(code, requireAcks, b)
	return WriteDatagramStream(bs, s, code, t.Instance, t.Req.Number, int32(t.Req.PropID), 0, requireAcks, t.Command, bcastF(bcastList, writers))
}

func (t *PrepareReply) WriteDatagrams(code uint8, requireAck bool, writer *net.UDPConn, addr *net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	s := 34 + fastrpc.TIBSLLEN
	bs := t.ToBuffer(code, requireAck, b)
	return WriteDatagramStream(bs, s, code, t.Instance, t.Req.Number, int32(t.Req.PropID), 0, requireAck, t.Command, sendF(writer))
}

func (t *PrepareReply) ToBuffer(code uint8, requireAck bool, b *[65507]byte) []byte {
	var bs = b[:]
	bs = fastrpc.EncodeTIBSL(code, t.Instance, t.Req.Number, int32(t.Req.PropID), 0, -1, requireAck, bs)
	bs = b[fastrpc.TIBSLLEN:]

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
	bs = b[:]
	return bs
}

func (t *PrepareReply) FromStrippedDatagrams(c fastrpc.CollectedM) error {
	bs := c.Messages[0][:]
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
	c.Messages[0] = c.Messages[0][34:]
	t.Command = FromDatagramStream(c)
	return nil
}

func (t *PrepareReply) NewUDP() fastrpc.UDPaxos {
	return new(PrepareReply)
}

func (t *Accept) BcastDatagrams(code uint8, requireAcks bool, bcastList []int32, writers []*net.UDPConn, addr []*net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	s := 18 + fastrpc.TIBSLLEN
	t.ToBuffer(code, requireAcks, b)
	return WriteDatagramStream(b[:], s, code, t.Instance, t.Number, int32(t.PropID), 0, requireAcks, t.Command, bcastF(bcastList, writers))
}

func (t *Accept) WriteDatagrams(code uint8, requireAck bool, writer *net.UDPConn, addr *net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	s := 18 + fastrpc.TIBSLLEN
	t.ToBuffer(code, requireAck, b)
	return WriteDatagramStream(b[:], s, code, t.Instance, t.Number, int32(t.PropID), 0, requireAck, t.Command, sendF(writer))
}

func (t *Accept) ToBuffer(code uint8, requireAck bool, b *[65507]byte) []byte {
	var bs = b[:]
	bs = fastrpc.EncodeTIBSL(code, t.Instance, t.Number, int32(t.PropID), 0, -1, requireAck, bs)
	bs = b[fastrpc.TIBSLLEN:]
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
	bs = b[:]
	return bs
}

func (t *Accept) FromStrippedDatagrams(c fastrpc.CollectedM) error {
	bs := c.Messages[0][:]
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Number = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.PropID = int16((uint16(bs[12]) | (uint16(bs[13]) << 8)))
	t.WhoseCmd = int32((uint32(bs[14]) | (uint32(bs[15]) << 8) | (uint32(bs[16]) << 16) | (uint32(bs[17]) << 24)))
	c.Messages[0] = c.Messages[0][18:]
	t.Command = FromDatagramStream(c)
	return nil
}

func (t *Accept) NewUDP() fastrpc.UDPaxos {
	return new(Accept)
}

func (t *AcceptReply) BcastDatagrams(code uint8, requireAcks bool, bcastList []int32, writers []*net.UDPConn, addr []*net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	s := 28 + fastrpc.TIBSLLEN
	bs := t.ToBuffer(code, requireAcks, b, s)
	bcastToGroup(bcastList, writers, bs)
	return t.getReceipt(code, requireAcks)
}

func (t *AcceptReply) WriteDatagrams(code uint8, requireAck bool, writer *net.UDPConn, addr *net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	s := 28 + fastrpc.TIBSLLEN
	bs := t.ToBuffer(code, requireAck, b, s)
	writer.Write(bs)
	return t.getReceipt(code, requireAck)
}

func (t *AcceptReply) getReceipt(code uint8, requireAck bool) fastrpc.MSGReceipt {
	return fastrpc.MSGReceipt{
		TIB: fastrpc.TIB{
			T:  code,
			I:  t.Instance,
			BB: t.Req.Number,
			BP: int32(t.Req.PropID),
		},
		Seq:  0,
		Last: 0,
		Ack:  requireAck,
	}
}

func (t *AcceptReply) ToBuffer(code uint8, requireAck bool, b *[65507]byte, s int) []byte {
	var bs = b[:]
	bs = fastrpc.EncodeTIBSL(code, t.Instance, t.Req.Number, int32(t.Req.PropID), 0, 0, requireAck, bs)
	bs = b[fastrpc.TIBSLLEN:]
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
	bs = b[:s]
	return bs
}

func (t *AcceptReply) FromStrippedDatagrams(c fastrpc.CollectedM) error {
	bs := c.Messages[0]
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

func (t *AcceptReply) NewUDP() fastrpc.UDPaxos {
	return new(AcceptReply)
}

func (t *Commit) BcastDatagrams(code uint8, requireAcks bool, bcastList []int32, writers []*net.UDPConn, addr []*net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	s := 22 + fastrpc.TIBSLLEN
	bs := t.ToBuffer(code, requireAcks, b)
	return WriteDatagramStream(bs, s, code, t.Instance, t.Number, int32(t.PropID), 0, requireAcks, t.Command, bcastF(bcastList, writers))
}

func (t *Commit) WriteDatagrams(code uint8, requireAck bool, writer *net.UDPConn, addr *net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	s := 22 + fastrpc.TIBSLLEN
	bs := t.ToBuffer(code, requireAck, b)
	return WriteDatagramStream(bs, s, code, t.Instance, t.Number, int32(t.PropID), 0, requireAck, t.Command, sendF(writer))
}

func (t *Commit) ToBuffer(code uint8, requireAck bool, b *[65507]byte) []byte {
	var bs = b[:]
	bs = fastrpc.EncodeTIBSL(code, t.Instance, t.Number, int32(t.PropID), 0, -1, requireAck, bs)
	bs = b[fastrpc.TIBSLLEN:]

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

	bs = b[:]
	return bs
}

func (t *Commit) FromStrippedDatagrams(c fastrpc.CollectedM) error {
	bs := c.Messages[0][:]
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Number = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.PropID = int16((uint16(bs[12]) | (uint16(bs[13]) << 8)))
	t.WhoseCmd = int32((uint32(bs[14]) | (uint32(bs[15]) << 8) | (uint32(bs[16]) << 16) | (uint32(bs[17]) << 24)))
	t.MoreToCome = int32((uint32(bs[18]) | (uint32(bs[19]) << 8) | (uint32(bs[20]) << 16) | (uint32(bs[21]) << 24)))
	c.Messages[0] = c.Messages[0][22:]
	t.Command = FromDatagramStream(c)
	return nil
}

func (t *Commit) NewUDP() fastrpc.UDPaxos {
	return new(Commit)
}

func (t *CommitShort) BcastDatagrams(code uint8, requireAcks bool, bcastList []int32, writers []*net.UDPConn, addr []*net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	s := 22 + fastrpc.TIBSLLEN
	bs := t.ToBuffer(code, requireAcks, b, s)
	bcastToGroup(bcastList, writers, bs)
	return t.getMsgReceipt(code, requireAcks)
}

func (t *CommitShort) WriteDatagrams(code uint8, requireAck bool, writer *net.UDPConn, addr *net.UDPAddr, b *[65507]byte) fastrpc.MSGReceipt {
	s := 22 + fastrpc.TIBSLLEN
	bs := t.ToBuffer(code, requireAck, b, s)
	writer.Write(bs)
	return t.getMsgReceipt(code, requireAck)
}

func (t *CommitShort) getMsgReceipt(code uint8, requireAck bool) fastrpc.MSGReceipt {
	return fastrpc.MSGReceipt{
		TIB: fastrpc.TIB{
			T:  code,
			I:  t.Instance,
			BB: t.Number,
			BP: int32(t.PropID),
		},
		Seq:  0,
		Last: 0,
		Ack:  requireAck,
	}
}

func (t *CommitShort) ToBuffer(code uint8, requireAck bool, b *[65507]byte, s int) []byte {
	var bs = b[:]
	bs = fastrpc.EncodeTIBSL(code, t.Instance, t.Number, int32(t.PropID), 0, 0, requireAck, bs)
	bs = b[fastrpc.TIBSLLEN:]

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
	bs = b[:s]
	return bs
}

func (t *CommitShort) FromStrippedDatagrams(c fastrpc.CollectedM) error {
	bs := c.Messages[0][:]
	t.LeaderId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Instance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Number = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.PropID = int16((uint16(bs[12]) | (uint16(bs[13]) << 8)))
	t.Count = int32((uint32(bs[14]) | (uint32(bs[15]) << 8) | (uint32(bs[16]) << 16) | (uint32(bs[17]) << 24)))
	t.WhoseCmd = int32((uint32(bs[18]) | (uint32(bs[19]) << 8) | (uint32(bs[20]) << 16) | (uint32(bs[21]) << 24)))
	return nil
}

func (t *CommitShort) NewUDP() fastrpc.UDPaxos {
	return new(CommitShort)
}

type CommandSlice []*state.Command

func WriteDatagramStream(bs []byte, nextBytePos int, t uint8, i, bb, bp int32, s int32, requireAck bool, c CommandSlice, writeFunc func([]byte)) fastrpc.MSGReceipt {
	// assume first tib is written if nextBytePos > 0
	// assume len bs <= max datagram size
	// assumes that a value is less than the size of a datagram - if isn't this will panic
	clen := len(c)
	if len(bs) < nextBytePos+4 {
		panic("Too short of buffer for command list")
	}
	// serialise num values total
	bs[nextBytePos] = byte(clen)
	nextBytePos += 1
	bs[nextBytePos] = byte(clen >> 8)
	nextBytePos += 1
	bs[nextBytePos] = byte(clen >> 16)
	nextBytePos += 1
	bs[nextBytePos] = byte(clen >> 24)
	nextBytePos += 1

	op := byte(0)
	k := state.Key(0)
	var v []byte
	vl := 0
	cmdLen := 0
	for ci := 0; ci < len(c); ci++ {
		op = byte(c[ci].Op)
		k = c[ci].K
		v = c[ci].V
		vl = len(v)
		cmdLen = 1 + 8 + 4 + vl
		if nextBytePos+cmdLen > len(bs) {
			//write and reset
			writeFunc(bs[:nextBytePos])
			//wire.Write(bs[:nextBytePos]) //, addr)
			s += 1
			bs = fastrpc.EncodeTIBSL(t, i, bb, bp, s, -1, requireAck, bs)
			nextBytePos = fastrpc.TIBSLLEN
		}
		// serialise op
		bs[nextBytePos] = op
		nextBytePos += 1
		// serialise k
		bs[nextBytePos] = byte(k)
		nextBytePos += 1
		bs[nextBytePos] = byte(k >> 8)
		nextBytePos += 1
		bs[nextBytePos] = byte(k >> 16)
		nextBytePos += 1
		bs[nextBytePos] = byte(k >> 24)
		nextBytePos += 1
		bs[nextBytePos] = byte(k >> 32)
		nextBytePos += 1
		bs[nextBytePos] = byte(k >> 40)
		nextBytePos += 1
		bs[nextBytePos] = byte(k >> 48)
		nextBytePos += 1
		bs[nextBytePos] = byte(k >> 56)
		nextBytePos += 1
		// serialise v len
		bs[nextBytePos] = byte(vl)
		nextBytePos += 1
		bs[nextBytePos] = byte(vl >> 8)
		nextBytePos += 1
		bs[nextBytePos] = byte(vl >> 16)
		nextBytePos += 1
		bs[nextBytePos] = byte(vl >> 24)
		nextBytePos += 1
		// serialise v
		for q := 0; q < vl; q++ {
			bs[nextBytePos] = v[q]
			nextBytePos += 1
		}
	}
	bs = fastrpc.EncodeTIBSL(t, i, bb, bp, s, s, requireAck, bs)
	writeFunc(bs[:nextBytePos])
	return fastrpc.MSGReceipt{
		TIB: fastrpc.TIB{
			T:  t,
			I:  i,
			BB: bb,
			BP: bp,
		},
		Seq:  s,
		Last: s,
		Ack:  requireAck,
	}
}

func FromDatagramStream(co fastrpc.CollectedM) []*state.Command {
	// assume that any other message has been stripped
	// first get number of commands
	coM0 := co.Messages[0]
	cmdLen := int32((uint32(coM0[0]) | (uint32(coM0[1]) << 8) | (uint32(coM0[2]) << 16) | (uint32(coM0[3]) << 24)))
	cur := 4
	cmds := make([]*state.Command, 0, cmdLen)

	// change to detect if num commands filled

	op := state.Operation(0)
	k := state.Key(0)
	vl := int32(0)
	for i := int32(0); i <= co.Last; i++ {
		if _, e := co.Messages[i]; !e {
			panic("Missing datagram")
		}
		m := co.Messages[i]
		for cur < len(m) {
			// deserialise op
			op = state.Operation(m[cur])
			cur += 1
			k = state.Key(int64((uint32(m[cur]) | (uint32(m[cur+1]) << 8) | (uint32(m[cur+2]) << 16) | (uint32(m[cur+3]) << 24)) |
				(uint32(m[cur+4])<<32 | (uint32(m[cur+5]) << 40) | (uint32(m[cur+6]) << 48) | (uint32(m[cur+7]) << 54))))
			cur += 8
			vl = int32((uint32(m[cur]) | (uint32(m[cur+1]) << 8) | (uint32(m[cur+2]) << 16) | (uint32(m[cur+3]) << 24)))
			cur += 4
			cmd := &state.Command{
				Op: op,
				K:  k,
				V:  m[cur : cur+int(vl)], // doesn't copy underlying buffer so can change
				//V: make([]byte, vl),
			}
			//copy(cmd.V, m[cur:cur+int(vl)])
			cmds = append(cmds, cmd)
			cur += int(vl)
		}
		cur = 0
	}

	if len(cmds) != int(cmdLen) {
		panic("lost cmds")
	}
	return cmds
}
