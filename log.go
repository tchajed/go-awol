package awol

import (
	"sync"

	"github.com/tchajed/goose/machine"
	"github.com/tchajed/goose/machine/disk"
)

// the header has 4096-8-8 bytes for addresses, each of which are 8 bytes long
const logLength = (disk.BlockSize - 8 - 8) / 8 // = 510

const dataStart = 1 + logLength

type Op struct {
	addrs  []uint64
	blocks []disk.Block
}

type Hdr struct {
	start  uint64
	length uint64
	addrs  []uint64 // is always logLength long
}

type Log struct {
	l       sync.RWMutex
	pending []Op

	// seqNum of the last committed transaction
	seqNum      uint
	hdr         Hdr
	applyLength uint64
	logData     []disk.Block

	hdrL sync.RWMutex
}

func encodeHdr(hdr Hdr) disk.Block {
	if uint64(len(hdr.addrs)) != logLength {
		panic("header ill-formed")
	}
	// TODO: we could do better by re-using a fixed buffer for every
	//  encoding, as long as that buffer was protected by a lock
	block := make(disk.Block, disk.BlockSize)
	machine.UInt64Put(block[0:0+8], hdr.start)
	machine.UInt64Put(block[8:8+8], hdr.length)
	for i := uint64(0); i < logLength; i++ {
		offset := (2 + i) * 8
		machine.UInt64Put(block[offset:offset+8], hdr.addrs[i])
	}
	return block
}

func decodeHdr(b disk.Block) Hdr {
	start := machine.UInt64Get(b[0 : 0+8])
	count := machine.UInt64Get(b[8 : 8+8])
	addrs := make([]uint64, logLength)
	for i := uint64(0); i < logLength; i++ {
		offset := (2 + i) * 8
		addrs[i] = machine.UInt64Get(b[offset : offset+8])
	}
	return Hdr{start: start, length: count, addrs: addrs}
}

// forall hdr:: blockToHdr(hdrToBlock(hdr)) == hdr

// New initializes a fresh log
func New() *Log {
	if disk.Size() <= dataStart {
		panic("disk is too small to host log")
	}
	logData := make([]disk.Block, logLength)
	addrs := make([]uint64, logLength)
	block0 := make(disk.Block, 4096)
	for i := uint64(0); i < logLength; i++ {
		logData[i] = block0
		disk.Write(0, block0)
	}
	hdr := Hdr{start: 0, length: 0, addrs: addrs}
	disk.Write(0, encodeHdr(hdr))

	return &Log{hdr: hdr, logData: logData}
}

func (l Log) Begin() *Op {
	return &Op{}
}

func (l Log) logRead(a0 uint64) (disk.Block, bool) {
	for i := 0; i < int(l.hdr.length); i++ {
		if l.hdr.addrs[i] == a0 {
			return l.logData[int(l.hdr.start)+i], true
		}
	}
	return nil, false
}

// Read from the logical disk.
//
// Reads must go through the log to return committed but un-applied writes.
func (l Log) Read(a uint64) disk.Block {
	l.l.RLock()
	defer l.l.RUnlock()
	v, ok := l.logRead(a)
	if ok {
		return v
	}
	dv := disk.Read(dataStart + a)
	return dv
}

func (l Log) Size() int {
	return int(disk.Size() - dataStart)
}

func (l Log) BeginOp() Op {
	return Op{}
}

// Write to an in-progress transaction
func (op *Op) Write(a uint64, v disk.Block) {
	// TODO: absorption
	op.addrs = append(op.addrs, a)
	op.blocks = append(op.blocks, v)
}

func (op *Op) length() int {
	return len(op.addrs)
}

func (op *Op) Valid() bool {
	return uint64(op.length()) < logLength
}

// Apply clears the log to make room for more operations
func (l *Log) Apply() {
	l.l.Lock()
	l.hdrL.Lock()
	hdr := l.hdr
	l.hdr.start = (hdr.start + hdr.length) % logLength
	l.hdr.length = 0
	l.applyLength = hdr.length
	l.l.Unlock()

	for i := uint64(0); i < hdr.length; i++ {
		// NOTE: this is really tricky; we somehow know the old addresses
		//  aren't being modified (the slice pointers themselves are never
		//  modified)
		a := hdr.addrs[i]
		v := l.logData[(hdr.start+i)%logLength]
		disk.Write(dataStart+a, v)
	}
	// ordering-only barrier, so data is on disk before header
	disk.Barrier()

	l.l.Lock()
	disk.Write(0, encodeHdr(l.hdr))
	l.applyLength = 0
	l.l.Unlock()
	disk.Barrier()
	l.hdrL.Unlock()
}

// addPending adds op to the list of pending operations eligible for group
// commit, and returns the operation's sequence number
// getEntry returns the ith entry in the logical log
//
// assumes l.l.Lock
func (l *Log) addPending(op Op) uint {
	l.pending = append(l.pending, op)
	return l.seqNum + uint(len(l.pending))
}

// getEntry returns the ith entry in the logical log
//
// requires i < l.hdr.length
func (l *Log) getEntry(i uint64) (uint64, disk.Block) {
	a := l.hdr.addrs[i]
	v := l.logData[(l.hdr.start+i)%logLength]
	return a, v
}

func (l *Log) appendEntry(a uint64, v disk.Block) {
	i := l.hdr.length
	l.hdr.addrs[i] = a
	l.hdr.length++
	physicalLogOffset := (l.hdr.start + i) % logLength
	l.logData[physicalLogOffset] = v
	disk.Write(1+physicalLogOffset, v)
}

func (l *Log) flushOps(ops []Op) {
	for _, op := range ops {
		for i := range op.addrs {
			a := op.addrs[i]
			v := op.blocks[i]
			l.appendEntry(a, v)
		}
	}
	disk.Barrier()
	l.hdrL.Lock()
	disk.Write(0, encodeHdr(l.hdr))
	l.hdrL.Unlock()
}

// splitTxns finds a number of ops n such that ops[:n] have at most
// maxLen blocks
func splitTxns(ops []Op, maxLen int) int {
	length := 0
	for i, op := range ops {
		opLen := op.length()
		if length+opLen > maxLen {
			return i
		}
		length += opLen
	}
	// all of the transactions fit
	return len(ops)
}

// flushTxn returns false if it made no progress
func (l *Log) flushTxn() bool {
	n := splitTxns(l.pending, int(logLength-l.hdr.length-l.applyLength))
	if n == 0 {
		return false
	}
	l.flushOps(l.pending[:n])
	l.seqNum += uint(n)
	return true
}

func (l *Log) Commit(op Op) {
	// assumes op is valid
	ok := op.Valid()
	if !ok {
		panic("operation overflows log")
	}
	if op.length() == 0 {
		// operation can be logically committed without doing anything
		return
	}
	l.l.Lock()
	seqNum := l.addPending(op)
	// invariant: l.l.Lock
	for {
		l.l.Unlock()
		// maybe get some more transactions to group in
		l.l.Lock()
		ok := l.flushTxn()
		if !ok {
			l.l.Unlock()
			l.Apply()
		}
		if l.seqNum >= seqNum {
			break
		}
	}
	l.l.Unlock()
}

// Open recovers the log following a crash or shutdown
func Open() *Log {
	hdr := decodeHdr(disk.Read(0))
	logData := make([]disk.Block, logLength)
	for i := uint64(0); i < logLength; i++ {
		logData[i] = disk.Read(1 + i)
	}
	return &Log{hdr: hdr, logData: logData}
}
