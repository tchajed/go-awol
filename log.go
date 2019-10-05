package awol

import (
	"sync"

	"github.com/tchajed/goose/machine"
	"github.com/tchajed/goose/machine/disk"
)

// the header has 4096-8-8 bytes for addresses, each of which are 8 bytes long
const logLength = (disk.BlockSize - 8 - 8) / 8 // = 510

const dataStart = 1 + logLength

const DEBUG = false

type Op struct {
	Addrs  []uint64
	Blocks []disk.Block
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

	// protects changes to the on-disk log structure (at least writes to the
	// header, and also the reserved space for a concurrent apply)
	//
	// the lock acquisition order is l then hdrL
	hdrL sync.RWMutex

	// protects the unused space after the log
	commitL sync.RWMutex
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
		disk.Write(1+i, block0)
	}
	hdr := Hdr{start: 0, length: 0, addrs: addrs}
	disk.Write(0, encodeHdr(hdr))

	return &Log{hdr: hdr, logData: logData}
}

func (l Log) Begin() *Op {
	return &Op{}
}

func (l Log) logRead(a0 uint64) (disk.Block, bool) {
	for i := int(l.hdr.length) - 1; i >= 0; i-- {
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

// Write to an in-progress transaction
func (op *Op) Write(a uint64, v disk.Block) {
	op.Addrs = append(op.Addrs, a)
	op.Blocks = append(op.Blocks, v)
}

func (op Op) length() int {
	return len(op.Addrs)
}

func (op Op) Valid() bool {
	return uint64(op.length()) < logLength
}

// Apply clears the log to make room for more operations
func (l *Log) Apply() {
	l.l.Lock()
	defer l.l.Unlock()
	l.apply()
}

// apply is the body of apply
//
// assumes l.l.Lock
func (l *Log) apply() {
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
	disk.Barrier()
	l.hdrL.Unlock()
}

// addPending adds op to the list of pending operations eligible for group
// commit, and returns the operation's sequence number
// getEntry returns the ith entry in the logical log
//
// assumes l.l.Lock
func (l *Log) addPending(op *Op) uint {
	l.pending = append(l.pending, *op)
	return l.seqNum + uint(len(l.pending))
}

// appendEntry flushes (a, v) to the end of the log
//
// assumes there is space for the new entry
//
// assumes l.commitL.Lock
func (l *Log) appendEntry(hdr *Hdr, a uint64, v disk.Block) {
	i := hdr.length
	// TODO: absorption
	hdr.addrs[i] = a
	hdr.length++
	physicalLogOffset := (l.hdr.start + i) % logLength
	l.logData[physicalLogOffset] = v
	disk.Write(1+physicalLogOffset, v)
}

// flushOps flushes a whole group commit transaction
//
// returns a new header that would commit this transaction
//
// assumes l.l.RLock
func (l *Log) flushOps(ops []Op) Hdr {
	hdr := l.hdr
	for _, op := range ops {
		for i := range op.Addrs {
			a := op.Addrs[i]
			v := op.Blocks[i]
			l.appendEntry(&hdr, a, v)
		}
	}
	disk.Barrier()
	return hdr
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

// flushTxn returns the latest sequence number flushed
//
// assumes l.l.RLock, releases it
func (l *Log) flushTxn() uint {
	n := splitTxns(l.pending, int(logLength-l.hdr.length-l.applyLength))
	if n == 0 {
		l.l.RUnlock()
		return l.seqNum
	}
	hdr := l.flushOps(l.pending[:n])
	// TODO: the locks don't make any sense
	//
	// I think the intuition for the commitL is moving in the right direction,
	// though: it protects the on-disk log after the valid region, and the
	// "hdrL" protects the on-disk log before (for Apply); we probably protect
	// the on-disk header block using a write lock on l.l.
	l.hdrL.Lock()
	l.commitL.Lock()
	l.l.RUnlock()
	disk.Write(0, encodeHdr(hdr))
	// NOTE: this barrier is not necessary for correctness but simplifies the
	//   helping argument.
	//
	// when the header write is persistent, then ops have logically committed
	// following a crash and we should store recovery helping assertions for all
	// of them.
	disk.Barrier()
	l.hdr = hdr
	l.commitL.Unlock()
	l.hdrL.Unlock()
	l.l.Lock()
	l.seqNum += uint(n)
	newSeqNum := l.seqNum
	l.pending = l.pending[n:]
	l.l.Unlock()
	return newSeqNum
}

func (l *Log) Commit(op *Op) {
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
	oldSeq := l.seqNum
	seqNum := l.addPending(op)
	l.l.Unlock()
	for {
		// maybe get some more transactions to group in
		l.l.RLock()
		newSeq := l.flushTxn()
		if newSeq >= seqNum {
			break
		}
		// it doesn't matter for correctness how this is decided
		if newSeq == oldSeq {
			// if we didn't make progress, we flush the transaction to make
			// progress (the only other reason we don't have space is an
			// in-progress apply, and this solves that problem, too)
			l.l.Lock()
			l.apply()
			l.l.Unlock()
		}
	}
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
