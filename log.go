package awol

import (
	"sync"

	"github.com/tchajed/goose/machine"

	"github.com/tchajed/goose/machine/disk"
)

// MaxTxnWrites is a guaranteed reservation for each transaction.
const MaxTxnWrites uint64 = 10 // 10 is completely arbitrary

const logLength = 1 + 2*MaxTxnWrites

type Log struct {
	// protects cache and length
	l     *sync.RWMutex
	cache map[uint64]disk.Block
	// length of current transaction, in blocks
	length *uint64
}

func intToBlock(a uint64) disk.Block {
	b := make([]byte, disk.BlockSize)
	machine.UInt64Put(b, a)
	return b
}

func blockToInt(v disk.Block) uint64 {
	a := machine.UInt64Get(v)
	return a
}

// New initializes a fresh log
func New() Log {
	diskSize := disk.Size()
	if diskSize <= logLength {
		panic("disk is too small to host log")
	}
	cache := make(map[uint64]disk.Block)
	header := intToBlock(0)
	disk.Write(0, header)
	lengthPtr := new(uint64)
	*lengthPtr = 0
	l := new(sync.RWMutex)
	return Log{cache: cache, length: lengthPtr, l: l}
}

func (l Log) lock() {
	l.l.Lock()
}

func (l Log) unlock() {
	l.l.Unlock()
}

// BeginTxn allocates space for a new transaction in the log.
//
// Returns true if the allocation succeeded.
func (l Log) BeginTxn() bool {
	l.lock()
	length := *l.length
	if length == 0 {
		l.unlock()
		return true
	}
	l.unlock()
	return false
}

// Read from the logical disk.
//
// Reads must go through the log to return committed but un-applied writes.
func (l Log) Read(a uint64) disk.Block {
	l.lock()
	v, ok := l.cache[a]
	if ok {
		l.unlock()
		return v
	}
	// TODO: maybe safe to unlock before reading from disk?
	l.unlock()
	dv := disk.Read(logLength + a)
	return dv
}

func (l Log) Size() uint64 {
	// safe to do lock-free
	sz := disk.Size()
	return sz - logLength
}

// Write to the disk through the log.
func (l Log) Write(a uint64, v disk.Block) {
	l.lock()
	length := *l.length
	if length >= MaxTxnWrites {
		panic("transaction is at capacity")
	}
	aBlock := intToBlock(a)
	nextAddr := 1 + 2*length
	disk.Write(nextAddr, aBlock)
	disk.Write(nextAddr+1, v)
	l.cache[a] = v
	*l.length = length + 1
	l.unlock()
}

// Commit the current transaction.
func (l Log) Commit() {
	l.lock()
	length := *l.length
	// TODO: maybe safe to unlock early?
	l.unlock()
	header := intToBlock(length)
	disk.Write(0, header)
}

func getLogEntry(logOffset uint64) (uint64, disk.Block) {
	diskAddr := 1 + 2*logOffset
	aBlock := disk.Read(diskAddr)
	a := blockToInt(aBlock)
	v := disk.Read(diskAddr + 1)
	return a, v
}

// applyLog assumes we are running sequentially
func applyLog(length uint64) {
	for i := uint64(0); ; {
		if i < length {
			a, v := getLogEntry(i)
			disk.Write(logLength+a, v)
			i = i + 1
			continue
		}
		break
	}
}
func clearLog() {
	header := intToBlock(0)
	disk.Write(0, header)
}

// Apply all the committed transactions.
//
// Frees all the space in the log.
func (l Log) Apply() {
	l.lock()
	length := *l.length
	applyLog(length)
	clearLog()
	*l.length = 0
	l.unlock()
	// technically the log cache is no longer needed, but it is still valid;
	// clearing it might make verification easier,
	// depending on the exact invariants
}

// Open recovers the log following a crash or shutdown
func Open() Log {
	header := disk.Read(0)
	length := blockToInt(header)
	applyLog(length)
	clearLog()

	cache := make(map[uint64]disk.Block)
	lengthPtr := new(uint64)
	*lengthPtr = 0
	l := new(sync.RWMutex)
	return Log{cache: cache, length: lengthPtr, l: l}
}
