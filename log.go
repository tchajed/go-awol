package awol

import (
	"github.com/tchajed/goose/machine"

	"github.com/tchajed/go-awol/disk"
)

// MaxTxnWrites is a guaranteed reservation for each transaction.
const MaxTxnWrites = 10 // 10 is completely arbitrary

const logLength = 1 + 2*MaxTxnWrites

type Log struct {
	cache map[int]disk.Block
	// length of current transaction, in blocks
	length *int
}

// New initializes a fresh log
func New() Log {
	cache := make(map[int]disk.Block)
	return Log{cache: cache}
}

func readLog(length int, cache map[int]disk.Block) {
	for i := 0; ; {
		if i < length {
			a, v := getLogEntry(i)
			cache[a] = v
			i = i + 1
			continue
		}
		break
	}
}

// Open recovers the log following a crash or shutdown
func Open() Log {
	header := disk.Read(0)
	length := blockToInt(header)
	cache := make(map[int]disk.Block)
	readLog(length, cache)
	lengthPtr := new(int)
	*lengthPtr = length
	return Log{cache: cache, length: lengthPtr}
}

// BeginTxn allocates space for a new transaction in the log.
//
// Returns true if the allocation succeeded.
func (l Log) BeginTxn() bool {
	length := *l.length
	if length == 0 {
		return true
	}
	return false
}

// Read from the logical disk.
//
// Reads must go through the log to return committed but un-applied writes.
func (l Log) Read(a int) disk.Block {
	v, ok := l.cache[a]
	if ok {
		return v
	}
	return disk.Read(logLength + a)
}

func intToBlock(a int) disk.Block {
	b := make([]byte, disk.BlockSize)
	machine.UInt64Put(b, uint64(a))
	return b
}

func blockToInt(v disk.Block) int {
	a := machine.UInt64Get(v)
	return int(a)
}

// Write to the disk through the log.
func (l Log) Write(a int, v disk.Block) {
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
}

// Commit the current transaction.
func (l Log) Commit() {
	length := *l.length
	header := intToBlock(length)
	disk.Write(0, header)
}

func getLogEntry(logOffset int) (int, disk.Block) {
	diskAddr := 1 + 2*logOffset
	aBlock := disk.Read(diskAddr)
	a := blockToInt(aBlock)
	v := disk.Read(diskAddr + 1)
	return a, v
}

// Apply all the committed transactions.
//
// Frees all the space in the log.
func (l Log) Apply() {
	length := *l.length
	for i := 0; ; {
		if i < length {
			a, v := getLogEntry(i)
			disk.Write(a, v)
			i = i + 1
			continue
		}
		break
	}
	*l.length = 0
}
