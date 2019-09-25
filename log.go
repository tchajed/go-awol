package awol

import (
	"sync"

	"github.com/tchajed/goose/machine"

	"github.com/tchajed/goose/machine/disk"
)

// MaxTxnWrites is a guaranteed reservation for each transaction.
const MaxTxnWrites uint64 = 10 // 10 is completely arbitrary

// assumes MaxTwnWrites addresses fit into one block;
// this holds when MaxTxnWrites <= 4096/8 = 512
// (actually the total number of writes in the log needs to be <= 512,
// which would be different if we supported group commit of multiple
// transactions)

const logLength = 1 + 1 + MaxTxnWrites

type Log struct {
	// protects cache and length
	l     *sync.RWMutex
	addrs *[]uint64
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

	addrs := make([]uint64, 0)
	addrPtr := new([]uint64)
	*addrPtr = addrs
	cache := make(map[uint64]disk.Block)
	header := intToBlock(0)
	disk.Write(0, header)
	lengthPtr := new(uint64)
	*lengthPtr = 0
	l := new(sync.RWMutex)
	return Log{l: l, addrs: addrPtr, cache: cache, length: lengthPtr}
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
		// note that we don't actually reserve this space,
		// so BeginTxn doesn't ensure anything about transactions succeeding
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
	// atomic, so safe to do lock-free
	sz := disk.Size()
	return sz - logLength
}

// Write to the disk through the log.
func (l Log) Write(a uint64, v disk.Block) {
	l.lock()
	_, ok := l.cache[a]
	if ok {
		l.unlock()
		return
	}
	length := *l.length
	if length >= MaxTxnWrites {
		panic("transaction is at capacity")
	}
	nextAddr := 1 + 1 + length
	addrs := *l.addrs
	newAddrs := append(addrs, a)
	*l.addrs = newAddrs
	disk.Write(nextAddr+1, v)
	l.cache[a] = v
	*l.length = length + 1
	l.unlock()
}

// encodeAddrs produces a disk block that encodes the addresses in the log
func (l Log) encodeAddrs() disk.Block {
	length := *l.length
	addrs := *l.addrs
	aBlock := make([]byte, 4096)
	for i := uint64(0); ; {
		if i < length {
			ai := addrs[i]
			machine.UInt64Put(aBlock[i*8:(i+1)*8], ai)
			i = i + 1
			continue
		}
		break
	}
	return aBlock
}

// decodeAddrs reads the address disk block and decodes it into length addresses
func decodeAddrs(length uint64) []uint64 {
	addrs := make([]uint64, length)
	aBlock := disk.Read(1)
	for i := uint64(0); ; {
		if i < length {
			a := machine.UInt64Get(aBlock[i*8 : (i+1)*8])
			addrs[i] = a
			i = i + 1
			continue
		}
		break
	}
	return addrs
}

// Commit the current transaction.
func (l Log) Commit() {
	l.lock()
	length := *l.length
	aBlock := l.encodeAddrs()
	// TODO: maybe safe to unlock early?
	l.unlock()
	disk.Write(1, aBlock)
	header := intToBlock(length)
	disk.Write(0, header)
}

func getLogEntry(addrs []uint64, logOffset uint64) (uint64, disk.Block) {
	diskAddr := 1 + 1 + logOffset
	a := addrs[logOffset]
	v := disk.Read(diskAddr + 1)
	return a, v
}

// applyLog assumes we are running sequentially
func applyLog(addrs []uint64) {
	length := uint64(len(addrs))
	for i := uint64(0); ; {
		if i < length {
			a, v := getLogEntry(addrs, i)
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
	addrs := *l.addrs
	applyLog(addrs)
	newAddrs := make([]uint64, 0)
	*l.addrs = newAddrs
	cache := l.cache
	for k := range cache {
		delete(cache, k)
	}
	clearLog()
	*l.length = 0
	l.unlock()
}

// Open recovers the log following a crash or shutdown
func Open() Log {
	header := disk.Read(0)
	length := blockToInt(header)
	addrs := decodeAddrs(length)
	addrPtr := new([]uint64)
	*addrPtr = addrs
	applyLog(addrs)
	clearLog()

	cache := make(map[uint64]disk.Block)
	lengthPtr := new(uint64)
	*lengthPtr = 0
	l := new(sync.RWMutex)
	return Log{l: l, addrs: addrPtr, cache: cache, length: lengthPtr}
}
