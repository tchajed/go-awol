package awol

import "github.com/tchajed/go-awol/disk"

type Log struct{}

// MaxTxnWrites is a guaranteed reservation for each transaction.
const MaxTxnWrites = 10 // 10 is completely arbitrary

// BeginTxn allocates space for a new transaction in the log.
//
// Returns true if the allocation succeeded.
func (l Log) BeginTxn() bool {
	return true
}

// Read from the logical disk.
//
// Reads must go through the log to return committed but un-applied writes.
func (l Log) Read(a int) disk.Block {
	panic("unimplemented")
}

// Write to the disk through the log.
func (l Log) Write(a int, v disk.Block) {
	panic("unimplemented")
}

// Commit the current transaction.
func (l Log) Commit() {
	panic("unimplemented")
}

// Apply all the committed transactions.
//
// Frees all the space in the log.
func (l Log) Apply() {
	panic("unimplemented")
}
