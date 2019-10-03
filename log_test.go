package awol_test

import (
	"testing"

	"github.com/tchajed/go-awol"
	"github.com/tchajed/goose/machine/disk"
)

type Log interface {
	Read(a uint64) disk.Block
	Size() int
	Begin() *awol.Op
	Commit(op awol.Op)
	Apply()
}

var _ Log = &awol.Log{}

func BenchmarkLogCommit(b *testing.B) {
	disk.Init(disk.NewMemDisk(10000))
	l := awol.New()
	block := make([]byte, disk.BlockSize)
	block[0] = 1
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := l.BeginOp()
		for a := 0; a < 3; a++ {
			op.Write(uint64(a), block)
		}
		l.Commit(op)
	}
}
