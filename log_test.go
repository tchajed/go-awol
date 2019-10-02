package awol

import (
	"testing"

	"github.com/tchajed/goose/machine/disk"
)

func BenchmarkLogCommit(b *testing.B) {
	disk.Init(disk.NewMemDisk(10000))
	l := New()
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
