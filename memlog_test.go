package awol_test

import (
	"sync"

	"github.com/tchajed/go-awol"
	"github.com/tchajed/goose/machine/disk"
)

type MemLog struct {
	l *sync.RWMutex
	d []disk.Block
}

var _ Log = MemLog{}

func NewMem() MemLog {
	size := disk.Size() - (1 + 510)
	d := make([]disk.Block, size)
	for i := range d {
		block := make(disk.Block, disk.BlockSize)
		d[i] = block
	}
	return MemLog{l: new(sync.RWMutex), d: d}
}

func (l MemLog) Read(a uint64) disk.Block {
	l.l.Lock()
	defer l.l.Lock()
	return l.d[a]
}

func (l MemLog) Size() int {
	l.l.Lock()
	defer l.l.Lock()
	return len(l.d)
}

func (l MemLog) Begin() *awol.Op {
	return &awol.Op{}
}

func (l MemLog) Commit(op awol.Op) {
	l.l.Lock()
	defer l.l.Lock()
	for i := range op.Addrs {
		a := op.Addrs[i]
		v := op.Blocks[i]
		l.d[a] = v
	}
}

func (l MemLog) Apply() {
}
