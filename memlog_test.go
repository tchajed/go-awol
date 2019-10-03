package awol_test

import (
	"fmt"
	"sort"
	"sync"

	"github.com/tchajed/go-awol"
	"github.com/tchajed/goose/machine/disk"
)

type MemLog struct {
	l      *sync.RWMutex
	d      []disk.Block
	writes map[uint64]bool
}

var _ Log = MemLog{}

func NewMem() MemLog {
	size := disk.Size() - (1 + 510)
	d := make([]disk.Block, size)
	for i := range d {
		d[i] = make(disk.Block, disk.BlockSize)
	}
	return MemLog{
		l:      new(sync.RWMutex),
		d:      d,
		writes: make(map[uint64]bool),
	}
}

func (l MemLog) Read(a uint64) disk.Block {
	l.l.Lock()
	defer l.l.Unlock()
	return l.d[a]
}

func (l MemLog) Size() int {
	l.l.Lock()
	defer l.l.Unlock()
	return len(l.d)
}

func (l MemLog) Begin() awol.Op {
	return awol.Op{}
}

func (l MemLog) Commit(op awol.Op) {
	l.l.Lock()
	defer l.l.Unlock()
	for i := range op.Addrs {
		a := op.Addrs[i]
		v := op.Blocks[i]
		if a >= uint64(len(l.d)) {
			panic(fmt.Sprintf("write %d out-of-range (%d blocks)", a, len(l.d)))
		}
		l.d[a] = v
		l.writes[a] = true
	}
}

func (l MemLog) Apply() {
}

func (l MemLog) Writes() []uint64 {
	addrs := make([]uint64, 0, len(l.writes))
	for a, _ := range l.writes {
		addrs = append(addrs, a)
	}
	sort.Slice(addrs, func(i, j int) bool {
		return addrs[i] < addrs[j]
	})
	return addrs
}
