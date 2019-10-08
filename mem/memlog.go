package mem

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/tchajed/go-awol"
	"github.com/tchajed/goose/machine/disk"
)

var debug bool

func init() {
	flag.BoolVar(&debug, "memlog-debug",
		false,
		"print all log writes to stderr")
}

type Log struct {
	l      *sync.RWMutex
	d      []disk.Block
	writes map[uint64]bool
}

func New(size int) Log {
	d := make([]disk.Block, size)
	for i := range d {
		d[i] = make(disk.Block, disk.BlockSize)
	}
	return Log{
		l:      new(sync.RWMutex),
		d:      d,
		writes: make(map[uint64]bool),
	}
}

func (l Log) Read(a uint64) disk.Block {
	l.l.Lock()
	defer l.l.Unlock()
	return l.d[a]
}

func (l Log) Size() int {
	return len(l.d)
}

func (l Log) Begin() *awol.Op {
	return &awol.Op{}
}

func showBlock(b disk.Block) string {
	lastNonZero := -1
	for i := 4096 - 1; i >= 0; i-- {
		if b[i] != 0 {
			lastNonZero = int(i)
			break
		}
	}
	var components []string
	for i := 0; i <= lastNonZero; i++ {
		components = append(components, fmt.Sprintf("%d", b[i]))
	}
	trailingZeros := int(disk.BlockSize) - lastNonZero - 1
	if trailingZeros > 0 {
		components = append(components, fmt.Sprintf("0 ×%d", trailingZeros))
	}
	return "[" + strings.Join(components, " ") + "]"
}

func (l Log) Commit(op *awol.Op) {
	l.l.Lock()
	defer l.l.Unlock()
	if debug {
		fmt.Fprintf(os.Stderr, "log: txn %p\n", op)
	}
	for i := range op.Addrs {
		a := op.Addrs[i]
		v := op.Blocks[i]
		if a >= uint64(len(l.d)) {
			panic(fmt.Sprintf("write %d out-of-range (%d blocks)", a, len(l.d)))
		}
		l.d[a] = v
		if debug {
			fmt.Fprintf(os.Stderr, "  d[%d] := %s\n", a, showBlock(v))
		}
		l.writes[a] = true
	}
}

func (l Log) Apply() {
}

// Crash simulates the effect of a crash
//
// since the log is completely synchronous this is a no-op; it only serves as
// documentation to signal intent in tests.
func (l *Log) Crash() {}

func (l Log) Writes() []uint64 {
	addrs := make([]uint64, 0, len(l.writes))
	for a, _ := range l.writes {
		addrs = append(addrs, a)
	}
	sort.Slice(addrs, func(i, j int) bool {
		return addrs[i] < addrs[j]
	})
	return addrs
}
