package disk

import "fmt"

// Block is a 4096-byte buffer
type Block []byte

const BlockSize int = 4096

// Disk provides access to a logical block-based disk
type Disk interface {
	// Read reads a disk block by address
	Read(a int) Block

	// Write updates a disk block by address
	Write(a int, v Block)

	// Barrier ensures data is persisted.
	//
	// When it returns, all outstanding writes are guaranteed to be durably on
	// disk
	Barrier()
}

type MemDisk []Block

var _ Disk = MemDisk(nil)

func NewMemDisk(numBlocks int) MemDisk {
	disk := make([]Block, numBlocks)
	for i := range disk {
		disk[i] = make([]byte, BlockSize)
	}
	return disk
}

func (d MemDisk) Read(a int) Block {
	return d[a]
}

func (d MemDisk) Write(a int, v Block) {
	if len(v) != BlockSize {
		panic(fmt.Errorf("v is not block-sized (%d bytes)", len(v)))
	}
	d[a] = v
}

func (d MemDisk) Barrier() {}

var implicitDisk Disk

// Init sets up the global disk
func Init(d Disk) {
	implicitDisk = d
}

// Read (see the Disk documentation)
func Read(a int) Block {
	return implicitDisk.Read(a)
}

// Write (see the Disk documentation)
func Write(a int, v Block) {
	implicitDisk.Write(a, v)
}

// Barrier (see the Disk documentation)
func Barrier() {
	implicitDisk.Barrier()
}
