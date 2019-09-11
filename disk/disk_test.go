package disk

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type DiskSuite struct {
	suite.Suite
	D FileDisk
}

var diskPath = "/tmp/test-disk"

func (suite *DiskSuite) SetupTest() {
	d, err := NewFileDisk(diskPath, 100)
	if err != nil {
		panic(err)
	}
	suite.D = d
	Init(d)
}

func (suite *DiskSuite) TearDownTest() {
	suite.D.Close()
	_ = os.Remove(diskPath)
}

func TestDiskSuite(t *testing.T) {
	suite.Run(t, new(DiskSuite))
}

var block0 Block = make([]byte, BlockSize)
var block1 Block = make([]byte, BlockSize)
var block2 Block = make([]byte, BlockSize)

func init() {
	block1[0] = 1
	block2[0] = 2
	rand.Seed(time.Now().UnixNano())
	diskPath += fmt.Sprintf("%d", rand.Int())
}

func (suite *DiskSuite) TestReadWrite() {
	Write(3, block1)
	Write(4, block2)
	suite.Equal(block0, Read(2))
	suite.Equal(block1, Read(3))
	suite.Equal(block2, Read(4))
}

func (suite *DiskSuite) TestSize() {
	suite.Equal(100, Size())
}

func (suite *DiskSuite) TestBarrier() {
	Write(99, block1)
	Barrier()
	suite.Equal(block1, Read(99))
}

func (suite *DiskSuite) TestCrash() {
	Write(2, block1)
	Barrier()

	suite.D.Close()
	d, err := OpenFileDisk(diskPath)
	if err != nil {
		panic(err)
	}
	suite.D = d
	Init(suite.D)

	suite.Equal(block1, Read(2))
	suite.Equal(block0, Read(3))
}
