package awol

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/tchajed/goose/machine/disk"
)

type LogSuite struct {
	suite.Suite
	D disk.FileDisk
	Log
}

var diskPath = "/tmp/test-disk"

func (suite *LogSuite) SetupTest() {
	d, err := disk.NewFileDisk(diskPath, 100)
	if err != nil {
		panic(err)
	}
	suite.D = d
	disk.Init(d)
	suite.Log = New()
}

func (suite *LogSuite) TearDownTest() {
	suite.D.Close()
	_ = os.Remove(diskPath)
}

func TestLogSuite(t *testing.T) {
	suite.Run(t, new(LogSuite))
}

var block0 disk.Block = make([]byte, disk.BlockSize)
var block1 disk.Block = make([]byte, disk.BlockSize)
var block2 disk.Block = make([]byte, disk.BlockSize)

func init() {
	block1[0] = 1
	block2[0] = 2
	diskPath += fmt.Sprintf(".%d", time.Now().UnixNano()/1000%1000)
}

func (suite *LogSuite) TestBasicLogWrite() {
	l := suite.Log
	suite.Equal(block0, l.Read(3))
	suite.Equal(true, l.BeginTxn())
	l.Write(3, block1)
	l.Write(2, block2)
	l.Commit()
	suite.Equal(block1, l.Read(3))
}

func (suite *LogSuite) TestMultipleCommits() {
	l := suite.Log
	l.BeginTxn()
	l.Write(3, block1)
	l.Write(2, block2)
	l.Commit()

	suite.Equal(false, l.BeginTxn())
	l.Apply()
	suite.Equal(true, l.BeginTxn())

	l.Write(2, block0)
	l.Commit()
	suite.Equal(block1, l.Read(3))
	suite.Equal(block0, l.Read(2))
}

func (suite *LogSuite) OpenCommit(apply bool) {
	l := suite.Log
	suite.True(l.BeginTxn())
	l.Write(3, block2)
	l.Write(2, block2)
	l.Write(1, block0)
	l.Commit()
	l.Apply()

	suite.True(l.BeginTxn())
	l.Write(1, block0)
	l.Write(2, block1)
	l.Commit()
	if apply {
		l.Apply()
	}

	l = Open()
	suite.Equal(block0, l.Read(0))
	suite.Equal(block0, l.Read(1))
	suite.Equal(block1, l.Read(2))
	suite.Equal(block2, l.Read(3))
}

func (suite *LogSuite) TestOpenCommitNoApply() {
	suite.OpenCommit(false)
}

func (suite *LogSuite) TestOpenCommitApply() {
	suite.OpenCommit(true)
}

func (suite *LogSuite) TestOpenAfterCrash() {
	l := suite.Log
	suite.True(l.BeginTxn())
	l.Write(3, block2)
	l.Write(2, block2)
	l.Write(1, block0)
	l.Commit()
	l.Apply()

	suite.True(l.BeginTxn())
	l.Write(1, block0)
	l.Write(2, block1)

	l = Open()
	suite.Log = l
	suite.Equal(block0, l.Read(0))
	suite.Equal(block0, l.Read(1))
	// this is the crucial test
	// (this write should be lost without a commit)
	suite.Equal(block2, l.Read(2))
	suite.Equal(block2, l.Read(3))
}

func (suite *LogSuite) TestCommitAfterCrash() {
	l := suite.Log
	l.BeginTxn()
	l.Write(3, block2)
	l.Write(2, block2)
	l.Write(1, block0)
	l.Commit()
	l.Apply()

	l.BeginTxn()
	l.Write(1, block0)
	l.Write(2, block1)

	l = Open()
	suite.Log = l
	suite.True(l.BeginTxn())
	l.Write(1, block0)
	l.Write(2, block1)
	l.Commit()

	suite.Equal(block1, l.Read(2))
}

func (suite *LogSuite) TestBoundaryWrite() {
	l := suite.Log
	lastAddr := l.Size() - 1
	l.BeginTxn()
	for i := uint64(0); i < MaxTxnWrites; i++ {
		l.Write(lastAddr, block1)
	}
	l.Commit()
	l.Apply()
	l.Apply()
	l = Open()

	suite.Equal(block1, l.Read(lastAddr))

	l.BeginTxn()
	for i := uint64(0); i < MaxTxnWrites; i++ {
		l.Write(lastAddr, block2)
	}
	l.Commit()
	l.Apply()
	l.Apply()
	l = Open()

	suite.Equal(block2, l.Read(lastAddr))
}

func BenchmarkLogCommit(b *testing.B) {
	disk.Init(disk.NewMemDisk(10000))
	l := New()
	block := make([]byte, disk.BlockSize)
	block[0] = 1
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok := l.BeginTxn()
		if !ok {
			b.Fatalf("could not allocate %dth transaction", i)
		}
		for a := 0; a < 3; a++ {
			l.Write(uint64(a), block)
		}
		l.Commit()
		l.Apply()
	}
}
