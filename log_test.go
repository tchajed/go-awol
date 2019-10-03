package awol_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tchajed/go-awol"
	"github.com/tchajed/goose/machine/disk"
)

type Log interface {
	Read(a uint64) disk.Block
	Size() int
	Begin() awol.Op
	Commit(op awol.Op)
	Apply()
}

var _ Log = &awol.Log{}

type LogSuite struct {
	suite.Suite
	l      *awol.Log
	memLog MemLog
}

func TestLogSuite(t *testing.T) {
	suite.Run(t, new(LogSuite))
}

func (suite *LogSuite) SetupTest() {
	disk.Init(disk.NewMemDisk(10000))
	suite.memLog = NewMem()
	suite.l = awol.New()
	if suite.memLog.Size() != suite.l.Size() {
		suite.FailNow("mem log size is not set up correctly")
	}
}

func (suite *LogSuite) Read(a uint64) (expected disk.Block, v disk.Block) {
	v = suite.l.Read(a)
	expected = suite.memLog.Read(a)
	return
}

func (suite *LogSuite) Check(a uint64, msgAndArgs ...interface{}) {
	expect, v := suite.Read(a)
	if !reflect.DeepEqual(expect, v) {
		for i := uint64(1); i < disk.BlockSize; i++ {
			if expect[i] != 0 || v[i] != 0 {
				break
			}
		}
		// blocks are of the form [i, 0...]
		suite.Fail("blocks differ",
			"d[%d] disk.Block{%d, 0...} != disk.Block{%d, 0...}",
			a, expect[0], v[0])
		return
	}
	suite.Equal(expect, v, msgAndArgs...)
}

func (suite *LogSuite) CheckAll(msgAndArgs ...interface{}) {
	as := suite.memLog.Writes()
	suite.Check(0, "addr: %d", 0)
	for _, a := range as {
		suite.Check(a, "addr: %d", a)
	}
	suite.Check(uint64(suite.memLog.Size())-1, "addr: (last)")
	if suite.T().Failed() {
		suite.FailNow("check failed", msgAndArgs...)
	}
}

type Write struct {
	A uint64
	V disk.Block
}

func (suite *LogSuite) Commit(writes []Write) {
	for _, l := range []Log{suite.l, suite.memLog} {
		op := l.Begin()
		for _, w := range writes {
			op.Write(w.A, w.V)
		}
		l.Commit(op)
	}
}

func (suite *LogSuite) Apply() {
	suite.l.Apply()
	suite.memLog.Apply()
}

func block(n int) disk.Block {
	if n >= 256 {
		panic("block value out of range")
	}
	b := make(disk.Block, disk.BlockSize)
	b[0] = byte(n)
	return b
}

func (suite *LogSuite) TestBasicCommit() {
	suite.CheckAll()
	suite.Commit([]Write{
		{2, block(1)},
		{3, block(2)},
	})
	suite.CheckAll()
}

func (suite *LogSuite) TestMultipleCommit() {
	suite.Commit([]Write{
		{2, block(1)},
		{3, block(2)},
	})
	suite.Commit([]Write{
		{2, block(1)},
		{3, block(2)},
	})
	suite.CheckAll()
}

func (suite *LogSuite) TestApplyEarly() {
	suite.Commit([]Write{
		{2, block(1)},
		{3, block(2)},
	})
	suite.Commit([]Write{
		{2, block(1)},
		{3, block(2)},
	})
	suite.Apply()
	suite.CheckAll()
}

func (suite *LogSuite) TestFillLog() {
	for opNum := 0; opNum < 60; opNum++ {
		var writes []Write
		for i := 0; i < 10; i++ {
			a := uint64(opNum*10 + i)
			writes = append(writes, Write{a, block(int(a % 256))})
		}
		suite.Commit(writes)
	}
	suite.CheckAll()
}

func (suite *LogSuite) TestEmptyCommit() {
	suite.Commit([]Write{})
	suite.Commit([]Write{
		{2, block(1)},
		{3, block(2)},
	})
	suite.Commit([]Write{})
	suite.CheckAll()
}

func (suite *LogSuite) TestRepeatedWrites() {
	for opNum := 0; opNum < 100; opNum++ {
		suite.Commit([]Write{
			{2, block(opNum % 256)},
		})
	}
	suite.CheckAll()
}

func (suite *LogSuite) TestOpenAfterCommit() {
	suite.Commit([]Write{
		{2, block(2)},
		{3, block(3)},
		{4, block(4)},
	})
	// crash should have no visible effect, so don't do anything to memLog
	suite.l = awol.Open()
	suite.CheckAll()
}

func (suite *LogSuite) TestOpenAfterApply() {
	suite.Commit([]Write{
		{2, block(2)},
		{3, block(3)},
		{4, block(4)},
	})
	suite.Apply()
	suite.l = awol.Open()
	suite.CheckAll()
}

func (suite *LogSuite) TestOpenAfterLogOverflow() {
	for a := uint64(0); a < 1000; a++ {
		suite.Commit([]Write{
			{a, block(2)},
		})
	}
	suite.l = awol.Open()
	suite.CheckAll()
}

func BenchmarkLogCommit(b *testing.B) {
	disk.Init(disk.NewMemDisk(10000))
	l := awol.New()
	block := make([]byte, disk.BlockSize)
	block[0] = 1
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := l.Begin()
		for a := 0; a < 3; a++ {
			op.Write(uint64(a), block)
		}
		l.Commit(op)
	}
}
