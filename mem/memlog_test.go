package mem

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tchajed/goose/machine/disk"
)

func TestShowBlock(t *testing.T) {
	assert := assert.New(t)
	b := make(disk.Block, disk.BlockSize)
	assert.Equal("[0 ×4096]", showBlock(b))
	b[0] = 1
	assert.Equal("[1 0 ×4095]", showBlock(b))
	b[3] = 2
	assert.Equal("[1 0 0 2 0 ×4092]", showBlock(b))
}
