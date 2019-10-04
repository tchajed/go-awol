package awol

import (
	"testing"

	"github.com/magiconair/properties/assert"
)

// test private encoding/decoding functions

func TestEncodeDecodeHdr(t *testing.T) {
	for _, hdr := range []Hdr{
		{0, 0, []uint64{}},
		{0, 10, []uint64{}},
		{0, 0, []uint64{3, 4, 10, 24}},
		{10, 500, []uint64{7, 8}},
		{400, 200, []uint64{1 << 20}},
	} {
		for uint64(len(hdr.addrs)) < logLength {
			hdr.addrs = append(hdr.addrs, 0)
		}
		newHdr := decodeHdr(encodeHdr(hdr))
		assert.Equal(t, hdr, newHdr)
	}
}
