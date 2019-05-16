package crc64

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCanonicalInputs(t *testing.T) {
	for _, i := range []struct {
		result uint64
		input  string
	}{
		{
			0x0000000000000000,
			"",
		}, {
			0x74b42565ce6232d5,
			"a",
		}, {
			0x5f02be5e81cf7b1c,
			"ab",
		}, {
			0xaadaac6d7d340c20,
			"abc",
		}, {
			0xd35b54234f7f70a0,
			"abcd",
		}, {
			0xe729d85f050fa861,
			"abcde",
		}, {
			0x4852bb31b666ae4f,
			"abcdef",
		}, {
			0xab31ee2e0fe39abb,
			"abcdefg",
		}, {
			0x3dc543531acca62b,
			"abcdefgh",
		}, {
			0x43c501e26fc35778,
			"abcdefghi",
		}, {
			0x4cc4843d59c1373e,
			"abcdefghij",
		}, {
			0x481ac76eee0d3ebd,
			"There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977",
		},
	} {
		h := New()
		_, _ = h.Write([]byte(i.input))
		assert.Equalf(t, i.result, h.Sum64(), "%064b != %064b", i.result, h.Sum64())
	}
}
