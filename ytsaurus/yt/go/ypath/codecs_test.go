package ypath

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressionBrotli(t *testing.T) {
	assert.Equal(t, CompressionCodec("brotli_1"), CompressionBrotli(1))
}
