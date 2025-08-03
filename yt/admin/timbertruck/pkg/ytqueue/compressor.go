package ytqueue

import (
	"encoding/binary"
	"fmt"

	"github.com/klauspost/compress/zstd"

	"go.ytsaurus.tech/yt/go/compression"
)

type compressor struct {
	codecName         string
	encoder           *zstd.Encoder
	compressedDataBuf []byte
}

func newCompressor(compressedDataBufSize int, compressionCodecName string) (*compressor, error) {
	e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(newZstdEncoderLevel(compressionCodecName)))
	if err != nil {
		return nil, err
	}
	return &compressor{
		codecName:         compressionCodecName,
		encoder:           e,
		compressedDataBuf: make([]byte, 0, compressedDataBufSize),
	}, nil
}

func (c *compressor) compress(value []byte) []byte {
	compressed := c.encoder.EncodeAll(value, c.compressedDataBuf[:0])
	out := make([]byte, 8+len(compressed))
	binary.LittleEndian.PutUint64(out[:8], uint64(len(value)))
	copy(out[8:], compressed)
	return out
}

func (c *compressor) codec() string {
	return c.codecName
}

func (c *compressor) close() {
	_ = c.encoder.Close()
}

func newZstdEncoderLevel(codecName string) zstd.EncoderLevel {
	var level int
	switch codecName {
	case compression.CodecIDZstd6.String():
		level = 6
	default:
		panic(fmt.Sprintf("unexpected codec name: %s", codecName))
	}

	return zstd.EncoderLevelFromZstd(level)
}
