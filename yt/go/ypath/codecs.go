package ypath

import "fmt"

type CompressionCodec string

func (c *CompressionCodec) UnmarshalText(text []byte) error {
	*c = CompressionCodec(text)
	return nil
}

func (c CompressionCodec) MarshalText() (text []byte, err error) {
	return []byte(c), nil
}

func compressionLevel(codec string, n, max, min int) CompressionCodec {
	if n < min {
		n = min
	}

	if n > max {
		n = max
	}

	return CompressionCodec(fmt.Sprintf("%s_%d", codec, n))
}

func CompressionBrotli(n int) CompressionCodec {
	return compressionLevel("brotli", n, 1, 11)
}

func CompressionZLIB(n int) CompressionCodec {
	return compressionLevel("zlib", n, 1, 9)
}

func CompressionZSTD(n int) CompressionCodec {
	return compressionLevel("zstd", n, 1, 21)
}

func CompressionLZMA(n int) CompressionCodec {
	return compressionLevel("lzma", n, 0, 9)
}

func CompressionBZIP2(n int) CompressionCodec {
	return compressionLevel("bzip2", n, 1, 9)
}

const (
	CompressionNode               CompressionCodec = "none"
	CompressionSnappy             CompressionCodec = "snappy"
	CompressionLZ4                CompressionCodec = "lz4"
	CompressionLZ4HighCompression CompressionCodec = "lz4_high_compression"
	CompressionQuickLZ            CompressionCodec = "quick_lz"
)

type ErasureCodec string

func (c *ErasureCodec) UnmarshalText(text []byte) error {
	*c = ErasureCodec(text)
	return nil
}

func (c ErasureCodec) MarshalText() (text []byte, err error) {
	return []byte(c), nil
}

const (
	ErasureNone        ErasureCodec = "none"
	ErasureReedSolomon ErasureCodec = "reed_solomon_6_3"
	ErasureLRC         ErasureCodec = "lrc_12_2_2"
)
