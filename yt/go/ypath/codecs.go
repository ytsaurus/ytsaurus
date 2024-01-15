package ypath

import "fmt"

// CompressionCodec is compression codec used for storing data.
type CompressionCodec string

// UnmarshalText is implementation of encoding.TextUnmarshaler.
func (c *CompressionCodec) UnmarshalText(text []byte) error {
	*c = CompressionCodec(text)
	return nil
}

// MarshalText is implementation of encoding.TextMarshaler.
func (c CompressionCodec) MarshalText() (text []byte, err error) {
	return []byte(c), nil
}

func compressionLevel(codec string, n, min, max int) CompressionCodec {
	if n < min {
		n = min
	}

	if n > max {
		n = max
	}

	return CompressionCodec(fmt.Sprintf("%s_%d", codec, n))
}

// CompressionBrotli returns brotli compression codec with quality n.
//
// Supports quality between 1 and 11.
func CompressionBrotli(n int) CompressionCodec {
	return compressionLevel("brotli", n, 1, 11)
}

// CompressionZLIB returns zlib compression codec with quality n.
//
// Supports quality between 1 and 9.
func CompressionZLIB(n int) CompressionCodec {
	return compressionLevel("zlib", n, 1, 9)
}

// CompressionZSTD returns zstd compression codec with quality n.
//
// Supports quality between 1 and 21.
func CompressionZSTD(n int) CompressionCodec {
	return compressionLevel("zstd", n, 1, 21)
}

// CompressionLZMA returns lzma compression codec with quality n.
//
// Supports quality between 0 and 9.
func CompressionLZMA(n int) CompressionCodec {
	return compressionLevel("lzma", n, 0, 9)
}

// CompressionBZIP2 returns bzip2 compression codec with quality n.
//
// Supports quality between 1 and 9.
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

// ErasureCodec is erasure codec used for storing data.
type ErasureCodec string

// UnmarshalText is implementation of encoding.TextUnmarshaler.
func (c *ErasureCodec) UnmarshalText(text []byte) error {
	*c = ErasureCodec(text)
	return nil
}

// MarshalText is implementation of encoding.TextMarshaler.
func (c ErasureCodec) MarshalText() (text []byte, err error) {
	return []byte(c), nil
}

const (
	ErasureNone        ErasureCodec = "none"
	ErasureReedSolomon ErasureCodec = "reed_solomon_6_3"
	ErasureLRC         ErasureCodec = "lrc_12_2_2"
	ErasureISALRC      ErasureCodec = "isa_lrc_12_2_2"
)
