package compression

import (
	"bytes"
	"encoding/binary"

	"github.com/klauspost/compress/zstd"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

type CodecZstd int

func (c CodecZstd) Compress(block []byte) ([]byte, error) {
	var out bytes.Buffer
	if err := binary.Write(&out, binary.LittleEndian, uint64(len(block))); err != nil {
		return nil, err
	}

	// todo consider bringing CGO zstd: https://pkg.go.dev/github.com/DataDog/zstd that supports all 20+ levels
	w, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(c))))
	if err != nil {
		return nil, err
	}
	defer func() { _ = w.Close() }()

	if _, err := out.Write(w.EncodeAll(block, nil)); err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}

func (c CodecZstd) Decompress(block []byte) ([]byte, error) {
	if len(block) < 8 {
		return nil, xerrors.Errorf("invalid block length: %d", len(block))
	}
	decompressedSize := binary.LittleEndian.Uint64(block[:8])

	r, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	decompressed, err := r.DecodeAll(block[8:], nil)
	if err != nil {
		return nil, err
	}

	if decompressedSize != uint64(len(decompressed)) {
		return nil, xerrors.Errorf("decompressed size mismatch: expected %d, got %d",
			decompressedSize, len(decompressed))
	}

	return decompressed, nil
}

func (c CodecZstd) GetID() CodecID {
	switch c {
	case 1:
		return CodecIDZstd1
	case 3:
		return CodecIDZstd3
	case 7:
		return CodecIDZstd7
	}
	return 0
}
