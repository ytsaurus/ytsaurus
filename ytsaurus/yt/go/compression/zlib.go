package compression

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

type CodecZlib int

func (c CodecZlib) Compress(block []byte) ([]byte, error) {
	var out bytes.Buffer
	if err := binary.Write(&out, binary.LittleEndian, uint64(len(block))); err != nil {
		return nil, err
	}

	w, err := zlib.NewWriterLevel(&out, int(c))
	if err != nil {
		return nil, err
	}

	if _, err := io.Copy(w, bytes.NewReader(block)); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}

func (c CodecZlib) Decompress(block []byte) ([]byte, error) {
	if len(block) < 8 {
		return nil, xerrors.Errorf("invalid block length: %d", len(block))
	}
	decompressedSize := binary.LittleEndian.Uint64(block[:8])

	r, err := zlib.NewReader(bytes.NewReader(block[8:]))
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	if _, err := io.Copy(&out, r); err != nil {
		return nil, err
	}
	if err := r.Close(); err != nil {
		return nil, err
	}

	if decompressedSize != uint64(out.Len()) {
		return nil, xerrors.Errorf("decompressed data size mismatch: expected %d, got %d",
			decompressedSize, out.Len())
	}

	return out.Bytes(), nil
}

func (c CodecZlib) GetID() CodecID {
	switch c {
	case 1:
		return CodecIDZlib1
	case 2:
		return CodecIDZlib2
	case 3:
		return CodecIDZlib3
	case 4:
		return CodecIDZlib4
	case 5:
		return CodecIDZlib5
	case 6:
		return CodecIDZlib6
	case 7:
		return CodecIDZlib7
	case 8:
		return CodecIDZlib8
	case 9:
		return CodecIDZlib9
	}
	return 0
}
