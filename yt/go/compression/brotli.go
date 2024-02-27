package compression

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/andybalholm/brotli"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

type CodecBrotli int

func (c CodecBrotli) Compress(block []byte) ([]byte, error) {
	var out bytes.Buffer
	if err := binary.Write(&out, binary.LittleEndian, uint64(len(block))); err != nil {
		return nil, err
	}

	w := brotli.NewWriterLevel(&out, int(c))
	if _, err := w.Write(block); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}

func (c CodecBrotli) Decompress(block []byte) ([]byte, error) {
	if len(block) < 8 {
		return nil, xerrors.Errorf("invalid block length: %d", len(block))
	}

	rb := bytes.NewBuffer(block[8:])
	r := brotli.NewReader(rb)

	var out bytes.Buffer
	if _, err := io.Copy(&out, r); err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}

func (c CodecBrotli) GetID() CodecID {
	switch c {
	case 1:
		return CodecIDBrotli1
	case 2:
		return CodecIDBrotli2
	case 3:
		return CodecIDBrotli3
	case 4:
		return CodecIDBrotli4
	case 5:
		return CodecIDBrotli5
	case 6:
		return CodecIDBrotli6
	case 7:
		return CodecIDBrotli7
	case 8:
		return CodecIDBrotli8
	case 9:
		return CodecIDBrotli9
	case 10:
		return CodecIDBrotli10
	case 11:
		return CodecIDBrotli11
	}
	return 0
}
