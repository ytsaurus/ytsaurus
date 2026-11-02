package blockzstd

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"

	"go.ytsaurus.tech/library/go/blockcodecs"
)

// EncodeAll's bit writer can retain the caller's destination. This limit bounds
// retained output arrays, not the encoder's compression workspace or total RSS.
const maxRetainedEncoderOutputBytes = 1 << 20

type pooledCodec struct {
	zstdCodec
	encoders sync.Pool
}

// NewPooledCodec creates an opt-in codec for level 1, 3, or 7 without registering
// it or changing the existing zstd codecs. Its wire format and destination
// semantics are identical to those of the corresponding registered codec.
//
// The codec is safe for concurrent use. Encoder reuse reduces allocations but
// retains warm compression workspaces and output arrays up to 1 MiB per cached
// encoder. The output limit does not bound workspace memory, which depends on
// the level and concurrency. sync.Pool may discard cached encoders during GC;
// it is not a hard RSS budget. Use the registered codec to avoid this tradeoff.
func NewPooledCodec(level int) (blockcodecs.Codec, error) {
	switch level {
	case 1, 3, 7:
		return &pooledCodec{zstdCodec: zstdCodec(level)}, nil
	default:
		return nil, fmt.Errorf("unsupported zstd level: %d", level)
	}
}

func (z *pooledCodec) Encode(dst, src []byte) ([]byte, error) {
	if cap(dst) < 8 {
		dst = make([]byte, 8)
	}
	dst = dst[:8]
	binary.LittleEndian.PutUint64(dst, uint64(len(src)))

	var encoder *zstd.Encoder
	if cached := z.encoders.Get(); cached != nil {
		encoder = cached.(*zstd.Encoder)
	} else {
		var err error
		encoder, err = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(z.zstdCodec))),
			zstd.WithEncoderConcurrency(1))
		if err != nil {
			return nil, err
		}
	}

	// EncodeAll starts an independent frame without a streaming writer or
	// background goroutine. Every codec instance owns its own pool.
	encoded := encoder.EncodeAll(src, dst)
	if cap(encoded) <= maxRetainedEncoderOutputBytes {
		z.encoders.Put(encoder)
	}
	return encoded, nil
}
