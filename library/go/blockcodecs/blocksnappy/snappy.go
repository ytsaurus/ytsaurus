package blocksnappy

import (
	"github.com/klauspost/compress/s2"

	"go.ytsaurus.tech/library/go/blockcodecs"
)

type snappyCodec struct{}

func (s snappyCodec) ID() blockcodecs.CodecID {
	return 50986
}

func (s snappyCodec) Name() string {
	return "snappy"
}

func (s snappyCodec) DecodedLen(in []byte) (int, error) {
	return s2.DecodedLen(in)
}

func (s snappyCodec) Encode(dst, src []byte) ([]byte, error) {
	return s2.EncodeSnappy(dst, src), nil
}

func (s snappyCodec) Decode(dst, src []byte) ([]byte, error) {
	return s2.Decode(dst, src)
}

func init() {
	blockcodecs.Register(snappyCodec{})
}
