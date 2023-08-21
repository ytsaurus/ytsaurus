package compression

import "github.com/golang/snappy"

type CodecSnappy struct{}

func (c *CodecSnappy) Compress(block []byte) ([]byte, error) {
	return snappy.Encode(nil, block), nil
}

func (c *CodecSnappy) Decompress(block []byte) ([]byte, error) {
	return snappy.Decode(nil, block)
}

func (c *CodecSnappy) GetID() CodecID {
	return CodecIDSnappy
}
