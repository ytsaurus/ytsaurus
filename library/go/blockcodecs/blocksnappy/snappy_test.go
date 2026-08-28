package blocksnappy

import (
	"bytes"
	"testing"

	"github.com/golang/snappy"
)

func TestSnappyCodec_CrossCompatible(t *testing.T) {
	payloads := [][]byte{
		nil,
		[]byte("hello"),
		bytes.Repeat([]byte("compressible payload "), 1000),
		make([]byte, 65536),
	}

	c := snappyCodec{}
	for _, payload := range payloads {
		encoded, err := c.Encode(nil, payload)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, err := snappy.Decode(nil, encoded)
		if err != nil {
			t.Fatalf("reference decoder rejected output: %v", err)
		}
		if !bytes.Equal(payload, decoded) {
			t.Fatalf("reference decoder round trip mismatch")
		}

		refEncoded := snappy.Encode(nil, payload)
		decoded, err = c.Decode(nil, refEncoded)
		if err != nil {
			t.Fatalf("decode reference output: %v", err)
		}
		if !bytes.Equal(payload, decoded) {
			t.Fatalf("round trip of reference output mismatch")
		}

		n, err := c.DecodedLen(refEncoded)
		if err != nil || n != len(payload) {
			t.Fatalf("DecodedLen = %d, %v; want %d", n, err, len(payload))
		}
	}
}
