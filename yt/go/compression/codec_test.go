package compression

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"a.yandex-team.ru/library/go/test/yatest"
)

type Action string

const (
	ActionCompress   Action = "compress"
	ActionDecompress Action = "decompress"
)

func runCPPCodec(t *testing.T, codecID CodecID, in []byte, action Action) []byte {
	t.Helper()

	binary, err := yatest.BinaryPath("yt/yt/tools/run_codec/run_codec")
	require.NoError(t, err)

	cmd := exec.Command(binary, string(action), codecID.String())
	cmd.Stdin = bytes.NewReader(in)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr

	require.NoError(t, cmd.Run())
	return out.Bytes()
}

func cppCompress(t *testing.T, codecID CodecID, block []byte) []byte {
	t.Helper()

	return runCPPCodec(t, codecID, block, ActionCompress)
}

func cppDecompress(t *testing.T, codecID CodecID, block []byte) []byte {
	t.Helper()

	return runCPPCodec(t, codecID, block, ActionDecompress)
}

func goCompress(t *testing.T, codec Codec, block []byte) []byte {
	t.Helper()

	out, err := codec.Compress(block)
	require.NoError(t, err)
	return out
}

func goDecompress(t *testing.T, codec Codec, block []byte) []byte {
	t.Helper()

	out, err := codec.Decompress(block)
	require.NoError(t, err)
	return out
}

func TestCodecs(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Overwrite lz4SignatureV1MaxLength to test extended header on large inputs.
	defaultLz4SignatureV1MaxLength := lz4SignatureV1MaxLength
	defer func() {
		lz4SignatureV1MaxLength = defaultLz4SignatureV1MaxLength
	}()
	lz4SignatureV1MaxLength = 1000

	codecs := []CodecID{
		CodecIDSnappy,
		CodecIDLz4, CodecIDLz4HighCompression,
		CodecIDBrotli1, CodecIDBrotli2, CodecIDBrotli3, CodecIDBrotli4, CodecIDBrotli5, CodecIDBrotli6, CodecIDBrotli7, CodecIDBrotli8, CodecIDBrotli9, CodecIDBrotli10, CodecIDBrotli11,
		CodecIDZlib1, CodecIDZlib2, CodecIDZlib3, CodecIDZlib4, CodecIDZlib5, CodecIDZlib6, CodecIDZlib7, CodecIDZlib8, CodecIDZlib9,
		CodecIDZstd1, CodecIDZstd3, CodecIDZstd7,
	}

	for _, tc := range []struct {
		name  string
		input []byte
	}{
		{
			name:  "small",
			input: []byte("Hello, Codecs!"),
		},
		{
			name:  "large",
			input: []byte(strings.Repeat("Hello, Codecs!", 100000)),
		},
		{
			name:  "random-bytes-small",
			input: randomBytes(64),
		},
		{
			name:  "random-bytes-large",
			input: randomBytes(2 * 1000 * 1000),
		},
		{
			name:  "empty-string",
			input: []byte(""),
		},
		{
			name:  "nil",
			input: nil,
		},
	} {
		for _, codecID := range codecs {
			testName := fmt.Sprintf("%s/%s", tc.name, codecID)
			t.Run(testName, func(t *testing.T) {
				codec := NewCodec(codecID)

				compressed := goCompress(t, codec, tc.input)
				decompressed := goDecompress(t, codec, compressed)
				checkBytesEqual(t, tc.input, decompressed)

				cppCompressed := cppCompress(t, codecID, tc.input)
				cppDecompressed := cppDecompress(t, codecID, cppCompressed)
				checkBytesEqual(t, tc.input, cppDecompressed)

				checkBytesEqual(t, tc.input, cppDecompress(t, codecID, compressed),
					"cpp decoder should be able to decode data encoded with go encoder")
				checkBytesEqual(t, tc.input, goDecompress(t, codec, cppCompressed),
					"go decoder should be able to decode data encoded with cpp encoder")

				t.Logf("compressed data sizes: go: %d, cpp: %d; input length: %d",
					len(compressed), len(cppCompressed), len(tc.input))

				if l := len(tc.input); l > 0 {
					lenDiff := len(cppCompressed) - len(compressed)
					require.True(t, math.Abs(float64(lenDiff))/float64(l) < 1.1,
						"compressed data sizes should not differ much")
				}
			})
		}
	}
}

func checkBytesEqual(t *testing.T, expected, actual []byte, msgAndArgs ...interface{}) {
	if len(expected) == 0 {
		require.Empty(t, actual, msgAndArgs...)
	} else {
		require.Equal(t, expected, actual, msgAndArgs...)
	}
}

func randomBytes(len int) []byte {
	raw := make([]byte, len)
	_, _ = rand.Read(raw[:])
	return raw
}
