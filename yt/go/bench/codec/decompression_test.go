package codec

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/blockcodecs"
)

func benchmarkDecompression(
	b *testing.B,
	testCase string,
	newEncoder func(o io.Writer) io.WriteCloser,
	newDecoder func(i io.Reader) io.Reader,
) {
	data := readData(b, testCase)

	var buf bytes.Buffer
	out := newEncoder(&buf)

	_, err := out.Write(data)
	require.NoError(b, err)
	require.NoError(b, out.Close())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d := newDecoder(bytes.NewBuffer(buf.Bytes()))
		_, err := io.ReadAll(d)
		require.NoError(b, err)
	}

	// Workaround for DEVTOOLS-7700
	if *dataDir != "" {
		b.SetBytes(int64(len(data)))
		b.ReportMetric(0, "ns/op")
	}
}

func BenchmarkDecompression_ProxyLog_Gzip(b *testing.B) {
	benchmarkDecompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return gzip.NewWriter(o)
	}, func(i io.Reader) io.Reader {
		r, err := gzip.NewReader(i)
		require.NoError(b, err)
		return r
	})
}

func BenchmarkDecompression_ProxyLog_Block_Snappy(b *testing.B) {
	benchmarkDecompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("snappy"))
	}, NewDecoder)
}

func BenchmarkDecompression_ProxyLog_Block_LZ4(b *testing.B) {
	benchmarkDecompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("lz4-fast14-safe"))
	}, NewDecoder)
}

func BenchmarkDecompression_ProxyLog_Block_LZ4HC(b *testing.B) {
	benchmarkDecompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("lz4-hc-safe"))
	}, NewDecoder)
}

func BenchmarkDecompression_ProxyLog_Block_ZSTD_1(b *testing.B) {
	benchmarkDecompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_1"))
	}, NewDecoder)
}

func BenchmarkDecompression_ProxyLog_Block_ZSTD_3(b *testing.B) {
	benchmarkDecompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_3"))
	}, NewDecoder)
}

func BenchmarkDecompression_ProxyLog_Block_ZSTD_7(b *testing.B) {
	benchmarkDecompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_7"))
	}, NewDecoder)
}

func BenchmarkDecompression_ProxyLog_Block_Brotli_1(b *testing.B) {
	benchmarkDecompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("brotli_1"))
	}, NewDecoder)
}

func BenchmarkDecompression_ProxyLog_Block_Brotli_6(b *testing.B) {
	benchmarkDecompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("brotli_3"))
	}, NewDecoder)
}

func BenchmarkDecompression_LF_Gzip(b *testing.B) {
	benchmarkDecompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return gzip.NewWriter(o)
	}, func(i io.Reader) io.Reader {
		r, err := gzip.NewReader(i)
		require.NoError(b, err)
		return r
	})
}

func BenchmarkDecompression_LF_Block_Snappy(b *testing.B) {
	benchmarkDecompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("snappy"))
	}, NewDecoder)
}

func BenchmarkDecompression_LF_Block_LZ4(b *testing.B) {
	benchmarkDecompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("lz4-fast14-safe"))
	}, NewDecoder)
}

func BenchmarkDecompression_LF_Block_LZ4HC(b *testing.B) {
	benchmarkDecompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("lz4-hc-safe"))
	}, NewDecoder)
}

func BenchmarkDecompression_LF_Block_ZSTD_1(b *testing.B) {
	benchmarkDecompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_1"))
	}, NewDecoder)
}

func BenchmarkDecompression_LF_Block_ZSTD_3(b *testing.B) {
	benchmarkDecompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_3"))
	}, NewDecoder)
}

func BenchmarkDecompression_LF_Block_ZSTD_7(b *testing.B) {
	benchmarkDecompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_7"))
	}, NewDecoder)
}

func BenchmarkDecompression_LF_Block_Brotli_1(b *testing.B) {
	benchmarkDecompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("brotli_1"))
	}, NewDecoder)
}

func BenchmarkDecompression_LF_Block_Brotli_6(b *testing.B) {
	benchmarkDecompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("brotli_3"))
	}, NewDecoder)
}

func NewDecoder(r io.Reader) io.Reader {
	return blockcodecs.NewDecoder(r)
}
