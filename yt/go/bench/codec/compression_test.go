package codec

import (
	"bytes"
	"compress/gzip"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/blockcodecs"
	_ "a.yandex-team.ru/library/go/blockcodecs/all"
	"a.yandex-team.ru/library/go/test/yatest"
)

var dataDir = flag.String("datadir", "", "")

func readData(b *testing.B, testCase string) []byte {
	var path string
	if *dataDir != "" {
		path = filepath.Join(*dataDir, testCase)
	} else {
		path = filepath.Join(yatest.WorkPath(filepath.Join("testdata", testCase)))
	}

	data, err := os.ReadFile(path)
	require.NoError(b, err)
	return data
}

func benchmarkCompression(b *testing.B, testCase string, newEncoder func(o io.Writer) io.WriteCloser) {
	data := readData(b, testCase)

	b.ResetTimer()

	ratio := 0.0
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		out := newEncoder(&buf)

		_, err := out.Write(data)
		require.NoError(b, err)
		require.NoError(b, out.Close())

		ratio = float64(buf.Len()) / float64(len(data))
	}

	// Workaround for DEVTOOLS-7700
	if *dataDir != "" {
		b.SetBytes(int64(len(data)))
		b.ReportMetric(ratio, "ratio")
		b.ReportMetric(0, "ns/op")
	}
}

func BenchmarkCompression_ProxyLog_Gzip(b *testing.B) {
	benchmarkCompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return gzip.NewWriter(o)
	})
}

func BenchmarkCompression_ProxyLog_Block_Snappy(b *testing.B) {
	benchmarkCompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("snappy"))
	})
}

func BenchmarkCompression_ProxyLog_Block_LZ4(b *testing.B) {
	benchmarkCompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("lz4-fast14-safe"))
	})
}

func BenchmarkCompression_ProxyLog_Block_LZ4HC(b *testing.B) {
	benchmarkCompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("lz4-hc-safe"))
	})
}

func BenchmarkCompression_ProxyLog_Block_ZSTD_1(b *testing.B) {
	benchmarkCompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_1"))
	})
}

func BenchmarkCompression_ProxyLog_Block_ZSTD_3(b *testing.B) {
	benchmarkCompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_3"))
	})
}

func BenchmarkCompression_ProxyLog_Block_ZSTD_7(b *testing.B) {
	benchmarkCompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_7"))
	})
}

func BenchmarkCompression_ProxyLog_Block_Brotli_1(b *testing.B) {
	benchmarkCompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("brotli_1"))
	})
}

func BenchmarkCompression_ProxyLog_Block_Brotli_6(b *testing.B) {
	benchmarkCompression(b, "yt-structured-http-proxy-log.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("brotli_3"))
	})
}

func BenchmarkCompression_LF_Gzip(b *testing.B) {
	benchmarkCompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return gzip.NewWriter(o)
	})
}

func BenchmarkCompression_LF_Block_Snappy(b *testing.B) {
	benchmarkCompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("snappy"))
	})
}

func BenchmarkCompression_LF_Block_LZ4(b *testing.B) {
	benchmarkCompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("lz4-fast14-safe"))
	})
}

func BenchmarkCompression_LF_Block_LZ4HC(b *testing.B) {
	benchmarkCompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("lz4-hc-safe"))
	})
}

func BenchmarkCompression_LF_Block_ZSTD_1(b *testing.B) {
	benchmarkCompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_1"))
	})
}

func BenchmarkCompression_LF_Block_ZSTD_3(b *testing.B) {
	benchmarkCompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_3"))
	})
}

func BenchmarkCompression_LF_Block_ZSTD_7(b *testing.B) {
	benchmarkCompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_7"))
	})
}

func BenchmarkCompression_LF_Block_Brotli_1(b *testing.B) {
	benchmarkCompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("brotli_1"))
	})
}

func BenchmarkCompression_LF_Block_Brotli_6(b *testing.B) {
	benchmarkCompression(b, "lf.yson", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("brotli_3"))
	})
}

func BenchmarkCompression_Urandom_Gzip(b *testing.B) {
	benchmarkCompression(b, "urandom", func(o io.Writer) io.WriteCloser {
		return gzip.NewWriter(o)
	})
}

func BenchmarkCompression_Urandom_Block_Snappy(b *testing.B) {
	benchmarkCompression(b, "urandom", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("snappy"))
	})
}

func BenchmarkCompression_Urandom_Block_LZ4(b *testing.B) {
	benchmarkCompression(b, "urandom", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("lz4-fast14-safe"))
	})
}

func BenchmarkCompression_Urandom_Block_LZ4HC(b *testing.B) {
	benchmarkCompression(b, "urandom", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("lz4-hc-safe"))
	})
}

func BenchmarkCompression_Urandom_Block_ZSTD_1(b *testing.B) {
	benchmarkCompression(b, "urandom", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_1"))
	})
}

func BenchmarkCompression_Urandom_Block_ZSTD_3(b *testing.B) {
	benchmarkCompression(b, "urandom", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_3"))
	})
}

func BenchmarkCompression_Urandom_Block_ZSTD_7(b *testing.B) {
	benchmarkCompression(b, "urandom", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("zstd_7"))
	})
}

func BenchmarkCompression_Urandom_Block_Brotli_1(b *testing.B) {
	benchmarkCompression(b, "urandom", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("brotli_1"))
	})
}

func BenchmarkCompression_Urandom_Block_Brotli_6(b *testing.B) {
	benchmarkCompression(b, "urandom", func(o io.Writer) io.WriteCloser {
		return blockcodecs.NewEncoder(o, blockcodecs.FindCodecByName("brotli_3"))
	})
}
