package skiff

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/schema"
)

func benchmarkEncode(b *testing.B, value interface{}) {
	ytSchema := schema.MustInfer(value)
	skiffSchema := FromTableSchema(ytSchema)

	encoder, err := NewEncoder(ioutil.Discard, skiffSchema)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, encoder.Write(value))
	}
	require.NoError(b, encoder.Flush())

	b.ReportAllocs()
}

type benchStructString struct {
	K0, K1, K2, K3, K4, K5, K6, K7, K8, K9 string
}

type benchStructInt struct {
	K0, K1, K2, K3, K4, K5, K6, K7, K8, K9 int64
}

type benchStructFloat struct {
	K0, K1, K2, K3, K4, K5, K6, K7, K8, K9 float64
}

func BenchmarkStructEncoding(b *testing.B) {
	b.Run("String", func(b *testing.B) {
		benchmarkEncode(b, &benchStructString{
			K0: "foobar",
			K1: "foobar",
			K2: "foobar",
			K3: "foobar",
			K4: "foobar",
			K5: "foobar",
			K6: "foobar",
			K7: "foobar",
			K8: "foobar",
			K9: "foobar",
		})
	})

	b.Run("Int", func(b *testing.B) {
		benchmarkEncode(b, &benchStructInt{})
	})

	b.Run("Float", func(b *testing.B) {
		benchmarkEncode(b, &benchStructFloat{})
	})
}
