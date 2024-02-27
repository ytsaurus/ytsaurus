package skiff

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
)

func benchmarkEncode(b *testing.B, value any) {
	ytSchema := schema.MustInfer(value)
	skiffSchema := FromTableSchema(ytSchema)

	var buf bytes.Buffer
	encoder, err := NewEncoder(&buf, skiffSchema)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, encoder.Write(value))
	}
	require.NoError(b, encoder.Flush())
	b.SetBytes(int64(len(buf.String()) * b.N))
	b.ReportAllocs()
}

func benchmarkDecode(b *testing.B, value any) {
	ytSchema := schema.MustInfer(value)
	skiffSchema := FromTableSchema(ytSchema)

	var buf bytes.Buffer
	encoder, err := NewEncoder(&buf, skiffSchema)
	require.NoError(b, err)

	for i := 0; i < 100; i++ {
		require.NoError(b, encoder.Write(value))
	}
	require.NoError(b, encoder.Flush())
	testFormat = Format{
		Name:           "skiff",
		TableSchemas:   []any{&skiffSchema},
		SchemaRegistry: nil,
	}
	b.ResetTimer()
	data := buf.Bytes()
	for i := 0; i < b.N; i++ {
		rb := bytes.NewBuffer(data)
		decoder, err := NewDecoder(rb, testFormat)
		require.NoError(b, err)
		res := make([]any, 0)
		for decoder.Next() {
			var v map[string]any
			require.NoError(b, decoder.Scan(&v))
			res = append(res, v)
		}
		require.NoError(b, decoder.Err())
		require.Equal(b, len(res), 100)
	}
	b.SetBytes(int64(len(buf.String()) * b.N))
	b.ReportAllocs()
}

type benchNestedStruct struct {
	K0    string
	Child struct {
		K1 string
		K2 int64
		K3 float64
	}
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

func BenchmarkStructDecoding(b *testing.B) {
	b.Run("String", func(b *testing.B) {
		benchmarkDecode(b, &benchStructString{
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
		benchmarkDecode(b, &benchStructInt{})
	})

	b.Run("Float", func(b *testing.B) {
		benchmarkDecode(b, &benchStructFloat{})
	})

	b.Run("Nested", func(b *testing.B) {
		benchmarkDecode(b, &benchNestedStruct{
			K0: "foo",
			Child: struct {
				K1 string
				K2 int64
				K3 float64
			}{K1: "bar", K2: 1, K3: 2.0},
		})
	})
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

	b.Run("Nested", func(b *testing.B) {
		benchmarkEncode(b, &benchNestedStruct{
			K0: "foo",
			Child: struct {
				K1 string
				K2 int64
				K3 float64
			}{K1: "bar", K2: 1, K3: 2.0},
		})
	})
}
