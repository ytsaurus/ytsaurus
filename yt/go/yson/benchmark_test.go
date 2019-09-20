package yson

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func BenchmarkMarshal(b *testing.B) {
	for _, input := range []interface{}{
		1,
		uint64(1),
		"foo",
		ValueWithAttrs{
			map[string]interface{}{
				"foo":  "bar",
				"foo1": nil,
			},
			[]interface{}{1, "zog", nil},
		},
	} {
		b.Run(fmt.Sprintf("%T", input), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = Marshal(input)
			}
		})
	}
}

func RunBenchmark(b *testing.B, do func(input []byte, typ Type) error) {
	for _, format := range []Format{FormatBinary, FormatText} {
		for _, typ := range []Type{TypeString, TypeInt64, TypeFloat64} {
			b.Run(fmt.Sprint(format, "/", typ), func(b *testing.B) {
				var input bytes.Buffer
				w := NewWriterFormat(&input, format)

				w.BeginList()
				w.BeginMap()
				for i := 0; input.Len() < 10*1024*1024; i++ {
					if i%10 == 0 {
						w.EndMap()
						w.BeginMap()
					}

					w.MapKeyString(fmt.Sprintf("K%d", i%10))
					switch typ {
					case TypeString:
						w.String(strings.Repeat(" ", 1000))
					case TypeInt64:
						w.Int64(42)
					case TypeFloat64:
						w.Float64(3.14)
					}
				}

				w.EndMap()
				w.EndList()
				_ = w.Finish()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := do(input.Bytes(), typ); err != nil {
						b.Fatal(err)
					}
				}

				b.SetBytes(int64(input.Len()) * int64(b.N))
				b.ReportAllocs()
			})
		}
	}
}

func BenchmarkScanner(b *testing.B) {
	RunBenchmark(b, func(input []byte, typ Type) error {
		return Valid(input)
	})
}

func BenchmarkReader(b *testing.B) {
	RunBenchmark(b, func(input []byte, typ Type) error {
		r := NewReaderFromBytes(input)
		for {
			e, err := r.Next(false)
			if err != nil {
				b.Fatal(err)
			}

			if e == EventEOF {
				return nil
			}
		}
	})
}

func BenchmarkDecoderInterface(b *testing.B) {
	RunBenchmark(b, func(input []byte, typ Type) error {
		var value interface{}
		return Unmarshal(input, &value)
	})
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

func BenchmarkDecoderStruct(b *testing.B) {
	RunBenchmark(b, func(input []byte, typ Type) error {
		switch typ {
		case TypeString:
			var s []benchStructString
			return Unmarshal(input, &s)
		case TypeInt64:
			var s []benchStructInt
			return Unmarshal(input, &s)
		case TypeFloat64:
			var s []benchStructFloat
			return Unmarshal(input, &s)
		}

		return nil
	})
}
