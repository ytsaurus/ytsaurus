package blockzstd_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"go.ytsaurus.tech/library/go/blockcodecs"
	"go.ytsaurus.tech/library/go/blockcodecs/blockzstd"
)

// The registered codec is the unchanged baseline. The opt-in codec is created
// once and warmed outside timing, as for a long-lived caller. Nil destinations
// retain the cost of allocating each returned block in both cases. This serial
// microbenchmark does not measure retained RSS, cold-after-GC or parallel costs.
func BenchmarkPooledCodecEncode(b *testing.B) {
	small := bytes.Repeat([]byte("EventLog request response backend activation"), 24)
	random := make([]byte, 64<<10)
	if _, err := rand.New(rand.NewSource(1)).Read(random); err != nil {
		b.Fatal(err)
	}
	for _, input := range []struct {
		name   string
		data   []byte
		levels []int
	}{
		{"bytes_1056", small, []int{1, 3, 7}},
		{"bytes_65536_seeded", random, []int{1}},
	} {
		for _, level := range input.levels {
			b.Run(fmt.Sprintf("%s/level_%d", input.name, level), func(b *testing.B) {
				registered := blockcodecs.FindCodecByName(fmt.Sprintf("zstd_%d", level))
				pooled, err := blockzstd.NewPooledCodec(level)
				if err != nil {
					b.Fatal(err)
				}
				expected, err := registered.Encode(nil, input.data)
				if err != nil {
					b.Fatal(err)
				}
				for _, implementation := range []struct {
					name  string
					codec blockcodecs.Codec
				}{
					{"registered", registered}, {"pooled", pooled},
				} {
					b.Run(implementation.name, func(b *testing.B) {
						for range 8 {
							encoded, err := implementation.codec.Encode(nil, input.data)
							if err != nil || !bytes.Equal(expected, encoded) {
								b.Fatalf("warm codec differs from the registered wire format: %v", err)
							}
						}
						b.ReportAllocs()
						b.SetBytes(int64(len(input.data)))
						b.ResetTimer()
						var encoded []byte
						for b.Loop() {
							encoded, err = implementation.codec.Encode(nil, input.data)
							if err != nil {
								b.Fatal(err)
							}
						}
						if !bytes.Equal(expected, encoded) {
							b.Fatal("measured codec returned a different native block")
						}
					})
				}
			})
		}
	}
}
