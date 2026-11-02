package blockzstd_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/blockcodecs"
	"go.ytsaurus.tech/library/go/blockcodecs/blockzstd"
)

func TestPooledCodecDoesNotChangeRegisteredCodecs(t *testing.T) {
	registeredCount := len(blockcodecs.ListCodecs())
	for _, tc := range []struct {
		level int
		id    blockcodecs.CodecID
	}{
		{1, 55019}, {3, 23308}, {7, 33533},
	} {
		t.Run(fmt.Sprint(tc.level), func(t *testing.T) {
			name := fmt.Sprintf("zstd_%d", tc.level)
			alias := fmt.Sprintf("zstd08_%d", tc.level)
			registered := blockcodecs.FindCodecByName(name)
			require.NotNil(t, registered)
			require.Equal(t, registered, blockcodecs.FindCodecByName(alias))
			require.Equal(t, tc.id, registered.ID())
			require.Equal(t, alias, registered.Name())
			for range 2 {
				pooled, err := blockzstd.NewPooledCodec(tc.level)
				require.NoError(t, err)
				require.Equal(t, registered.ID(), pooled.ID())
				require.Equal(t, registered.Name(), pooled.Name())
			}
			require.Equal(t, registered, blockcodecs.FindCodecByName(name))
			require.Equal(t, registered, blockcodecs.FindCodecByName(alias))
			require.Equal(t, registered, blockcodecs.FindCodec(tc.id))
			require.Len(t, blockcodecs.ListCodecs(), registeredCount)
		})
	}
}

func TestPooledCodecRejectsUnsupportedLevels(t *testing.T) {
	for _, level := range []int{-7, -1, 0, 2, 4, 6, 8, 22} {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			codec, err := blockzstd.NewPooledCodec(level)
			require.Error(t, err)
			require.Nil(t, codec)
		})
	}
}

func TestPooledCodecRetainsWireFormatAndDestinationSemantics(t *testing.T) {
	random := make([]byte, 64<<10)
	large := make([]byte, 2<<20)
	generator := rand.New(rand.NewSource(1))
	_, err := generator.Read(random)
	require.NoError(t, err)
	_, err = generator.Read(large)
	require.NoError(t, err)
	inputs := []struct {
		name string
		data []byte
	}{
		{"nil", nil},
		{"tiny", []byte("one")},
		{"repeated", bytes.Repeat([]byte("EventLog request response backend activation"), 24)},
		{"random", random},
		{"large", large},
		{"after_large", []byte("independent small frame after large input")},
		{"nil_after_large", nil},
	}
	for _, level := range []int{1, 3, 7} {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			registered := blockcodecs.FindCodecByName(fmt.Sprintf("zstd_%d", level))
			pooled, err := blockzstd.NewPooledCodec(level)
			require.NoError(t, err)
			for _, destination := range []string{"nil", "tiny", "reused", "large_capacity"} {
				t.Run(destination, func(t *testing.T) {
					var dst []byte
					switch destination {
					case "tiny":
						dst = make([]byte, 2, 4)
					case "reused":
						dst = bytes.Repeat([]byte{0xcc}, 256)
					case "large_capacity":
						dst = make([]byte, 16, 3<<20)
					}
					for _, input := range inputs {
						t.Run(input.name, func(t *testing.T) {
							original := bytes.Clone(input.data)
							expected, err := registered.Encode(nil, input.data)
							require.NoError(t, err)
							encoded, err := pooled.Encode(dst, input.data)
							require.NoError(t, err)
							require.Equal(t, expected, encoded, "same independent native block bytes")
							require.GreaterOrEqual(t, len(encoded), 8)
							require.Equal(t, uint64(len(input.data)), binary.LittleEndian.Uint64(encoded[:8]))
							decodedLen, err := pooled.DecodedLen(encoded)
							require.NoError(t, err)
							require.Equal(t, len(input.data), decodedLen)
							for _, decoder := range []blockcodecs.Codec{registered, pooled} {
								decoded, err := decoder.Decode([]byte("overwritten destination"), encoded)
								require.NoError(t, err)
								require.True(t, bytes.Equal(input.data, decoded))
							}
							require.True(t, bytes.Equal(original, input.data), "source must remain owned by the caller")
							if destination == "large_capacity" {
								require.Equal(t, cap(dst), cap(encoded))
								require.True(t, &dst[:1][0] == &encoded[0], "use sufficient caller-provided capacity")
							}
							dst = encoded[:0]
						})
					}
				})
			}
		})
	}
}

func TestPooledCodecPreservesPreviouslyReturnedBytes(t *testing.T) {
	for _, level := range []int{1, 3, 7} {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			codec, err := blockzstd.NewPooledCodec(level)
			require.NoError(t, err)
			for _, capacity := range []int{0, 2 << 20} {
				var dst []byte
				if capacity != 0 {
					dst = make([]byte, 0, capacity)
				}
				encoded, err := codec.Encode(dst, []byte("retained output"))
				require.NoError(t, err)
				saved := bytes.Clone(encoded)
				for index := range 12 {
					_, err := codec.Encode(nil, bytes.Repeat([]byte(fmt.Sprintf("different-%d", index)), 128))
					require.NoError(t, err)
					require.Equal(t, saved, encoded)
				}
			}
		})
	}
}

func TestPooledCodecConcurrentEncodesAreIndependent(t *testing.T) {
	for _, level := range []int{1, 3, 7} {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			codec, err := blockzstd.NewPooledCodec(level)
			require.NoError(t, err)
			const workers = 4
			const iterations = 12
			var wait sync.WaitGroup
			failures := make(chan error, workers)
			for worker := range workers {
				wait.Add(1)
				go func() {
					defer wait.Done()
					var previous, snapshot []byte
					for iteration := range iterations {
						input := bytes.Repeat([]byte(fmt.Sprintf("worker-%d-iteration-%d", worker, iteration)), 64)
						encoded, err := codec.Encode(nil, input)
						if err != nil {
							failures <- err
							return
						}
						decoded, err := codec.Decode(nil, encoded)
						if err != nil || !bytes.Equal(input, decoded) {
							failures <- fmt.Errorf("concurrent independent block mismatch: %v", err)
							return
						}
						if previous != nil && !bytes.Equal(previous, snapshot) {
							failures <- fmt.Errorf("previous output mutated by another Encode")
							return
						}
						previous, snapshot = encoded, bytes.Clone(encoded)
					}
				}()
			}
			wait.Wait()
			close(failures)
			for err := range failures {
				t.Error(err)
			}
		})
	}
}
