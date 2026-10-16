package rpcclient

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/wire"
)

func splitAttachments(data []byte, chunkSize int) [][]byte {
	var parts [][]byte
	for len(data) > 0 {
		n := min(chunkSize, len(data))
		parts = append(parts, data[:n:n])
		data = data[n:]
	}
	return parts
}

func TestMergeAttachments(t *testing.T) {
	data := make([]byte, 600<<10)
	for i := range data {
		data[i] = byte(i)
	}

	for _, tc := range []struct {
		name        string
		attachments [][]byte
		want        []byte
	}{
		{name: "none"},
		{name: "all_empty", attachments: [][]byte{nil, {}, nil}},
		{name: "single", attachments: [][]byte{data[:100]}, want: data[:100]},
		{name: "split", attachments: splitAttachments(data, 64<<10), want: data},
		{name: "with_empty", attachments: [][]byte{nil, data[:10], {}, data[10:20], nil}, want: data[:20]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, mergeAttachments(tc.attachments))
		})
	}
}

func TestMergeAttachmentsCopies(t *testing.T) {
	for _, chunkSize := range []int{1, 3, 6} {
		t.Run(fmt.Sprint(chunkSize), func(t *testing.T) {
			attachment := []byte("rowset")
			merged := mergeAttachments(splitAttachments(attachment, chunkSize))

			attachment[0] = 'X'
			require.Equal(t, []byte("rowset"), merged)

			merged[1] = 'Y'
			require.Equal(t, []byte("Xowset"), attachment)
		})
	}
}

func TestDecodeFromWire(t *testing.T) {
	rows := []wire.Row{
		nil,
		{},
		{wire.NewInt64(0, -42), wire.NewBytes(1, []byte("rowset")), wire.NewAny(2, []byte("[1;2]"))},
	}
	for _, chunkSize := range []int{1024, 7} {
		t.Run(fmt.Sprint(chunkSize), func(t *testing.T) {
			data, err := wire.MarshalRowset(rows)
			require.NoError(t, err)
			parts := splitAttachments(data, chunkSize)

			decoded, err := decodeFromWire(parts)
			require.NoError(t, err)
			require.Equal(t, rows, decoded)

			clear(data)
			require.Equal(t, rows, decoded)
		})
	}
}

func TestDecodeFromWireEmpty(t *testing.T) {
	data, err := wire.MarshalRowset(nil)
	require.NoError(t, err)

	rows, err := decodeFromWire([][]byte{data})
	require.NoError(t, err)
	require.Nil(t, rows)
}

func TestDecodeFromWireTruncated(t *testing.T) {
	data, err := wire.MarshalRowset([]wire.Row{{wire.NewBytes(0, []byte("rowset"))}})
	require.NoError(t, err)

	rows, err := decodeFromWire(splitAttachments(data[:len(data)-1], 7))
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "unable to deserialize attachments:"))
	require.Nil(t, rows)
}

var benchmarkMergedAttachments []byte
var benchmarkDecodedRows []wire.Row

func BenchmarkMergeAttachments(b *testing.B) {
	for _, tc := range []struct {
		name      string
		size      int
		chunkSize int
	}{
		{name: "single/4KiB", size: 4 << 10},
		{name: "single/4MiB", size: 4 << 20},
		{name: "chunks64KiB/256KiB", size: 256 << 10, chunkSize: 64 << 10},
		{name: "chunks64KiB/1MiB", size: 1 << 20, chunkSize: 64 << 10},
		{name: "chunks64KiB/4MiB", size: 4 << 20, chunkSize: 64 << 10},
		{name: "chunks256KiB/4MiB", size: 4 << 20, chunkSize: 256 << 10},
	} {
		data := make([]byte, tc.size)
		parts := [][]byte{data}
		if tc.chunkSize > 0 {
			parts = splitAttachments(data, tc.chunkSize)
		}
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(tc.size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkMergedAttachments = mergeAttachments(parts)
			}
		})
	}
}

func BenchmarkDecodeFromWireBatch(b *testing.B) {
	for _, rowSize := range []int{192, 320} {
		b.Run(fmt.Sprintf("8000x%dB", rowSize), func(b *testing.B) {
			rows := make([]wire.Row, 8000)
			for i := range rows {
				rows[i] = wire.Row{wire.NewInt64(0, int64(i)), wire.NewBytes(1, make([]byte, rowSize-32))}
			}
			data, err := wire.MarshalRowset(rows)
			require.NoError(b, err)
			parts := splitAttachments(data, 64<<10)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkDecodedRows, err = decodeFromWire(parts)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
