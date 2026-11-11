package yt

import (
	"context"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
)

type tableStream struct {
	data string
	err  error
}

type scriptedTableReads struct {
	streams []tableStream
	paths   []ypath.Rich
}

func (s *scriptedTableReads) ReadTableRaw(_ context.Context, path ypath.YPath, _ *ReadTableOptions) (io.ReadCloser, error) {
	s.paths = append(s.paths, *path.(*ypath.Rich))
	stream := s.streams[0]
	s.streams = s.streams[1:]

	var r io.Reader = strings.NewReader(stream.data)
	if stream.err != nil {
		r = io.MultiReader(r, iotest.ErrReader(stream.err))
	}
	return io.NopCloser(r), nil
}

func TestReadTableRawResumesFromFirstNotReturnedRow(t *testing.T) {
	t.Parallel()

	reads := &scriptedTableReads{streams: []tableStream{
		{data: "{\"a\":0}\n{\"a\"", err: io.ErrUnexpectedEOF},
		{data: "{\"a\":1}\n{\"a\":2}\n"},
	}}
	r := &retryingTableReader{ctx: context.Background(), yc: reads, resumable: true, retryCount: 1}

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "{\"a\":0}\n{\"a\":1}\n{\"a\":2}\n", string(data))
	require.Len(t, reads.paths, 2)
	require.Equal(t, []ypath.Range{{Lower: &ypath.ReadLimit{RowIndex: ptr.T(int64(1))}}}, reads.paths[1].Ranges)
}

func TestReadTableRawRetriesRowCutByEOF(t *testing.T) {
	t.Parallel()

	reads := &scriptedTableReads{streams: []tableStream{
		{data: "a\tb\nc\t"},
		{data: "c\td\n"},
	}}
	r := &retryingTableReader{
		ctx:         context.Background(),
		yc:          reads,
		path:        ypath.Rich{Ranges: []ypath.Range{{Lower: &ypath.ReadLimit{RowIndex: ptr.T(int64(10))}}}},
		resumeRange: ypath.Range{Lower: &ypath.ReadLimit{RowIndex: ptr.T(int64(10))}},
		resumable:   true,
		retryCount:  1,
	}

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "a\tb\nc\td\n", string(data))
	require.Equal(t, []ypath.Range{{Lower: &ypath.ReadLimit{RowIndex: ptr.T(int64(11))}}}, reads.paths[1].Ranges)
}

func TestReadTableRawFailsAfterReturningRowsWhenNotResumable(t *testing.T) {
	reads := &scriptedTableReads{streams: []tableStream{
		{data: "abc", err: io.ErrUnexpectedEOF},
	}}
	r := &retryingTableReader{ctx: context.Background(), yc: reads, retryCount: 1}

	data, err := io.ReadAll(r)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, "abc", string(data))
	require.Len(t, reads.paths, 1)
}

func TestIsRowPerLineFormat(t *testing.T) {
	require.True(t, isRowPerLineFormat("json"))
	require.True(t, isRowPerLineFormat(yson.RawValue("<encode_utf8=%false>json")))
	require.True(t, isRowPerLineFormat(yson.ValueWithAttrs{Value: "schemaful_dsv", Attrs: map[string]any{"columns": []string{"a"}}}))
	require.True(t, isRowPerLineFormat(yson.ValueWithAttrs{Value: "schemaful_dsv", Attrs: map[string]any{"enable_escaping": true, "missing_value_mode": "print_sentinel"}}))
	require.False(t, isRowPerLineFormat(yson.ValueWithAttrs{Value: "json", Attrs: map[string]any{"format": "pretty"}}))
	require.False(t, isRowPerLineFormat(yson.ValueWithAttrs{Value: "dsv", Attrs: map[string]any{"enable_escaping": false}}))
	require.False(t, isRowPerLineFormat(yson.ValueWithAttrs{Value: "schemaful_dsv", Attrs: map[string]any{"missing_value_mode": "skip_row"}}))
	require.False(t, isRowPerLineFormat(yson.ValueWithAttrs{Value: "schemaful_dsv", Attrs: map[string]any{"enable_column_names_header": true}}))
	require.False(t, isRowPerLineFormat("yson"))
	require.False(t, isRowPerLineFormat(nil))
}

func TestTableReaderKeepsAllRows(t *testing.T) {
	require.True(t, tableReaderKeepsAllRows(nil))
	require.True(t, tableReaderKeepsAllRows(map[string]any{"max_row_weight": 1024, "unavailable_chunk_strategy": "restore"}))
	require.False(t, tableReaderKeepsAllRows(map[string]any{"sampling_rate": 0.5}))
	require.False(t, tableReaderKeepsAllRows(yson.RawValue("{unavailable_chunk_strategy=skip}")))
}

func TestResumeRangeReplacesExactLimit(t *testing.T) {
	require.Equal(t,
		ypath.Range{Upper: &ypath.ReadLimit{RowIndex: ptr.T(int64(6))}},
		resumeRange([]ypath.Range{{Exact: &ypath.ReadLimit{RowIndex: ptr.T(int64(5))}}}))
	require.Equal(t,
		ypath.Range{Upper: &ypath.ReadLimit{Key: []any{"k", yson.ValueWithAttrs{Attrs: map[string]any{"type": "max"}}}}},
		resumeRange([]ypath.Range{{Exact: &ypath.ReadLimit{Key: []any{"k"}}}}))
}
