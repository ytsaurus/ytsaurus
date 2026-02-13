package timbertruck

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/zstdsync"
)

type stringWriter string

func (s stringWriter) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte(s))
	return int64(n), err
}

func TestSkippedRowsWriter_PlainFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "skipped.txt")

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	writer := newSkippedRowsWriter(path, logger)

	require.NoError(t, writer.WriteFrom(stringWriter("first line")))
	require.NoError(t, writer.WriteFrom(stringWriter("second line\n")))
	require.NoError(t, writer.WriteFrom(stringWriter("third line")))
	require.NoError(t, writer.Close())

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "first line\nsecond line\nthird line\n", string(content))

	// Reopen and append.
	writer = newSkippedRowsWriter(path, logger)
	require.NoError(t, writer.WriteFrom(stringWriter("fourth line")))
	require.NoError(t, writer.WriteFrom(stringWriter("fifth line")))
	require.NoError(t, writer.Close())

	content, err = os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimRight(string(content), "\n"), "\n")
	require.Equal(t, []string{"first line", "second line", "third line", "fourth line", "fifth line"}, lines)
}

func TestSkippedRowsWriter_CompressedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "skipped.zst")

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	writer := newSkippedRowsWriter(path, logger)

	require.NoError(t, writer.WriteFrom(stringWriter("first line")))
	require.NoError(t, writer.WriteFrom(stringWriter("second line\n")))
	require.NoError(t, writer.WriteFrom(stringWriter("third line")))
	require.NoError(t, writer.Close())

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	records := extractCompressedRecords(t, content)
	require.Equal(t, []string{"first line\n", "second line\n", "third line\n"}, records)
}

func TestSkippedRowsWriter_CompressedFileReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "skipped.zst")

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	{
		writer := newSkippedRowsWriter(path, logger)
		require.NoError(t, writer.WriteFrom(stringWriter("first line")))
		require.NoError(t, writer.WriteFrom(stringWriter("second line")))
		require.NoError(t, writer.Close())
	}

	// Reopen and append.
	{
		writer := newSkippedRowsWriter(path, logger)
		require.NoError(t, writer.WriteFrom(stringWriter("third line")))
		require.NoError(t, writer.WriteFrom(stringWriter("fourth line")))
		require.NoError(t, writer.Close())
	}

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	records := extractCompressedRecords(t, content)
	require.Equal(t, []string{"first line\n", "second line\n", "third line\n", "fourth line\n"}, records)
}

func TestSkippedRowsWriter_CompressedFileRepairCorrupted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "skipped.zst")

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	{
		writer := newSkippedRowsWriter(path, logger)
		require.NoError(t, writer.WriteFrom(stringWriter("first line")))
		require.NoError(t, writer.WriteFrom(stringWriter("second line")))
		require.NoError(t, writer.Close())
	}

	// Corrupt the file by appending garbage.
	{
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
		require.NoError(t, err)
		_, err = file.Write([]byte("garbage data that is not a valid zstd frame"))
		require.NoError(t, err)
		require.NoError(t, file.Close())
	}

	// Reopen - should repair.
	{
		writer := newSkippedRowsWriter(path, logger)
		require.NoError(t, writer.WriteFrom(stringWriter("third line")))
		require.NoError(t, writer.Close())
	}

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	records := extractCompressedRecords(t, content)
	require.Equal(t, []string{"first line\n", "second line\n", "third line\n"}, records)
}

func TestSkippedRowsWriter_CompressedFileRepairPartial(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "skipped.zst")

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	{
		writer := newSkippedRowsWriter(path, logger)
		require.NoError(t, writer.WriteFrom(stringWriter("first line")))
		require.NoError(t, writer.WriteFrom(stringWriter("second line")))
		require.NoError(t, writer.Close())
	}

	// Truncate the file to simulate partial write.
	stat, err := os.Stat(path)
	require.NoError(t, err)
	originalSize := stat.Size()

	file, err := os.OpenFile(path, os.O_WRONLY, 0644)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(originalSize-10))
	require.NoError(t, file.Close())

	// Reopen - should repair.
	{
		writer := newSkippedRowsWriter(path, logger)
		require.NoError(t, writer.WriteFrom(stringWriter("third line")))
		require.NoError(t, writer.Close())
	}

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	records := extractCompressedRecords(t, content)
	require.Equal(t, []string{"first line\n", "third line\n"}, records)
}

func TestSkippedRowsWriter_CompressedFileMultipleReopens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "skipped.zst")

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	expectedRecords := []string{}

	for i := 1; i <= 5; i++ {
		writer := newSkippedRowsWriter(path, logger)

		line := stringWriter("line " + string(rune('0'+i)))
		require.NoError(t, writer.WriteFrom(line))
		require.NoError(t, writer.Close())

		expectedRecords = append(expectedRecords, string(line)+"\n")
	}

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	records := extractCompressedRecords(t, content)
	require.Equal(t, expectedRecords, records)
}

func extractCompressedRecords(t *testing.T, content []byte) []string {
	t.Helper()

	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()

	var records []string
	pos := 0

	for pos < len(content) {
		if bytes.HasPrefix(content[pos:], zstdsync.SyncTagPrefix) {
			pos += zstdsync.SyncTagLength
			continue
		}

		nextSyncPos := bytes.Index(content[pos:], zstdsync.SyncTagPrefix)
		var frameEnd int
		if nextSyncPos == -1 {
			frameEnd = len(content)
		} else {
			frameEnd = pos + nextSyncPos
		}

		frame := content[pos:frameEnd]
		if len(frame) > 0 {
			decompressed, err := decoder.DecodeAll(frame, nil)
			require.NoError(t, err)
			records = append(records, string(decompressed))
		}

		pos = frameEnd
	}

	return records
}
