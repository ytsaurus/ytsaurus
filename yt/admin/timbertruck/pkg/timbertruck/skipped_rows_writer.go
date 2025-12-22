package timbertruck

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/zstdsync"
)

// recordWriter writes complete records with proper line separation.
type recordWriter interface {
	WriteRecord(data io.WriterTo) error
	io.Closer
}

// SkippedRowsWriter writes skipped rows to a file.
type SkippedRowsWriter struct {
	path        string
	logger      *slog.Logger
	mu          sync.Mutex
	initialized bool
	writer      recordWriter
}

func newSkippedRowsWriter(path string, logger *slog.Logger) *SkippedRowsWriter {
	return &SkippedRowsWriter{
		path:   path,
		logger: logger,
	}
}

func (w *SkippedRowsWriter) WriteFrom(data io.WriterTo) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.initialize(); err != nil {
		return err
	}

	return w.writer.WriteRecord(data)
}

func (w *SkippedRowsWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.initialized {
		return nil
	}

	return w.writer.Close()
}

func (w *SkippedRowsWriter) initialize() error {
	if w.initialized {
		return nil
	}

	dir := filepath.Dir(w.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %q: %w", dir, err)
	}

	file, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open skipped rows file %q: %w", w.path, err)
	}

	if strings.HasSuffix(w.path, ".zst") {
		w.writer, err = newCompressedFileWriter(file, w.logger, w.path)
		if err != nil {
			_ = file.Close()
			return err
		}
	} else {
		w.writer = &plainFileWriter{file: file}
	}

	w.initialized = true
	return nil
}

// plainFileWriter writes records to a plain file.
type plainFileWriter struct {
	file *os.File
}

func (w *plainFileWriter) WriteRecord(data io.WriterTo) error {
	return writeDataWithNewline(w.file, data)
}

func (w *plainFileWriter) Close() error {
	return w.file.Close()
}

// compressedFileWriter writes compressed frames with sync tags.
type compressedFileWriter struct {
	file    *os.File
	encoder *zstd.Encoder
	logger  *slog.Logger
}

func newCompressedFileWriter(file *os.File, logger *slog.Logger, path string) (*compressedFileWriter, error) {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	w := &compressedFileWriter{
		file:    file,
		encoder: encoder,
		logger:  logger,
	}

	if err := w.repair(path); err != nil {
		return nil, fmt.Errorf("failed to repair compressed file: %w", err)
	}

	return w, nil
}

func (w *compressedFileWriter) WriteRecord(data io.WriterTo) error {
	w.encoder.Reset(w.file)

	if err := writeDataWithNewline(w.encoder, data); err != nil {
		return err
	}

	if err := w.encoder.Close(); err != nil {
		return fmt.Errorf("failed to close encoder: %w", err)
	}

	// Get current file size for sync tag offset.
	stat, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	fileOffset := stat.Size()

	syncTag := zstdsync.WriteSyncTag(fileOffset)

	if _, err := w.file.Write(syncTag); err != nil {
		return fmt.Errorf("failed to write sync tag: %w", err)
	}

	return nil
}

func (w *compressedFileWriter) Close() error {
	if w.encoder != nil {
		_ = w.encoder.Close()
	}
	return w.file.Close()
}

// repair finds the last valid sync tag and truncates the file.
func (w *compressedFileWriter) repair(path string) error {
	stat, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := stat.Size()

	// Empty file is OK, no repair needed.
	if fileSize == 0 {
		return nil
	}

	// Constants from server code (yt/yt/core/logging/zstd_log_codec.cpp).
	const scanOverlap = zstdsync.SyncTagLength - 1
	const maxZstdFrameLength = zstdsync.MaxZstdFrameLength
	const tailScanLength = maxZstdFrameLength + 2*zstdsync.SyncTagLength

	bufSize := fileSize
	pos := max(bufSize-tailScanLength, 0)
	bufSize -= pos

	outputPosition := int64(0)

	for bufSize >= zstdsync.SyncTagLength {
		buffer := make([]byte, bufSize)

		n, err := w.file.ReadAt(buffer, pos)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read file at offset %d: %w", pos, err)
		}
		buffer = buffer[:n]

		if offset := findLastZstdSyncTagOffset(buffer, pos); offset != nil {
			outputPosition = *offset + zstdsync.SyncTagLength
			break
		}

		newPos := max(pos-tailScanLength, 0)
		bufSize = max(pos+scanOverlap-newPos, 0)
		pos = newPos
	}

	if err := w.file.Truncate(outputPosition); err != nil {
		return fmt.Errorf("failed to truncate file to %d: %w", outputPosition, err)
	}

	if outputPosition != fileSize {
		w.logger.Info("Repaired compressed skipped rows file",
			"path", path,
			"original_size", fileSize,
			"truncated_size", outputPosition)
	}

	return nil
}

// findLastZstdSyncTagOffset finds the last valid sync tag in buffer.
// bufferStartOffset is the file offset where buffer starts.
func findLastZstdSyncTagOffset(buffer []byte, bufferStartOffset int64) *int64 {
	var lastValid *int64

	for i := 0; i <= len(buffer)-zstdsync.SyncTagLength; {
		// Search for sync tag prefix.
		idx := bytes.Index(buffer[i:], zstdsync.SyncTagPrefix)
		if idx == -1 {
			break
		}

		pos := i + idx
		if pos+zstdsync.SyncTagLength > len(buffer) {
			break
		}

		// Read offset from sync tag.
		tagOffset := binary.LittleEndian.Uint64(buffer[pos+len(zstdsync.SyncTagPrefix):])
		expectedOffset := uint64(bufferStartOffset + int64(pos))

		// Validate: offset in tag should match actual position.
		if tagOffset == expectedOffset {
			offset := int64(tagOffset)
			lastValid = &offset
		}

		i = pos + len(zstdsync.SyncTagPrefix)
	}

	return lastValid
}

func writeDataWithNewline(w io.Writer, data io.WriterTo) error {
	wrapper := &lastByteTracker{w: w}

	if _, err := data.WriteTo(wrapper); err != nil {
		return err
	}

	// Ensure line ends with newline for proper separation.
	if wrapper.lastByte != '\n' {
		if _, err := w.Write([]byte{'\n'}); err != nil {
			return err
		}
	}

	return nil
}

// lastByteTracker wraps io.Writer and tracks the last byte written.
type lastByteTracker struct {
	w        io.Writer
	lastByte byte
}

func (t *lastByteTracker) Write(p []byte) (n int, err error) {
	if len(p) > 0 {
		t.lastByte = p[len(p)-1]
	}
	return t.w.Write(p)
}
