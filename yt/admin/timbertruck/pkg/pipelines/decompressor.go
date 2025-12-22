package pipelines

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/klauspost/compress/zstd"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/zstdsync"
)

type compressedFile struct {
	decompressor            *decompressor
	decompressedFrameReader *bytes.Reader
	filePosition            FilePosition
}

func newCompressedFile(logger *slog.Logger, filepath string, filePosition FilePosition) (*compressedFile, error) {
	decompressor, err := newDecompressor(logger, filepath, filePosition.BlockPhysicalOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to create decompressor: %w", err)
	}
	f := &compressedFile{
		decompressor:            decompressor,
		decompressedFrameReader: bytes.NewReader(nil),
		filePosition:            filePosition,
	}
	if filePosition.BlockPhysicalOffset == 0 && filePosition.InsideBlockOffset == 0 {
		return f, nil
	}
	decompressedFrameReader, err := f.decompressor.nextDecompressedFrame(context.Background(), false)
	if err != nil {
		var decodingError *frameDecodingError
		if errors.As(err, &decodingError) {
			logger.Warn("Detected log corruption; all data will be skipped until the next zstd sync tag", "error", err)
			return f, nil
		}
		if errors.Is(err, io.EOF) {
			return f, nil
		}
		return nil, fmt.Errorf("failed to read frame at offset %d: %w", filePosition.InsideBlockOffset, err)
	}
	f.decompressedFrameReader = decompressedFrameReader
	n, err := io.CopyN(io.Discard, f.decompressedFrameReader, filePosition.InsideBlockOffset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf(
			"failed to skip %d bytes of decompressed data (block physical offset: %d): %w",
			filePosition.InsideBlockOffset, filePosition.BlockPhysicalOffset, err,
		)
	}
	if n != filePosition.InsideBlockOffset {
		logger.Warn(
			"Detected log corruption: frame size mismatch",
			"file_position", filePosition, "expected_skip_bytes", filePosition.InsideBlockOffset, "actually_skipped_bytes", n,
		)
	}
	return f, nil

}

func (d *compressedFile) ReadContext(ctx context.Context, buf []byte) (read int, err error) {
	if d.decompressedFrameReader.Len() == 0 {
		r, err := d.decompressor.nextDecompressedFrame(ctx, true)
		if err != nil {
			return 0, fmt.Errorf("failed to get next decompressed frame: %w", err)
		}
		d.decompressedFrameReader = r
		d.filePosition.BlockPhysicalOffset = d.decompressor.filePosition()
		d.filePosition.InsideBlockOffset = 0
	}
	read, err = d.decompressedFrameReader.Read(buf)
	d.filePosition.LogicalOffset += int64(read)
	d.filePosition.InsideBlockOffset += int64(read)
	if errors.Is(err, io.EOF) {
		err = nil
	} else if err != nil {
		err = fmt.Errorf("compressedFile: failed to read decompressed data (read: %d): %w", read, err)
	}
	return
}

func (d *compressedFile) FilePosition() FilePosition {
	return d.filePosition
}

func (d *compressedFile) Stop() {
	d.decompressor.stop()
}

func (d *compressedFile) Close() error {
	return d.decompressor.close()
}

type decompressor struct {
	logger *slog.Logger

	frameIterator *zstdFrameIterator

	decoder             *zstd.Decoder
	decompressedDataBuf []byte
}

type frameDecodingError struct {
	err           error
	frameStartPos int64
	frameSize     int
}

func (e *frameDecodingError) Error() string {
	return fmt.Sprintf(
		"failed to decode frame (frame_start_pos: %d, frame_size: %d): %s", e.frameStartPos, e.frameSize, e.err.Error())
}

func newDecompressor(logger *slog.Logger, filepath string, filePosition int64) (*decompressor, error) {
	frameIterator, err := newZstdFrameIterator(logger, filepath, filePosition)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd frame iterator: %w", err)
	}
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderMaxMemory(100<<20), // 100 MiB
		zstd.WithDecoderConcurrency(1),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd.Decoder: %w", err)
	}
	return &decompressor{
		logger:              logger,
		frameIterator:       frameIterator,
		decoder:             decoder,
		decompressedDataBuf: make([]byte, 0, zstdsync.MaxZstdFrameLength*15),
	}, nil
}

func (d *decompressor) nextDecompressedFrame(ctx context.Context, skipBrokenFrames bool) (*bytes.Reader, error) {
	brokenFramesCount := 0
	for {
		rawFrame, err := d.frameIterator.next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get next raw frame: %w", err)
		}
		if len(rawFrame) == 0 {
			if !skipBrokenFrames {
				return nil, &frameDecodingError{err: errors.New("empty frame"), frameStartPos: d.frameIterator.filePosition()}
			}
			d.logger.Warn("Detected empty frame", "frame_start_position", d.frameIterator.filePosition())
			continue
		}
		d.decompressedDataBuf = d.decompressedDataBuf[:0]
		decompressedData, err := d.decoder.DecodeAll(rawFrame, d.decompressedDataBuf)
		if err != nil {
			if !skipBrokenFrames {
				return nil, &frameDecodingError{err: err, frameStartPos: d.frameIterator.filePosition(), frameSize: len(rawFrame)}
			}
			brokenFramesCount++
			if err := d.handleBrokenFrame(err, brokenFramesCount, len(rawFrame)); err != nil {
				return nil, &frameDecodingError{err: err, frameStartPos: d.frameIterator.filePosition(), frameSize: len(rawFrame)}
			}
			continue
		}
		return bytes.NewReader(decompressedData), nil
	}
}

func (d *decompressor) handleBrokenFrame(err error, brokenFramesCount int, frameSize int) error {
	if brokenFramesCount > 1 {
		d.logger.Warn(
			"Error decoding frame, it will be skipped", "error", err,
			"frame_start_position", d.frameIterator.filePosition(), "frame_size", frameSize,
			"broken_frames_count", brokenFramesCount)
		if brokenFramesCount > 3 {
			return fmt.Errorf("failed to decode %d frames in a row: %w", brokenFramesCount, err)
		}
		return nil
	}
	d.logger.Warn("Error decoding frame: log may have been truncated; will attempt to read again", "error", err,
		"frame_start_position", d.frameIterator.filePosition(), "frame_size", frameSize)

	if err := d.frameIterator.seekToFrameStart(); err != nil {
		return err
	}
	return nil
}

func (d *decompressor) filePosition() int64 {
	return d.frameIterator.filePosition()
}

func (d *decompressor) stop() {
	d.frameIterator.stop()
}

func (d *decompressor) close() error {
	d.decoder.Close()
	return d.frameIterator.close()
}

type zstdFrameIterator struct {
	logger *slog.Logger

	ff         *FollowingFile
	buffer     []byte
	end        int
	frameStart int
	frameSize  int

	filePos       int64
	fileFullyRead bool
}

func newZstdFrameIterator(logger *slog.Logger, filepath string, filePosition int64) (*zstdFrameIterator, error) {
	ff, err := OpenFollowingFile(filepath, filePosition)
	if err != nil {
		return nil, fmt.Errorf("failed to open following file: %w", err)
	}
	return &zstdFrameIterator{
		logger:  logger,
		ff:      ff,
		buffer:  make([]byte, 5*(zstdsync.MaxZstdFrameLength+zstdsync.SyncTagLength)),
		filePos: filePosition,
	}, nil
}

func (d *zstdFrameIterator) next(ctx context.Context) ([]byte, error) {
	d.advanceToNextFrame()
	for {
		tagPos := d.findFirstSyncTagPos()
		if tagPos >= 0 {
			d.frameSize = tagPos + zstdsync.SyncTagLength - d.frameStart
			return d.buffer[d.frameStart:tagPos], nil
		}
		if d.fileFullyRead {
			return nil, io.EOF
		}

		if d.end-d.frameStart > zstdsync.MaxZstdFrameLength+zstdsync.SyncTagLength {
			return nil, fmt.Errorf("no zstd sync tag in buffer of size %d > maxZstdFrameLength+zstdSyncTagLength (%d)", d.end-d.frameStart, zstdsync.MaxZstdFrameLength+zstdsync.SyncTagLength)
		}
		read, err := d.ff.ReadContext(ctx, d.buffer[d.end:])
		d.end += read
		if errors.Is(err, io.EOF) {
			d.fileFullyRead = true
		} else if err != nil {
			return nil, err
		}
	}
}

func (d *zstdFrameIterator) advanceToNextFrame() {
	d.frameStart += d.frameSize
	d.filePos += int64(d.frameSize)
	d.frameSize = 0
	if len(d.buffer)-d.frameStart < zstdsync.MaxZstdFrameLength+zstdsync.SyncTagLength {
		copy(d.buffer[0:], d.buffer[d.frameStart:d.end])
		d.end -= d.frameStart
		d.frameStart = 0
	}
}

func (d *zstdFrameIterator) findFirstSyncTagPos() int {
	firstSyncTagPos := -1
	begin := d.frameStart
	for {
		tagInd := bytes.Index(d.buffer[begin:d.end], zstdsync.SyncTagPrefix)
		if tagInd < 0 {
			break
		}

		tagPos := begin + tagInd
		if d.end-tagPos < zstdsync.SyncTagLength {
			break
		}

		tagOffset := zstdsync.ReadSyncTagOffset(d.buffer[tagPos:])
		expectedOffset := d.filePos + int64(tagPos-d.frameStart)
		if tagOffset == expectedOffset {
			firstSyncTagPos = tagPos
			break
		}

		begin = tagPos + 1
	}
	return firstSyncTagPos
}

func (d *zstdFrameIterator) seekToFrameStart() error {
	if err := d.ff.seekBack(d.end - d.frameStart); err != nil {
		return fmt.Errorf(
			"failed to seek back to start of frame (cur_pos: %d, offset: %d): %w", d.ff.filePosition, -(d.end - d.frameStart), err,
		)
	}
	d.end = d.frameStart
	d.frameSize = 0
	return nil
}

func (d *zstdFrameIterator) filePosition() int64 {
	return d.filePos
}

func (d *zstdFrameIterator) stop() {
	d.ff.Stop()
}

func (d *zstdFrameIterator) close() error {
	return d.ff.Close()
}
