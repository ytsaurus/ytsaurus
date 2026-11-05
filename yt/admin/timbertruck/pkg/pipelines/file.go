package pipelines

import (
	"context"
	"log/slog"
	"strings"
)

type LogFile interface {
	ReadContext(ctx context.Context, buf []byte) (read int, err error)
	FilePosition() FilePosition
	Stop()
	Close() error
}

func IsZstdPath(filepath string) bool {
	return strings.HasSuffix(filepath, ".zst") || strings.HasSuffix(filepath, ".zstd")
}

func openLogFile(logger *slog.Logger, filepath string, filePosition FilePosition) (f LogFile, err error) {
	if IsZstdPath(filepath) {
		return newCompressedFile(logger, filepath, filePosition)
	} else {
		return OpenFollowingFile(filepath, filePosition.LogicalOffset)
	}
}
