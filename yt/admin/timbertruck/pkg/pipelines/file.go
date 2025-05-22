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

func openLogFile(logger *slog.Logger, filepath string, filePosition FilePosition) (f LogFile, err error) {
	if strings.HasSuffix(filepath, ".zst") {
		return newCompressedFile(logger, filepath, filePosition)
	} else {
		return OpenFollowingFile(filepath, filePosition.LogicalOffset)
	}
}
