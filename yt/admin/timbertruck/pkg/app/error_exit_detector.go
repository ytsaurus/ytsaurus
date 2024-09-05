package app

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
)

func errorExitDetector(logger *slog.Logger, workDir string) (cleanExit bool, closeF func(), err error) {
	cleanExit = true
	errorExitMarkerPath := path.Join(workDir, "error_exit_marker.txt")
	var f *os.File
	f, err = os.OpenFile(errorExitMarkerPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		err = fmt.Errorf("failed to open error exit marker: %w", err)
		return
	}

	errorMsg, err := io.ReadAll(f)
	if err != nil {
		err = fmt.Errorf("error reading error exit marker: %w", err)
		return
	}
	if len(errorMsg) > 0 {
		errorMsg = bytes.SplitN(errorMsg, []byte("\n"), 2)[0]
		logger.Error("Error exit detected", "error", string(errorMsg))
		cleanExit = false
	}
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		err = fmt.Errorf("error seeking error exit marker: %w", err)
		return
	}

	err = f.Truncate(0)
	if err != nil {
		err = fmt.Errorf("error truncating error exit marker: %w", err)
		return
	}

	_, err = f.WriteString("unknown error")
	if err != nil {
		err = fmt.Errorf("cannot write to error exit marker: %w", err)
		return
	}

	closeF = func() {
		err := os.Remove(errorExitMarkerPath)
		if err != nil {
			logger.Error("Failed to remove error exit marker", "error", err)
		} else {
			logger.Info("Error exit marker removed successfully")
		}
	}

	return
}
