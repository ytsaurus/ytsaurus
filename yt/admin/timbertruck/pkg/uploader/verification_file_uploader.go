package uploader

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go.ytsaurus.tech/yt/admin/timbertruck/internal/misc"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

const (
	defaultWriteLogFileToYTBufferSize = 10 * 1024 * 1024
	defaultMinLogFileAge              = 5 * time.Hour
	defaultMaxLogFileAge              = 10 * time.Hour
	defaultScanLogFilesInterval       = 6 * time.Hour

	pathTimeLayout = "2006-01-02T15:04:05"
)

// VerificationFileUploaderConfig contains configuration for the log files uploader.
type VerificationFileUploaderConfig struct {
	// YTCluster specifies the YT cluster to upload log files to.
	YTCluster string `yaml:"yt_cluster"`

	// YTLogFilesDir specifies the directory path in YT where log files will be stored.
	YTLogFilesDir string `yaml:"yt_log_files_dir"`

	// MinLogFileAge specifies the minimum time since last modification a log file must have before it can be uploaded.
	// Files modified more recently than this will be skipped to ensure they are no longer being written to.
	// Default: 5 hours.
	MinLogFileAge time.Duration `yaml:"min_log_file_age"`

	// MaxLogFileAge specifies the maximum time since last modification a log file can have to be eligible for upload.
	// Files modified longer ago than this will be skipped to avoid uploading very old files.
	// Default: 10 hours.
	MaxLogFileAge time.Duration `yaml:"max_log_file_age"`

	// ScanLogFilesInterval specifies how often the uploader scans for new log files to upload.
	// Default: 6 hours.
	ScanLogFilesInterval time.Duration `yaml:"scan_log_files_interval"`

	// Streams contains the list of log file stream configurations to monitor and upload.
	Streams []VerificationFileUploaderStreamConfig `yaml:"streams"`
}

// VerificationFileUploaderStreamConfig contains configuration for a single log file stream.
type VerificationFileUploaderStreamConfig struct {
	// FilePattern specifies the glob pattern to match log files for this stream.
	FilePattern string `yaml:"file_pattern"`

	// LogFormat specifies the format of the log files in this stream.
	LogFormat string `yaml:"log_format"`
}

type AuthConfig struct {
	TVMFn   yt.TVMFn
	YTToken string
}

type VerificationFileUploader interface {
	Run(context.Context) error
}

type verificationFileUploader struct {
	config     VerificationFileUploaderConfig
	authConfig AuthConfig

	logger *slog.Logger
}

func NewVerificationFileUploader(
	logger *slog.Logger,
	config VerificationFileUploaderConfig,
	authConfig AuthConfig,
) VerificationFileUploader {
	if config.MinLogFileAge == 0 {
		config.MinLogFileAge = defaultMinLogFileAge
	}
	if config.MaxLogFileAge == 0 {
		config.MaxLogFileAge = defaultMaxLogFileAge
	}
	if config.ScanLogFilesInterval == 0 {
		config.ScanLogFilesInterval = defaultScanLogFilesInterval
	}

	return &verificationFileUploader{
		config:     config,
		authConfig: authConfig,
		logger:     logger.With("component", "VerificationFileUploader"),
	}
}

func (u *verificationFileUploader) Run(ctx context.Context) error {
	ticker := time.NewTicker(u.config.ScanLogFilesInterval)
	defer ticker.Stop()

	u.logger.Info("Log files uploader started", "scan_interval", u.config.ScanLogFilesInterval)

	for {
		select {
		case <-ctx.Done():
			u.logger.Info("Log files uploader stopped")
			return ctx.Err()
		case <-ticker.C:
			u.uploadLogFiles(ctx)
		}
	}
}

func (u *verificationFileUploader) uploadLogFiles(ctx context.Context) {
	u.logger.Debug("Start searching for log files")
	now := time.Now()
	for _, streamConfig := range u.config.Streams {
		files, err := filepath.Glob(streamConfig.FilePattern)
		if err != nil {
			u.logger.Error("Failed to find log files", "pattern", streamConfig.FilePattern, "error", err)
			continue
		}

		var candidateFiles []string
		for _, file := range files {
			stat, err := os.Stat(file)
			if err != nil {
				u.logger.Warn("Failed to stat file", "file", file, "error", err)
				continue
			}
			age := now.Sub(stat.ModTime())
			if age >= u.config.MinLogFileAge && age <= u.config.MaxLogFileAge {
				candidateFiles = append(candidateFiles, file)
			}
		}

		if len(candidateFiles) == 0 {
			u.logger.Error("No candidate files found for stream", "pattern", streamConfig.FilePattern)
			continue
		}

		selectedFile := candidateFiles[rand.Intn(len(candidateFiles))]

		u.logger.Debug("Selected file to upload", "file", selectedFile, "total_candidates", len(candidateFiles))

		if err := u.uploadFileToYT(ctx, selectedFile, streamConfig); err != nil {
			u.logger.Warn("Failed to upload file to YT", "error", err, "file", selectedFile)
		} else {
			u.logger.Info("Successfully uploaded file to YT", "file", selectedFile, "yt_directory", u.config.YTLogFilesDir)
		}
	}
}

func (u *verificationFileUploader) uploadFileToYT(ctx context.Context, filePath string, streamConfig VerificationFileUploaderStreamConfig) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat log file: %w", err)
	}
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		err := fmt.Errorf("expected fileInfo.Sys() type is *syscall.Stat_t, actual: %T", fileInfo.Sys())
		u.logger.Error("Unexpected fileInfo.Sys() type", "error", err)
		return err
	}
	ino := int64(stat.Ino)

	fileName := filepath.Base(filePath)
	_, extensions, hasExtensions := strings.Cut(fileName, ".")
	if hasExtensions {
		extensions = "." + extensions
	}

	now := time.Now()

	ytFileName := fmt.Sprintf("%v_ino:%v%v", now.Format(pathTimeLayout), ino, extensions)
	logFilesDir := ypath.Path(u.config.YTLogFilesDir)
	ytPath := logFilesDir.Child(ytFileName)

	u.logger.Debug("Start file uploading", "filepath", filePath, "ytpath", ytPath)

	ytConfig := yt.Config{
		Proxy:  u.config.YTCluster,
		Logger: misc.NewArcadiaLevelCappingLogger(u.logger, "ytclient").Structured(),
	}
	if u.authConfig.TVMFn != nil {
		ytConfig.TVMFn = u.authConfig.TVMFn
	} else if u.authConfig.YTToken != "" {
		ytConfig.Token = u.authConfig.YTToken
	}

	yc, err := ythttp.NewClient(&ytConfig)
	if err != nil {
		return fmt.Errorf("failed to create YT client: %w", err)
	}
	defer yc.Stop()

	tx, err := yc.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = tx.Abort() }()

	expirationTime := now.Add(24 * time.Hour).UnixMilli()
	_, err = tx.CreateNode(ctx, ytPath, yt.NodeFile, &yt.CreateNodeOptions{
		Recursive: true,
		Attributes: map[string]any{
			"expiration_time": expirationTime,
			"inode":           ino,
			"filepath":        filePath,
			"log_format":      streamConfig.LogFormat,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create file node: %w", err)
	}

	writer, err := tx.WriteFile(ctx, ytPath, nil)
	if err != nil {
		return fmt.Errorf("failed to start write_file request: %w", err)
	}
	defer func() { _ = writer.Close() }()

	buffer := make([]byte, defaultWriteLogFileToYTBufferSize)

	_, err = io.CopyBuffer(writer, file, buffer)
	if err != nil {
		return fmt.Errorf("failed to copy file to YT file writer: %w", err)
	}

	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close file writer: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}

	return nil
}
