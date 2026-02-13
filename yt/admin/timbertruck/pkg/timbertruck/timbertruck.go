package timbertruck

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"

	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
)

const (
	stateFile = "log_pusher_state.sqlite"

	createPipelineMaxBackoff         = 5 * time.Minute
	createPipelineMaxBackoffTestMode = 1 * time.Second

	minSkippedRowsCleanupInterval = 1 * time.Minute
)

type Config struct {
	// A place where timbertruck keeps it's file to store info about sent data.
	// It's also the directory where it keeps hardlinks for unprocessed files.
	WorkDir string `yaml:"work_dir"`

	// How much time timbertruck waits for new data after log file rotation
	LogCompletionDelay *time.Duration `yaml:"log_completion_delay"`

	// How much time timbertruck waits before removing completed tasks from datastore
	CompletedTaskRetainPeriod *time.Duration `yaml:"completed_task_retain_period"`

	// The period with which timbertruck scans the datastore for active tasks.
	//
	// Default value is 30s.
	ListActiveTasksPeriod *time.Duration `yaml:"list_acive_tasks_period"`

	// DeprecatedStreams lists stream names that are explicitly deprecated.
	// On start Timbertruck will:
	//  - Complete active tasks for these streams if found in the datastore and not configured.
	//  - Remove their working directories under WorkDir (only if a "staging" subdirectory exists).
	// If a stream listed here is also present in the current configuration, Timbertruck will panic.
	DeprecatedStreams []string `yaml:"deprecated_streams"`
}

type TimberTruck struct {
	config    Config
	logger    *slog.Logger
	fsWatcher *FsWatcher
	datastore *Datastore

	metrics metrics.Registry

	handlers []streamHandler
}

func NewTimberTruck(config Config, logger *slog.Logger, metrics metrics.Registry) (result *TimberTruck, err error) {
	logPusher := TimberTruck{
		config:  config,
		logger:  logger,
		metrics: metrics,
	}

	if logPusher.config.LogCompletionDelay == nil {
		defaultDelay := 5 * time.Second
		logPusher.config.LogCompletionDelay = &defaultDelay
	}
	if logPusher.config.CompletedTaskRetainPeriod == nil {
		defaultPeriod := 7 * 24 * time.Hour
		logPusher.config.CompletedTaskRetainPeriod = &defaultPeriod
	}
	if logPusher.config.ListActiveTasksPeriod == nil {
		defaultPeriod := 30 * time.Second
		logPusher.config.ListActiveTasksPeriod = &defaultPeriod
	}

	logPusher.fsWatcher, err = NewFsWatcher(logger.With("component", "FsWatcher"))
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			logPusher.fsWatcher.Close()
		}
	}()

	logPusher.datastore, err = NewDatastore(logger.With("component", "Datastore"), path.Join(config.WorkDir, stateFile))
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			err2 := logPusher.datastore.Close()
			if err2 != nil {
				logger.Warn("Error closing datastore", "error", err2)
			}
		}
	}()

	result = &logPusher
	return
}

type TaskController interface {
	// This method is to be used by pipelines to notify last sent
	NotifyProgress(pipelines.FilePosition)
	Logger() *slog.Logger
	OnSkippedRow(data io.WriterTo, info pipelines.SkippedRowInfo)
}

type TaskArgs struct {
	Context    context.Context
	Path       string
	Position   pipelines.FilePosition
	Controller TaskController
}

type NewPipelineFunc func(task TaskArgs) (*pipelines.Pipeline, error)

type StreamConfig struct {
	// Name of the stream. Can contain [-_A-Za-z0-9].
	// This name identifies stream inside timbertruck storage
	// so renaming existing stream will result in progress loss.
	Name string `yaml:"name"`

	// TODO: should be named SourceFile
	// Path to main log file
	LogFile string `yaml:"log_file"`

	// How many active tasks can hold timbertruck for this stream
	MaxActiveTaskCount *int `yaml:"max_active_task_count"`

	// SkippedRowsMaxAge defines how long to keep skipped rows files.
	// Files older than this duration will be automatically deleted.
	//
	// If not set, defaults to 168h (7 days).
	SkippedRowsMaxAge time.Duration
}

func (tt *TimberTruck) AddStream(config StreamConfig, newPipeline NewPipelineFunc) {
	handler := streamHandler{
		timberTruck: tt,
		logger: tt.logger.With(
			"stream", config.Name,
			"file", path.Base(config.LogFile),
		),
		config:             config,
		newPipelineFunc:    newPipeline,
		skippedRowsMetrics: newSkippedRowsMetrics(tt.logger, config.Name, tt.metrics),
		haveTasks:          make(chan struct{}),
	}
	if handler.config.MaxActiveTaskCount == nil {
		defaultMaxActiveTaskCount := 100
		handler.config.MaxActiveTaskCount = &defaultMaxActiveTaskCount
	}
	tt.handlers = append(tt.handlers, handler)
	handler.logger.Info("Pipeline added")
}

// AddAndRunStream adds a new stream and launches its handler immediately.
// It can be called while Serve(ctx) is running.
//
// Note: this method is not concurrent safe.
func (tt *TimberTruck) AddAndRunStream(ctx context.Context, config StreamConfig, newPipeline NewPipelineFunc) error {
	tt.AddStream(config, newPipeline)
	return tt.initializeStream(ctx, &tt.handlers[len(tt.handlers)-1])
}

func (tt *TimberTruck) Serve(ctx context.Context) error {
	deprecatedStreams := tt.deprecatedStreams()

	activeTasks, err := tt.datastore.ListActiveTasks()
	if err != nil {
		panic(fmt.Sprintf("unexpected error ListActiveTasks(): %v", err))
	}
	for _, task := range activeTasks {
		if _, isDeprecated := deprecatedStreams[task.StreamName]; isDeprecated {
			completeErr := tt.datastore.CompleteTask(task.StagedPath, time.Now(), fmt.Errorf("stream is deprecated: %s", task.StreamName))
			if completeErr != nil {
				panic(fmt.Sprintf("unexpected error CompleteTask(%s): %v", task.StagedPath, completeErr))
			}
			tt.logger.Info("Task completed for a deprecated stream", "stagedpath", task.StagedPath, "stream", task.StreamName)
		}

		_, err := os.Stat(task.StagedPath)
		if err != nil {
			tt.logger.Warn("Unavailable file for active task, task is completed with error", "error", err, "stagedpath", task.StagedPath)
			err = tt.datastore.CompleteTask(task.StagedPath, time.Now(), fmt.Errorf("file unavailable: %w", err))
			if err != nil {
				panic(fmt.Sprintf("unexpected error CompleteTask(%v): %v", task.StagedPath, err))
			}
		}
	}

	tt.cleanupDeprecatedStreamDirs()

	for i := range tt.handlers {
		if err := tt.initializeStream(ctx, &tt.handlers[i]); err != nil {
			tt.logger.Error("Error initializing stream", "error", err, "stream", tt.handlers[i].config.Name)
		}
	}

	tt.launchMetricsProc(ctx)

	tt.logger.Info("Serving")

	return tt.fsWatcher.Run(ctx)
}

// BoundActiveTasks marks all active tasks for specified streams as bounded,
// using current time plus LogCompletionDelay as BoundTime.
func (tt *TimberTruck) BoundActiveTasks(streamNames []string, boundTime time.Time) {
	if len(streamNames) == 0 {
		return
	}

	streamSet := make(map[string]struct{})
	for _, name := range streamNames {
		streamSet[name] = struct{}{}
	}

	for i := range tt.handlers {
		stream := tt.handlers[i].config.Name
		if _, ok := streamSet[stream]; !ok {
			continue
		}
		if err := tt.datastore.BoundAllTasks(stream, boundTime); err != nil {
			tt.logger.Warn("Failed to bound all tasks for stream", "stream", stream, "error", err)
		} else {
			tt.logger.Info("Bound all active tasks for stream", "stream", stream, "bound_time", boundTime)
		}
	}
}

// ListActiveTasks returns all active tasks from the datastore.
func (tt *TimberTruck) ListActiveTasks() ([]Task, error) {
	return tt.datastore.ListActiveTasks()
}

// deprecatedStreams returns a set of deprecated stream names.
// Panics if any deprecated stream is present in the current configuration.
func (tt *TimberTruck) deprecatedStreams() map[string]struct{} {
	result := make(map[string]struct{})
	configured := make(map[string]struct{})
	for i := range tt.handlers {
		configured[tt.handlers[i].config.Name] = struct{}{}
	}
	for _, name := range tt.config.DeprecatedStreams {
		if _, ok := configured[name]; ok {
			panic(fmt.Sprintf("stream %q is marked as deprecated but present in config", name))
		}
		result[name] = struct{}{}
	}
	return result
}

func (tt *TimberTruck) initializeStream(ctx context.Context, handler *streamHandler) error {
	err := handler.initStagingDir()
	if err != nil {
		return fmt.Errorf("failed to initialize staging directory: %w", err)
	}
	err = handler.initSkippedRowsDir()
	if err != nil {
		return fmt.Errorf("failed to initialize skipped rows directory: %w", err)
	}
	fileEventChan := make(chan FileEvent, 1000)
	err = tt.fsWatcher.AddLogPath(handler.config.LogFile, fileEventChan)
	if err != nil {
		close(fileEventChan)
		return fmt.Errorf("failed to watch stream path: %w", err)
	}
	go handler.ProcessFileEventQueue(ctx, fileEventChan)
	go handler.ProcessTaskQueue(ctx)
	go handler.launchSkippedRowsCleanup(ctx)

	if _, err := os.Stat(handler.config.LogFile); err != nil {
		if err != ErrNotFound {
			tt.logger.Warn("Cannot stat log path", "path", handler.config.LogFile, "error", err)
		} else {
			fileEventChan <- FileRemoveOrRenameEvent
		}
	} else {
		fileEventChan <- FileCreateEvent
	}
	handler.logger.Info("Stream initialized ok")
	return nil
}

// cleanupDeprecatedStreamDirs removes per-stream working directories for streams explicitly listed as deprecated.
// It only touches directories that look like stream directories (i.e., contain a "staging" subdirectory).
func (tt *TimberTruck) cleanupDeprecatedStreamDirs() {
	deprecatedStreams := tt.deprecatedStreams()
	for name := range deprecatedStreams {
		stagingDirPath := stagingDir(tt.config.WorkDir, name)
		fi, err := os.Stat(stagingDirPath)
		if err != nil {
			tt.logger.Warn("Failed to stat deprecated stream staging directory", "stream", name, "dir", stagingDirPath, "error", err)
			continue
		}
		if !fi.IsDir() {
			continue
		}
		dirPath := path.Join(tt.config.WorkDir, name)
		if err := os.RemoveAll(dirPath); err != nil {
			tt.logger.Warn("Failed to remove deprecated stream directory", "stream", name, "dir", dirPath, "error", err)
		} else {
			tt.logger.Info("Removed deprecated stream directory", "stream", name, "dir", dirPath)
		}
	}
}

func (tt *TimberTruck) launchMetricsProc(ctx context.Context) {
	if tt.metrics == nil {
		return
	}

	streamNames := []string{}
	for i := range tt.handlers {
		streamNames = append(streamNames, tt.handlers[i].config.Name)
	}

	activeTaskCounter := newActiveTaskCounter(tt.logger, streamNames, tt.metrics, tt.datastore)

	go func() {
		ticker := time.NewTicker(*tt.config.ListActiveTasksPeriod)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				activeTaskCounter.Do()
			}
		}
	}()

}

type streamHandler struct {
	timberTruck *TimberTruck
	logger      *slog.Logger

	config          StreamConfig
	newPipelineFunc NewPipelineFunc

	skippedRowsMetrics skippedRowsMetrics
	haveTasks          chan struct{}
}

func (h *streamHandler) getExtensions() string {
	basePath := path.Base(h.config.LogFile)
	_, extensions, hasExtensions := strings.Cut(basePath, ".")
	if hasExtensions {
		return "." + extensions
	}
	return ""
}

func (h *streamHandler) stagingDir() string {
	return stagingDir(h.timberTruck.config.WorkDir, h.config.Name)
}

func stagingDir(workDir string, streamName string) string {
	return path.Join(workDir, streamName, "staging")
}

func (h *streamHandler) skippedRowsDir() string {
	return skippedRowsDir(h.timberTruck.config.WorkDir, h.config.Name)
}

func skippedRowsDir(workDir string, streamName string) string {
	return path.Join(workDir, streamName, "skipped_rows")
}

var dateRegexp = regexp.MustCompile(`^[[:digit:]]{4}-[[:digit:]]{2}-[[:digit:]]{2}T[[:digit:]]{2}:[[:digit:]]{2}:[[:digit:]]{2}_`)
var inoRegexp = regexp.MustCompile(`^ino:[[:digit:]]+`)

const pathTimeLayout = "2006-01-02T15:04:05"

func parseStagedPath(filePath string, ino int64) (creationTime time.Time, isFinalPath bool) {
	filePath = path.Base(filePath)
	datePrefix := dateRegexp.FindString(filePath)
	if datePrefix == "" {
		return
	}

	creationTime, err := time.Parse(pathTimeLayout, strings.TrimSuffix(datePrefix, "_"))
	if err != nil {
		// unprobable but possible situation, date is malformed 9999-99-99
		return
	}

	inoInfix := inoRegexp.FindString(filePath[len(datePrefix):])
	if inoInfix == "" {
		return
	}

	inoString := strings.TrimPrefix(inoInfix, "ino:")
	parsedIno, err := strconv.ParseInt(inoString, 10, 64)
	if err != nil {
		return
	}

	isFinalPath = (parsedIno == ino)
	return
}

func tempStagedName(now time.Time, uuid uuid.UUID, extensions string) string {
	return fmt.Sprintf("%v_%v%v", now.Format(pathTimeLayout), uuid.String(), extensions)
}

func (h *streamHandler) tempStagedPath(now time.Time, uuid uuid.UUID) string {
	resultName := tempStagedName(now, uuid, h.getExtensions())
	return path.Join(h.stagingDir(), resultName)
}

func finalStagedName(now time.Time, ino int64, extensions string) string {
	return fmt.Sprintf("%v_ino:%v%v", now.Format(pathTimeLayout), ino, extensions)
}

func (h *streamHandler) finalStagedPath(now time.Time, ino int64) string {
	resultName := finalStagedName(now, ino, h.getExtensions())
	return path.Join(h.stagingDir(), resultName)
}

func (h *streamHandler) ProcessFileEventQueue(ctx context.Context, events <-chan FileEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-events:
			if !ok {
				h.logger.Info("File event channel closed")
				return
			}
			switch e {
			case FileCreateEvent:
				h.logger.Info("Received file create event")
				h.handleCreate()
				h.resetActiveTask()
			case FileRemoveOrRenameEvent:
				h.logger.Info("Received file remove or rename event")
				h.resetActiveTask()
			}
		}
	}
}

func (h *streamHandler) initSkippedRowsDir() error {
	skippedRowsDir := h.skippedRowsDir()
	err := os.MkdirAll(skippedRowsDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create skipped_rows directory %q: %w", skippedRowsDir, err)
	}
	return nil
}

func (h *streamHandler) initStagingDir() (err error) {
	stagingDir := h.stagingDir()
	err = os.MkdirAll(stagingDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create staging directory %q: %w", stagingDir, err)
	}

	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		return fmt.Errorf("failed to read staging directory %q: %w", stagingDir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		curPath := path.Join(stagingDir, entry.Name())
		h.handleStagedPath(curPath)
	}

	return
}

func (h *streamHandler) handleCreate() {
	stagingDir := h.stagingDir()

	// We could create this dir once at start time,
	// but it would be more reliable to create it each time
	err := os.MkdirAll(stagingDir, 0755)
	if err != nil {
		h.logger.Warn("Failed to create staging directory", "error", err, "directory", stagingDir)
		return
	}
	now := time.Now()
	linkedPath := h.tempStagedPath(now, uuid.Must(uuid.NewRandom()))

	err = os.Link(h.config.LogFile, linkedPath)
	if err != nil {
		h.logger.Warn("Failed to link log file", "error", err, "filepath", h.config.LogFile, "stagingFilepath", linkedPath)
		return
	}
	h.logger.Info("Linked task to temporary name", "tmpname", linkedPath)
	h.handleStagedPath(linkedPath)
}

func (h *streamHandler) handleStagedPath(stagedPath string) {
	removeStagedPath := func(removeReason string) {
		err := os.Remove(stagedPath)
		if err != nil {
			h.logger.Warn("Failed to remove task file", "removeReason", removeReason, "stagedpath", stagedPath, "error", err)
		} else {
			h.logger.Info("Removed task file", "removeReason", removeReason, "stagedPath", stagedPath)
		}
	}

	var err error
	var stat syscall.Stat_t
	logger := h.logger.With("path", path.Base(stagedPath))
	err = syscall.Stat(stagedPath, &stat)
	if err != nil {
		logger.Warn("Failed to stat staging file", "error", err)
		return
	}

	ino := int64(stat.Ino)
	creationTime, isFinalPath := parseStagedPath(stagedPath, ino)

	if !isFinalPath {
		if creationTime.IsZero() {
			creationTime = time.Now()
		}
		oldStagedPath := stagedPath
		stagedPath = h.finalStagedPath(creationTime, ino)
		err = os.Rename(oldStagedPath, stagedPath)
		if err != nil {
			h.logger.Warn("Failed to move staged file", "tempstagedpath", oldStagedPath, "error", err)
			return
		}
		h.logger.Info("Renamed to final staged name", "tempstagedpath", oldStagedPath, "stagedpath", stagedPath)
	}

	task, err := h.timberTruck.datastore.ActiveTaskByIno(ino, h.config.Name)
	if err == nil && task.CompletionTime.IsZero() { // NO ERROR, we already have this task
		if task.StagedPath != stagedPath {
			removeStagedPath("duplicate task")
		}
		h.logger.Info("Task already exists", "stagedpath", stagedPath, "originalPath", task.StagedPath)
		return
	} else if err != ErrNotFound {
		panic(fmt.Sprintf("unexpected error ActiveTaskByIno(%v, %v): %v", ino, h.config.Name, err))
	}

	task, err = h.timberTruck.datastore.TaskByPath(stagedPath)
	if err == nil { // NO ERROR, we already have this task
		if !task.CompletionTime.IsZero() {
			removeStagedPath("completed task")
		}
		h.logger.Info("Task already exists", "stagedpath", stagedPath, "originalpath", task.StagedPath)
		return
	} else if err != ErrNotFound {
		panic(fmt.Sprintf("unexpected error TaskByPath(%v): %v", stagedPath, err))
	}

	task = Task{
		StreamName:   h.config.Name,
		INode:        ino,
		StagedPath:   stagedPath,
		CreationTime: creationTime,
	}
	warns, err := h.timberTruck.datastore.AddTask(&task, *h.config.MaxActiveTaskCount)
	if errors.Is(err, ErrTaskLimitExceeded) {
		removeStagedPath("task limit exceeded")
		h.logger.Error("Task was not added", "error", err)
		return
	} else if err != nil {
		panic(fmt.Sprintf("unexpected error AddTask(%v): %v", task, err))
	}

	for _, warn := range warns {
		h.logger.Error("Warning generated while adding task", "error", warn)
	}

	go func() {
		h.haveTasks <- struct{}{}
	}()
	h.logger.Info("Added task", "stagedpath", stagedPath, "ino", ino, "ctime", creationTime)
}

func (h *streamHandler) resetActiveTask() {
	var err error
	var stat syscall.Stat_t
	err = syscall.Stat(h.config.LogFile, &stat)

	boundTime := time.Now().Add(*h.timberTruck.config.LogCompletionDelay)

	if errors.Is(err, syscall.ENOENT) {
		err := h.timberTruck.datastore.BoundAllTasks(h.config.Name, boundTime)
		if err != nil {
			h.logger.Error("Failed to bound events in datastore", "error", err)
			return
		}
		h.logger.Info("All active writers logs were bound")
		return
	} else if err != nil {
		h.logger.Error("Failed to stat staging file", "error", err)
		return
	}

	ino := int64(stat.Ino)
	err = h.timberTruck.datastore.ResetUnboundTask(h.config.Name, ino, boundTime)
	if err != nil {
		h.logger.Error("ResetUnboundTask failed", "error", err)
		return
	}
	h.logger.Info("All tasks except active are bound", "active_inode", ino)
}

func (h *streamHandler) createTaskQueue(ctx context.Context) (taskChan chan Task) {
	taskChan = make(chan Task)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
	loop:
		for {
			if ctx.Err() != nil {
				return
			}
			task, err := h.timberTruck.datastore.PeekNextTask(h.config.Name)
			if err != nil {
				if !errors.Is(err, ErrNotFound) {
					h.logger.Error("Error peeking new task", "error", err)
				}
				select {
				case <-ctx.Done():
					break loop
				case <-ticker.C:
				case <-h.haveTasks:
				}
				continue
			}

			err = h.timberTruck.datastore.SetPeeked(h.config.Name, task.StagedPath)
			if err != nil {
				panic(fmt.Sprintf("unexpected error SetPeeked(%v, %v): %v", h.config.Name, task.StagedPath, err))
			}

			select {
			case <-ctx.Done():
				break loop
			case taskChan <- task:
			}
		}
		close(taskChan)
	}()

	return
}

func (h *streamHandler) ProcessTaskQueue(ctx context.Context) {
	taskChan := h.createTaskQueue(ctx)
	for task := range taskChan {
		h.logger.Info("Peeked task", "stagedpath", task.StagedPath)

		taskController := taskController{
			path:               task.StagedPath,
			datastore:          h.timberTruck.datastore,
			logger:             h.logger.With("component", "Pipeline", "stagedpath", task.StagedPath),
			skippedRowsWriter:  newSkippedRowsWriter(path.Join(h.skippedRowsDir(), path.Base(task.StagedPath)), h.logger),
			skippedRowsMetrics: h.skippedRowsMetrics,
		}

		var p *pipelines.Pipeline
		var err error
		backoff := time.Second
		maxBackoff := getCreatePipelineMaxBackoff()
		for retry := 1; ; retry += 1 {
			p, err = h.newPipelineFunc(TaskArgs{
				Context:    ctx,
				Path:       task.StagedPath,
				Position:   task.EndPosition,
				Controller: &taskController,
			})
			if err == nil { // no error
				break
			} else {
				level := slog.LevelWarn
				if backoff == maxBackoff {
					level = slog.LevelError
				}
				h.logger.Log(context.Background(), level, "Error creating pipeline", "error", err,
					slog.Duration("backoff", backoff), slog.Int("retry", retry),
				)

				time.Sleep(backoff)
				backoff *= 2

				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}

		h.logger.Info("Pipeline created", "stagedpath", task.StagedPath)
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
		loop:
			for {
				select {
				case now := <-ticker.C:
					updatedTask, err := h.timberTruck.datastore.TaskByPath(task.StagedPath)
					if err != nil {
						h.logger.Error("Cannot get task from datastore")
						break loop
					}
					if !updatedTask.BoundTime.IsZero() && now.After(updatedTask.BoundTime) {
						break loop
					}
				case <-ctx.Done():
					// Context is done, but task is not bound yet. We should not call NotifyComplete.
					return
				}
			}
			p.NotifyComplete()
		}()
		err = p.Run(ctx)
		if taskController.skippedRowsWriter != nil {
			_ = taskController.skippedRowsWriter.Close()
		}
		if err != nil {
			h.logger.Error("Pipeline error", "error", err)
		} else {
			h.completeTask(task, nil)
			if err := os.Remove(task.StagedPath); err != nil {
				h.logger.Warn("Failed to remove staged file", "error", err, "stagedpath", task.StagedPath)
			}
			h.logger.Info("Pipeline completed", "stagedpath", task.StagedPath, "ino", task.INode)
		}
	}
}

func (h *streamHandler) completeTask(task Task, taskError error) {
	b := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
		backoff.WithMaxInterval(20*time.Second),
		backoff.WithMaxElapsedTime(0), // Must never stop.
	)
	for {
		err := h.timberTruck.datastore.CompleteTask(task.StagedPath, time.Now(), taskError)
		if err == nil {
			break
		}
		next := b.NextBackOff()
		if next == b.Stop {
			panic(fmt.Sprintf("CompleteTask failed, retries exhausted: %v", err))
		}
		h.timberTruck.logger.Warn("CompleteTask failed, will retry", "error", err, "stagedpath", task.StagedPath, slog.Duration("backoff", next))
		time.Sleep(next)
	}
	err := h.timberTruck.datastore.CleanupOldCompletedTasks(time.Now().Add(-*h.timberTruck.config.CompletedTaskRetainPeriod))
	if err != nil {
		h.timberTruck.logger.Warn("Failed to cleanup old completed tasks", "error", err)
	}
}

func (h *streamHandler) cleanupOldSkippedRowsFiles() error {
	skippedRowsDir := h.skippedRowsDir()
	entries, err := os.ReadDir(skippedRowsDir)
	if err != nil {
		return fmt.Errorf("failed to read skipped_rows directory: %w", err)
	}

	cutoffTime := time.Now().Add(-h.config.SkippedRowsMaxAge)
	var lastError error

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filePath := path.Join(skippedRowsDir, entry.Name())

		info, err := entry.Info()
		if err != nil {
			h.logger.Warn("Failed to get file info", "file", filePath, "error", err)
			lastError = fmt.Errorf("failed to get file info for %q: %w", filePath, err)
			continue
		}

		if info.ModTime().Before(cutoffTime) {
			if err := os.Remove(filePath); err != nil {
				h.logger.Warn("Failed to remove old skipped rows file", "file", filePath, "error", err)
				lastError = fmt.Errorf("failed to remove %q: %w", filePath, err)
			} else {
				h.logger.Info("Removed old skipped rows file",
					"file", filePath,
					"age", time.Since(info.ModTime()),
					"size_bytes", info.Size())
			}
		}
	}

	return lastError
}

func (h *streamHandler) launchSkippedRowsCleanup(ctx context.Context) {
	cleanupInterval := h.config.SkippedRowsMaxAge / 10
	if cleanupInterval < minSkippedRowsCleanupInterval {
		cleanupInterval = minSkippedRowsCleanupInterval
	}

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	consecutiveFailures := 0
	if err := h.cleanupOldSkippedRowsFiles(); err != nil {
		consecutiveFailures++
		h.logger.Warn("Failed to cleanup old skipped rows files", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := h.cleanupOldSkippedRowsFiles(); err != nil {
				consecutiveFailures++
				logLevel := slog.LevelWarn
				if consecutiveFailures >= 3 {
					logLevel = slog.LevelError
				}
				h.logger.Log(ctx, logLevel, "Failed to cleanup old skipped rows files", "error", err, "consecutive_failures", consecutiveFailures)
			} else {
				consecutiveFailures = 0
			}
		}
	}
}

type taskController struct {
	path               string
	datastore          *Datastore
	logger             *slog.Logger
	skippedRowsWriter  *SkippedRowsWriter
	skippedRowsMetrics skippedRowsMetrics
}

func (c *taskController) NotifyProgress(pos pipelines.FilePosition) {
	c.logger.Debug("Update end position", "progress", pos)
	b := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
		backoff.WithMaxInterval(5*time.Second),
		backoff.WithMaxElapsedTime(0), // Must never stop.
	)
	for {
		err := c.datastore.UpdateEndPosition(c.path, pos)
		if err == nil {
			break
		}
		next := b.NextBackOff()
		if next == b.Stop {
			panic(fmt.Sprintf("NotifyProgress failed, retries exhausted: %v", err))
		}
		c.logger.Warn("Failed to notify progress, will retry", "error", err, slog.Duration("backoff", next))
		time.Sleep(next)
	}
	c.logger.Debug("Task progress", "progress", pos)
}

func (c *taskController) Logger() *slog.Logger {
	return c.logger
}

func (c *taskController) OnSkippedRow(data io.WriterTo, info pipelines.SkippedRowInfo) {
	if c.skippedRowsWriter != nil {
		if err := c.skippedRowsWriter.WriteFrom(data); err != nil {
			c.logger.Warn("Failed to write skipped row", "error", err)
		}
	}
	c.skippedRowsMetrics.Inc(info.Reason)

	attrs := []any{
		"reason", string(info.Reason),
		"offset", info.Offset,
	}
	for k, v := range info.Attrs {
		attrs = append(attrs, k, v)
	}
	c.logger.Warn("Row skipped", attrs...)
}

func getCreatePipelineMaxBackoff() time.Duration {
	if os.Getenv("TIMBERTRUCK_TEST_MODE") != "" {
		return createPipelineMaxBackoffTestMode
	}
	return createPipelineMaxBackoff
}
