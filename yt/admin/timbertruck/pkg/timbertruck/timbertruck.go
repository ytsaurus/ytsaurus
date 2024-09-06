package timbertruck

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
)

const (
	stateFile = "log_pusher_state.sqlite"
)

type Config struct {
	// A place where timbertruck keeps it's file to store info about sent data.
	// It's also the directory where it keeps hardlinks for unprocessed files.
	WorkDir string `yaml:"work_dir"`

	// How much time timbertruck waits for new data after log file rotation
	LogCompletionDelay *time.Duration `yaml:"log_completion_delay"`

	// How much time timbertruck waits before removing completed tasks from datastore
	CompletedTaskRetainPeriod *time.Duration `yaml:"completed_task_retain_period"`
}

type TimberTruck struct {
	config    Config
	logger    *slog.Logger
	fsWatcher *FsWatcher
	datastore *Datastore

	handlers []streamHandler
}

func NewTimberTruck(config Config, logger *slog.Logger) (result *TimberTruck, err error) {
	logPusher := TimberTruck{
		config: config,
		logger: logger,
	}

	if logPusher.config.LogCompletionDelay == nil {
		defaultDelay := 5 * time.Second
		logPusher.config.LogCompletionDelay = &defaultDelay
	}
	if logPusher.config.CompletedTaskRetainPeriod == nil {
		defaultPeriod := 7 * 24 * time.Hour
		logPusher.config.CompletedTaskRetainPeriod = &defaultPeriod
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

	logPusher.datastore, err = NewDatastore(path.Join(config.WorkDir, stateFile))
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
}

type TaskArgs struct {
	Context    context.Context
	Path       string
	Position   pipelines.FilePosition
	Controller TaskController
}

type NewPipelineFunc func(task TaskArgs) (*pipelines.Pipeline, error)

// TODO: should be named PipelineConfig
type StreamConfig struct {
	// Name of the stream. Can contain [-_A-Za-z0-9].
	// This name identifies stream inside timbertruck storage
	// so renaming existing stream will result in progress loss.
	Name string `yaml:"name"`

	// TODO: should be named SourceFile
	// Path to main log file
	LogFile string `yaml:"log_file"`
}

// TODO: Should be named AddPipeline
func (tt *TimberTruck) AddStream(stream StreamConfig, newPipeline NewPipelineFunc) {
	handler := streamHandler{
		timberTruck: tt,
		logger: tt.logger.With(
			"stream", stream.Name,
			"file", path.Base(stream.LogFile),
		),
		config:          stream,
		newPipelineFunc: newPipeline,
		haveTasks:       make(chan struct{}),
	}
	tt.handlers = append(tt.handlers, handler)
	handler.logger.Info("Stream created")
}

func (tt *TimberTruck) Serve(ctx context.Context) error {

	activeTasks, err := tt.datastore.ListActiveTasks()
	if err != nil {
		panic(fmt.Sprintf("unexpected datastore error: %v", err))
	}
	for _, task := range activeTasks {
		_, err := os.Stat(task.StagedPath)
		if err != nil {
			tt.logger.Error("unavailable file for active task, task is completed with error", "error", err, "file", task.StagedPath)
			err = tt.datastore.CompleteTask(task.StagedPath, time.Now(), fmt.Errorf("file unavailable: %w", err))
			if err != nil {
				panic(fmt.Sprintf("unexpected datastore error: %v", err))
			}
		}
	}

	for i := range tt.handlers {
		err = tt.handlers[i].initStagingDir()
		if err != nil {
			tt.logger.Error("Error initializing stream", "error", err)
			continue
		}
		fileEventChan := make(chan FileEvent, 16)
		err = tt.fsWatcher.AddLogPath(tt.handlers[i].config.LogFile, fileEventChan)
		if err != nil {
			close(fileEventChan)
			tt.logger.Error("Error initializing stream", "error", err)
			continue
		}
		defer close(fileEventChan)

		go tt.handlers[i].ProcessFileEventQueue(ctx, fileEventChan)
		go tt.handlers[i].ProcessTaskQueue(ctx)

		_, err = os.Stat(tt.handlers[i].config.LogFile)
		if err != nil {
			if err != ErrNotFound {
				tt.logger.Error("Cannot stat log path", "path", tt.handlers[i].config.LogFile, "error", err)
			} else {
				fileEventChan <- FileRemoveOrRenameEvent
			}
		} else {
			fileEventChan <- FileCreateEvent
		}
	}

	tt.logger.Info("Serving")

	return tt.fsWatcher.Run(ctx)
}

type streamHandler struct {
	timberTruck *TimberTruck
	logger      *slog.Logger

	config          StreamConfig
	newPipelineFunc NewPipelineFunc

	haveTasks chan struct{}
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
	return path.Join(h.timberTruck.config.WorkDir, h.config.Name, "staging")
}

func (h *streamHandler) linkedLogPath(now time.Time, randomSuffix string) string {
	resultName := fmt.Sprintf("%v-%v%v", now.Format("2006-01-02T15:04:05"), randomSuffix, h.getExtensions())
	return path.Join(h.stagingDir(), resultName)
}

func (h *streamHandler) taskLogPath(createTime time.Time, ino int64) string {
	resultName := fmt.Sprintf("%v_ino:%v%v", createTime.Format("2006-01-02T15:04:05"), ino, h.getExtensions())
	return path.Join(h.stagingDir(), resultName)
}

func (h *streamHandler) ProcessFileEventQueue(ctx context.Context, events <-chan FileEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-events:
			if !ok {
				return
			}
			switch e {
			case FileCreateEvent:
				h.handleCreate()
				h.resetActiveTask()
			case FileRemoveOrRenameEvent:
				h.resetActiveTask()
			}
		}
	}
}

func (h *streamHandler) initStagingDir() (err error) {
	stagingDir := h.stagingDir()
	err = os.MkdirAll(stagingDir, 0755)
	if err != nil {
		// TODO: metrics
		h.logger.Error("Failed to create staging directory", "error", err, "directory", stagingDir)
		return
	}

	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		return
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
		// TODO: metrics
		h.logger.Error("Failed to create staging directory", "error", err, "directory", stagingDir)
		return
	}
	now := time.Now()
	linkedPath := h.linkedLogPath(now, uuid.NewString())

	err = os.Link(h.config.LogFile, linkedPath)
	if err != nil {
		h.logger.Error("Failed to link log file", "error", err, "filepath", h.config.LogFile, "stagingFilepath", linkedPath)
		return
	}
	h.handleStagedPath(linkedPath)
}

func (h *streamHandler) handleStagedPath(linkedPath string) {
	var err error
	var stat syscall.Stat_t
	logger := h.logger.With("path", path.Base(linkedPath))
	err = syscall.Stat(linkedPath, &stat)
	if err != nil {
		logger.Error("Failed to stat staging file", "error", err)
		return
	}

	ino := int64(stat.Ino)
	createTime := time.Now()

	task, err := h.timberTruck.datastore.ActiveTaskByIno(ino)
	if err == nil && task.CompletionTime.IsZero() { // NO ERROR, we already have this task
		if task.StagedPath != linkedPath {
			err = os.Remove(linkedPath)
			if err != nil {
				h.logger.Error("Failed to remove linked file", "error", err)
			}
		}
		return
	} else if err != ErrNotFound {
		panic(fmt.Sprintf("unexpected datastore error: %v", err))
	}
	task, err = h.timberTruck.datastore.TaskByPath(linkedPath)
	if err == nil {
		err = os.Remove(linkedPath)
		if err != nil {
			h.logger.Error("Failed to remove linked file", "error", err)
		}
		return
	} else if err != ErrNotFound {
		panic(fmt.Sprintf("unexpected datastore error: %v", err))
	}

	taskLogPath := h.taskLogPath(createTime, ino)
	err = os.Rename(linkedPath, taskLogPath)
	if err != nil {
		h.logger.Error("Failed to move linked file", "error", err)
	}

	h.handleTaskLogPath(taskLogPath, createTime, ino)
}

func (h *streamHandler) handleTaskLogPath(taskLogPath string, createTime time.Time, ino int64) {
	var err error
	task := Task{
		StreamName:   h.config.Name,
		INode:        ino,
		StagedPath:   taskLogPath,
		CreationTime: createTime,
	}
	err = h.timberTruck.datastore.AddTask(&task)
	if err != nil {
		panic(fmt.Sprintf("Unexpected datastore error: %v", err))
	}

	go func() {
		h.haveTasks <- struct{}{}
	}()
	h.logger.Info("Added task", "ino", ino, "ctime", createTime)
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
	h.logger.Info("Reset unbound task", "inode", ino)
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
				panic(fmt.Sprintf("cannot set task peeked: %v", err))
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
			path:      task.StagedPath,
			datastore: h.timberTruck.datastore,
			logger:    h.logger.With("component", "pipeline"),
		}
		p, err := h.newPipelineFunc(TaskArgs{
			Context:    ctx,
			Path:       task.StagedPath,
			Position:   task.EndPosition,
			Controller: &taskController,
		})
		if err != nil {
			h.logger.Error("Error creating pipeline", "error", err)
			// TODO: metrics
			err = h.completeTask(task, err)
			if err != nil {
				panic(fmt.Sprintf("unexpected error while completing task: %v", err))
			}
			return
		}
		h.logger.Info("Pipeline created", "path", task.StagedPath)
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
					return
				}
			}
			p.NotifyComplete()
		}()
		err = p.Run(ctx)
		if err != nil {
			h.logger.Error("Pipeline error", "error", err)
		} else {
			err = h.completeTask(task, nil)
			if err != nil {
				h.logger.Error("Failed to complete task", "error", err, "task", task.StagedPath)
				return
			}
			err = os.Remove(task.StagedPath)
			if err != nil {
				h.logger.Error("Failed to remove staged file", "error", err, "task", task.StagedPath)
			}
			h.logger.Info("Pipeline completed", "path", task.StagedPath)
		}
	}
}

func (h *streamHandler) completeTask(task Task, taskError error) (err error) {
	err = h.timberTruck.datastore.CompleteTask(task.StagedPath, time.Now(), taskError)
	if err != nil {
		return
	}
	err = h.timberTruck.datastore.CleanupOldCompletedTasks(time.Now().Add(-*h.timberTruck.config.CompletedTaskRetainPeriod))
	if err != nil {
		h.timberTruck.logger.Error("Failed to cleanup old completed tasks", "error", err)
	}
	return
}

type taskController struct {
	path      string
	datastore *Datastore
	logger    *slog.Logger
}

func (c *taskController) NotifyProgress(pos pipelines.FilePosition) {
	for {
		err := c.datastore.UpdateEndPosition(c.path, pos)
		if err != nil {
			c.logger.Error("Failed to notify progress: %v", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
}

func (c *taskController) Logger() *slog.Logger {
	return c.logger
}
