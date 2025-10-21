package timbertruck

import (
	"context"
	"log/slog"
	"path"
	"sync"
)

type FileEvent int32

const (
	FileCreateEvent FileEvent = iota
	FileRemoveOrRenameEvent
)

type FsWatcher struct {
	watcher *InotifyWatcher
	logger  *slog.Logger

	handlerMap map[string][]chan FileEvent
	mu         sync.RWMutex
}

func NewFsWatcher(logger *slog.Logger) (fsWatcher *FsWatcher, err error) {
	watcher, err := NewInotifyWatcher()
	if err != nil {
		return
	}
	fsWatcher = &FsWatcher{
		watcher:    watcher,
		logger:     logger,
		handlerMap: make(map[string][]chan FileEvent),
	}
	return
}

func (w *FsWatcher) Close() {
	err := w.watcher.Close()
	if err != nil {
		slog.Error("Error closing fswatcher", "error", err)
	}
}

func (w *FsWatcher) AddLogPath(logFilePath string, ch chan FileEvent) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	logDir := path.Dir(logFilePath)
	err = w.watcher.Add(logDir)
	if err != nil {
		return
	}
	w.handlerMap[logFilePath] = append(w.handlerMap[logFilePath], ch)
	w.logger.Info("Added watched directory", "path", logDir)
	return
}

func (w *FsWatcher) trySendEvent(ch chan<- FileEvent, event FileEvent, path string) {
	select {
	case ch <- event:
	default:
		w.logger.Error("Channel overflow", "path", path)
	}
}

func (w *FsWatcher) Run(ctx context.Context) error {
	ctxDone := ctx.Done()
	w.logger.Info("Launched FsWatcher")
loop:
	for {
		select {
		case <-ctxDone:
			break loop
		case event, ok := <-w.watcher.Events:
			if !ok {
				break loop
			}
			w.mu.RLock()
			handlers, ok := w.handlerMap[event.Name]
			w.mu.RUnlock()
			if !ok {
				continue
			}
			if event.Has(FileOpCreate) {
				w.logger.Info("Detected event on watched file", "event", event)
				for i := range handlers {
					w.trySendEvent(handlers[i], FileCreateEvent, event.Name)
				}
			} else if event.Has(FileOpRemove) || event.Has(FileOpRename) {
				w.logger.Info("Detected event on watched file", "event", event)
				for i := range handlers {
					w.trySendEvent(handlers[i], FileRemoveOrRenameEvent, event.Name)
				}
			}
		case watcherErr, ok := <-w.watcher.Errors:
			if !ok {
				break loop
			}
			w.logger.Warn("Detected fsnotify error", "error", watcherErr)
		}
	}
	err := w.watcher.Close()
	if err != nil {
		w.logger.Warn("Error closing watcher")
		return err
	}
	w.logger.Info("Stopped FsWatcher")
	return ctx.Err()
}
