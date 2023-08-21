// Package selfrotate provides zap sync that automatically rotates log files according to provided config.
package selfrotate

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func New(options Options) (*Writer, error) {
	options.setDefault()
	if err := options.valid(); err != nil {
		return nil, err
	}

	if dir, _ := filepath.Split(options.Name); dir == "" {
		// The dirname should be non-empty in order for the rotation to work properly.
		// To ensure that, we prepend the name with "./". Note that we cannot use
		// filepath.Join here since it cleans the resulting path effectively removing the dot.
		options.Name = "." + string(filepath.Separator) + options.Name
	}

	w := &Writer{
		options: options,
		stop:    make(chan struct{}),
	}

	if err := w.reopen(); err != nil {
		return nil, err
	}

	w.stopped.Add(1)
	go w.run()
	return w, nil
}

type Writer struct {
	options Options

	stopOnce sync.Once
	stop     chan struct{}
	stopped  sync.WaitGroup

	mu      sync.Mutex
	buf     bytes.Buffer
	file    *os.File
	lastErr error
}

func (w *Writer) reopen() error {
	name := formatFilename(w.options.Name, 0, w.options.Compress == CompressAtomic)
	newFile, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	w.mu.Lock()
	oldFile := w.file
	w.file = newFile
	w.mu.Unlock()

	_ = oldFile.Close()
	return nil
}

func (w *Writer) compressFile(dir string, plan fileRename) error {
	out, err := os.Create(filepath.Join(dir, plan.to+tmpSuffix))
	if err != nil {
		return err
	}
	defer out.Close()

	in, err := os.Open(filepath.Join(dir, plan.from))
	if err != nil {
		return err
	}
	defer in.Close()

	gz := gzip.NewWriter(out)
	if _, err := io.Copy(gz, in); err != nil {
		return err
	}

	if err := gz.Close(); err != nil {
		return err
	}

	if err := os.Rename(out.Name(), filepath.Join(dir, plan.to)); err != nil {
		return err
	}

	return os.Remove(in.Name())
}

func (w *Writer) rotate() error {
	if err := removeTmpFiles(w.options.Name); err != nil {
		return err
	}

	dir, _ := filepath.Split(w.options.Name)

	logFiles, err := listLogs(w.options.Name)
	if err != nil {
		return err
	}

	remove, rename, compress, err := planRotation(dir, logFiles, w.options)
	if err != nil {
		return err
	}

	for _, name := range remove {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("failed to remove log file: %w", err)
		}
	}

	for _, plan := range rename {
		if err := os.Rename(filepath.Join(dir, plan.from), filepath.Join(dir, plan.to)); err != nil {
			return fmt.Errorf("failed to rotate log file: %w", err)
		}
	}

	if err = w.reopen(); err != nil {
		return err
	}

	for _, plan := range compress {
		if err := w.compressFile(dir, plan); err != nil {
			return fmt.Errorf("failed to compress log file: %w", err)
		}
	}

	return nil
}

func (w *Writer) run() {
	defer w.stopped.Done()

	for {
		next := nextRotation(time.Now(), w.options.RotateInterval)

		select {
		case <-w.stop:
			return

		case <-time.After(time.Until(next)):
		}

		err := w.rotate()

		w.mu.Lock()
		w.lastErr = err
		w.mu.Unlock()
	}
}

func (w *Writer) Sync() error {
	// We write logs to local file system without buffering. Sync is no-op.
	return nil
}

func (w *Writer) Close() error {
	w.stopOnce.Do(func() {
		close(w.stop)
	})

	w.stopped.Wait()
	return nil
}

func (w *Writer) Write(b []byte) (int, error) {
	if w.options.Compress == CompressAtomic {
		w.buf.Reset()
		gz := gzip.NewWriter(&w.buf)

		if _, err := gz.Write(b); err != nil {
			return 0, err
		}

		if err := gz.Close(); err != nil {
			return 0, err
		}

		b = w.buf.Bytes()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.lastErr != nil {
		return 0, w.lastErr
	}

	return w.file.Write(b)
}
