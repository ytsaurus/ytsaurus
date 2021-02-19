package jobfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/mapreduce"
)

const (
	slowOperationTimeout = time.Minute * 5
)

func logSlow(l log.Structured, msg string, args ...log.Field) (done func()) {
	doneCh := make(chan struct{})

	go func() {
		select {
		case <-doneCh:
			return
		case <-time.After(slowOperationTimeout):
		}

		args = append(args, log.Duration("timeout", slowOperationTimeout))
		l.Warn(msg, args...)
	}()

	return func() {
		close(doneCh)
	}
}

func emitDir(l log.Structured, w mapreduce.Writer, dir string) error {
	defer logSlow(l, "directory processing exceeded timeout", log.String("dir", dir))()

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if errors.Is(err, os.ErrPermission) {
			l.Warn("file walk failed",
				log.String("path", path),
				log.Error(err))
			return nil
		}

		if os.IsNotExist(err) {
			l.Warn("file walk failed",
				log.String("path", path),
				log.Error(err))
			return nil
		}

		if err != nil {
			return err
		}

		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}

		if info.IsDir() {
			return w.Write(FileRow{FilePath: path, IsDir: true})
		} else if info.Mode().IsRegular() {
			err := emitFile(l, w, path)
			if os.IsNotExist(err) {
				l.Warn("file vanished during dir walk",
					log.String("path", path),
					log.Error(err))

				return nil
			}
			return err
		}

		return nil
	})

	if os.IsNotExist(err) {
		l.Warn("file walk failed", log.String("dir", dir), log.Error(err))
		return nil
	}

	if err != nil {
		return fmt.Errorf("error writing %s: %w", dir, err)
	}

	return nil
}

func emitFile(l log.Structured, w mapreduce.Writer, filename string) error {
	defer logSlow(l, "file processing exceeded timeout", log.String("filename", filename))()

	f, err := os.Open(filename)
	if errors.Is(err, os.ErrPermission) {
		l.Warn("file open failed", log.Error(err))
		return nil
	} else if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, maxRowSize)
	for i := 0; true; i++ {
		n, err := f.Read(buf)
		if err == io.EOF {
			if i != 0 {
				break
			}
		} else if err != nil {
			return err
		}

		row := FileRow{
			FilePath:  filename,
			PartIndex: i,
			Data:      buf[:n],
		}

		if err := w.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func (fs *FS) EmitOutputs(l log.Structured, output mapreduce.Writer) error {
	for path := range fs.Outputs {
		st, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}

			return err
		}

		err = func() error {
			if st.IsDir() {
				if err := emitDir(l, output, path); err != nil {
					return err
				}
			} else {
				if err := emitFile(l, output, path); err != nil {
					return err
				}
			}

			return nil
		}()

		if err != nil {
			return fmt.Errorf("output %s: %w", path, err)
		}
	}

	if err := mapreduce.SwitchTable(output, 1); err != nil {
		return err
	}

	for dir := range fs.YTOutputs {
		if err := emitDir(l, output, dir); err != nil {
			return err
		}
	}

	if err := emitCoreDumps(l, output, fs.CoredumpDir); err != nil {
		return err
	}

	if err := mapreduce.SwitchTable(output, 0); err != nil {
		return err
	}

	return nil
}

func emitCoreDumps(l log.Structured, w mapreduce.Writer, dir string) error {
	defer logSlow(l, "coredump directory processing exceeded timeout", log.String("dir", dir))()

	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if errors.Is(err, os.ErrPermission) {
			// TODO(prime@): log
			return nil
		}

		if err != nil {
			return err
		}

		if info.IsDir() {
			return w.Write(FileRow{FilePath: path, IsDir: true})
		} else if info.Mode().IsRegular() {
			return emitSparseFile(l, w, path)
		}

		return nil
	})
}

const maxRowSize = 4 * 1024 * 1024

const (
	SeekData = 3
	SeekHole = 4
)

func emitSparseFile(l log.Structured, w mapreduce.Writer, filename string) error {
	defer logSlow(l, "coredump processing exceeded timeout", log.String("filename", filename))()

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	var partIndex int
	buf := make([]byte, maxRowSize)

	emitHole := func(size int64) error {
		row := FileRow{
			FilePath:  filename,
			PartIndex: partIndex,
			DataSize:  size,
		}
		partIndex++

		return w.Write(row)
	}

	emitData := func(offset, size int64) error {
		_, err := f.Seek(offset, io.SeekStart)
		if err != nil {
			return err
		}

		lr := io.LimitReader(f, size)
		for {
			n, err := lr.Read(buf)
			if err != nil && err != io.EOF {
				return err
			}

			if n > 0 {
				row := FileRow{
					FilePath:  filename,
					PartIndex: partIndex,
					Data:      buf[:n],
				}
				partIndex++

				if err := w.Write(row); err != nil {
					return err
				}
			}

			if err == io.EOF {
				return nil
			}
		}
	}

	var dataOffset, holeOffset int64

	for partIndex := 0; true; partIndex++ {
		dataOffset, err = f.Seek(holeOffset, SeekData)
		if errors.Is(err, syscall.ENXIO) {
			// File ends with hole

			st, err := f.Stat()
			if err != nil {
				return err
			}

			return emitHole(st.Size() - holeOffset)
		} else if err != nil {
			return err
		}

		// File begins with data
		if dataOffset != holeOffset {
			if err := emitHole(dataOffset - holeOffset); err != nil {
				return err
			}
		}

		holeOffset, err = f.Seek(dataOffset, SeekHole)
		if err != nil {
			return err
		}

		if err := emitData(dataOffset, holeOffset-dataOffset); err != nil {
			return err
		}
	}

	return nil
}
