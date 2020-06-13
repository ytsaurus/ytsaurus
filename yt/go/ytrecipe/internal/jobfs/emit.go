package jobfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"a.yandex-team.ru/yt/go/mapreduce"
)

func emitDir(w mapreduce.Writer, dir string) error {
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if errors.Is(err, os.ErrPermission) {
			// TODO(prime@): log
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
		}

		return emitFile(w, path)
	})

	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("error writing %s: %w", dir, err)
	}

	return nil
}

func emitFile(w mapreduce.Writer, filename string) error {
	f, err := os.Open(filename)
	if errors.Is(err, os.ErrPermission) {
		// TODO(prime@): log
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

func (fs *FS) EmitOutputs(output mapreduce.Writer) error {
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
				if err := emitDir(output, path); err != nil {
					return err
				}
			} else {
				if err := emitFile(output, path); err != nil {
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
		if err := emitDir(output, dir); err != nil {
			return err
		}
	}

	if err := emitCoreDumps(output, fs.CoredumpDir); err != nil {
		return err
	}

	if err := mapreduce.SwitchTable(output, 0); err != nil {
		return err
	}

	return nil
}

func emitCoreDumps(w mapreduce.Writer, dir string) error {
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
		}

		return emitSparseFile(w, path)
	})
}

const maxRowSize = 4 * 1024 * 1024

const (
	SeekData = 3
	SeekHole = 4
)

func emitSparseFile(w mapreduce.Writer, filename string) error {
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
