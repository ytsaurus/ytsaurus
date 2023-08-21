package selfrotate

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSink(t *testing.T) {
	for i, testCase := range []struct {
		options Options
		files   []string
	}{
		{
			options: Options{
				Compress:       CompressNone,
				RotateInterval: RotateInterval(time.Second),
			},
			files: []string{
				"my.log",
				"my.log.1",
				"my.log.2",
				"my.log.3",
			},
		},
		{
			options: Options{
				Compress:       CompressAtomic,
				RotateInterval: RotateInterval(time.Second),
			},
			files: []string{
				"my.log.gz",
				"my.log.1.gz",
				"my.log.2.gz",
				"my.log.3.gz",
			},
		},
		{
			options: Options{
				Compress:       CompressDelayed,
				RotateInterval: RotateInterval(time.Second),
			},
			files: []string{
				"my.log",
				"my.log.1.gz",
				"my.log.2.gz",
				"my.log.3.gz",
			},
		},
		{
			options: Options{
				Compress:       CompressDelayed,
				MaxKeep:        3,
				RotateInterval: RotateInterval(time.Second),
			},
			files: []string{
				"my.log",
				"my.log.1.gz",
				"my.log.2.gz",
				"my.log.3.gz",
			},
		},
		{
			options: Options{
				Compress:       CompressDelayed,
				MaxSize:        1000,
				RotateInterval: RotateInterval(time.Second),
			},
			files: []string{
				"my.log",
				"my.log.1.gz",
				"my.log.2.gz",
				"my.log.3.gz",
			},
		},
		{
			options: Options{
				Compress:       CompressDelayed,
				MinFreeSpace:   1, // Always rotate based of free disk space.
				RotateInterval: RotateInterval(time.Second),
			},
			files: []string{
				"my.log",
				"my.log.1.gz",
			},
		},
	} {
		testCase := testCase
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()

			dir, err := os.MkdirTemp("", "sink")
			require.NoError(t, err)

			testCase.options.Name = filepath.Join(dir, "my.log")

			w, err := New(testCase.options)
			require.NoError(t, err)
			defer w.Close()

			for i := 0; i < 100; i++ {
				_, err := w.Write([]byte("line\n"))
				require.NoError(t, err)

				time.Sleep(time.Second / 10)
			}

			_ = w.Close()

			files, err := os.ReadDir(dir)
			require.NoError(t, err)

			var names []string
			for _, f := range files {
				names = append(names, f.Name())
			}

			for _, expectedFile := range testCase.files {
				require.Contains(t, names, expectedFile)
			}
		})
	}
}
