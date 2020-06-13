package job

import (
	"sync"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/jobfs"
)

const maxRowSize = 4 * 1024 * 1024

type stdWriter struct {
	mu     *sync.Mutex
	w      mapreduce.Writer
	stdout bool
}

func (s *stdWriter) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var row jobfs.OutputRow

	for n < len(p) {
		end := len(p)
		if end-n > maxRowSize {
			end = n + maxRowSize
		}

		if s.stdout {
			row.Stdout = p[n:end]
		} else {
			row.Stderr = p[n:end]
		}

		if err = s.w.Write(row); err != nil {
			return
		}

		n = end
	}

	return
}
