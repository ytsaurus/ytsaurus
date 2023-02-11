// Package pproflog implements periodic self-profiling.
package pproflog

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	"a.yandex-team.ru/library/go/core/log"
)

type Options struct {
	// Dir is directory where profiles should be stored.
	Dir string

	// Keep specifies number of files to keep. 24 * 60 by default.
	Keep int

	// ProfilingPeriod is 1 minute by default.
	ProfilingPeriod time.Duration

	// ProfilingDuration is 10 seconds by default.
	ProfilingDuration time.Duration
}

func (o *Options) setDefaults() {
	if o.Keep == 0 {
		o.Keep = 24 * 60
	}

	if o.ProfilingPeriod == 0 {
		o.ProfilingPeriod = time.Minute
	}

	if o.ProfilingDuration == 0 {
		o.ProfilingDuration = time.Second * 10
	}
}

// LogProfile starts infinite loop, running periodic profiler.
func LogProfile(ctx context.Context, l log.Structured, opts Options) error {
	opts.setDefaults()

	if err := os.MkdirAll(opts.Dir, 0777); err != nil {
		l.Error("failed to create output directory", log.Error(err))
		return err
	}

	for {
		next := time.Until(time.Now().Add(opts.ProfilingPeriod).Truncate(opts.ProfilingPeriod))

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(next):
		}

		if err := rotateFiles(opts, "goroutine"); err != nil {
			l.Error("failed to rotate cpu profile", log.Error(err))
			continue
		}

		gp := pprof.Lookup("goroutine")
		if gp == nil {
			panic("goroutine profile is not available")
		}

		var buf bytes.Buffer
		_ = gp.WriteTo(&buf, 1)
		if err := os.WriteFile(filepath.Join(opts.Dir, "goroutine.pprof"), buf.Bytes(), 0666); err != nil {
			l.Error("error writing goroutine profile", log.Error(err))
			continue
		}

		if err := rotateFiles(opts, "cpu"); err != nil {
			l.Error("failed to rotate cpu profile", log.Error(err))
			continue
		}

		f, err := os.Create(filepath.Join(opts.Dir, "cpu.pprof.tmp"))
		if err != nil {
			l.Error("failed to create cpu profile", log.Error(err))
			continue
		}

		if err := pprof.StartCPUProfile(f); err != nil {
			l.Error("cpu profile is running", log.Error(err))
			_ = f.Close()
			continue
		}

		time.Sleep(opts.ProfilingDuration)
		pprof.StopCPUProfile()
		_ = f.Close()

		if err := os.Rename(filepath.Join(opts.Dir, "cpu.pprof.tmp"), filepath.Join(opts.Dir, "cpu.pprof")); err != nil {
			l.Error("error committing cpu profile", log.Error(err))
		}
	}
}
