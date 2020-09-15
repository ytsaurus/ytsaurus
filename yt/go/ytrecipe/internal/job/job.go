package job

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/jobfs"
)

func init() {
	mapreduce.Register(&Job{})
}

type Job struct {
	mapreduce.Untyped

	DmesgLogPath string

	Cmd             Cmd
	OperationConfig OperationConfig

	FS *jobfs.FS

	L log.Structured
}

func (j *Job) OutputTypes() []interface{} {
	return []interface{}{
		&jobfs.OutputRow{},
		&jobfs.OutputRow{},
	}
}

func (j *Job) scheduleSignals() chan syscall.Signal {
	quit := make(chan syscall.Signal, 1)

	if j.Cmd.SIGUSR2Timeout != 0 {
		go func() {
			time.Sleep(j.Cmd.SIGUSR2Timeout)

			j.L.Warn("SIGUSR2 timeout reached")
			quit <- syscall.SIGUSR2
		}()

		go func() {
			time.Sleep(j.Cmd.SIGQUITTimeout)

			j.L.Warn("SIGQUIT timeout reached")
			quit <- syscall.SIGQUIT
		}()

		go func() {
			time.Sleep(j.Cmd.SIGKILLTimeout)

			j.L.Warn("SIGKILL timeout reached")
			quit <- syscall.SIGKILL
		}()
	}

	return quit
}

func (j *Job) saveDmesg() error {
	logFile, err := os.Create(j.DmesgLogPath)
	if err != nil {
		return err
	}

	var dmesgErr bytes.Buffer

	cmd := exec.Command("/bin/dmesg", "-T")
	cmd.Stdout = logFile
	cmd.Stderr = &dmesgErr

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("dmesg failed: %q: %w", dmesgErr.String(), err)
	}
	return nil
}

func (j *Job) runJob(out mapreduce.Writer) error {
	stdout := &stdWriter{w: out, mu: new(sync.Mutex), stdout: true}
	stderr := &stdWriter{w: out, mu: stdout.mu, stdout: false}

	var exitRow jobfs.ExitRow
	exitRow.StartedAt = time.Now()

	prepareFS := func() error {
		if err := j.FS.Recreate(j.L, "fs"); err != nil {
			return fmt.Errorf("error recreating fs: %w", err)
		}

		exitRow.UnpackedAt = time.Now()
		j.L.Info("finished creating FS")
		return nil
	}

	err := j.spawnPorto(j.scheduleSignals, prepareFS, stdout, stderr)
	exitRow.FinishedAt = time.Now()

	if dmesgErr := j.saveDmesg(); dmesgErr != nil && err == nil {
		return dmesgErr
	}

	var ctErr *ContainerExitError
	if errors.As(err, &ctErr) {
		if ctErr.IsOOM() {
			j.L.Warn("test was killed by OOM", log.Int("limit", j.OperationConfig.MemoryLimit))
			_, err = fmt.Fprintf(stderr, "\n\n\nTest killed by OOM (limit %d)\n", j.OperationConfig.MemoryLimit)
			if err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	}

	j.L.Info("test process finished", log.Error(err))
	if err := j.FS.EmitOutputs(out); err != nil {
		return fmt.Errorf("error saving outputs: %w", err)
	}

	j.L.Info("finished writing results")
	if ctErr != nil {
		exitRow.ExitCode = ctErr.ExitCode
		exitRow.KilledBySignal = ctErr.KilledBySignal
		exitRow.IsOOM = ctErr.IsOOM()
	}

	return out.Write(exitRow)
}

func (j *Job) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	if err := j.initLog(); err != nil {
		return err
	}

	if err := j.runJob(out[0]); err != nil {
		j.L.Error("internal job error", log.Error(err))
		os.Exit(1)
	}

	return nil
}
