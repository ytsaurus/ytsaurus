package job

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	porto "a.yandex-team.ru/library/go/porto"
	"a.yandex-team.ru/yt/go/guid"
)

const (
	tmpfsStorage = "/slot/sandbox/tmpfs"
	ext4Storage  = "/slot/sandbox/ext4"
)

func (j *Job) createRootFSVolumes(conn porto.PortoAPI) error {
	bindPoints, err := j.FS.LocateBindPoints()
	if err != nil {
		return err
	}
	j.L.Debug("located bind points", log.Strings("paths", bindPoints))

	mkdirRoot := func(path string) error {
		const ctName = "mkdir"
		if err := conn.Create(ctName); err != nil {
			return err
		}

		var err error
		setProperty := func(name, value string) {
			if err != nil {
				return
			}

			err = conn.SetProperty(ctName, name, value)
			if err != nil {
				err = fmt.Errorf("error setting property %s=%q: %w", name, value, err)
			}
		}

		setProperty("command", strings.Join([]string{os.Args[0], "-mkdir", path}, " "))
		setProperty("user", "root")
		setProperty("stderr_path", "/dev/fd/2")

		if err != nil {
			return err
		}

		if err := conn.Start(ctName); err != nil {
			return err
		}

		state, err := conn.WaitContainer(ctName, time.Minute)
		if err != nil {
			return err
		}

		if state != "dead" {
			return fmt.Errorf("mkdir container timed out")
		}

		exitCode, err := conn.GetProperty(ctName, "exit_code")
		if err != nil {
			return err
		}

		exitCodeInt, err := strconv.Atoi(exitCode)
		if err != nil {
			return fmt.Errorf("can't parse exit_code: %w", err)
		}

		if exitCodeInt != 0 {
			return fmt.Errorf("mkdir failed: exit_code=%d", exitCodeInt)
		}

		if err := conn.Destroy(ctName); err != nil {
			return err
		}

		return nil
	}

	createBind := func(storage, path string) error {
		if err := mkdirRoot(path); err != nil {
			return err
		}

		if err := os.MkdirAll(storage, 0777); err != nil {
			return err
		}

		_, err := conn.CreateVolume(path, map[string]string{
			"backend": "rbind",
			"storage": storage,
		})
		if err != nil {
			return fmt.Errorf("failed to bind %s to %s: %w", storage, path, err)
		}

		j.L.Info("created volume",
			log.String("name", path),
			log.String("storage", storage))
		return nil
	}

	for _, dir := range bindPoints {
		storage := filepath.Join(tmpfsStorage, guid.New().String())

		if err := createBind(storage, dir); err != nil {
			return fmt.Errorf("failed to create bind: %w", err)
		}
	}

	for _, path := range j.FS.Ext4Dirs {
		storage := filepath.Join(ext4Storage, guid.New().String())

		if err := createBind(storage, path); err != nil {
			return err
		}
	}

	return nil
}

func (j *Job) spawnPorto(
	startTimeouts func() chan syscall.Signal,
	prepareFS func() error,
	stdout, stderr io.Writer,
) error {
	conn, err := porto.Dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	err = j.createRootFSVolumes(conn)
	if err != nil {
		return err
	}

	if err := prepareFS(); err != nil {
		return err
	}

	const ctName = "self/T"

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		return err
	}

	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		return err
	}

	if err := conn.Create(ctName); err != nil {
		return err
	}

	//for i, v := range volumes {
	//	if err := conn.LinkVolumeTarget(*v.Path, ctName, bindPaths[i], false); err != nil {
	//		return fmt.Errorf("failed to link volume: %w", err)
	//	}
	//}

	setProperty := func(name, value string) {
		if err != nil {
			return
		}

		err = conn.SetProperty(ctName, name, value)
		if err != nil {
			err = fmt.Errorf("error setting property %s=%q: %w", name, value, err)
		}
	}

	setProperty("command", strings.Join(j.Cmd.Args, " "))
	if j.OperationConfig.RunAsRoot {
		setProperty("user", "root")
	}

	if !j.OperationConfig.EnableNetwork {
		setProperty("net", "none")
		setProperty("hostname", "localhost")
	}

	setProperty("cwd", j.Cmd.Cwd)
	setProperty("env", strings.Join(j.Cmd.Environ, ";"))
	setProperty("stdout_path", fmt.Sprintf("/dev/fd/%d", stdoutW.Fd()))
	setProperty("stderr_path", fmt.Sprintf("/dev/fd/%d", stderrW.Fd()))
	setProperty("controllers[memory]", "true")
	setProperty("memory_limit", fmt.Sprintf("%d", j.OperationConfig.MemoryLimit))
	setProperty("ulimit", "core: unlimited")
	setProperty("core_command", fmt.Sprintf("cp --sparse=always /dev/stdin %s/${CORE_EXE_NAME}-${CORE_PID}.core", j.FS.CoredumpDir))

	if err != nil {
		return err
	}

	j.L.Info("spawning nested porto container", log.Int("memory_limit", j.OperationConfig.MemoryLimit))
	if err := conn.Start(ctName); err != nil {
		return err
	}

	stopSignals := startTimeouts()

	_ = stdoutW.Close()
	_ = stderrW.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	defer wg.Wait()

	go func() {
		defer wg.Done()
		_, _ = io.Copy(stdout, stdoutR)
	}()

	go func() {
		defer wg.Done()
		_, _ = io.Copy(stderr, stderrR)
	}()

	var lastSignal syscall.Signal
	for {
		select {
		case lastSignal = <-stopSignals:
			j.L.Info("sending signal to container", log.String("signal", lastSignal.String()))
			if err := conn.Kill(ctName, lastSignal); err != nil {
				j.L.Error("error sending signal", log.Error(err))
			}
		default:
		}

		state, err := conn.WaitContainer(ctName, time.Second)
		if err != nil {
			return err
		}

		if state == "dead" {
			break
		}
	}

	exitCode, err := conn.GetProperty(ctName, "exit_code")
	if err != nil {
		return err
	}

	exitCodeInt, err := strconv.Atoi(exitCode)
	if err != nil {
		return fmt.Errorf("can't parse exit_code: %w", err)
	}

	return &ContainerExitError{
		ExitCode:       exitCodeInt,
		KilledBySignal: lastSignal,
	}
}

type ContainerExitError struct {
	ExitCode       int
	KilledBySignal syscall.Signal
}

func (e *ContainerExitError) Error() string {
	if e.IsOOM() {
		return "container terminated: OOM"
	}

	return fmt.Sprintf("container terminated: exit_code=%d", e.ExitCode)
}

func (e *ContainerExitError) IsOOM() bool {
	return e.ExitCode == -99
}
