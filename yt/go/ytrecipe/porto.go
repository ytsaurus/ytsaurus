package ytrecipe

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

	porto "a.yandex-team.ru/infra/porto/api_go"
	pporto "a.yandex-team.ru/infra/porto/proto"
	"a.yandex-team.ru/library/go/core/log"
)

const (
	bindRootPath     = "/slot/sandbox/tmpfs/bind"
	hddDirPath       = "/slot/sandbox/ytrecipe_hdd"
	coreDumpsDirPath = "/slot/sandbox/ytrecipe_coredumps"
)

func (j *Job) spawnPorto(quit chan syscall.Signal, stdout, stderr io.Writer) error {
	conn, err := porto.Dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	bindPoints, err := j.FS.LocateBindPoints()
	if err != nil {
		return err
	}

	var bindVolumes []*pporto.TVolumeDescription
	createBind := func(at, storage string) error {
		if err := os.MkdirAll(at, 0777); err != nil {
			return err
		}

		if err := os.MkdirAll(storage, 0777); err != nil {
			return err
		}

		v, err := conn.CreateVolume(at, map[string]string{
			"backend": "rbind",
			"storage": storage,
		})
		if err != nil {
			return err
		}

		bindVolumes = append(bindVolumes, v)
		return nil
	}

	for _, p := range bindPoints {
		bindPoint := filepath.Join(bindRootPath, p)

		if err := createBind(bindPoint, bindPoint); err != nil {
			return err
		}
	}

	if err := createBind(filepath.Join(bindRootPath, j.FS.YTHDD), hddDirPath); err != nil {
		return err
	}
	bindPoints = append(bindPoints, j.FS.YTHDD)

	if err := createBind(filepath.Join(bindRootPath, j.FS.YTCoreDumps), coreDumpsDirPath); err != nil {
		return err
	}
	bindPoints = append(bindPoints, j.FS.YTCoreDumps)

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

	for i, v := range bindVolumes {
		if err := conn.LinkVolumeTarget(*v.Path, ctName, bindPoints[i], false); err != nil {
			return err
		}
	}

	setProperty := func(name, value string) {
		if err != nil {
			return
		}

		err = conn.SetProperty(ctName, name, value)
		if err != nil {
			err = fmt.Errorf("error setting property %s=%q: %w", name, value, err)
		}
	}

	setProperty("command", strings.Join(j.Env.Args, " "))
	setProperty("user", "root")
	setProperty("net", "none")
	setProperty("cwd", j.Env.WorkPath)
	setProperty("env", strings.Join(j.Env.Environ, ";"))
	setProperty("stdout_path", fmt.Sprintf("/dev/fd/%d", stdoutW.Fd()))
	setProperty("stderr_path", fmt.Sprintf("/dev/fd/%d", stderrW.Fd()))
	setProperty("controllers[memory]", "true")
	setProperty("memory_limit", fmt.Sprintf("%d", j.Config.ResourceLimits.MemoryLimit))
	setProperty("ulimit", "core: unlimited")
	setProperty("core_command", fmt.Sprintf("cp --sparse=always /dev/stdin %s/${CORE_EXE_NAME}-${CORE_PID}.core", YTRecipeCoreDumps))

	if err != nil {
		return err
	}

	j.L.Info("spawning nested porto container", log.Int("memory_limit", j.Config.ResourceLimits.MemoryLimit))
	if err := conn.Start(ctName); err != nil {
		return err
	}

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
		case lastSignal = <-quit:
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

	if exitCodeInt != 0 {
		return &ContainerExitError{
			ExitCode:       exitCodeInt,
			KilledBySignal: lastSignal,
		}
	}

	return nil
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
