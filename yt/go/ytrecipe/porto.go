package ytrecipe

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	porto "a.yandex-team.ru/infra/porto/api_go"
	"a.yandex-team.ru/library/go/core/log"
)

const bindRoot = "/slot/sandbox/bind"

func (j *Job) spawnPorto(stdout, stderr io.Writer) error {
	bindPoints, err := j.FS.LocateBindPoints()
	if err != nil {
		return err
	}

	var bindArgs []string
	for _, p := range bindPoints {
		if err := os.MkdirAll("bind"+p, 0777); err != nil {
			return err
		}

		bindArgs = append(bindArgs, fmt.Sprintf("%s%s %s", bindRoot, p, p))
	}

	conn, err := porto.Dial()
	if err != nil {
		return err
	}
	defer conn.Close()

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
	setProperty("cwd", j.Env.WorkPath)
	setProperty("env", strings.Join(j.Env.Environ, ";"))
	setProperty("bind", strings.Join(bindArgs, ";"))
	setProperty("stdout_path", fmt.Sprintf("/dev/fd/%d", stdoutW.Fd()))
	setProperty("stderr_path", fmt.Sprintf("/dev/fd/%d", stderrW.Fd()))
	setProperty("controllers", "memory")
	setProperty("memory_limit", fmt.Sprintf("%d", j.Config.ResourceLimits.MemoryLimit))
	setProperty("ulimit", "core: unlimited")
	setProperty("core_command", fmt.Sprintf("cp --sparse=always /dev/stdin %s/${CORE_EXE_NAME}-${CORE_PID}.core", YTRecipeOutput))

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

	_, err = conn.WaitContainer(ctName, -1)
	if err != nil {
		return err
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
		return &ContainerExitError{ExitCode: exitCodeInt}
	}

	return nil
}

type ContainerExitError struct {
	ExitCode int
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
