package ytrecipe

import (
	"encoding/json"
	"fmt"
	"os"

	"a.yandex-team.ru/library/go/test/yatest"
)

type Env struct {
	Args      []string
	Cwd       string
	Environ   []string
	BuildRoot string
	TmpDir    string
	TestTool  string
	OutputDir string
	WorkPath  string
	TraceFile string

	ProjectPath         string
	Modulo, ModuloIndex string
	TestFileFilter      string

	GlobalResource map[string]string
}

func CaptureEnv() (*Env, error) {
	e := &Env{
		Args: os.Args[1:],
	}

	var err error
	e.Cwd, err = os.Getwd()
	if err != nil {
		return nil, err
	}

	e.Environ = os.Environ()
	e.BuildRoot = os.Getenv("ARCADIA_BUILD_ROOT")
	if e.BuildRoot == "" {
		return nil, fmt.Errorf("ARCADIA_BUILD_ROOT is not defined")
	}

	gr := os.Getenv("YA_GLOBAL_RESOURCES")
	if err := json.Unmarshal([]byte(gr), &e.GlobalResource); err != nil {
		return nil, fmt.Errorf("YA_GLOBAL_RESOURCES is invalid: %q", gr)
	}

	e.TmpDir = os.TempDir()
	e.TestTool = os.Getenv("TEST_TOOL")
	e.WorkPath = yatest.WorkPath(".")

	for i, arg := range e.Args {
		switch arg {
		case "--output-dir":
			e.OutputDir = e.Args[i+1]
		case "--tracefile", "--ya-trace":
			e.TraceFile = e.Args[i+1]
		case "--project-path":
			e.ProjectPath = e.Args[i+1]
		case "--modulo":
			e.Modulo = e.Args[i+1]
		case "--modulo-index":
			e.ModuloIndex = e.Args[i+1]
		case "--test-file-filter":
			e.TestFileFilter = e.Args[i+1]
		}
	}

	if e.OutputDir == "" {
		return nil, fmt.Errorf("failed to detect test output-dir")
	}

	if e.TraceFile == "" {
		return nil, fmt.Errorf("failed to detect test trace file")
	}

	return e, nil
}
