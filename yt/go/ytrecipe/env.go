package ytrecipe

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"a.yandex-team.ru/library/go/test/yatest"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/ytexec"
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
	GDBRoot   string

	ProjectPath         string
	Modulo, ModuloIndex string
	TestFileFilter      string

	GlobalResource map[string]string
}

func (e *Env) FillConfig(c *ytexec.Config) {
	c.Cmd.Cwd = e.Cwd
	c.Cmd.Args = e.Args
	c.Cmd.Environ = e.Environ

	fileTitle := ""
	if e.TestFileFilter != "" {
		fileTitle = " [" + e.TestFileFilter + "]"
	}
	moduloTitle := ""
	if e.ModuloIndex != "" {
		moduloTitle = fmt.Sprintf(" [%s/%s]", e.ModuloIndex, e.Modulo)
	}

	c.Operation.Title = fmt.Sprintf("[TS] %s%s%s", e.ProjectPath, fileTitle, moduloTitle)

	c.FS.StdoutFile = "/dev/stdout"
	c.FS.StderrFile = "/dev/stderr"

	c.FS.Outputs = []string{
		e.TraceFile,
		e.OutputDir,
	}

	c.FS.UploadStructure = []string{
		e.BuildRoot,
		e.TmpDir,
		e.WorkPath,
	}

	c.FS.Ext4Dirs = []string{yatest.WorkPath("ytrecipe_hdd")}
	c.FS.YTOutputs = []string{yatest.WorkPath("ytrecipe_output")}
	c.FS.CoredumpDir = yatest.WorkPath("ytrecipe_coredumps")

	c.FS.UploadFile = append(c.FS.UploadFile, e.TestTool)

	c.FS.Download = map[string]string{
		yatest.OutputPath("."):                "testing_out_stuff",
		yatest.WorkPath("ytrecipe_hdd"):       "ytrecipe_hdd",
		yatest.WorkPath("ytrecipe_output"):    "ytrecipe_output",
		yatest.WorkPath("ytrecipe_coredumps"): "ytrecipe_coredumps",
		"/dev/stdout":                         "testing_out_stuff/stdout",
		"/dev/stderr":                         "testing_out_stuff/stderr",
	}

	if e.GDBRoot != "" {
		c.FS.UploadTarDir = append(c.FS.UploadTarDir, e.GDBRoot)
	}
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
		case "--gdb-path":
			e.GDBRoot = filepath.Dir(filepath.Dir(e.Args[i+1]))
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
