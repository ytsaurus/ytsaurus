package test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/test/yatest"
)

type Testcase struct {
	Text       string `xml:",chardata"`
	Assertions string `xml:"assertions,attr"`
	Classname  string `xml:"classname,attr"`
	Name       string `xml:"name,attr"`
	Status     string `xml:"status,attr"`
	Time       string `xml:"time,attr"`
	Skipped    string `xml:"skipped"`
	Error      struct {
		Text    string `xml:",chardata"`
		Message string `xml:"message,attr"`
		Type    string `xml:"type,attr"`
	} `xml:"error"`
	Failure struct {
		Text    string `xml:",chardata"`
		Message string `xml:"message,attr"`
		Type    string `xml:"type,attr"`
	} `xml:"failure"`
	SystemOut string `xml:"system-out"`
	SystemErr string `xml:"system-err"`
}

type Testsuite struct {
	XMLName    xml.Name `xml:"testsuite"`
	Text       string   `xml:",chardata"`
	Disabled   string   `xml:"disabled,attr"`
	Errors     string   `xml:"errors,attr"`
	Failures   string   `xml:"failures,attr"`
	Hostname   string   `xml:"hostname,attr"`
	ID         string   `xml:"id,attr"`
	Name       string   `xml:"name,attr"`
	Package    string   `xml:"package,attr"`
	Skipped    string   `xml:"skipped,attr"`
	Tests      string   `xml:"tests,attr"`
	Time       string   `xml:"time,attr"`
	Timestamp  string   `xml:"timestamp,attr"`
	Properties struct {
		Text     string `xml:",chardata"`
		Property struct {
			Text  string `xml:",chardata"`
			Name  string `xml:"name,attr"`
			Value string `xml:"value,attr"`
		} `xml:"property"`
	} `xml:"properties"`

	Testcases []Testcase `xml:"testcase"`
	SystemOut string     `xml:"system-out"`
	SystemErr string     `xml:"system-err"`
}

type Testsuites struct {
	XMLName  xml.Name `xml:"testsuites"`
	Text     string   `xml:",chardata"`
	Disabled string   `xml:"disabled,attr"`
	Errors   string   `xml:"errors,attr"`
	Failures string   `xml:"failures,attr"`
	Name     string   `xml:"name,attr"`
	Tests    string   `xml:"tests,attr"`
	Time     string   `xml:"time,attr"`

	Testsuites []Testsuite `xml:"testsuite"`
}

const contribPath = "yt/python/yt/wrapper/tests/system_python/contrib/"

func GetPythonPaths(pythonVersion string) []string {
	var contribNames = []string{
		"pytest",
		"pytest-timeout",
		"apipkg",
		"six",
		"execnet",
		"flaky",
		"atomicwrites",
		"py",
		"attrs",
		"pluggy",
		"more-itertools",
		"pathlib2",
		"funcsigs",
		"scandir",
		"contextlib2",
		"configparser",
		"importlib-metadata",
	}

	var sharedLibraries = []string{
		"yt/yt/python/yson_shared",
		"yt/yt/python/driver/native_shared",
		"yt/yt/python/driver/rpc_shared",
	}

	pythonPaths := []string{}
	for _, name := range contribNames {
		pythonPaths = append(pythonPaths, yatest.SourcePath(contribPath+name))
	}
	for _, path := range sharedLibraries {
		pythonPaths = append(pythonPaths, yatest.BuildPath(path))
	}
	return pythonPaths
}

func PreparePython(preparedPythonPath string, t *testing.T) error {
	var err error
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	if _, err := os.Stat(preparedPythonPath); err == nil {
		err = os.RemoveAll(preparedPythonPath)
		if err != nil {
			return fmt.Errorf("Failed to remove: %s", err)
		}
	}

	err = os.Mkdir(preparedPythonPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("Failed to create python directory: %s", err)
	}

	cmdPrepareSourceTree := exec.Command(
		"python3", "-m", "yt_setup.prepare_source_tree",
		"--source-root", yatest.SourcePath(""),
		"--output-path", preparedPythonPath,
		"--fix-type-info-package",
	)

	cmdPrepareSourceTree.Env = append(
		os.Environ(),
		"PYTHONPATH="+yatest.SourcePath("yt/python/packages/"),
	)

	cmdPrepareSourceTree.Stdout = &stdout
	cmdPrepareSourceTree.Stderr = &stderr

	err = cmdPrepareSourceTree.Run()
	if err != nil {
		t.Logf("Prepare source tree command: %v", cmdPrepareSourceTree)
		t.Logf("Prepare source tree command stdout: %s", stdout.String())
		t.Logf("Prepare source tree command stderr: %s", stderr.String())
		return fmt.Errorf("Prepare source tree failed: %s", err)
	}
	return nil
}

func TestPyTest(t *testing.T) {
	var err error
	var pythonVersion string

	useSystemPython, ok := yatest.BuildFlag("USE_SYSTEM_PYTHON")
	if !ok || useSystemPython == "" {
		t.Skipf("You should specify USE_SYSTEM_PYTHON")
		return
	} else {
		pythonVersion = useSystemPython
	}

	testsRoot := os.Getenv("YT_TESTS_SANDBOX")
	if testsRoot == "" {
		if yatest.HasRAMDrive() {
			testsRoot = yatest.OutputRAMDrivePath("")
		} else {
			testsRoot = yatest.OutputPath("")
		}
	}

	preparedPythonPath := filepath.Join(testsRoot, "prepared_python")
	err = PreparePython(preparedPythonPath, t)
	require.NoError(t, err, "failed to prepare python")

	sandboxDir := filepath.Join(testsRoot, "sandbox")

	pythonPaths := GetPythonPaths(pythonVersion)
	pythonPaths = append(pythonPaths, preparedPythonPath)

	testPathsFilePath := path.Join(preparedPythonPath, "yt/wrapper/tests/system_python/test_paths.txt")
	testPathsBlob, err := os.ReadFile(testPathsFilePath)
	require.NoError(t, err)

	testPaths := []string{}
	for _, testName := range strings.Fields(string(testPathsBlob)) {
		testPaths = append(testPaths, path.Join(preparedPythonPath, "yt/wrapper/tests", testName))
	}

	pytestArgs := []string{
		yatest.SourcePath(path.Join(contribPath, "pytest/pytest.py")),
		"--junit-xml=" + yatest.OutputPath("junit.xml"),
		"--cache-clear",
		"--debug",
		"--verbose",
		"--capture=no",
		"--durations=0",
		"--timeout=2400",
	}
	pytestArgs = append(pytestArgs, testPaths...)

	if pytestFilter := os.Getenv("PYTEST_FILTER"); pytestFilter != "" {
		pytestArgs = append(pytestArgs, "-k", pytestFilter)
	}

	cmdPytest := exec.Command(
		yatest.PythonBinPath(),
		pytestArgs...,
	)
	cmdPytest.Stdout = os.Stdout
	cmdPytest.Stderr = os.Stderr

	cmdPytest.Env = append(
		os.Environ(),
		"PYTHONPATH="+strings.Join(pythonPaths, ":"),
		"LD_LIBRARY_PATH="+yatest.PythonLibPath(),
		"YT_TESTS_SANDBOX="+sandboxDir,
		"YT_BUILD_ROOT="+yatest.BuildPath(""),
		"YT_ENABLE_VERBOSE_LOGGING=1",
	)
	cmdPytest.Env = append(
		cmdPytest.Env,
		"PYTEST_PLUGINS=pytest_timeout",
	)

	t.Logf("env: %s", cmdPytest.Env)
	t.Logf("running %s", cmdPytest.String())

	if err = cmdPytest.Run(); err != nil {
		t.Errorf("running pytest command failed: %s", err)
	}

	junitBlob, err := os.ReadFile(yatest.OutputPath("junit.xml"))
	require.NoError(t, err)

	var testSuite Testsuite
	require.NoError(t, xml.Unmarshal(junitBlob, &testSuite))

	for _, testcase := range testSuite.Testcases {
		t.Run(fmt.Sprintf("%s::%s", testcase.Classname, testcase.Name), func(t *testing.T) {
			if testCaseError := testcase.Failure.Text; testCaseError != "" {
				t.Logf(testCaseError)
				t.Fail()
			}

			if testCaseError := testcase.Error.Text; testCaseError != "" {
				t.Logf(testCaseError)
				t.Fail()
			}
		})
	}
}
