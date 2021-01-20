package test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"a.yandex-team.ru/library/go/test/yatest"
	"github.com/stretchr/testify/require"
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

func PrepareBinaries(destination string) error {
	var err error

	if _, err := os.Stat(destination); err == nil {
		err = os.RemoveAll(destination)
		if err != nil {
			return fmt.Errorf("Failed to remove %s: %s", destination, err)
		}
	}
	err = os.MkdirAll(destination, os.ModePerm)
	if err != nil {
		return fmt.Errorf("Failed to create %s: %s", destination, err)
	}

	ytserverAll := yatest.BuildPath("yt/yt/server/all/ytserver-all")
	_, err = Copy(ytserverAll, path.Join(destination, "ytserver-all") /* followSymlinks */, true)
	if err != nil {
		return fmt.Errorf("Failed to copy ytserver-all: %s", err)
	}

	var serverBinaries = []string{
		"master",
		"clock",
		"node",
		"job-proxy",
		"exec",
		"proxy",
		"http-proxy",
		"tools",
		"scheduler",
		"controller-agent",
	}
	for _, binary := range serverBinaries {
		binaryPath := path.Join(destination, "ytserver-"+binary)
		err = os.Symlink(path.Join(destination, "ytserver-all"), binaryPath)
		if err != nil {
			return fmt.Errorf("Failed to make a symlink: %s", err)
		}
	}
	fixupBinary := yatest.BuildPath("yt/yt/tools/yt_sudo_fixup/yt-sudo-fixup")

	var binariesToFix = []string{
		"exec",
		"job-proxy",
		"tools",
	}

	for _, binary := range binariesToFix {
		binaryPath := path.Join(destination, "ytserver-"+binary)
		origPath := path.Join(destination, "ytserver-"+binary+".orig")
		err = os.Rename(binaryPath, origPath)
		if err != nil {
			return fmt.Errorf("Failed to rename %s to %s: %s", binaryPath, origPath, err)
		}
		trampolineBash := `#!/bin/bash

exec sudo -En %v %v %v %v "$@"
`

		err = ioutil.WriteFile(
			binaryPath,
			[]byte(fmt.Sprintf(trampolineBash, fixupBinary, os.Getuid(), origPath, "ytserver-"+binary)),
			0755,
		)
		if err != nil {
			return fmt.Errorf("Failed to write file %s: %s", binaryPath, err)
		}
	}

	// TODO: support logrotate

	return nil
}

func GetPythonPaths() []string {
	var contribPaths = []string{
		"contrib/python/pytest",
		"contrib/python/apipkg",
		"contrib/python/six",
		"contrib/python/execnet",
		"contrib/python/flaky",
		"contrib/python/atomicwrites",
		"contrib/python/py",
		"contrib/python/attrs",
		"contrib/python/pluggy",
		"contrib/python/more-itertools",
		"contrib/python/pathlib2",
		"contrib/python/funcsigs",
		"contrib/python/scandir",
		"contrib/python/importlib-metadata",
		"contrib/python/contextlib2",
		"contrib/python/configparser",
	}

	var sharedLibraries = []string{
		"yt/yt/python/yson_shared",
		"yt/yt/python/driver/native_shared",
		"yt/yt/python/driver/rpc_shared",
	}

	pythonPaths := []string{}
	for _, p := range contribPaths {
		pythonPaths = append(pythonPaths, yatest.SourcePath(p))
	}
	for _, p := range sharedLibraries {
		pythonPaths = append(pythonPaths, yatest.BuildPath(p))
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

	err = CopyTree(yatest.SourcePath("yt/python"), preparedPythonPath, nil)
	if err != nil {
		return fmt.Errorf("Failed to copy yt/python source files: %s", err)
	}

	cmdPrepareSourceTree := exec.Command(
		path.Join(preparedPythonPath, "prepare_source_tree/prepare_source_tree.py"),
		"--python-root", preparedPythonPath,
		"--yt-root", yatest.SourcePath("yt"),
		"--arcadia-root", yatest.SourcePath(""),
	)
	stdout.Reset()
	stderr.Reset()
	cmdPrepareSourceTree.Stdout = &stdout
	cmdPrepareSourceTree.Stderr = &stderr

	err = cmdPrepareSourceTree.Run()
	if err != nil {
		t.Logf("Prepare source tree command stdout: %s", stdout.String())
		t.Logf("Prepare source tree command stderr: %s", stderr.String())
		return fmt.Errorf("Prepare source tree failed: %s", err)
	}
	return nil
}

func TestPyTest(t *testing.T) {
	useSystemPython, ok := yatest.BuildFlag("USE_SYSTEM_PYTHON")
	if !ok || useSystemPython == "" {
		t.Skipf("You should specify USE_SYSTEM_PYTHON")
		return
	}

	testsRoot := os.Getenv("TESTS_SANDBOX")
	if testsRoot == "" {
		if yatest.HasRAMDrive() {
			testsRoot = yatest.OutputRAMDrivePath("")
		} else {
			testsRoot = yatest.OutputPath("")
		}
	}

	preparedPythonPath := filepath.Join(testsRoot, "prepared_python")
	err := PreparePython(preparedPythonPath, t)
	require.NoError(t, err, "failed to prepare python")

	binariesPath := filepath.Join(testsRoot, "bin")
	err = PrepareBinaries(binariesPath)
	require.NoError(t, err, "failed to prepare binaries")

	sandboxDir := filepath.Join(testsRoot, "sandbox")

	pythonPaths := GetPythonPaths()
	pythonPaths = append(pythonPaths, preparedPythonPath)

	testPathsFilePath := path.Join(preparedPythonPath, "yt/wrapper/new_system_python_tests/test_paths.txt")
	testPathsBlob, err := ioutil.ReadFile(testPathsFilePath)
	require.NoError(t, err)

	testPaths := []string{}
	for _, testName := range strings.Fields(string(testPathsBlob)) {
		testPaths = append(testPaths, path.Join(preparedPythonPath, "yt/wrapper/tests", testName))
	}

	pytestArgs := []string{
		yatest.SourcePath("contrib/python/pytest/pytest.py"),
		"--junit-xml=" + yatest.OutputPath("junit.xml"),
		"--cache-clear",
		"--debug",
		"--verbose",
		"--durations=0",
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

	err = os.Setenv("PATH", strings.Join([]string{binariesPath, os.Getenv("PATH")}, ":"))
	require.NoError(t, err)

	cmdPytest.Env = append(
		os.Environ(),
		"PYTHONPATH="+strings.Join(pythonPaths, ":"),
		"LD_LIBRARY_PATH="+yatest.PythonLibPath(),
		"TESTS_SANDBOX="+sandboxDir,
		"YT_CAPTURE_STDERR_TO_FILE=1",
		"YT_ENABLE_VERBOSE_LOGGING=1",
	)

	t.Logf("env: %s", cmdPytest.Env)
	t.Logf("running %s", cmdPytest.String())

	if err = cmdPytest.Run(); err != nil {
		t.Errorf("running pytest command failed: %s", err)
	}

	require.NoError(t, os.RemoveAll(binariesPath))

	junitBlob, err := ioutil.ReadFile(yatest.OutputPath("junit.xml"))
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
