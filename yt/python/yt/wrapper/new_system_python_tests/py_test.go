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

func PreparePython(preparedPythonPath string) error {
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
		return fmt.Errorf("Prepare source tree failed: %s", err)
	}
	return nil
}

func TestPyTest(t *testing.T) {
	var err error
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	testsRoot := os.Getenv("TESTS_SANDBOX")
	if testsRoot == "" {
		testsRoot = yatest.OutputRAMDrivePath("")
	}

	preparedPythonPath := filepath.Join(testsRoot, "prepared_python")
	err = PreparePython(preparedPythonPath)
	if err != nil {
		t.Logf("Failed to prepare python: %s", err)
		return
	}

	binariesPath := filepath.Join(testsRoot, "bin")
	err = PrepareBinaries(binariesPath)
	if err != nil {
		t.Logf("Failed to prepare python: %s", err)
		return
	}

	sandboxDir := filepath.Join(testsRoot, "sandbox")

	pythonPaths := GetPythonPaths()
	pythonPaths = append(pythonPaths, preparedPythonPath)

	testPathsFilePath := path.Join(preparedPythonPath, "yt/wrapper/system_python_tests/test_paths.txt")
	testPathsFile, err := os.Open(testPathsFilePath)
	if err != nil {
		t.Logf("Failed to open %s: %s", testPathsFilePath, err)
		return
	}
	defer testPathsFile.Close()

	data, err := ioutil.ReadAll(testPathsFile)
	if err != nil {
		t.Logf("Failed to read %s: %s", testPathsFilePath, err)
		return
	}

	testPaths := []string{}
	for _, testName := range strings.Fields(string(data)) {
		testPaths = append(testPaths, path.Join(preparedPythonPath, "yt/wrapper/tests", testName))
	}

	pytestArgs := []string{
		yatest.SourcePath("contrib/python/pytest/pytest.py"),
		"--junit-xml=" + yatest.OutputPath("junit.xml"),
		"--cache-clear",
		"--debug",
		"--verbose",
		"--verbose",
		"--process-count", "6",
	}
	pytestArgs = append(pytestArgs, testPaths...)

	cmdPytest := exec.Command(
		yatest.PythonBinPath(),
		pytestArgs...,
	)
	stdout.Reset()
	stderr.Reset()
	cmdPytest.Stdout = &stdout
	cmdPytest.Stderr = &stderr

	err = os.Setenv("PATH", strings.Join([]string{binariesPath, os.Getenv("PATH")}, ":"))
	if err != nil {
		t.Logf("Failed to setenv: %s", err)
		return
	}
	cmdPytest.Env = append(
		os.Environ(),
		"PYTHONPATH="+strings.Join(pythonPaths, ":"),
		"LD_LIBRARY_PATH="+yatest.PythonLibPath(),
		"TESTS_SANDBOX="+sandboxDir,
		"YT_CAPTURE_STDERR_TO_FILE=1",
		"YT_ENABLE_VERBOSE_LOGGING=1",
	)

	t.Logf("Env: %s", cmdPytest.Env)
	t.Logf("Running %s", cmdPytest.String())

	err = cmdPytest.Run()

	t.Logf("Pytest command stdout: %s", stdout.String())
	t.Logf("Pytest command stderr: %s", stderr.String())

	if err != nil {
		t.Logf("Running pytest command failed: %s", err)
	}

	xmlFile, err := os.Open(yatest.OutputPath("junit.xml"))
	if err != nil {
		t.Fatal(err)
	}
	defer xmlFile.Close()

	byteValue, _ := ioutil.ReadAll(xmlFile)

	var testSuite Testsuite
	err = xml.Unmarshal(byteValue, &testSuite)
	if err != nil {
		t.Fatal(err)
	}

	for _, testcase := range testSuite.Testcases {
		t.Run(
			fmt.Sprintf("%s::%s", testcase.Classname, testcase.Name),
			func(t *testing.T) {
				var testCaseError = testcase.Failure.Text
				if testCaseError != "" {
					fmt.Print(testCaseError)
					t.Fail()
				}
			},
		)
	}
}
