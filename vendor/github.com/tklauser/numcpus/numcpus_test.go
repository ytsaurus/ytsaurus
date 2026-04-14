// Copyright 2019-2020 Tobias Klauser
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package numcpus_test

import (
	"bytes"
	"errors"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/tklauser/numcpus"
)

func testGetconf(t *testing.T, got int, name, getconfWhich string) {
	t.Helper()

	getconf, err := exec.LookPath("getconf")
	if err != nil {
		t.Skipf("getconf not found in PATH: %v", err)
	}

	cmd := exec.Command(getconf, getconfWhich)
	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Skipf("failed to run %v: %v (%v)", cmd, err, strings.TrimSpace(errb.String()))
	}

	want, err := strconv.ParseInt(strings.TrimSpace(outb.String()), 10, 64)
	if err != nil {
		t.Skipf("failed to parse getconf output: %v", err)
	}

	if got != int(want) {
		t.Errorf("%s returned %v, want %v (getconf %s)", name, got, want, getconfWhich)
	}
}

func confName(name string) string {
	switch runtime.GOOS {
	case "illumos", "netbsd", "openbsd", "solaris":
		return strings.TrimPrefix(name, "_")
	}
	return name
}

func TestGetConfigured(t *testing.T) {
	n, err := numcpus.GetConfigured()
	if errors.Is(err, numcpus.ErrNotSupported) {
		t.Skipf("GetConfigured not supported on %s", runtime.GOOS)
	} else if err != nil {
		t.Fatalf("GetConfigured: %v", err)
	}
	t.Logf("Configured = %v", n)

	testGetconf(t, n, "GetConfigured", confName("_NPROCESSORS_CONF"))
}

func TestGetKernelMax(t *testing.T) {
	n, err := numcpus.GetKernelMax()
	if errors.Is(err, numcpus.ErrNotSupported) {
		t.Skipf("GetKernelMax not supported on %s", runtime.GOOS)
	} else if err != nil {
		t.Fatalf("GetKernelMax: %v", err)
	}
	t.Logf("KernelMax = %v", n)
}

func testNumAndList(t *testing.T, name string, get func() (int, error), list func() ([]int, error)) int {
	t.Helper()

	n, errGet := get()
	if errors.Is(errGet, numcpus.ErrNotSupported) {
		t.Logf("Get%s not supported on %s", name, runtime.GOOS)
	} else if errGet != nil {
		t.Errorf("Get%s: %v", name, errGet)
	} else {
		t.Logf("%s = %v", name, n)
	}

	l, errList := list()
	if errors.Is(errList, numcpus.ErrNotSupported) {
		t.Skipf("List%s not supported on %s", name, runtime.GOOS)
	} else if errList != nil {
		t.Fatalf("List%s: %v", name, errList)
	}
	t.Logf("List%s = %v", name, l)

	if errGet == nil && len(l) != n {
		t.Errorf("number of online CPUs in list %v doesn't match expected number of CPUs %d", l, n)
	}

	return n
}

func TestOffline(t *testing.T) {
	testNumAndList(t, "Offline", numcpus.GetOffline, numcpus.ListOffline)
}

func TestOnline(t *testing.T) {
	n := testNumAndList(t, "Online", numcpus.GetOnline, numcpus.ListOnline)

	testGetconf(t, n, "GetOnline", confName("_NPROCESSORS_ONLN"))
}

func TestPossible(t *testing.T) {
	testNumAndList(t, "Possible", numcpus.GetPossible, numcpus.ListPossible)
}

func TestPresent(t *testing.T) {
	testNumAndList(t, "Present", numcpus.GetPresent, numcpus.ListPresent)
}
