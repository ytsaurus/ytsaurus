// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// WycheproofSuite represents the common elements of the top level
// object in a Wycheproof json file. Implementations should embed
// WycheproofSuite in a struct that strongly types the testGroups
// field. See wycheproofutil_test.go for an example.
type WycheproofSuite struct {
	Algorithm     string            `json:"algorithm"`
	NumberOfTests int               `json:"numberOfTests"`
	Notes         map[string]string `json:"notes"`
}

// WycheproofGroup represents the common elements of a testGroups
// object in a Wycheproof suite. Implementations should embed
// WycheproofGroup in a struct that strongly types its list of cases.
// See wycheproofutil_test.go for an example.
type WycheproofGroup struct {
	Type string `json:"type"`
}

// WycheproofCase represents the common elements of a tests object
// in a Wycheproof group. Implementation should embed WycheproofCase
// in a struct that contains fields specific to the test type.
// See wycheproofutil_test.go for an example.
type WycheproofCase struct {
	CaseID  int      `json:"tcId"`
	Comment string   `json:"comment"`
	Result  string   `json:"result"`
	Flags   []string `json:"flags"`
}

// HexBytes is a helper type for unmarshalling a byte sequence represented as a
// hex encoded string.
type HexBytes []byte

// UnmarshalText converts a hex encoded string into a sequence of bytes.
func (a *HexBytes) UnmarshalText(text []byte) error {
	decoded, err := hex.DecodeString(string(text))
	if err != nil {
		return err
	}

	*a = decoded
	return nil
}

const testvectorsDir = "testvectors"

var (
	// This is populated in init() depending on whether the test is running with
	// Bazel or not.
	wycheproofDir string
)

// PopulateSuite opens filename from the Wycheproof test vectors directory and
// populates suite with the decoded JSON data.
func PopulateSuite(suite any, filename string) error {
	f, err := os.Open(filepath.Join(wycheproofDir, testvectorsDir, filename))
	if err != nil {
		return err
	}
	parser := json.NewDecoder(f)
	if err := parser.Decode(suite); err != nil {
		return err
	}
	return nil
}

// Wycheproof version to fetch.
const wycheproofModVer = "v0.0.0-20250901140545-b51abcfb8daf"

// downloadWycheproofTestVectors downloads the JSON test files from
// the Wycheproof repository with `go mod download -json` and returns the
// absolute path to the root of the downloaded source tree.
func downloadWycheproofTestVectors() (string, error) {
	path := "github.com/C2SP/wycheproof@" + wycheproofModVer
	cmd := exec.Command("go", "mod", "download", "-json", path)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to run `go mod download -json %s`, output: %s", path, output)
	}
	var dm struct {
		Dir string // absolute path to cached source root directory
	}
	if err := json.Unmarshal(output, &dm); err != nil {
		return "", err
	}
	return dm.Dir, nil
}

const testdataDir = "testdata"

func init() {
	srcDir, ok := os.LookupEnv("TEST_SRCDIR")
	if ok {
		// If running with `bazel test` TEST_WORKSPACE is set.
		// We don't panic if TEST_WORKSPACE is not set to allow running benchmarks
		// internally at Google, which set TEST_SRCDIR but not TEST_WORKSPACE.
		wycheproofDir = filepath.Join(srcDir, os.Getenv("TEST_WORKSPACE"), testdataDir)
	} else {
		// Running tests with `go test`.
		var err error
		wycheproofDir, err = downloadWycheproofTestVectors()
		if err != nil {
			panic(err)
		}
	}
}
