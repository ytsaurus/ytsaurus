/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package platforms

import (
	"reflect"
	"testing"

	specs "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestNormalize(t *testing.T) {
	s := DefaultSpec()
	n := Normalize(DefaultSpec())
	if !reflect.DeepEqual(s, n) {
		t.Errorf("Normalize returned %+v, expected %+v", n, s)
	}
}

func TestFallbackOnOSVersion(t *testing.T) {
	p := specs.Platform{
		OS:           "windows",
		Architecture: "amd64",
		OSVersion:    "99.99.99.99",
	}

	other := specs.Platform{OS: p.OS, Architecture: p.Architecture}

	m := NewMatcher(p)
	if !m.Match(other) {
		t.Errorf("Expected %+v to match", other)
	}
}
