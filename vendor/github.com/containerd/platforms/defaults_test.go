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
	"runtime"
	"sort"
	"testing"

	imagespec "github.com/opencontainers/image-spec/specs-go/v1"
)

// TestMatchComparerMatch_ABICheckWCOW checks windows platform matcher
// behavior for stable ABI and non-stable ABI compliant versions
func TestMatchComparerMatch_ABICheckWCOW(t *testing.T) {
	platformNoVersion := imagespec.Platform{
		Architecture: "amd64",
		OS:           "windows",
	}
	platformWS2019 := imagespec.Platform{
		Architecture: "amd64",
		OS:           "windows",
		OSVersion:    "10.0.17763",
	}
	platformWS2022 := imagespec.Platform{
		Architecture: "amd64",
		OS:           "windows",
		OSVersion:    "10.0.20348",
	}
	platformWindows11 := imagespec.Platform{
		Architecture: "amd64",
		OS:           "windows",
		OSVersion:    "10.0.22621",
	}

	for _, test := range []struct {
		hostPlatform imagespec.Platform
		testPlatform imagespec.Platform
		match        bool
	}{
		{
			hostPlatform: platformWS2019,
			testPlatform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				OSVersion:    "10.0.17763",
			},
			match: true,
		},
		{
			hostPlatform: platformWS2019,
			testPlatform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				OSVersion:    "10.0.20348",
			},
			match: false,
		},
		{
			hostPlatform: platformWS2022,
			testPlatform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				OSVersion:    "10.0.17763",
			},
			match: false,
		},
		{
			hostPlatform: platformWS2022,
			testPlatform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				OSVersion:    "10.0.20348",
			},
			match: true,
		},
		{
			hostPlatform: platformWindows11,
			testPlatform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				OSVersion:    "10.0.17763",
			},
			match: false,
		},
		{
			hostPlatform: platformWindows11,
			testPlatform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				OSVersion:    "10.0.20348",
			},
			match: true,
		},
		{
			hostPlatform: platformNoVersion,
			testPlatform: platformWS2019,
			match:        true,
		},
		{
			hostPlatform: platformNoVersion,
			testPlatform: platformNoVersion,
			match:        true,
		},
		{
			hostPlatform: platformNoVersion,
			testPlatform: platformWindows11,
			match:        true,
		},
	} {
		matcher := NewMatcher(test.hostPlatform)
		if actual := matcher.Match(test.testPlatform); actual != test.match {
			t.Errorf("should match: %t, test(%s) to host(%s)", test.match, test.hostPlatform, test.testPlatform)
		}
	}
}

func TestWindowsMatchComparerLess(t *testing.T) {
	p := imagespec.Platform{
		Architecture: "amd64",
		OS:           "windows",
		OSVersion:    "10.0.17763.1",
	}

	m := NewMatcher(p)
	if runtime.GOOS != "windows" {
		// By default NewMatcher only returns the MatchComparer interface on Windows (which is only for backwards compatibility).
		// On other platforms, we need to wrap the matcher in a windowsMatchComparer since the test is using it.
		m = &windowsMatchComparer{m}
	}
	platforms := []imagespec.Platform{
		{
			Architecture: "amd64",
			OS:           "windows",
			OSVersion:    "10.0.17764.1",
		},
		{
			Architecture: "amd64",
			OS:           "windows",
		},
		{
			Architecture: "amd64",
			OS:           "windows",
			OSVersion:    "10.0.17763.1",
		},
		{
			Architecture: "amd64",
			OS:           "windows",
			OSVersion:    "10.0.17763.2",
		},
		{
			Architecture: "amd64",
			OS:           "windows",
			OSVersion:    "10.0.17762.1",
		},
	}
	expected := []imagespec.Platform{
		{
			Architecture: "amd64",
			OS:           "windows",
			OSVersion:    "10.0.17763.2",
		},
		{
			Architecture: "amd64",
			OS:           "windows",
			OSVersion:    "10.0.17763.1",
		},
		{
			Architecture: "amd64",
			OS:           "windows",
		},
		{
			Architecture: "amd64",
			OS:           "windows",
			OSVersion:    "10.0.17764.1",
		},
		{
			Architecture: "amd64",
			OS:           "windows",
			OSVersion:    "10.0.17762.1",
		},
	}
	sort.SliceStable(platforms, func(i, j int) bool {
		return m.(MatchComparer).Less(platforms[i], platforms[j])
	})
	if !reflect.DeepEqual(platforms, expected) {
		t.Errorf("expected: %s\nactual  : %s", expected, platforms)
	}
}
