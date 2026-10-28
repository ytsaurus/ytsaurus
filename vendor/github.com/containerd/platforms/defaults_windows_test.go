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
	"fmt"
	"reflect"
	"runtime"
	"testing"

	imagespec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/windows"
)

func TestDefault(t *testing.T) {
	major, minor, build := windows.RtlGetNtVersionNumbers()
	expected := imagespec.Platform{
		OS:           runtime.GOOS,
		Architecture: runtime.GOARCH,
		OSVersion:    fmt.Sprintf("%d.%d.%d", major, minor, build),
		Variant:      cpuVariant(),
	}
	p := DefaultSpec()
	if !reflect.DeepEqual(p, expected) {
		t.Fatalf("default platform not as expected: %#v != %#v", p, expected)
	}

	s := DefaultString()
	if s != FormatAll(p) {
		t.Fatalf("default specifier should match formatted default spec: %v != %v", s, p)
	}
}

func TestDefaultMatchComparer(t *testing.T) {
	defaultMatcher := Default()

	for _, test := range []struct {
		platform imagespec.Platform
		match    bool
	}{
		{
			platform: DefaultSpec(),
			match:    true,
		},
		{
			platform: imagespec.Platform{
				OS:           "linux",
				Architecture: runtime.GOARCH,
			},
			match: false,
		},
	} {
		if actual := defaultMatcher.Match(test.platform); actual != test.match {
			t.Errorf("expected: %v, actual: %v", test.match, actual)
		}
	}
}

func TestMatchComparerMatch_WCOW(t *testing.T) {
	major, minor, build := windows.RtlGetNtVersionNumbers()
	buildStr := fmt.Sprintf("%d.%d.%d", major, minor, build)
	platform := DefaultSpec()
	m := NewMatcher(platform)

	for _, test := range []struct {
		platform imagespec.Platform
		match    bool
	}{
		{
			platform: DefaultSpec(),
			match:    true,
		},
		{
			platform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				OSVersion:    buildStr + ".1",
			},
			match: true,
		},
		{
			platform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				OSVersion:    buildStr + ".2",
			},
			match: true,
		},
		{
			platform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				// Use an nonexistent Windows build so we don't get a match. Ws2019's build is 17763/
				OSVersion: "10.0.17762.1",
			},
			match: false,
		},
		{
			platform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				// Use an nonexistent Windows build so we don't get a match. Ws2019's build is 17763/
				OSVersion: "10.0.17764.1",
			},
			match: false,
		},
		{
			platform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
			},
			match: true,
		},
		{
			platform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "linux",
			},
			match: false,
		},
	} {
		if actual := m.Match(test.platform); actual != test.match {
			t.Errorf("should match: %t, %s to %s", test.match, platform, test.platform)
		}
	}
}

func TestMatchComparerMatch_LCOW(t *testing.T) {
	major, minor, build := windows.RtlGetNtVersionNumbers()
	buildStr := fmt.Sprintf("%d.%d.%d", major, minor, build)

	pLinux := imagespec.Platform{OS: "linux", Architecture: "amd64"}
	m := NewMatcher(pLinux)
	for _, test := range []struct {
		platform imagespec.Platform
		match    bool
	}{
		{
			platform: DefaultSpec(),
			match:    false,
		},
		{
			platform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
			},
			match: false,
		},
		{
			platform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				OSVersion:    buildStr + ".2",
			},
			match: false,
		},
		{
			platform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "windows",
				// Use an nonexistent Windows build so we don't get a match. Ws2019's build is 17763/
				OSVersion: "10.0.17762.1",
			},
			match: false,
		},
		{
			platform: imagespec.Platform{
				Architecture: "amd64",
				OS:           "linux",
			},
			match: true,
		},
	} {
		if actual := m.Match(test.platform); actual != test.match {
			t.Errorf("should match: %t, %s to %s", test.match, pLinux, test.platform)
		}
	}
}
