// Copyright 2016 Matt T. Proud
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

package pbutil

import (
	"bytes"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/matttproud/golang_protobuf_extensions/v2/testdata"
	"google.golang.org/protobuf/proto"
)

func TestWriteDelimitedMarshalErr(t *testing.T) {
	// data will not successfully marshal due to required field not being set,
	// but with the way that package proto is designed at the moment this only
	// matches on a coarse-grained error type.
	data := new(testdata.Required)
	var buf bytes.Buffer
	n, err := WriteDelimited(&buf, data)
	if got, want := n, 0; !cmp.Equal(got, want) {
		t.Errorf("WriteDelimited(buf, %#v) = %#v, ?; want = %#v, ?", data, got, want)
	}
	if got, want := err, proto.Error; !errors.Is(got, want) {
		t.Errorf("WriteDelimited(buf, %#v) = ?, %#v; want = ?, %#v", data, got, want)
	}
}

var errWrite = errors.New("pbutil: can't write")

type cantWrite struct{}

func (cantWrite) Write([]byte) (int, error) { return 0, errWrite }

func TestWriteDelimitedWriteErr(t *testing.T) {
	data := new(testdata.Record)
	var buf cantWrite
	n, err := WriteDelimited(buf, data)
	if got, want := n, 0; !cmp.Equal(got, want) {
		t.Errorf("WriteDelimited(buf, %#v) = %#v, ?; want = %#v, ?", data, got, want)
	}
	if got, want := err, errWrite; !errors.Is(got, want) {
		t.Errorf("WriteDelimited(buf, %#v) = ?, %#v; want = ?, %#v", data, got, want)
	}
}
