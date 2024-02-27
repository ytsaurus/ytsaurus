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
	"io"
	"testing"
	"testing/iotest"

	"github.com/google/go-cmp/cmp"
	"github.com/matttproud/golang_protobuf_extensions/v2/testdata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestReadDelimitedIllegalVarint(t *testing.T) {
	var tests = []struct {
		in  []byte
		n   int
		err error
	}{
		{
			in:  []byte{255, 255, 255, 255, 255},
			n:   5,
			err: errInvalidVarint,
		},
		{
			in:  []byte{255, 255, 255, 255, 255, 255},
			n:   5,
			err: errInvalidVarint,
		},
	}
	for _, test := range tests {
		n, err := ReadDelimited(bytes.NewReader(test.in), nil)
		if got, want := n, test.n; !cmp.Equal(got, want) {
			t.Errorf("ReadDelimited(%#v, nil) = %#v, ?; want = %#v, ?", test.in, got, want)
		}
		if got, want := err, test.err; !errors.Is(got, want) {
			t.Errorf("ReadDelimited(%#v, nil) = ?, %#v; want = ?, %#v", test.in, got, want)
		}
	}
}

func TestReadDelimitedPrematureHeader(t *testing.T) {
	var data = []byte{128, 5} // 256 + 256 + 128
	n, err := ReadDelimited(bytes.NewReader(data[0:1]), nil)
	if got, want := n, 1; !cmp.Equal(got, want) {
		t.Errorf("ReadDelimited(%#v, nil) = %#v, ?; want = %#v, ?", data[0:1], got, want)
	}
	if got, want := err, io.EOF; !errors.Is(got, want) {
		t.Errorf("ReadDelimited(%#v, nil) = ?, %#v; want = ?, %#v", data[0:1], got, want)
	}
}

func TestReadDelimitedPrematureBody(t *testing.T) {
	var data = []byte{128, 5, 0, 0, 0} // 256 + 256 + 128
	n, err := ReadDelimited(bytes.NewReader(data[:]), nil)
	if got, want := n, 5; !cmp.Equal(got, want) {
		t.Errorf("ReadDelimited(%#v, nil) = %#v, ?; want = %#v, ?", data, got, want)
	}
	if got, want := err, io.ErrUnexpectedEOF; !errors.Is(got, want) {
		t.Errorf("ReadDelimited(%#v, nil) = ?, %#v; want = ?, %#v", data, got, want)
	}
}

func TestReadDelimitedPrematureHeaderIncremental(t *testing.T) {
	var data = []byte{128, 5} // 256 + 256 + 128
	n, err := ReadDelimited(iotest.OneByteReader(bytes.NewReader(data[0:1])), nil)
	if got, want := n, 1; !cmp.Equal(got, want) {
		t.Errorf("ReadDelimited(%#v, nil) = %#v, ?; want = %#v, ?", data[0:1], got, want)
	}
	if got, want := err, io.EOF; !errors.Is(got, want) {
		t.Errorf("ReadDelimited(%#v, nil) = ?, %#v; want = ?, %#v", data[0:1], got, want)
	}
}

func TestReadDelimitedPrematureBodyIncremental(t *testing.T) {
	var data = []byte{128, 5, 0, 0, 0} // 256 + 256 + 128
	n, err := ReadDelimited(iotest.OneByteReader(bytes.NewReader(data[:])), nil)
	if got, want := n, 5; !cmp.Equal(got, want) {
		t.Errorf("ReadDelimited(%#v, nil) = %#v, ?; want = %#v, ?", data, got, want)
	}
	if got, want := err, io.ErrUnexpectedEOF; !errors.Is(got, want) {
		t.Errorf("ReadDelimited(%#v, nil) = ?, %#v; want = ?, %#v", data, got, want)
	}
}

var _ io.Reader = (*firstReadNoop)(nil)

type firstReadNoop struct {
	read bool
	r    io.Reader
}

func (r *firstReadNoop) Read(p []byte) (int, error) {
	if !r.read {
		r.read = true
		return 0, nil
	}
	return r.r.Read(p)
}

func TestFirstNoop(t *testing.T) {
	r := &firstReadNoop{
		r: bytes.NewReader([]byte{6, 26, 4, 110, 111, 111, 112}),
	}
	var msg testdata.Record
	n, err := ReadDelimited(r, &msg)
	if got, want := n, 7; !cmp.Equal(got, want) {
		t.Errorf("ReadDelimited(r, &msg) n = %v, want %v", got, want)
	}
	if got, want := err, error(nil); !errors.Is(got, want) {
		t.Errorf("ReadDelimited(r, &msg) err = %v, want %v", got, want)
	}
	if got, want := &msg, (&testdata.Record{Third: proto.String("noop")}); !cmp.Equal(got, want, protocmp.Transform()) {
		t.Errorf("ReadDelimited(r, &msg) msg = %v, want %v", got, want)
	}
}
