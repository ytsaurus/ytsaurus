// Copyright 2013 Matt T. Proud
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

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/google/go-cmp/cmp"
	"github.com/matttproud/golang_protobuf_extensions/v2/testdata"
)

func TestWriteDelimited(t *testing.T) {
	for _, test := range []struct {
		name string
		msg  proto.Message
		buf  []byte
		n    int
		err  error
	}{
		{
			name: "empty",
			msg:  new(testdata.Record),
			n:    1,
			buf:  []byte{0},
		},
		{
			name: "firstfield",
			msg:  &testdata.Record{First: proto.Uint64(1)},
			n:    3,
			buf:  []byte{2, 8, 1},
		},
		{
			name: "thirdfield",
			msg: &testdata.Record{
				Third: proto.String(`This is my gigantic, unhappy string.  It exceeds
the encoding size of a single byte varint.  We are using it to fuzz test the
correctness of the header decoding mechanisms, which may prove problematic.
I expect it may.  Let's hope you enjoy testing as much as we do.`),
			},
			n: 271,
			buf: []byte{141, 2, 26, 138, 2, 84, 104, 105, 115, 32, 105, 115, 32, 109,
				121, 32, 103, 105, 103, 97, 110, 116, 105, 99, 44, 32, 117, 110, 104,
				97, 112, 112, 121, 32, 115, 116, 114, 105, 110, 103, 46, 32, 32, 73,
				116, 32, 101, 120, 99, 101, 101, 100, 115, 10, 116, 104, 101, 32, 101,
				110, 99, 111, 100, 105, 110, 103, 32, 115, 105, 122, 101, 32, 111, 102,
				32, 97, 32, 115, 105, 110, 103, 108, 101, 32, 98, 121, 116, 101, 32,
				118, 97, 114, 105, 110, 116, 46, 32, 32, 87, 101, 32, 97, 114, 101, 32,
				117, 115, 105, 110, 103, 32, 105, 116, 32, 116, 111, 32, 102, 117, 122,
				122, 32, 116, 101, 115, 116, 32, 116, 104, 101, 10, 99, 111, 114, 114,
				101, 99, 116, 110, 101, 115, 115, 32, 111, 102, 32, 116, 104, 101, 32,
				104, 101, 97, 100, 101, 114, 32, 100, 101, 99, 111, 100, 105, 110, 103,
				32, 109, 101, 99, 104, 97, 110, 105, 115, 109, 115, 44, 32, 119, 104,
				105, 99, 104, 32, 109, 97, 121, 32, 112, 114, 111, 118, 101, 32, 112,
				114, 111, 98, 108, 101, 109, 97, 116, 105, 99, 46, 10, 73, 32, 101, 120,
				112, 101, 99, 116, 32, 105, 116, 32, 109, 97, 121, 46, 32, 32, 76, 101,
				116, 39, 115, 32, 104, 111, 112, 101, 32, 121, 111, 117, 32, 101, 110,
				106, 111, 121, 32, 116, 101, 115, 116, 105, 110, 103, 32, 97, 115, 32,
				109, 117, 99, 104, 32, 97, 115, 32, 119, 101, 32, 100, 111, 46},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			// TODO: Split out error arm in next patch version.
			if n, err := WriteDelimited(&buf, test.msg); !cmp.Equal(n, test.n) || !errors.Is(err, test.err) {
				t.Errorf("WriteDelimited(buf, %#v) = %v, %v; want %v, %v", test.msg, n, err, test.n, test.err)
			}
			if out := buf.Bytes(); !cmp.Equal(out, test.buf) {
				t.Errorf("WriteDelimited(buf, %#v); buf = %v; want %v", test.msg, out, test.buf)
			}
		})
	}
}

func TestReadDelimited(t *testing.T) {
	for _, test := range []struct {
		name string
		buf  []byte
		msg  proto.Message
		n    int
		err  error
	}{
		{
			name: "empty",
			buf:  []byte{0},
			msg:  new(testdata.Record),
			n:    1,
		},
		{
			name: "firstfield",
			n:    3,
			buf:  []byte{2, 8, 1},
			msg:  &testdata.Record{First: proto.Uint64(1)},
		},
		{
			name: "thirdfield",
			buf: []byte{141, 2, 26, 138, 2, 84, 104, 105, 115, 32, 105, 115, 32, 109,
				121, 32, 103, 105, 103, 97, 110, 116, 105, 99, 44, 32, 117, 110, 104,
				97, 112, 112, 121, 32, 115, 116, 114, 105, 110, 103, 46, 32, 32, 73,
				116, 32, 101, 120, 99, 101, 101, 100, 115, 10, 116, 104, 101, 32, 101,
				110, 99, 111, 100, 105, 110, 103, 32, 115, 105, 122, 101, 32, 111, 102,
				32, 97, 32, 115, 105, 110, 103, 108, 101, 32, 98, 121, 116, 101, 32,
				118, 97, 114, 105, 110, 116, 46, 32, 32, 87, 101, 32, 97, 114, 101, 32,
				117, 115, 105, 110, 103, 32, 105, 116, 32, 116, 111, 32, 102, 117, 122,
				122, 32, 116, 101, 115, 116, 32, 116, 104, 101, 10, 99, 111, 114, 114,
				101, 99, 116, 110, 101, 115, 115, 32, 111, 102, 32, 116, 104, 101, 32,
				104, 101, 97, 100, 101, 114, 32, 100, 101, 99, 111, 100, 105, 110, 103,
				32, 109, 101, 99, 104, 97, 110, 105, 115, 109, 115, 44, 32, 119, 104,
				105, 99, 104, 32, 109, 97, 121, 32, 112, 114, 111, 118, 101, 32, 112,
				114, 111, 98, 108, 101, 109, 97, 116, 105, 99, 46, 10, 73, 32, 101, 120,
				112, 101, 99, 116, 32, 105, 116, 32, 109, 97, 121, 46, 32, 32, 76, 101,
				116, 39, 115, 32, 104, 111, 112, 101, 32, 121, 111, 117, 32, 101, 110,
				106, 111, 121, 32, 116, 101, 115, 116, 105, 110, 103, 32, 97, 115, 32,
				109, 117, 99, 104, 32, 97, 115, 32, 119, 101, 32, 100, 111, 46},
			msg: &testdata.Record{
				Third: proto.String(`This is my gigantic, unhappy string.  It exceeds
the encoding size of a single byte varint.  We are using it to fuzz test the
correctness of the header decoding mechanisms, which may prove problematic.
I expect it may.  Let's hope you enjoy testing as much as we do.`),
			},
			n: 271,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := proto.Clone(test.msg)
			proto.Reset(msg)
			// TODO: Split out error arm in next patch version.
			if n, err := ReadDelimited(bytes.NewBuffer(test.buf), msg); !cmp.Equal(n, test.n) || !errors.Is(err, test.err) {
				t.Errorf("ReadDelimited(%v, msg) = %v, %v; want %v, %v", test.buf, n, err, test.n, test.err)
			}
			if !cmp.Equal(msg, test.msg, protocmp.Transform()) {
				t.Errorf("ReadDelimited(%v, msg); msg = %v; want %v", test.buf, msg, test.msg)
			}
		})
	}
}

func TestEndToEndValid(t *testing.T) {
	for _, test := range []struct {
		name string
		data []proto.Message
	}{
		{
			name: "empty",
			data: []proto.Message{new(testdata.Record)},
		},
		{
			name: "simpleseq",
			data: []proto.Message{&testdata.Record{First: proto.Uint64(1)}, new(testdata.Record), &testdata.Record{First: proto.Uint64(1)}},
		},
		{
			name: "singleton",
			data: []proto.Message{&testdata.Record{First: proto.Uint64(1)}},
		},
		{
			name: "headerlength",
			data: []proto.Message{&testdata.Record{
				Third: proto.String(`This is my gigantic, unhappy string.  It exceeds
the encoding size of a single byte varint.  We are using it to fuzz test the
correctness of the header decoding mechanisms, which may prove problematic.
I expect it may.  Let's hope you enjoy testing as much as we do.`),
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			var written int
			for i, msg := range test.data {
				n, err := WriteDelimited(&buf, msg)
				if err != nil {
					// Assumption: TestReadDelimited and TestWriteDelimited are sufficient
					//             and inputs for this test are explicitly exercised there.
					t.Fatalf("WriteDelimited(buf, %v[%d]) = ?, %v; wanted ?, nil", test.data, i, err)
				}
				written += n
			}
			var read int
			for i, msg := range test.data {
				out := proto.Clone(msg)
				proto.Reset(out)
				n, err := ReadDelimited(&buf, out)
				read += n
				if !cmp.Equal(out, msg, protocmp.Transform()) {
					t.Errorf("out = %v; want %v[%d] = %#v", out, test, i, msg)
				}
				if got, want := err, error(nil); !errors.Is(got, want) {
					t.Errorf("err = %v, want %v", got, want)
				}
			}
			if read != written {
				t.Errorf("%v read = %d; want %d", test, read, written)
			}
		})
	}
}
