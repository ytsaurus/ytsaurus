// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package telemetry

import "testing"

func TestProtoInt64Encoding(t *testing.T) {
	testCases := []struct {
		Name   string
		Input  []byte
		Expect protoInt64
	}{
		{
			Name:   "string negative int64",
			Input:  []byte(`"-123"`),
			Expect: protoInt64(-123),
		},
		{
			Name:   "string positive int64",
			Input:  []byte(`"123"`),
			Expect: protoInt64(123),
		},
		{
			Name:   "negative int64",
			Input:  []byte(`-123`),
			Expect: protoInt64(-123),
		},
		{
			Name:   "positive int64",
			Input:  []byte(`123`),
			Expect: protoInt64(123),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, runJSONUnmarshalTest(tc.Expect, tc.Input))
	}
}

func TestProtoUint64(t *testing.T) {
	testCases := []struct {
		Name   string
		Input  []byte
		Expect protoUint64
	}{
		{
			Name:   "string uint64",
			Input:  []byte(`"1"`),
			Expect: protoUint64(1),
		},
		{
			Name:   "uint64",
			Input:  []byte(`1`),
			Expect: protoUint64(1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, runJSONUnmarshalTest(tc.Expect, tc.Input))
	}
}
