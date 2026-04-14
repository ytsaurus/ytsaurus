// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTraceIDEncoding(t *testing.T) {
	testCases := []struct {
		Name   string
		Input  TraceID
		Expect []byte
	}{
		{
			Name:   "trace id",
			Input:  TraceID{0x1},
			Expect: []byte(`"01000000000000000000000000000000"`),
		},
		{
			Name:   "empty trace id",
			Input:  TraceID{},
			Expect: []byte(`""`),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, runJSONEncodingTests(tc.Input, tc.Expect))
	}
}

func TestInvalidTraceID(t *testing.T) {
	testCases := []struct {
		Name      string
		Input     []byte
		ExpectErr error
	}{
		{
			Name:      "invalid length",
			Input:     []byte(`"0102030405060708090a0b0c0d0e0f1011"`),
			ExpectErr: errors.New("invalid length for ID"),
		},
		{
			Name:      "invalid hex id",
			Input:     []byte(`"234567890abcdefgh1234567890abcdef"`),
			ExpectErr: errors.New("cannot unmarshal ID from string"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			var id TraceID
			err := json.Unmarshal(tc.Input, &id)
			assert.ErrorContains(t, err, tc.ExpectErr.Error())
		})
	}
}

func TestSpanIDEncoding(t *testing.T) {
	testCases := []struct {
		Name   string
		Input  SpanID
		Expect []byte
	}{
		{
			Name:   "span id",
			Input:  SpanID{0x1},
			Expect: []byte(`"0100000000000000"`),
		},
		{
			Name:   "empty span id",
			Input:  SpanID{},
			Expect: []byte(`""`),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, runJSONEncodingTests(tc.Input, tc.Expect))
	}
}

func TestInvalidSpanID(t *testing.T) {
	testCases := []struct {
		Name      string
		Input     []byte
		ExpectErr error
	}{
		{
			Name:      "invalid length",
			Input:     []byte(`"0102030405060708090a0b0c0d0e0f1011"`),
			ExpectErr: errors.New("invalid length for ID"),
		},
		{
			Name:      "invalid hex id",
			Input:     []byte(`"234abcacgfdfgrty"`),
			ExpectErr: errors.New("cannot unmarshal ID from string"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			var id SpanID
			err := json.Unmarshal(tc.Input, &id)
			assert.ErrorContains(t, err, tc.ExpectErr.Error())
		})
	}
}
