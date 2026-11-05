// Copyright 2024 Google LLC
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

// Package aead contains types for testing AEAD primitives.
package aead

import "github.com/tink-crypto/tink-go/v2/testutil"

// WycheproofSuite is a Wycheproof test suite for AEAD primitives.
type WycheproofSuite struct {
	testutil.WycheproofSuite
	TestGroups []*WycheproofGroup `json:"testGroups"`
}

// WycheproofGroup is a Wycheproof test group for AEAD primitives.
type WycheproofGroup struct {
	testutil.WycheproofGroup
	IvSize  uint32            `json:"ivSize"`
	KeySize uint32            `json:"keySize"`
	TagSize uint32            `json:"tagSize"`
	Type    string            `json:"type"`
	Tests   []*WycheproofCase `json:"tests"`
}

// WycheproofCase is a Wycheproof test case for AEAD primitives.
type WycheproofCase struct {
	testutil.WycheproofCase
	Aad testutil.HexBytes `json:"aad"`
	Ct  testutil.HexBytes `json:"ct"`
	Iv  testutil.HexBytes `json:"iv"`
	Key testutil.HexBytes `json:"key"`
	Msg testutil.HexBytes `json:"msg"`
	Tag testutil.HexBytes `json:"tag"`
}
