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

// Package outputprefix provides constants and shared utility functions for
// computing the prefix applied to the output of a cryptographic function.
package outputprefix

import (
	"encoding/binary"
)

const (
	// nonRawPrefixSize is the prefix size of Tink and Legacy key types.
	nonRawPrefixSize = 5
	// legacyStartByte is the first byte of the prefix of legacy key types.
	legacyStartByte = byte(0)
	// tinkStartByte is the first byte of the prefix of Tink key types.
	tinkStartByte = byte(1)
)

// calculatePrefixBytes calculates the bytes prefixed to the output
// of a cryptographic function.
//
// The prefix of prefixSize bytes consists of a start byte and a 4-byte key id.
func calculatePrefixBytes(startByte byte, id uint32) []byte {
	prefix := make([]byte, nonRawPrefixSize)
	prefix[0] = startByte
	binary.BigEndian.PutUint32(prefix[1:], id)
	return prefix
}

// Tink returns the output prefix bytes from keyID for TINK keys.
func Tink(keyID uint32) []byte { return calculatePrefixBytes(tinkStartByte, keyID) }

// Legacy returns the output prefix bytes from keyID for LEGACY keys.
func Legacy(keyID uint32) []byte { return calculatePrefixBytes(legacyStartByte, keyID) }
