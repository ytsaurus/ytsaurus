// Copyright 2022 Google LLC
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

package aead

import (
	"fmt"
)

const (
	// aesGCMMaxPlaintextSize is the maximum plaintext size defined by RFC 5116.
	aesGCMMaxPlaintextSize = (1 << 36) - 31

	intSize             = 32 << (^uint(0) >> 63) // 32 or 64
	maxInt              = 1<<(intSize-1) - 1
	maxIntPlaintextSize = maxInt - AESGCMIVSize - AESGCMTagSize
)

// CheckPlaintextSize checks if the given plaintext size is valid for AES-GCM.
func CheckPlaintextSize(size uint64) error {
	var maxPlaintextSize uint64 = maxIntPlaintextSize
	if maxIntPlaintextSize > aesGCMMaxPlaintextSize {
		maxPlaintextSize = aesGCMMaxPlaintextSize
	}
	if size > maxPlaintextSize {
		return fmt.Errorf("plaintext too long: got %d", size)
	}
	return nil
}
