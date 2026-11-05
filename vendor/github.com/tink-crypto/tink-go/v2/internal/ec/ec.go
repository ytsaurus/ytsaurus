// Copyright 2025 Google LLC
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

// Package ec provides utility functions for Elliptic Curves.
package ec

import "fmt"

// BigIntBytesToFixedSizeBuffer converts a big integer representation to a
// fixed size buffer.
//
// If the bytes representation is smaller, it is padded with leading zeros.
// If the bytes representation is larger, the leading bytes are removed.
// If the bytes representation is larger than the given size, an error is
// returned.
func BigIntBytesToFixedSizeBuffer(bigIntBytes []byte, size int) ([]byte, error) {
	// Nothing to do if the big integer representation is already of the given size.
	if len(bigIntBytes) == size {
		return bigIntBytes, nil
	}
	if len(bigIntBytes) < size {
		// Pad the big integer representation with leading zeros to the given size.
		buf := make([]byte, size-len(bigIntBytes), size)
		return append(buf, bigIntBytes...), nil
	}
	// Remove the leading len(bigIntValue)-size bytes. Fail if any is not zero.
	for i := 0; i < len(bigIntBytes)-size; i++ {
		if bigIntBytes[i] != 0 {
			return nil, fmt.Errorf("big int has invalid size: %v, want %v", len(bigIntBytes)-i, size)
		}
	}
	return bigIntBytes[len(bigIntBytes)-size:], nil
}
