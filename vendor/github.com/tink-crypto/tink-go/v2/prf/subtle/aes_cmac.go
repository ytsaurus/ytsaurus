// Copyright 2020 Google LLC
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

package subtle

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/mac/aescmac"

	// Placeholder for internal crypto/cipher allowlist, please ignore.
	// Placeholder for internal crypto/subtle allowlist, please ignore.
)

// AESCMACPRF is a type that can be used to compute several CMACs with the same
// key material.
type AESCMACPRF struct {
	cmac *aescmac.CMAC
}

// NewAESCMACPRF creates a new AESCMACPRF object and initializes it with the
// correct key material.
func NewAESCMACPRF(key []byte) (*AESCMACPRF, error) {
	cmac, err := aescmac.New(key)
	if err != nil {
		return nil, fmt.Errorf("aescmacprf: %v", err)
	}
	return &AESCMACPRF{cmac: cmac}, nil
}

// ValidateAESCMACPRFParams checks that the key is the recommended size for
// AES-CMAC.
func ValidateAESCMACPRFParams(keySize uint32) error {
	if keySize != 32 {
		return fmt.Errorf("aescmacprf: got key size %d, want recommended size 32", keySize)
	}
	return nil
}

// ComputePRF computes the AES-CMAC for the given key and data, returning
// outputLength bytes.
//
// The timing of this function will only depend on len(data), and not leak any
// additional information about the key or the data.
func (a AESCMACPRF) ComputePRF(data []byte, outputLength uint32) ([]byte, error) {
	if outputLength > aescmac.BlockSize {
		return nil, fmt.Errorf("aescmacprf: invalid output length %d, want between 0 and %d", outputLength, aescmac.BlockSize)
	}
	return a.cmac.Compute(data)[:outputLength], nil
}
