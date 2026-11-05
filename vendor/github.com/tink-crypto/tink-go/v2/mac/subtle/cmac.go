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
	"crypto/subtle"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/mac/aescmac"

	// Placeholder for internal crypto/subtle allowlist, please ignore.
)

const (
	minCMACKeySizeInBytes         = 16
	recommendedCMACKeySizeInBytes = uint32(32)
	minTagLengthInBytes           = uint32(10)
	maxTagLengthInBytes           = aescmac.BlockSize
)

// AESCMAC represents an AES-CMAC struct that implements the MAC interface.
type AESCMAC struct {
	cmac      *aescmac.CMAC
	tagLength uint32
}

// NewAESCMAC creates a new AESCMAC object that implements the MAC interface.
func NewAESCMAC(key []byte, tagLength uint32) (*AESCMAC, error) {
	if len(key) < minCMACKeySizeInBytes {
		return nil, fmt.Errorf("aescmac: invalid key size %d, want at least %d", len(key), minCMACKeySizeInBytes)
	}
	if tagLength < minTagLengthInBytes {
		return nil, fmt.Errorf("aescmac: invalid tag length %d, want at least %d", tagLength, minTagLengthInBytes)
	}
	if tagLength > maxTagLengthInBytes {
		return nil, fmt.Errorf("aescmac: invalid tag length %d, want at most %d", tagLength, maxTagLengthInBytes)
	}
	cmac, err := aescmac.New(key)
	if err != nil {
		return nil, fmt.Errorf("aescmac: %v", err)
	}
	return &AESCMAC{cmac: cmac, tagLength: tagLength}, nil
}

// ComputeMAC computes message authentication code (MAC) for code data.
func (a AESCMAC) ComputeMAC(data []byte) ([]byte, error) {
	return a.cmac.Compute(data)[:a.tagLength], nil
}

// VerifyMAC returns nil if mac is a correct authentication code (MAC) for data,
// otherwise it returns an error.
func (a AESCMAC) VerifyMAC(mac, data []byte) error {
	computed := a.cmac.Compute(data)[:a.tagLength]
	if subtle.ConstantTimeCompare(mac, computed) != 1 {
		return fmt.Errorf("aescmac: invalid MAC")
	}
	return nil
}

// ValidateCMACParams validates the parameters for an AES-CMAC against the
// recommended parameters.
func ValidateCMACParams(keySize, tagSize uint32) error {
	if keySize != recommendedCMACKeySizeInBytes {
		return fmt.Errorf("aescmac: key size %d is not the recommended size %d", keySize, recommendedCMACKeySizeInBytes)
	}
	if tagSize < minTagLengthInBytes {
		return fmt.Errorf("aescmac: invalid tag length %d, want at least %d", tagSize, minTagLengthInBytes)
	}
	if tagSize > maxTagLengthInBytes {
		return fmt.Errorf("aescmac: invalid tag length %d, want at most %d", tagSize, maxTagLengthInBytes)
	}
	return nil
}
