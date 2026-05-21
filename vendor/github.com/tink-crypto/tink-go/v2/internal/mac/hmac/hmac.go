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

// Package hmac implements the hmac algorithm.
package hmac

import (
	"crypto/hmac"
	"errors"
	"fmt"
	"hash"

	"github.com/tink-crypto/tink-go/v2/subtle"
)

const (
	// Minimum key size in bytes.
	minKeySizeInBytes = uint32(16)

	// Minimum tag size in bytes. This provides minimum 80-bit security strength.
	minTagSizeInBytes = uint32(10)
)

// HMAC implements the MAC interface.
type HMAC struct {
	HashFunc func() hash.Hash
	key      []byte
	tagSize  uint32
}

// ValidateHMACParams validates parameters of HMAC constructor.
func ValidateHMACParams(hash string, keySize uint32, tagSize uint32) error {
	// validate tag size
	digestSize, err := subtle.GetHashDigestSize(hash)
	if err != nil {
		return err
	}
	if tagSize > digestSize {
		return fmt.Errorf("tag size too big")
	}
	if tagSize < minTagSizeInBytes {
		return fmt.Errorf("tag size too small")
	}
	// validate key size
	if keySize < minKeySizeInBytes {
		return fmt.Errorf("key too short")
	}
	return nil
}

// New returns a new HMAC instance.
func New(hashAlg string, key []byte, tagSize uint32) (*HMAC, error) {
	if err := ValidateHMACParams(hashAlg, uint32(len(key)), tagSize); err != nil {
		return nil, err
	}
	hashFunc := subtle.GetHashFunc(hashAlg)
	if hashFunc == nil {
		return nil, fmt.Errorf("hmac: invalid hash algorithm")
	}
	return &HMAC{
		HashFunc: hashFunc,
		key:      key,
		tagSize:  tagSize,
	}, nil
}

// ComputeMAC computes message authentication code (MAC) for the given data.
func (h *HMAC) ComputeMAC(data ...[]byte) ([]byte, error) {
	if h.HashFunc == nil {
		return nil, fmt.Errorf("hmac: invalid hash algorithm")
	}
	mac := hmac.New(h.HashFunc, h.key)
	for _, d := range data {
		if _, err := mac.Write(d); err != nil {
			return nil, fmt.Errorf("hmac: failed to write data: %v", err)
		}
	}
	tag := mac.Sum(nil)
	return tag[:h.tagSize], nil
}

// VerifyMAC verifies whether the given MAC is a correct message authentication
// code (MAC) the given data.
func (h *HMAC) VerifyMAC(mac []byte, data ...[]byte) error {
	expectedMAC, err := h.ComputeMAC(data...)
	if err != nil {
		return err
	}
	if hmac.Equal(expectedMAC, mac) {
		return nil
	}
	return errors.New("HMAC: invalid MAC")
}
