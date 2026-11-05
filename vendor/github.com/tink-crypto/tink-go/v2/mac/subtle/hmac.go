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

// Package subtle provides subtle implementations of the MAC primitive.
package subtle

import (
	"errors"
	"hash"

	"github.com/tink-crypto/tink-go/v2/internal/mac/hmac"
)

var errHMACInvalidInput = errors.New("HMAC: invalid input")

// HMAC implementation of interface tink.MAC
type HMAC struct {
	HashFunc func() hash.Hash
	TagSize  uint32
	hmac     *hmac.HMAC
}

// NewHMAC creates a new instance of HMAC with the specified key and tag size.
func NewHMAC(hashAlg string, key []byte, tagSize uint32) (*HMAC, error) {
	h, err := hmac.New(hashAlg, key, tagSize)
	if err != nil {
		return nil, err
	}
	return &HMAC{hmac: h, TagSize: tagSize, HashFunc: h.HashFunc}, nil
}

// ValidateHMACParams validates parameters of HMAC constructor.
func ValidateHMACParams(hash string, keySize uint32, tagSize uint32) error {
	return hmac.ValidateHMACParams(hash, keySize, tagSize)
}

// ComputeMAC computes message authentication code (MAC) for the given data.
func (h *HMAC) ComputeMAC(data []byte) ([]byte, error) {
	return h.hmac.ComputeMAC(data)
}

// VerifyMAC verifies whether the given MAC is a correct message authentication
// code (MAC) the given data.
func (h *HMAC) VerifyMAC(mac []byte, data []byte) error {
	return h.hmac.VerifyMAC(mac, data)
}
