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

package streamingprf

import (
	"fmt"
	"hash"
	"io"

	"golang.org/x/crypto/hkdf"
	"github.com/tink-crypto/tink-go/v2/subtle"
)

// minHKDFStreamingPRFKeySize is the minimum allowed key size in bytes.
const minHKDFStreamingPRFKeySize = 32

// HKDFStreamingPRF is a HKDF Streaming PRF that implements StreamingPRF.
type HKDFStreamingPRF struct {
	h    func() hash.Hash
	key  []byte
	salt []byte
}

// Asserts that HKDFStreamingPRF implements the StreamingPRF interface.
var _ StreamingPRF = (*HKDFStreamingPRF)(nil)

// NewHKDFStreamingPRF constructs a new hkdfStreamingPRF using hashName, key,
// and salt. Salt can be nil.
func NewHKDFStreamingPRF(hashName string, key, salt []byte) (*HKDFStreamingPRF, error) {
	if err := validateHKDFStreamingPRFParams(hashName, len(key)); err != nil {
		return nil, err
	}
	return &HKDFStreamingPRF{
		h:    subtle.GetHashFunc(hashName),
		key:  key,
		salt: salt,
	}, nil
}

// Compute computes and returns the HKDF as a Reader.
func (h *HKDFStreamingPRF) Compute(data []byte) (io.Reader, error) {
	return hkdf.New(h.h, h.key, h.salt, data), nil
}

func validateHKDFStreamingPRFParams(hash string, keySize int) error {
	if hash != "SHA256" && hash != "SHA512" {
		return fmt.Errorf("only SHA-256, SHA-512 allowed for HKDF")
	}
	if keySize < minHKDFStreamingPRFKeySize {
		return fmt.Errorf("key too short, require %d-bytes: %d", minHKDFStreamingPRFKeySize, keySize)
	}
	return nil
}
