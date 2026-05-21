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

package aesgcmhkdf

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/key"
)

// HashType is the hash function used in HKDF.
type HashType int

const (
	// HashTypeUnknown is the default and invalid value of HashType.
	HashTypeUnknown HashType = iota
	// SHA1 specifies SHA-1.
	SHA1
	// SHA256 specifies SHA-256.
	SHA256
	// SHA512 specifies SHA-512.
	SHA512
)

func (ht HashType) String() string {
	switch ht {
	case SHA1:
		return "SHA1"
	case SHA256:
		return "SHA256"
	case SHA512:
		return "SHA512"
	default:
		return "UNKNOWN"
	}
}

// Parameters specifies an AES-GCM-HKDF streaming key.
type Parameters struct {
	keySizeInBytes        int
	derivedKeySizeInBytes int
	hkdfHashType          HashType
	segmentSizeInBytes    int32 // For compatibility with Tink Java.
}

var _ key.Parameters = (*Parameters)(nil)

// KeySizeInBytes returns the size of the key in bytes.
func (p *Parameters) KeySizeInBytes() int { return p.keySizeInBytes }

// DerivedKeySizeInBytes returns the size of the derived key in bytes.
func (p *Parameters) DerivedKeySizeInBytes() int { return p.derivedKeySizeInBytes }

// HKDFHashType returns the hash function for HKDF.
func (p *Parameters) HKDFHashType() HashType { return p.hkdfHashType }

// SegmentSizeInBytes returns the ciphertext segment size in bytes.
func (p *Parameters) SegmentSizeInBytes() int32 { return p.segmentSizeInBytes }

// ParametersOpts specifies options for creating AES-GCM-HKDF parameters.
type ParametersOpts struct {
	KeySizeInBytes        int
	DerivedKeySizeInBytes int
	HKDFHashType          HashType
	SegmentSizeInBytes    int32
}

func validateOpts(opts *ParametersOpts) error {
	if opts.DerivedKeySizeInBytes != 16 && opts.DerivedKeySizeInBytes != 32 {
		return fmt.Errorf("unsupported derived key size; want 16 or 32, got: %v", opts.DerivedKeySizeInBytes)
	}
	if opts.KeySizeInBytes < opts.DerivedKeySizeInBytes {
		return fmt.Errorf("key size must be at least %v, got: %v", opts.DerivedKeySizeInBytes, opts.KeySizeInBytes)
	}
	if opts.HKDFHashType == HashTypeUnknown {
		return fmt.Errorf("unsupported HKDF hash type: %v", opts.HKDFHashType)
	}
	// See
	// https://developers.google.com/tink/streaming-aead/aes_gcm_hkdf_streaming#key_and_parameters.
	minSegmentSize := int32(opts.DerivedKeySizeInBytes + 24 + 1)
	if opts.SegmentSizeInBytes < minSegmentSize {
		return fmt.Errorf("ciphertext segment size must be at least %d, got %d", minSegmentSize, opts.SegmentSizeInBytes)
	}
	return nil
}

// NewParameters creates a new AES-GCM-HKDF [Parameters] object.
func NewParameters(opts ParametersOpts) (*Parameters, error) {
	if err := validateOpts(&opts); err != nil {
		return nil, fmt.Errorf("aesgcmhkdf.NewParameters: %v", err)
	}
	return &Parameters{
		keySizeInBytes:        opts.KeySizeInBytes,
		derivedKeySizeInBytes: opts.DerivedKeySizeInBytes,
		hkdfHashType:          opts.HKDFHashType,
		segmentSizeInBytes:    opts.SegmentSizeInBytes,
	}, nil
}

// HasIDRequirement returns false because AES-GCM-HKDF keys do not have an ID
// requirement.
func (p *Parameters) HasIDRequirement() bool { return false }

// Equal returns whether this Parameters object is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	actualParams, ok := other.(*Parameters)
	return ok &&
		p.keySizeInBytes == actualParams.keySizeInBytes &&
		p.derivedKeySizeInBytes == actualParams.derivedKeySizeInBytes &&
		p.hkdfHashType == actualParams.hkdfHashType &&
		p.segmentSizeInBytes == actualParams.segmentSizeInBytes
}
