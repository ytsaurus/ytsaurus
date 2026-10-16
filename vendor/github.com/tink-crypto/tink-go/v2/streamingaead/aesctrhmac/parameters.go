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

package aesctrhmac

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/key"
)

// HashType is the hash algorithm used.
type HashType int

const (
	// UnknownHashType is the default value of HashType.
	UnknownHashType HashType = iota
	// SHA1 is the SHA1 hash type.
	SHA1
	// SHA256 is the SHA256 hash type.
	SHA256
	// SHA512 is the SHA512 hash type.
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

// Parameters defines the parameters for an AES-CTR-HMAC Streaming AEAD key.
type Parameters struct {
	keySizeInBytes        int
	derivedKeySizeInBytes int
	hkdfHashType          HashType
	hmacHashType          HashType
	hmacTagSizeInBytes    int
	segmentSizeInBytes    int32 // For compatibility with Tink Java.
}

// KeySizeInBytes returns the key size in bytes.
func (p *Parameters) KeySizeInBytes() int { return p.keySizeInBytes }

// DerivedKeySizeInBytes returns the derived key size in bytes.
func (p *Parameters) DerivedKeySizeInBytes() int {
	return p.derivedKeySizeInBytes
}

// HkdfHashType returns the HKDF hash type.
func (p *Parameters) HkdfHashType() HashType { return p.hkdfHashType }

// HmacHashType returns the HMAC hash type.
func (p *Parameters) HmacHashType() HashType { return p.hmacHashType }

// HmacTagSizeInBytes returns the HMAC tag size in bytes.
func (p *Parameters) HmacTagSizeInBytes() int { return p.hmacTagSizeInBytes }

// SegmentSizeInBytes returns the ciphertext segment size in bytes.
func (p *Parameters) SegmentSizeInBytes() int32 { return p.segmentSizeInBytes }

// HasIDRequirement returns false because AES-CTR-HMAC keys do not have an ID
// requirement.
func (p *Parameters) HasIDRequirement() bool { return false }

// Equal returns true if p and other are equal.
func (p *Parameters) Equal(other key.Parameters) bool {
	that, ok := other.(*Parameters)
	return ok && p.keySizeInBytes == that.keySizeInBytes &&
		p.derivedKeySizeInBytes == that.derivedKeySizeInBytes &&
		p.hkdfHashType == that.hkdfHashType &&
		p.hmacHashType == that.hmacHashType &&
		p.hmacTagSizeInBytes == that.hmacTagSizeInBytes &&
		p.segmentSizeInBytes == that.segmentSizeInBytes
}

// This ensures that the Parameters type implements the key.Parameters interface.
var _ key.Parameters = (*Parameters)(nil)

// ParametersOpts holds options for creating new AES-CTR-HMAC Streaming AEAD
// parameters.
type ParametersOpts struct {
	KeySizeInBytes        int
	DerivedKeySizeInBytes int
	HkdfHashType          HashType
	HmacHashType          HashType
	HmacTagSizeInBytes    int
	SegmentSizeInBytes    int32
}

const (
	// See
	// https://developers.google.com/tink/streaming-aead/aes_ctr_hmac_streaming#splitting_the_message.
	noncePrefixSize = 7
	// Header length is encoded as a single byte.
	headerLengthSize = 1
)

// NewParameters creates a new Parameters object from ParametersOpts.
func NewParameters(opts ParametersOpts) (*Parameters, error) {
	if opts.DerivedKeySizeInBytes != 16 && opts.DerivedKeySizeInBytes != 32 {
		return nil, fmt.Errorf("invalid derived key size: %d", opts.DerivedKeySizeInBytes)
	}
	if opts.KeySizeInBytes < opts.DerivedKeySizeInBytes {
		return nil, fmt.Errorf("invalid key size: %d", opts.KeySizeInBytes)
	}

	switch opts.HkdfHashType {
	case SHA1:
	case SHA256:
	case SHA512:
	default:
		return nil, fmt.Errorf("unsupported HKDF hash type: %v", opts.HkdfHashType)
	}

	if opts.HmacTagSizeInBytes < 10 {
		return nil, fmt.Errorf("HMAC tag size too small, need at least 10 bytes, got %d", opts.HmacTagSizeInBytes)
	}
	switch opts.HmacHashType {
	case SHA1:
		if opts.HmacTagSizeInBytes > 20 {
			return nil, fmt.Errorf("HMAC tag size too large for SHA1, want at most %d, got %d", 20, opts.HmacTagSizeInBytes)
		}
	case SHA256:
		if opts.HmacTagSizeInBytes > 32 {
			return nil, fmt.Errorf("HMAC tag size too large for SHA256, want at most %d, got %d", 32, opts.HmacTagSizeInBytes)
		}
	case SHA512:
		if opts.HmacTagSizeInBytes > 64 {
			return nil, fmt.Errorf("HMAC tag size too large for SHA512, want at most %d, got %d", 64, opts.HmacTagSizeInBytes)
		}
	default:
		return nil, fmt.Errorf("unsupported HMAC hash type: %v", opts.HmacHashType)
	}

	minCiphertextSegmentSize := int32(opts.DerivedKeySizeInBytes + noncePrefixSize + headerLengthSize + opts.HmacTagSizeInBytes + 1)
	// The segment size must be at least the header + the HMAC tag size.
	if opts.SegmentSizeInBytes < minCiphertextSegmentSize {
		return nil, fmt.Errorf("ciphertext segment size is too small")
	}

	return &Parameters{
		keySizeInBytes:        opts.KeySizeInBytes,
		derivedKeySizeInBytes: opts.DerivedKeySizeInBytes,
		hkdfHashType:          opts.HkdfHashType,
		hmacHashType:          opts.HmacHashType,
		hmacTagSizeInBytes:    opts.HmacTagSizeInBytes,
		segmentSizeInBytes:    opts.SegmentSizeInBytes,
	}, nil
}
