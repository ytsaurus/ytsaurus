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

package aesctrhmac

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/outputprefix"
	"github.com/tink-crypto/tink-go/v2/key"
)

// Variant is the prefix variant of AES-CTR-HMAC keys.
//
// It describes how the prefix of the ciphertext is constructed. For AEAD there
// are three options:
//
// * TINK: prepends '0x01<big endian key id>' to the ciphertext.
// * CRUNCHY: prepends '0x00<big endian key id>' to the ciphertext.
// * NO_PREFIX: adds no prefix to the ciphertext.
type Variant int

const (
	// VariantUnknown is the default and invalid value of Variant.
	VariantUnknown Variant = iota
	// VariantTink prefixes '0x01<big endian key id>' to the ciphertext.
	VariantTink
	// VariantCrunchy prefixes '0x00<big endian key id>' to the ciphertext.
	VariantCrunchy
	// VariantNoPrefix adds no prefix to the ciphertext.
	VariantNoPrefix
)

func (variant Variant) String() string {
	switch variant {
	case VariantTink:
		return "TINK"
	case VariantCrunchy:
		return "CRUNCHY"
	case VariantNoPrefix:
		return "NO_PREFIX"
	default:
		return "UNKNOWN"
	}
}

// HashType is the hash type of the AES-CTR-HMAC key.
type HashType int

const (
	// UnknownHashType is the default value of HashType.
	UnknownHashType HashType = iota
	// SHA1 is the SHA1 hash type.
	SHA1
	// SHA224 is the SHA224 hash type.
	SHA224
	// SHA256 is the SHA256 hash type.
	SHA256
	// SHA384 is the SHA384 hash type.
	SHA384
	// SHA512 is the SHA512 hash type.
	SHA512
)

func (ht HashType) String() string {
	switch ht {
	case SHA1:
		return "SHA1"
	case SHA224:
		return "SHA224"
	case SHA256:
		return "SHA256"
	case SHA384:
		return "SHA384"
	case SHA512:
		return "SHA512"
	default:
		return "UNKNOWN"
	}
}

// calculateOutputPrefix calculates the output prefix from keyID.
func calculateOutputPrefix(variant Variant, keyID uint32) ([]byte, error) {
	switch variant {
	case VariantTink:
		return outputprefix.Tink(keyID), nil
	case VariantCrunchy:
		return outputprefix.Legacy(keyID), nil
	case VariantNoPrefix:
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid output prefix variant: %v", variant)
	}
}

// Parameters specifies a AES-CTR-HMAC key.
type Parameters struct {
	aesKeySizeInBytes  int
	hmacKeySizeInBytes int
	ivSizeInBytes      int
	tagSizeInBytes     int
	hashType           HashType
	variant            Variant
}

var _ key.Parameters = (*Parameters)(nil)

// AESKeySizeInBytes returns the size of the key in bytes.
func (p *Parameters) AESKeySizeInBytes() int { return p.aesKeySizeInBytes }

// HMACKeySizeInBytes returns the size of the HMAC key in bytes.
func (p *Parameters) HMACKeySizeInBytes() int { return p.hmacKeySizeInBytes }

// IVSizeInBytes returns the size of the IV in bytes.
func (p *Parameters) IVSizeInBytes() int { return p.ivSizeInBytes }

// TagSizeInBytes returns the size of the tag in bytes.
func (p *Parameters) TagSizeInBytes() int { return p.tagSizeInBytes }

// HashType returns the hash type.
func (p *Parameters) HashType() HashType { return p.hashType }

// Variant returns the variant of the key.
func (p *Parameters) Variant() Variant { return p.variant }

// ParametersOpts specifies options for creating AES-CTR-HMAC parameters.
type ParametersOpts struct {
	AESKeySizeInBytes  int
	HMACKeySizeInBytes int
	IVSizeInBytes      int
	TagSizeInBytes     int
	HashType           HashType
	Variant            Variant
}

func maxTagSize(ht HashType) (int, error) {
	switch ht {
	case SHA1:
		return 20, nil
	case SHA224:
		return 28, nil
	case SHA256:
		return 32, nil
	case SHA384:
		return 48, nil
	case SHA512:
		return 64, nil
	default:
		return 0, fmt.Errorf("unsupported hash type: %v", ht)
	}
}

const minTagSize = 10

func validateOpts(opts *ParametersOpts) error {
	if opts.AESKeySizeInBytes != 16 && opts.AESKeySizeInBytes != 24 && opts.AESKeySizeInBytes != 32 {
		return fmt.Errorf("unsupported key size: got: %v, want 16, 24, or 32", opts.AESKeySizeInBytes)
	}
	if opts.IVSizeInBytes < 12 || opts.IVSizeInBytes > 16 {
		return fmt.Errorf("unsupported IV size: got: %v, want between 12 and 16", opts.IVSizeInBytes)
	}
	if opts.HMACKeySizeInBytes < 16 {
		return fmt.Errorf("unsupported HMAC key size: got: %v, want >= 16", opts.HMACKeySizeInBytes)
	}
	maxTagSize, err := maxTagSize(opts.HashType)
	if err != nil {
		return fmt.Errorf("unsupported hash type: %v", opts.HashType)
	}
	if opts.TagSizeInBytes < minTagSize || opts.TagSizeInBytes > maxTagSize {
		return fmt.Errorf("unsupported tag size: got: %v, want between 10 and %v", opts.TagSizeInBytes, maxTagSize)
	}
	if opts.Variant == VariantUnknown {
		return fmt.Errorf("unsupported variant: %v", opts.Variant)
	}
	return nil
}

// NewParameters creates a new AES-CTR-HMAC Parameters object.
func NewParameters(opts ParametersOpts) (*Parameters, error) {
	if err := validateOpts(&opts); err != nil {
		return nil, fmt.Errorf("aesctrhmac.NewParameters: %v", err)
	}
	return &Parameters{
		aesKeySizeInBytes:  opts.AESKeySizeInBytes,
		hmacKeySizeInBytes: opts.HMACKeySizeInBytes,
		ivSizeInBytes:      opts.IVSizeInBytes,
		tagSizeInBytes:     opts.TagSizeInBytes,
		hashType:           opts.HashType,
		variant:            opts.Variant,
	}, nil
}

// HasIDRequirement returns whether the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// Equal returns whether this Parameters object is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	actualParams, ok := other.(*Parameters)
	return ok && p.HasIDRequirement() == actualParams.HasIDRequirement() &&
		p.aesKeySizeInBytes == actualParams.aesKeySizeInBytes &&
		p.hmacKeySizeInBytes == actualParams.hmacKeySizeInBytes &&
		p.ivSizeInBytes == actualParams.ivSizeInBytes &&
		p.tagSizeInBytes == actualParams.tagSizeInBytes &&
		p.hashType == actualParams.hashType &&
		p.variant == actualParams.variant
}
