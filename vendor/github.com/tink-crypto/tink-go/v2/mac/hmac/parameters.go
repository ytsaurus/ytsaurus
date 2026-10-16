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

package hmac

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/core/cryptofmt"
	"github.com/tink-crypto/tink-go/v2/key"
)

// Variant is the prefix variant of HMAC keys.
//
// It describes how the prefix of the tag is constructed. There are
// three options:
//
//   - VariantTink: prepends '0x01<big endian key id>' to the tag.
//   - VariantCrunchy: prepends '0x00<big endian key id>' to the tag.
//   - VariantLegacy: appends a 0-byte to the input message before computing
//     the tag, then prepends '0x00<big endian key id>' to the tag.
//   - VariantNoPrefix: adds no prefix to the tag.
type Variant int

const (
	// VariantUnknown is the default and invalid value of Variant.
	VariantUnknown Variant = iota
	// VariantTink prefixes '0x01<big endian key id>' to the tag.
	VariantTink
	// VariantCrunchy prefixes '0x00<big endian key id>' to the tag.
	VariantCrunchy
	// VariantLegacy appends '0x00' to the input message BEFORE computing
	// the tag, then prepends '0x00<big endian key id>' to the tag.
	VariantLegacy
	// VariantNoPrefix adds no prefix to the tag.
	VariantNoPrefix
)

func (variant Variant) String() string {
	switch variant {
	case VariantTink:
		return "TINK"
	case VariantCrunchy:
		return "CRUNCHY"
	case VariantLegacy:
		return "LEGACY"
	case VariantNoPrefix:
		return "NO_PREFIX"
	default:
		return "UNKNOWN"
	}
}

// HashType is the hash type of the HMAC key.
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

// Parameters specifies an HMAC key.
type Parameters struct {
	keySizeInBytes int
	tagSizeInBytes int
	hashType       HashType
	variant        Variant
}

var _ key.Parameters = (*Parameters)(nil)

// KeySizeInBytes returns the size of the key in bytes.
func (p *Parameters) KeySizeInBytes() int { return p.keySizeInBytes }

// CryptographicTagSizeInBytes returns the size of the tag in bytes.
func (p *Parameters) CryptographicTagSizeInBytes() int { return p.tagSizeInBytes }

// HashType returns the hash type of the HMAC key.
func (p *Parameters) HashType() HashType { return p.hashType }

// TotalTagSizeInBytes returns the size of the tag in bytes plus the size of
// the prefix with which this key prefixes every cryptographic tag.
func (p *Parameters) TotalTagSizeInBytes() int {
	if p.HasIDRequirement() {
		return cryptofmt.TinkPrefixSize + p.CryptographicTagSizeInBytes()
	}
	return p.CryptographicTagSizeInBytes()
}

// Variant returns the variant of the key.
func (p *Parameters) Variant() Variant { return p.variant }

// HasIDRequirement returns whether the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// Equal returns whether this Parameters object is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	actualParams, ok := other.(*Parameters)
	return ok && p.HasIDRequirement() == actualParams.HasIDRequirement() &&
		p.keySizeInBytes == actualParams.keySizeInBytes &&
		p.hashType == actualParams.hashType &&
		p.tagSizeInBytes == actualParams.tagSizeInBytes &&
		p.variant == actualParams.variant
}

// ParametersOpts is the options for creating parameters.
type ParametersOpts struct {
	KeySizeInBytes int
	TagSizeInBytes int
	HashType       HashType
	Variant        Variant
}

func maxTagSizeInBytes(hashType HashType) (int, error) {
	switch hashType {
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
		return 0, fmt.Errorf("invalid hash type: %v", hashType)
	}
}

// NewParameters creates a new HMAC parameters.
func NewParameters(opts ParametersOpts) (*Parameters, error) {
	maxTagSizeInBytes, err := maxTagSizeInBytes(opts.HashType)
	if err != nil {
		return nil, err
	}
	if opts.KeySizeInBytes < 16 {
		return nil, fmt.Errorf("key size must be >= 16, got %d", opts.KeySizeInBytes)
	}
	if opts.TagSizeInBytes < 10 || opts.TagSizeInBytes > maxTagSizeInBytes {
		return nil, fmt.Errorf("tag size must be between 10 and %d bytes, got %d", maxTagSizeInBytes, opts.TagSizeInBytes)
	}
	if opts.Variant == VariantUnknown {
		return nil, fmt.Errorf("variant must be specified")
	}
	if opts.HashType == UnknownHashType {
		return nil, fmt.Errorf("hash type must be specified")
	}
	return &Parameters{
		keySizeInBytes: opts.KeySizeInBytes,
		tagSizeInBytes: opts.TagSizeInBytes,
		hashType:       opts.HashType,
		variant:        opts.Variant,
	}, nil
}
