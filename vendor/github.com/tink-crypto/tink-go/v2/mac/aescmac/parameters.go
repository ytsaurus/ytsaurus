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

package aescmac

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/core/cryptofmt"
	"github.com/tink-crypto/tink-go/v2/key"
)

// Variant is the prefix variant of AES-CMAC keys.
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

// Parameters specifies an AES-CMAC key.
type Parameters struct {
	keySizeInBytes int
	tagSizeInBytes int
	variant        Variant
}

var _ key.Parameters = (*Parameters)(nil)

// KeySizeInBytes returns the size of the key in bytes.
func (p *Parameters) KeySizeInBytes() int { return p.keySizeInBytes }

// CryptographicTagSizeInBytes returns the size of the tag in bytes.
func (p *Parameters) CryptographicTagSizeInBytes() int { return p.tagSizeInBytes }

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
		p.tagSizeInBytes == actualParams.tagSizeInBytes &&
		p.variant == actualParams.variant
}

// ParametersOpts is the options for creating parameters.
type ParametersOpts struct {
	KeySizeInBytes int
	TagSizeInBytes int
	Variant        Variant
}

// NewParameters creates a new AES-CMAC parameters.
func NewParameters(opts ParametersOpts) (*Parameters, error) {
	if opts.KeySizeInBytes != 16 && opts.KeySizeInBytes != 32 {
		return nil, fmt.Errorf("key size must be between 16 and 32 bytes, got %d", opts.KeySizeInBytes)
	}
	if opts.TagSizeInBytes < 10 || opts.TagSizeInBytes > 16 {
		return nil, fmt.Errorf("tag size must be between 10 and 16 bytes, got %d", opts.TagSizeInBytes)
	}
	if opts.Variant == VariantUnknown {
		return nil, fmt.Errorf("variant must be specified")
	}
	return &Parameters{
		keySizeInBytes: opts.KeySizeInBytes,
		tagSizeInBytes: opts.TagSizeInBytes,
		variant:        opts.Variant,
	}, nil
}
