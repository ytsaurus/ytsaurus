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

package aessiv

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/key"
)

// Variant is the prefix variant of AES-SIV keys.
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

// Parameters specifies an AES-SIV key.
type Parameters struct {
	keySizeInBytes int
	variant        Variant
}

var _ key.Parameters = (*Parameters)(nil)

// KeySizeInBytes returns the size of the key in bytes.
func (p *Parameters) KeySizeInBytes() int { return p.keySizeInBytes }

// Variant returns the variant of the key.
func (p *Parameters) Variant() Variant { return p.variant }

func validateParams(params *Parameters) error {
	// AES-SIV key sizes specified in
	// https://www.rfc-editor.org/rfc/rfc5297#section-2.2.
	if params.KeySizeInBytes() != 32 && params.KeySizeInBytes() != 48 && params.KeySizeInBytes() != 64 {
		return fmt.Errorf("unsupported key size; want 32, 48, or 64, got: %v", params.KeySizeInBytes())
	}
	if params.Variant() == VariantUnknown {
		return fmt.Errorf("unsupported variant: %v", params.Variant())
	}
	return nil
}

// NewParameters creates a new AES-SIV Parameters object.
func NewParameters(keySizeInBytes int, variant Variant) (*Parameters, error) {
	p := &Parameters{
		keySizeInBytes: keySizeInBytes,
		variant:        variant,
	}
	if err := validateParams(p); err != nil {
		return nil, fmt.Errorf("aessiv.NewParameters: %v", err)
	}
	return p, nil
}

// HasIDRequirement returns whether the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// Equal returns whether this Parameters object is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	actualParams, ok := other.(*Parameters)
	return ok && p.keySizeInBytes == actualParams.keySizeInBytes &&
		p.variant == actualParams.variant
}
