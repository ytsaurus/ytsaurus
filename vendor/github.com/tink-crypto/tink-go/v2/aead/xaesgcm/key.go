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

package xaesgcm

import (
	"bytes"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/outputprefix"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Variant is the prefix variant of X-AES-GCM keys.
//
// It describes how the prefix of the ciphertext is constructed. For AEAD there
// are three options:
//
// * TINK: prepends '0x01<big endian key id>' to the ciphertext.
// * NO_PREFIX: adds no prefix to the ciphertext.
type Variant int

const (
	// VariantUnknown is the default and invalid value of Variant.
	VariantUnknown Variant = iota
	// VariantTink prefixes '0x01<big endian key id>' to the ciphertext.
	VariantTink
	// VariantNoPrefix adds no prefix to the ciphertext.
	VariantNoPrefix
)

func (variant Variant) String() string {
	switch variant {
	case VariantTink:
		return "TINK"
	case VariantNoPrefix:
		return "NO_PREFIX"
	default:
		return "UNKNOWN"
	}
}

// calculateOutputPrefix calculates the output prefix from keyID.
func calculateOutputPrefix(variant Variant, keyID uint32) ([]byte, error) {
	switch variant {
	case VariantTink:
		return outputprefix.Tink(keyID), nil
	case VariantNoPrefix:
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid output prefix variant: %v", variant)
	}
}

// Parameters specifies the parameters of an X-AES-GCM key.
type Parameters struct {
	variant         Variant
	saltSizeInBytes int
}

var _ key.Parameters = (*Parameters)(nil)

// Variant returns the variant of the key.
func (p *Parameters) Variant() Variant { return p.variant }

// NewParameters creates a new X-AES-GCM Parameters object.
//
// X-AES-GCM uses saltSizeInBytes bytes to derive the per-message key, from the
// key material. We use AES-CMAC to derive the per-message keys, as per
// NIST SP 800-108 Rev. 1. The salt size must be in [8, 12]; use a value of 12
// to meet the X-AES-GCM specifications (https://c2sp.org/XAES-256-GCM).
func NewParameters(variant Variant, saltSizeInBytes int) (*Parameters, error) {
	if variant == VariantUnknown {
		return nil, fmt.Errorf("xaesgcm.Parameters: unsupported variant: %v", variant)
	}
	if saltSizeInBytes < 8 || saltSizeInBytes > 12 {
		return nil, fmt.Errorf("xaesgcm.Parameters: saltSizeInBytes = %v, want in [8, 12]", saltSizeInBytes)
	}
	return &Parameters{
		variant:         variant,
		saltSizeInBytes: saltSizeInBytes,
	}, nil
}

// HasIDRequirement returns whether the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// SaltSizeInBytes returns the salt size in bytes.
func (p *Parameters) SaltSizeInBytes() int { return p.saltSizeInBytes }

// Equal returns whether this Parameters object is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	actualParams, ok := other.(*Parameters)
	return ok && p.variant == actualParams.variant &&
		p.saltSizeInBytes == actualParams.saltSizeInBytes
}

// Key represents an X-AES-GCM key.
type Key struct {
	keyBytes      secretdata.Bytes
	idRequirement uint32
	outputPrefix  []byte
	parameters    *Parameters
}

var _ key.Key = (*Key)(nil)

// NewKey creates a new [xaesgcm.Key] key with key, idRequirement and
// parameters.
//
// The key must be 32 bytes long. The tag size for X-AES-GCM is 16 bytes.
func NewKey(keyBytes secretdata.Bytes, idRequirement uint32, parameters *Parameters) (*Key, error) {
	if parameters == nil {
		return nil, fmt.Errorf("xaesgcm.NewKey: parameters is nil")
	}
	if keyBytes.Len() != 32 {
		return nil, fmt.Errorf("xaesgcm.NewKey: key.Len() = %v, want 32", keyBytes.Len())
	}
	if !parameters.HasIDRequirement() && idRequirement != 0 {
		return nil, fmt.Errorf("xaesgcm.NewKey: idRequirement = %v and parameters.HasIDRequirement() = false, want 0", idRequirement)
	}
	outputPrefix, err := calculateOutputPrefix(parameters.Variant(), idRequirement)
	if err != nil {
		return nil, fmt.Errorf("xaesgcm.NewKey: %v", err)
	}
	return &Key{
		keyBytes:      keyBytes,
		idRequirement: idRequirement,
		outputPrefix:  outputPrefix,
		parameters:    parameters,
	}, nil
}

// KeyBytes returns the key material.
//
// This function provides access to partial key material. See
// https://developers.google.com/tink/design/access_control#access_of_parts_of_a_key
// for more information.
func (k *Key) KeyBytes() secretdata.Bytes { return k.keyBytes }

// Parameters returns the parameters of this key.
func (k *Key) Parameters() key.Parameters { return k.parameters }

// IDRequirement returns the key ID and whether it is required or not.
//
// If not required, the returned key ID is not usable.
func (k *Key) IDRequirement() (uint32, bool) {
	return k.idRequirement, k.Parameters().HasIDRequirement()
}

// OutputPrefix returns the output prefix.
func (k *Key) OutputPrefix() []byte { return bytes.Clone(k.outputPrefix) }

// Equal returns whether this key object is equal to other.
func (k *Key) Equal(other key.Key) bool {
	that, ok := other.(*Key)
	return ok && k.Parameters().Equal(that.Parameters()) &&
		k.idRequirement == that.idRequirement &&
		k.keyBytes.Equal(that.keyBytes)
}

func createKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	xAESGCMParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed %T", p, (*Parameters)(nil))
	}
	keyBytes, err := secretdata.NewBytesFromRand(uint32(32))
	if err != nil {
		return nil, err
	}
	return NewKey(keyBytes, idRequirement, xAESGCMParams)
}
