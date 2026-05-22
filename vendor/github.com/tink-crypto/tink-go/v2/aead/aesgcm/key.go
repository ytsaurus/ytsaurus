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

package aesgcm

import (
	"bytes"
	"fmt"

	internalaead "github.com/tink-crypto/tink-go/v2/internal/aead"
	"github.com/tink-crypto/tink-go/v2/internal/outputprefix"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Variant is the prefix variant of AES-GCM keys.
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

// Parameters specifies an AES-GCM key.
type Parameters struct {
	keySizeInBytes int
	ivSizeInBytes  int
	tagSizeInBytes int
	variant        Variant
}

var _ key.Parameters = (*Parameters)(nil)

// KeySizeInBytes returns the size of the key in bytes.
func (p *Parameters) KeySizeInBytes() int { return p.keySizeInBytes }

// IVSizeInBytes returns the size of the IV in bytes.
func (p *Parameters) IVSizeInBytes() int { return p.ivSizeInBytes }

// TagSizeInBytes returns the size of the tag in bytes.
func (p *Parameters) TagSizeInBytes() int { return p.tagSizeInBytes }

// Variant returns the variant of the key.
func (p *Parameters) Variant() Variant { return p.variant }

// ParametersOpts specifies options for creating AES-GCM parameters.
type ParametersOpts struct {
	KeySizeInBytes int
	IVSizeInBytes  int
	TagSizeInBytes int
	Variant        Variant
}

func validateOpts(opts *ParametersOpts) error {
	if opts.KeySizeInBytes != 16 && opts.KeySizeInBytes != 24 && opts.KeySizeInBytes != 32 {
		return fmt.Errorf("unsupported key size; want 16, 24, or 32, got: %v", opts.KeySizeInBytes)
	}
	if opts.IVSizeInBytes <= 0 {
		return fmt.Errorf("unsupported IV size; want > 0, got: %v", opts.IVSizeInBytes)
	}
	if opts.TagSizeInBytes < 12 || opts.TagSizeInBytes > 16 {
		return fmt.Errorf("unsupported tag size; want >= 12 and <= 16, got: %v", opts.TagSizeInBytes)
	}
	if opts.Variant == VariantUnknown {
		return fmt.Errorf("unsupported variant: %v", opts.Variant)
	}
	return nil
}

// NewParameters creates a new AES-GCM Parameters object.
func NewParameters(opts ParametersOpts) (*Parameters, error) {
	if err := validateOpts(&opts); err != nil {
		return nil, fmt.Errorf("aesgcm.NewParameters: %v", err)
	}
	return &Parameters{
		keySizeInBytes: opts.KeySizeInBytes,
		ivSizeInBytes:  opts.IVSizeInBytes,
		tagSizeInBytes: opts.TagSizeInBytes,
		variant:        opts.Variant,
	}, nil
}

// HasIDRequirement returns whether the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// Equal returns whether this Parameters object is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	actualParams, ok := other.(*Parameters)
	return ok && p.HasIDRequirement() == actualParams.HasIDRequirement() &&
		p.keySizeInBytes == actualParams.keySizeInBytes &&
		p.ivSizeInBytes == actualParams.ivSizeInBytes &&
		p.tagSizeInBytes == actualParams.tagSizeInBytes &&
		p.variant == actualParams.variant
}

// Key represents an AES-GCM key.
type Key struct {
	keyBytes secretdata.Bytes
	// idRequirement is the ID requirement to be included in the output of the
	// AES-GCM function. If the key is in a keyset and the key has an ID
	// requirement, this matches the keyset key ID.
	idRequirement uint32
	outputPrefix  []byte
	parameters    *Parameters
}

var _ key.Key = (*Key)(nil)

// NewKey creates a new AES-GCM key with key, idRequirement and parameters.
//
// The idRequirement is the ID requirement to be included in the output of the
// AES-GCM function. If parameters.HasIDRequirement() == false, idRequirement
// must be zero.
func NewKey(keyBytes secretdata.Bytes, idRequirement uint32, parameters *Parameters) (*Key, error) {
	if parameters == nil {
		return nil, fmt.Errorf("aesgcm.NewKey: parameters is nil")
	}
	opts := &ParametersOpts{
		KeySizeInBytes: parameters.KeySizeInBytes(),
		IVSizeInBytes:  parameters.IVSizeInBytes(),
		TagSizeInBytes: parameters.TagSizeInBytes(),
		Variant:        parameters.Variant(),
	}
	if err := validateOpts(opts); err != nil {
		return nil, fmt.Errorf("aesgcm.NewKey: %v", err)
	}
	if !parameters.HasIDRequirement() && idRequirement != 0 {
		return nil, fmt.Errorf("aesgcm.NewKey: idRequirement = %v and parameters.HasIDRequirement() = false, want 0", idRequirement)
	}
	if keyBytes.Len() != int(parameters.KeySizeInBytes()) {
		return nil, fmt.Errorf("aesgcm.NewKey: key.Len() = %v, want %v", keyBytes.Len(), parameters.KeySizeInBytes())
	}
	outputPrefix, err := calculateOutputPrefix(parameters.Variant(), idRequirement)
	if err != nil {
		return nil, fmt.Errorf("aesgcm.NewKey: %v", err)
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

// IDRequirement returns required to indicate if this key requires an
// identifier. If it does, id will contain that identifier.
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
		k.keyBytes.Equal(that.keyBytes) &&
		bytes.Equal(k.outputPrefix, that.outputPrefix)
}

func createKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	aesGCMParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed *aesgcm.Parameters", p)
	}

	// Make sure AES key size is either 16 or 32 bytes for consistency with other Tink
	// implementations.
	if err := internalaead.ValidateAESKeySize(uint32(aesGCMParams.KeySizeInBytes())); err != nil {
		return nil, err
	}

	keyBytes, err := secretdata.NewBytesFromRand(uint32(aesGCMParams.KeySizeInBytes()))
	if err != nil {
		return nil, err
	}
	return NewKey(keyBytes, idRequirement, aesGCMParams)
}
