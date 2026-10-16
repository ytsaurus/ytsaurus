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

package ed25519

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/outputprefix"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Variant is the prefix variant of an ED25519 key.
//
// It describes the format of the signature. For ED25519, there are four options:
//
//   - TINK: prepends '0x01<big endian key id>' to the signature.
//   - CRUNCHY: prepends '0x00<big endian key id>' to the signature.
//   - LEGACY: appends a 0-byte to the input message before computing the
//     signature, then prepends '0x00<big endian key id>' to the signature.
//   - NO_PREFIX: adds no prefix to the signature.
type Variant int

const (
	// VariantUnknown is the default value of Variant.
	VariantUnknown Variant = iota
	// VariantTink prefixes '0x01<big endian key id>' to the signature.
	VariantTink
	// VariantCrunchy prefixes '0x00<big endian key id>' to the signature.
	VariantCrunchy
	// VariantLegacy appends a 0-byte to input message BEFORE computing the signature,
	// signature, then prepends '0x00<big endian key id>' to signature.
	VariantLegacy
	// VariantNoPrefix does not prefix the signature with the key id.
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

// Parameters represents the parameters of an ED25519 key.
type Parameters struct {
	variant Variant
}

var _ key.Parameters = (*Parameters)(nil)

// NewParameters creates a new Parameters.
func NewParameters(variant Variant) (Parameters, error) {
	if variant == VariantUnknown {
		return Parameters{}, fmt.Errorf("ed25519.NewParameters: variant must not be %v", VariantUnknown)
	}
	return Parameters{variant: variant}, nil
}

// Variant returns the prefix variant of the parameters.
func (p *Parameters) Variant() Variant { return p.variant }

// HasIDRequirement returns true if the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// Equal returns true if this parameters object is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	if p == other {
		return true
	}
	then, ok := other.(*Parameters)
	return ok && p.variant == then.variant
}

// PublicKey represents an ED25519 public key.
type PublicKey struct {
	keyBytes      []byte
	idRequirement uint32
	params        Parameters
	outputPrefix  []byte
}

var _ key.Key = (*PublicKey)(nil)

func calculateOutputPrefix(variant Variant, keyID uint32) ([]byte, error) {
	switch variant {
	case VariantTink:
		return outputprefix.Tink(keyID), nil
	case VariantCrunchy, VariantLegacy:
		return outputprefix.Legacy(keyID), nil
	case VariantNoPrefix:
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid output prefix variant: %v", variant)
	}
}

// NewPublicKey creates a new ED25519 public key.
//
// idRequirement is the ID of the key in the keyset. It must be zero if params
// doesn't have an ID requirement.
func NewPublicKey(keyBytes []byte, idRequirement uint32, params Parameters) (*PublicKey, error) {
	if !params.HasIDRequirement() && idRequirement != 0 {
		return nil, fmt.Errorf("ed25519.NewPublicKey: idRequirement must be zero if params doesn't have an ID requirement")
	}
	if len(keyBytes) != 32 {
		return nil, fmt.Errorf("ed25519.NewPublicKey: keyBytes must be 32 bytes")
	}
	outputPrefix, err := calculateOutputPrefix(params.variant, idRequirement)
	if err != nil {
		return nil, fmt.Errorf("ed25519.NewPublicKey: %w", err)
	}
	return &PublicKey{
		keyBytes:      bytes.Clone(keyBytes),
		idRequirement: idRequirement,
		params:        params,
		outputPrefix:  outputPrefix,
	}, nil
}

// KeyBytes returns the public key bytes.
func (k *PublicKey) KeyBytes() []byte { return bytes.Clone(k.keyBytes) }

// OutputPrefix returns the output prefix of this key.
func (k *PublicKey) OutputPrefix() []byte { return bytes.Clone(k.outputPrefix) }

// Parameters returns the parameters of the key.
func (k *PublicKey) Parameters() key.Parameters { return &k.params }

// IDRequirement returns the ID requirement of the key, and whether it is
// required.
func (k *PublicKey) IDRequirement() (uint32, bool) {
	return k.idRequirement, k.params.HasIDRequirement()
}

// Equal returns true if this key is equal to other.
func (k *PublicKey) Equal(other key.Key) bool {
	if k == other {
		return true
	}
	that, ok := other.(*PublicKey)
	return ok && k.params.Equal(that.Parameters()) &&
		bytes.Equal(k.keyBytes, that.keyBytes) &&
		k.idRequirement == that.idRequirement
}

// PrivateKey represents an ED25519 private key.
type PrivateKey struct {
	publicKey *PublicKey
	keyBytes  secretdata.Bytes
}

var _ key.Key = (*PrivateKey)(nil)

// NewPrivateKey creates a new ED25519 private key from privateKeyBytes, with
// idRequirement and params.
func NewPrivateKey(privateKeyBytes secretdata.Bytes, idRequirement uint32, params Parameters) (*PrivateKey, error) {
	if privateKeyBytes.Len() != 32 {
		return nil, fmt.Errorf("ed25519.NewPrivateKey: privateKeyBytes must be 32 bytes, got %d", privateKeyBytes.Len())
	}
	privKey := ed25519.NewKeyFromSeed(privateKeyBytes.Data(insecuresecretdataaccess.Token{}))
	pubKeyBytes := privKey.Public().(ed25519.PublicKey)
	pubKey, err := NewPublicKey(pubKeyBytes, idRequirement, params)
	if err != nil {
		return nil, fmt.Errorf("ed25519.NewPrivateKey: %w", err)
	}
	return &PrivateKey{
		publicKey: pubKey,
		keyBytes:  privateKeyBytes,
	}, nil
}

// NewPrivateKeyWithPublicKey creates a new ED25519 private key from
// privateKeyBytes and a [PublicKey].
func NewPrivateKeyWithPublicKey(privateKeyBytes secretdata.Bytes, pubKey *PublicKey) (*PrivateKey, error) {
	if pubKey == nil {
		return nil, fmt.Errorf("ed25519.NewPrivateKeyWithPublicKey: pubKey must not be nil")
	}
	if privateKeyBytes.Len() != 32 {
		return nil, fmt.Errorf("ed25519.NewPrivateKey: seed must be 32 bytes, got %d", privateKeyBytes.Len())
	}
	// Make sure the public key is correct.
	privKey := ed25519.NewKeyFromSeed(privateKeyBytes.Data(insecuresecretdataaccess.Token{}))
	if !bytes.Equal(privKey.Public().(ed25519.PublicKey), pubKey.KeyBytes()) {
		return nil, fmt.Errorf("ed25519.NewPrivateKeyWithPublicKey: public key does not match private key")
	}
	return &PrivateKey{
		publicKey: pubKey,
		keyBytes:  privateKeyBytes,
	}, nil
}

// PrivateKeyBytes returns the private key bytes.
func (k *PrivateKey) PrivateKeyBytes() secretdata.Bytes { return k.keyBytes }

// PublicKey returns the public key of the key.
//
// This implements the privateKey interface defined in handle.go.
func (k *PrivateKey) PublicKey() (key.Key, error) { return k.publicKey, nil }

// Parameters returns the parameters of the key.
func (k *PrivateKey) Parameters() key.Parameters { return &k.publicKey.params }

// IDRequirement returns the ID requirement of the key, and whether it is
// required.
func (k *PrivateKey) IDRequirement() (uint32, bool) { return k.publicKey.IDRequirement() }

// OutputPrefix returns the output prefix of this key.
func (k *PrivateKey) OutputPrefix() []byte { return bytes.Clone(k.publicKey.outputPrefix) }

// Equal returns true if this key is equal to other.
func (k *PrivateKey) Equal(other key.Key) bool {
	if k == other {
		return true
	}
	that, ok := other.(*PrivateKey)
	return ok && k.publicKey.Equal(that.publicKey) && k.keyBytes.Equal(that.keyBytes)
}

func createPrivateKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	ed25519Params, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: %T", p)
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("cannot generate ED25519 key: %s", err)
	}
	privateKeyValue := secretdata.NewBytesFromData(priv.Seed(), insecuresecretdataaccess.Token{})
	return NewPrivateKey(privateKeyValue, idRequirement, *ed25519Params)
}
