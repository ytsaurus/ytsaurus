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

package mldsa

import (
	"bytes"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/outputprefix"
	"github.com/tink-crypto/tink-go/v2/internal/signature/mldsa"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Variant is the prefix variant of a ML-DSA key.
//
// It describes the format of the signature. For ML-DSA, there are two options:
//
//   - TINK: prepends '0x01<big endian key id>' to the signature.
//   - NO_PREFIX: adds no prefix to the signature.
type Variant int

const (
	// VariantUnknown is the default value of Variant.
	VariantUnknown Variant = iota
	// VariantTink prefixes '0x01<big endian key id>' to the signature.
	VariantTink
	// VariantNoPrefix does not prefix the signature with the key id.
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

// Instance is the instance type of the ML-DSA key.
type Instance int

const (
	// UnknownInstance is the default value of Instance.
	UnknownInstance Instance = iota
	// MLDSA65 yields ML-DSA-65 parameters.
	MLDSA65
	// MLDSA87 yields ML-DSA-87 parameters.
	MLDSA87
)

func (instance Instance) String() string {
	switch instance {
	case MLDSA65:
		return "MLDSA65"
	case MLDSA87:
		return "MLDSA87"
	default:
		return "UNKNOWN"
	}
}

// Parameters represents the parameters of a ML-DSA key.
type Parameters struct {
	instance Instance
	variant  Variant
}

var _ key.Parameters = (*Parameters)(nil)

// NewParameters creates a new Parameters.
func NewParameters(instance Instance, variant Variant) (*Parameters, error) {
	if instance == UnknownInstance {
		return nil, fmt.Errorf("mldsa.NewParameters: instance must not be %v", UnknownInstance)
	}
	if variant == VariantUnknown {
		return nil, fmt.Errorf("mldsa.NewParameters: variant must not be %v", VariantUnknown)
	}
	return &Parameters{
		instance: instance,
		variant:  variant,
	}, nil
}

// Instance returns the instance type.
func (p *Parameters) Instance() Instance { return p.instance }

// Variant returns the prefix variant of the parameters.
func (p *Parameters) Variant() Variant { return p.variant }

// HasIDRequirement returns true if the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// Equal returns true if this parameters object is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	then, ok := other.(*Parameters)
	return ok && p.instance == then.instance &&
		p.variant == then.variant
}

// PublicKey represents a ML-DSA public key.
type PublicKey struct {
	keyBytes      []byte
	idRequirement uint32
	params        *Parameters
	outputPrefix  []byte
}

var _ key.Key = (*PublicKey)(nil)

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

func checkPublicKeyLengthForInstance(length int, instance Instance) error {
	switch instance {
	case MLDSA65:
		expectedLength := mldsa.MLDSA65.PublicKeyLength()
		if length != expectedLength {
			return fmt.Errorf("public key length must be %d bytes", expectedLength)
		}
	case MLDSA87:
		expectedLength := mldsa.MLDSA87.PublicKeyLength()
		if length != expectedLength {
			return fmt.Errorf("public key length must be %d bytes", expectedLength)
		}
	default:
		return fmt.Errorf("invalid instance: %v", instance)
	}
	return nil
}

// NewPublicKey creates a new ML-DSA public key.
//
// idRequirement is the ID of the key in the keyset. It must be zero if params
// doesn't have an ID requirement.
func NewPublicKey(keyBytes []byte, idRequirement uint32, params *Parameters) (*PublicKey, error) {
	if !params.HasIDRequirement() && idRequirement != 0 {
		return nil, fmt.Errorf("mldsa.NewPublicKey: idRequirement must be zero if params doesn't have an ID requirement")
	}
	if err := checkPublicKeyLengthForInstance(len(keyBytes), params.instance); err != nil {
		return nil, fmt.Errorf("mldsa.NewPublicKey: %w", err)
	}
	outputPrefix, err := calculateOutputPrefix(params.variant, idRequirement)
	if err != nil {
		return nil, fmt.Errorf("mldsa.NewPublicKey: %w", err)
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
func (k *PublicKey) Parameters() key.Parameters { return k.params }

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

// PrivateKey represents a ML-DSA private key.
type PrivateKey struct {
	publicKey *PublicKey
	// keyBytes is the seed used to generate the private key.
	keyBytes secretdata.Bytes
	// expandedKeyBytes is the expanded private key.
	// We cache the expanded private key as an optimization. Decoding it is
	// faster than recomputing it from the seed every time we create a signer.
	expandedKeyBytes secretdata.Bytes
}

var _ key.Key = (*PrivateKey)(nil)

// keyGenForInstance assumes that len(seed) == mldsa.SecretKeySeedSize.
func keyGenForInstance(seed secretdata.Bytes, instance Instance) ([]byte, secretdata.Bytes, error) {
	switch instance {
	case MLDSA65:
		var seedBytes [mldsa.SecretKeySeedSize]byte
		copy(seedBytes[:], seed.Data(insecuresecretdataaccess.Token{}))
		publicKey, secretKey := mldsa.MLDSA65.KeyGenFromSeed(seedBytes)
		return publicKey.Encode(), secretdata.NewBytesFromData(secretKey.Encode(), insecuresecretdataaccess.Token{}), nil
	case MLDSA87:
		var seedBytes [mldsa.SecretKeySeedSize]byte
		copy(seedBytes[:], seed.Data(insecuresecretdataaccess.Token{}))
		publicKey, secretKey := mldsa.MLDSA87.KeyGenFromSeed(seedBytes)
		return publicKey.Encode(), secretdata.NewBytesFromData(secretKey.Encode(), insecuresecretdataaccess.Token{}), nil
	default:
		return nil, secretdata.Bytes{}, fmt.Errorf("invalid instance: %v", instance)
	}
}

// NewPrivateKey creates a new ML-DSA private key from privateKeyBytes, with
// idRequirement and params.
func NewPrivateKey(privateKeyBytes secretdata.Bytes, idRequirement uint32, params *Parameters) (*PrivateKey, error) {
	if privateKeyBytes.Len() != mldsa.SecretKeySeedSize {
		return nil, fmt.Errorf("mldsa.NewPrivateKey: privateKeyBytes must be %v bytes", mldsa.SecretKeySeedSize)
	}
	if params == nil {
		return nil, fmt.Errorf("mldsa.NewPrivateKey: params must not be nil")
	}
	pubKeyBytes, expandedPrivateKeyBytes, err := keyGenForInstance(privateKeyBytes, params.instance)
	if err != nil {
		return nil, fmt.Errorf("mldsa.NewPrivateKey: %w", err)
	}
	pubKey, err := NewPublicKey(pubKeyBytes, idRequirement, params)
	if err != nil {
		return nil, fmt.Errorf("mldsa.NewPrivateKey: %w", err)
	}
	return &PrivateKey{
		publicKey:        pubKey,
		keyBytes:         privateKeyBytes,
		expandedKeyBytes: expandedPrivateKeyBytes,
	}, nil
}

// NewPrivateKeyWithPublicKey creates a new ML-DSA private key from
// privateKeyBytes and a [PublicKey].
func NewPrivateKeyWithPublicKey(privateKeyBytes secretdata.Bytes, pubKey *PublicKey) (*PrivateKey, error) {
	if privateKeyBytes.Len() != mldsa.SecretKeySeedSize {
		return nil, fmt.Errorf("mldsa.NewPrivateKeyWithPublicKey: privateKeyBytes must be %v bytes", mldsa.SecretKeySeedSize)
	}
	if pubKey == nil {
		return nil, fmt.Errorf("mldsa.NewPrivateKeyWithPublicKey: pubKey must not be nil")
	}
	if pubKey.params == nil {
		return nil, fmt.Errorf("mldsa.NewPrivateKeyWithPublicKey: pubKey.params must not be nil")
	}
	// Make sure the public key is correct.
	pubKeyBytes, expandedPrivateKeyBytes, err := keyGenForInstance(privateKeyBytes, pubKey.params.Instance())
	if err != nil {
		return nil, fmt.Errorf("mldsa.NewPrivateKey: %w", err)
	}
	if !bytes.Equal(pubKeyBytes, pubKey.KeyBytes()) {
		return nil, fmt.Errorf("mldsa.NewPrivateKeyWithPublicKey: public key does not match private key")
	}
	return &PrivateKey{
		publicKey:        pubKey,
		keyBytes:         privateKeyBytes,
		expandedKeyBytes: expandedPrivateKeyBytes,
	}, nil
}

// PrivateKeyBytes returns the private key seed.
func (k *PrivateKey) PrivateKeyBytes() secretdata.Bytes { return k.keyBytes }

// PublicKey returns the public key of the key.
//
// This implements the privateKey interface defined in handle.go.
func (k *PrivateKey) PublicKey() (key.Key, error) { return k.publicKey, nil }

// Parameters returns the parameters of the key.
func (k *PrivateKey) Parameters() key.Parameters { return k.publicKey.params }

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
	return ok && k.publicKey.Equal(that.publicKey) &&
		k.keyBytes.Equal(that.keyBytes) &&
		k.expandedKeyBytes.Equal(that.expandedKeyBytes)
}

func createPrivateKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	mlDSAParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: %T", p)
	}
	// Make sure the parameters are not "empty"; only MLDSA65 and MLDSA87 are supported.
	if mlDSAParams.Instance() != MLDSA65 && mlDSAParams.Instance() != MLDSA87 {
		return nil, fmt.Errorf("invalid parameters")
	}
	seed, err := secretdata.NewBytesFromRand(mldsa.SecretKeySeedSize)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random seed: %w", err)
	}
	return NewPrivateKey(seed, idRequirement, mlDSAParams)
}
