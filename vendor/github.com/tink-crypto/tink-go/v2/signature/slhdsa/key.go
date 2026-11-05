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

package slhdsa

import (
	"bytes"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/outputprefix"
	"github.com/tink-crypto/tink-go/v2/internal/signature/slhdsa"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Variant is the prefix variant of a SLH-DSA key.
//
// It describes the format of the signature. For SLH-DSA, there are two options:
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

// HashType is the hash type of the SLH-DSA key.
type HashType int

const (
	// UnknownHashType is the default value of HashType.
	UnknownHashType HashType = iota
	// SHA2 hashing.
	SHA2
	// SHAKE hashing.
	SHAKE
)

// SignatureType is the signature type of the SLH-DSA key.
type SignatureType int

const (
	// UnknownSignatureType is the default value of SignatureType.
	UnknownSignatureType SignatureType = iota
	// FastSigning selects fast signing.
	FastSigning
	// SmallSignature selects small signatures.
	SmallSignature
)

type parameterSet struct {
	hashType HashType
	keySize  int
	sigType  SignatureType
}

// Parameters represents the parameters of a SLH-DSA key.
//
// Currently, only the following parameters are supported:
//
//	SLH-DSA-SHA2-128s: {hashType: SHA2, keySize: 64, sigType: SmallSignature},
//	SLH-DSA-SHAKE-256f: {hashType: SHAKE, keySize: 128, sigType: FastSigning}.
type Parameters struct {
	paramSet parameterSet
	variant  Variant
}

func slhDSASHA2128s() parameterSet {
	return parameterSet{
		hashType: SHA2,
		keySize:  64,
		sigType:  SmallSignature,
	}
}

func slhDSASHAKE256f() parameterSet {
	return parameterSet{
		hashType: SHAKE,
		keySize:  128,
		sigType:  FastSigning,
	}
}

func checkSupportedParameters(paramSet parameterSet) error {
	switch paramSet {
	case slhDSASHA2128s(), slhDSASHAKE256f():
		return nil
	default:
		return fmt.Errorf("unsupported parameters: %v", paramSet)
	}
}

var _ key.Parameters = (*Parameters)(nil)

// NewParameters creates a new Parameters.
func NewParameters(hashType HashType, keySize int, sigType SignatureType, variant Variant) (*Parameters, error) {
	paramSet := parameterSet{hashType: hashType, keySize: keySize, sigType: sigType}
	if err := checkSupportedParameters(paramSet); err != nil {
		return nil, fmt.Errorf("slhdsa.NewParameters: %w", err)
	}
	if variant == VariantUnknown {
		return nil, fmt.Errorf("slhdsa.NewParameters: variant must not be %v", VariantUnknown)
	}
	return &Parameters{
		paramSet: paramSet,
		variant:  variant,
	}, nil
}

// HashType returns the hash type.
func (p *Parameters) HashType() HashType { return p.paramSet.hashType }

// KeySize returns the key size in bytes.
func (p *Parameters) KeySize() int { return p.paramSet.keySize }

// SignatureType returns the signature type.
func (p *Parameters) SignatureType() SignatureType { return p.paramSet.sigType }

// Variant returns the prefix variant of the parameters.
func (p *Parameters) Variant() Variant { return p.variant }

// HasIDRequirement returns true if the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// Equal returns true if this parameters object is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	then, ok := other.(*Parameters)
	return ok && p.paramSet == then.paramSet && p.variant == then.variant
}

// PublicKey represents a SLH-DSA public key.
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

func publicKeyLengthForParams(paramSet parameterSet) (int, error) {
	switch paramSet {
	case slhDSASHA2128s():
		return slhdsa.SLH_DSA_SHA2_128s.PublicKeyLength(), nil
	case slhDSASHAKE256f():
		return slhdsa.SLH_DSA_SHAKE_256f.PublicKeyLength(), nil
	default:
		return 0, fmt.Errorf("invalid parameters: %v", paramSet)
	}
}

func privateKeyLengthForParams(paramSet parameterSet) (int, error) {
	switch paramSet {
	case slhDSASHA2128s():
		return slhdsa.SLH_DSA_SHA2_128s.SecretKeyLength(), nil
	case slhDSASHAKE256f():
		return slhdsa.SLH_DSA_SHAKE_256f.SecretKeyLength(), nil
	default:
		return 0, fmt.Errorf("invalid parameters: %v", paramSet)
	}
}

// checkPublicKeyLengthForParameters assumes that params are not nil.
func checkPublicKeyLengthForParameters(length int, params *Parameters) error {
	expLength, err := publicKeyLengthForParams(params.paramSet)
	if err != nil {
		return err
	}
	if length != expLength {
		return fmt.Errorf("invalid public key length: %v", length)
	}
	return nil
}

// checkPrivateKeyLengthForParameters assumes that params are not nil.
func checkPrivateKeyLengthForParameters(length int, params *Parameters) error {
	expLength, err := privateKeyLengthForParams(params.paramSet)
	if err != nil {
		return err
	}
	if length != expLength {
		return fmt.Errorf("invalid private key length: %v", length)
	}
	return nil
}

// NewPublicKey creates a new SLH-DSA public key.
//
// idRequirement is the ID of the key in the keyset. It must be zero if params
// doesn't have an ID requirement.
func NewPublicKey(keyBytes []byte, idRequirement uint32, params *Parameters) (*PublicKey, error) {
	if !params.HasIDRequirement() && idRequirement != 0 {
		return nil, fmt.Errorf("slhdsa.NewPublicKey: idRequirement must be zero if params doesn't have an ID requirement")
	}
	if err := checkPublicKeyLengthForParameters(len(keyBytes), params); err != nil {
		return nil, fmt.Errorf("slhdsa.NewPublicKey: %w", err)
	}
	outputPrefix, err := calculateOutputPrefix(params.variant, idRequirement)
	if err != nil {
		return nil, fmt.Errorf("slhdsa.NewPublicKey: %w", err)
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

// PrivateKey represents a SLH-DSA private key.
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

// publicKeyForParameters assumes that len(privateKeyBytes) is correct and that params are not nil.
func publicKeyForParameters(privateKeyBytes secretdata.Bytes, params *Parameters) ([]byte, error) {
	var sk *slhdsa.SecretKey
	var err error
	switch params.paramSet {
	case slhDSASHA2128s():
		sk, err = slhdsa.SLH_DSA_SHA2_128s.DecodeSecretKey(privateKeyBytes.Data(insecuresecretdataaccess.Token{}))
	case slhDSASHAKE256f():
		sk, err = slhdsa.SLH_DSA_SHAKE_256f.DecodeSecretKey(privateKeyBytes.Data(insecuresecretdataaccess.Token{}))
	default:
		return nil, fmt.Errorf("invalid parameters: %v", params)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid private key bytes: %w", err)
	}
	return sk.PublicKey().Encode(), nil
}

// NewPrivateKey creates a new SLH-DSA private key from privateKeyBytes, with
// idRequirement and params.
func NewPrivateKey(privateKeyBytes secretdata.Bytes, idRequirement uint32, params *Parameters) (*PrivateKey, error) {
	if params == nil {
		return nil, fmt.Errorf("slhdsa.NewPrivateKey: params must not be nil")
	}
	if err := checkPrivateKeyLengthForParameters(privateKeyBytes.Len(), params); err != nil {
		return nil, fmt.Errorf("slhdsa.NewPrivateKey: %w", err)
	}
	pubKeyBytes, err := publicKeyForParameters(privateKeyBytes, params)
	if err != nil {
		return nil, fmt.Errorf("slhdsa.NewPrivateKey: %w", err)
	}
	pubKey, err := NewPublicKey(pubKeyBytes, idRequirement, params)
	if err != nil {
		return nil, fmt.Errorf("slhdsa.NewPrivateKey: %w", err)
	}
	return &PrivateKey{
		publicKey: pubKey,
		keyBytes:  privateKeyBytes,
	}, nil
}

// NewPrivateKeyWithPublicKey creates a new SLH-DSA private key from
// privateKeyBytes and a [PublicKey].
func NewPrivateKeyWithPublicKey(privateKeyBytes secretdata.Bytes, pubKey *PublicKey) (*PrivateKey, error) {
	if pubKey == nil {
		return nil, fmt.Errorf("slhdsa.NewPrivateKeyWithPublicKey: pubKey must not be nil")
	}
	if pubKey.params == nil {
		return nil, fmt.Errorf("slhdsa.NewPrivateKeyWithPublicKey: pubKey.params must not be nil")
	}
	if err := checkPrivateKeyLengthForParameters(privateKeyBytes.Len(), pubKey.params); err != nil {
		return nil, fmt.Errorf("slhdsa.NewPrivateKeyWithPublicKey: %w", err)
	}
	// Make sure the public key is correct.
	pubKeyBytes, err := publicKeyForParameters(privateKeyBytes, pubKey.params)
	if err != nil {
		return nil, fmt.Errorf("slhdsa.NewPrivateKey: %w", err)
	}
	if !bytes.Equal(pubKeyBytes, pubKey.KeyBytes()) {
		return nil, fmt.Errorf("slhdsa.NewPrivateKeyWithPublicKey: public key does not match private key")
	}
	return &PrivateKey{
		publicKey: pubKey,
		keyBytes:  privateKeyBytes,
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
	slhDSAParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: %T", p)
	}
	// Make sure the parameters are not "empty".
	var sk *slhdsa.SecretKey
	switch slhDSAParams.paramSet {
	case slhDSASHA2128s():
		sk, _ = slhdsa.SLH_DSA_SHA2_128s.KeyGen()
	case slhDSASHAKE256f():
		sk, _ = slhdsa.SLH_DSA_SHAKE_256f.KeyGen()
	default:
		return nil, fmt.Errorf("invalid parameters: %v", slhDSAParams)
	}
	return NewPrivateKey(secretdata.NewBytesFromData(sk.Encode(), insecuresecretdataaccess.Token{}), idRequirement, slhDSAParams)
}
