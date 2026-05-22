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

package ecdsa

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/outputprefix"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Variant is the prefix variant of an ECDSA key.
//
// It describes the format of the signature. For ECDSA there are four options:
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
	// VariantLegacy appends '0x00' to the input message BEFORE computing
	// the signature, then prepends '0x00<big endian key id>' to the signature.
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

// CurveType is the curve type of the ECDSA key.
type CurveType int

const (
	// UnknownCurveType is the default value of CurveType.
	UnknownCurveType CurveType = iota
	// NistP256 is the NIST P-256 curve.
	NistP256
	// NistP384 is the NIST P-384 curve.
	NistP384
	// NistP521 is the NIST P-521 curve.
	NistP521
)

func (ct CurveType) String() string {
	switch ct {
	case NistP256:
		return "NIST_P256"
	case NistP384:
		return "NIST_P384"
	case NistP521:
		return "NIST_P521"
	default:
		return "UNKNOWN"
	}
}

// HashType is the hash type of the ECDSA key.
type HashType int

const (
	// UnknownHashType is the default value of HashType.
	UnknownHashType HashType = iota
	// SHA256 is the SHA256 hash type.
	SHA256
	// SHA384 is the SHA384 hash type.
	SHA384
	// SHA512 is the SHA512 hash type.
	SHA512
)

func (ht HashType) String() string {
	switch ht {
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

// SignatureEncoding is the signature encoding of the ECDSA key.
type SignatureEncoding int

const (
	// UnknownSignatureEncoding is the default value of SignatureEncoding.
	UnknownSignatureEncoding SignatureEncoding = iota
	// DER is the DER encoding.
	DER
	// IEEEP1363 is the IEEE P1363 encoding.
	IEEEP1363
)

func (encoding SignatureEncoding) String() string {
	switch encoding {
	case DER:
		return "DER"
	case IEEEP1363:
		return "IEEE_P1363"
	default:
		return "UNKNOWN"
	}
}

// Parameters represents the parameters of an ECDSA key.
type Parameters struct {
	curveType         CurveType
	hashType          HashType
	signatureEncoding SignatureEncoding
	variant           Variant
}

var _ key.Parameters = (*Parameters)(nil)

// CurveType the curve type.
func (p *Parameters) CurveType() CurveType { return p.curveType }

// HashType returns the hash type.
func (p *Parameters) HashType() HashType { return p.hashType }

// SignatureEncoding returns the signature encoding.
func (p *Parameters) SignatureEncoding() SignatureEncoding { return p.signatureEncoding }

// Variant returns the output prefix variant of the key.
func (p *Parameters) Variant() Variant { return p.variant }

func checkValidHashForCurve(curveType CurveType, hashType HashType) error {
	switch curveType {
	case NistP256:
		if hashType != SHA256 {
			return fmt.Errorf("ecdsa.Parameters: unsupported hash type for curve type: %v, %v", curveType, hashType)
		}
	case NistP384:
		if hashType != SHA384 && hashType != SHA512 {
			return fmt.Errorf("ecdsa.Parameters: unsupported hash type for curve type: %v, %v", curveType, hashType)
		}
	case NistP521:
		if hashType != SHA512 {
			return fmt.Errorf("ecdsa.Parameters: unsupported hash type for curve type: %v, %v", curveType, hashType)
		}
	default:
		return fmt.Errorf("ecdsa.Parameters: unsupported curve type: %v", curveType)
	}
	return nil
}

func checkValidHash(hashType HashType) error {
	switch hashType {
	case SHA256, SHA384, SHA512:
		return nil
	default:
		return fmt.Errorf("unsupported hash type: %v", hashType)
	}
}

func checkValidSignatureEncoding(signatureEncoding SignatureEncoding) error {
	switch signatureEncoding {
	case DER, IEEEP1363:
		return nil
	default:
		return fmt.Errorf("unsupported signature encoding: %v", signatureEncoding)
	}
}

func checkValidVariant(variant Variant) error {
	switch variant {
	case VariantTink, VariantCrunchy, VariantLegacy, VariantNoPrefix:
		return nil
	default:
		return fmt.Errorf("unsupported output prefix variant: %v", variant)
	}
}

func validateParameters(p *Parameters) error {
	if p == nil {
		return fmt.Errorf("parameters is nil")
	}
	if err := checkValidHash(p.HashType()); err != nil {
		return fmt.Errorf("ecdsa.Parameters: %v", err)
	}
	if err := checkValidSignatureEncoding(p.SignatureEncoding()); err != nil {
		return fmt.Errorf("ecdsa.Parameters: %v", err)
	}
	if err := checkValidVariant(p.Variant()); err != nil {
		return fmt.Errorf("ecdsa.Parameters: %v", err)
	}
	if err := checkValidHashForCurve(p.CurveType(), p.HashType()); err != nil {
		return err
	}
	return nil
}

// NewParameters creates a new ECDSA Parameters value.
func NewParameters(curveType CurveType, hashType HashType, encoding SignatureEncoding, variant Variant) (*Parameters, error) {
	p := &Parameters{
		curveType:         curveType,
		hashType:          hashType,
		signatureEncoding: encoding,
		variant:           variant,
	}
	if err := validateParameters(p); err != nil {
		return nil, fmt.Errorf("ecdsa.NewParameters: %v", err)
	}
	return p, nil
}

// HasIDRequirement tells whether the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// Equal tells whether this parameters value is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	actualParams, ok := other.(*Parameters)
	return ok && p.HasIDRequirement() == actualParams.HasIDRequirement() &&
		p.curveType == actualParams.curveType &&
		p.hashType == actualParams.hashType &&
		p.signatureEncoding == actualParams.signatureEncoding &&
		p.variant == actualParams.variant
}

func calculateOutputPrefix(variant Variant, idRequirement uint32) ([]byte, error) {
	switch variant {
	case VariantTink:
		return outputprefix.Tink(idRequirement), nil
	case VariantCrunchy, VariantLegacy:
		return outputprefix.Legacy(idRequirement), nil
	case VariantNoPrefix:
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid output prefix variant: %v", variant)
	}
}

// ecdhCurveFromCurveType returns the corresponding ecdh.Curve value from ct.
func ecdhCurveFromCurveType(ct CurveType) (ecdh.Curve, error) {
	switch ct {
	case NistP256:
		return ecdh.P256(), nil
	case NistP384:
		return ecdh.P384(), nil
	case NistP521:
		return ecdh.P521(), nil
	default:
		return nil, fmt.Errorf("invalid curve type: %v", ct)
	}
}

// PublicKey represents an ECDSA public key.
type PublicKey struct {
	publicPoint   []byte
	idRequirement uint32
	outputPrefix  []byte
	parameters    *Parameters
}

var _ key.Key = (*PublicKey)(nil)

// NewPublicKey creates a new ECDSA PublicKey value from a public point,
//
// The point is expected to be encoded uncompressed as per [SEC 1 v2.0, Section
// 2.3.3].
//
// [SEC 1 v2.0, Section 2.3.3]: https://www.secg.org/sec1-v2.pdf#page=17.08
func NewPublicKey(publicPoint []byte, idRequirement uint32, parameters *Parameters) (*PublicKey, error) {
	if err := validateParameters(parameters); err != nil {
		return nil, fmt.Errorf("ecdsa.NewPublicKey: %v", err)
	}
	if parameters.Variant() == VariantNoPrefix && idRequirement != 0 {
		return nil, fmt.Errorf("ecdsa.NewPublicKey: key ID must be zero for VariantNoPrefix")
	}
	outputPrefix, err := calculateOutputPrefix(parameters.Variant(), idRequirement)
	if err != nil {
		return nil, fmt.Errorf("ecdsa.NewPublicKey: %v", err)
	}
	curve, err := ecdhCurveFromCurveType(parameters.CurveType())
	if err != nil {
		return nil, fmt.Errorf("ecdsa.NewPublicKey: %v", err)
	}
	// Validate the point.
	if _, err := curve.NewPublicKey(publicPoint); err != nil {
		return nil, fmt.Errorf("ecdsa.NewPublicKey: point validation failed: %v", err)
	}
	return &PublicKey{
		publicPoint:   bytes.Clone(publicPoint),
		idRequirement: idRequirement,
		outputPrefix:  outputPrefix,
		parameters:    parameters,
	}, nil
}

// PublicPoint returns the public key uncompressed point.
//
// Point format as per [SEC 1 v2.0, Section 2.3.3].
//
// [SEC 1 v2.0, Section 2.3.3]: https://www.secg.org/sec1-v2.pdf#page=17.08
func (k *PublicKey) PublicPoint() []byte { return bytes.Clone(k.publicPoint) }

// Parameters returns the parameters of this key.
func (k *PublicKey) Parameters() key.Parameters { return k.parameters }

// IDRequirement tells whether the key ID and whether it is required.
func (k *PublicKey) IDRequirement() (uint32, bool) {
	return k.idRequirement, k.Parameters().HasIDRequirement()
}

// OutputPrefix returns the output prefix of this key.
func (k *PublicKey) OutputPrefix() []byte { return bytes.Clone(k.outputPrefix) }

// Equal tells whether this key value is equal to other.
func (k *PublicKey) Equal(other key.Key) bool {
	actualKey, ok := other.(*PublicKey)
	return ok && k.Parameters().Equal(actualKey.Parameters()) &&
		k.idRequirement == actualKey.idRequirement &&
		bytes.Equal(k.publicPoint, actualKey.publicPoint) &&
		bytes.Equal(k.outputPrefix, actualKey.outputPrefix)
}

// PrivateKey represents an ECDSA private key.
type PrivateKey struct {
	publicKey       *PublicKey
	privateKeyValue secretdata.Bytes
}

var _ key.Key = (*PrivateKey)(nil)

// NewPrivateKey creates a new ECDSA PrivateKey object from a secret private
// key value and parameters.
//
// The private key value must be octet encoded as per [SEC 1 v2.0, Section
// 2.3.5].
//
// [SEC 1 v2.0, Section 2.3.5]: https://www.secg.org/sec1-v2.pdf#page=17.08
func NewPrivateKey(privateKeyValue secretdata.Bytes, idRequirement uint32, params *Parameters) (*PrivateKey, error) {
	if err := validateParameters(params); err != nil {
		return nil, fmt.Errorf("ecdsa.NewPrivateKey: %v", err)
	}
	curve, err := ecdhCurveFromCurveType(params.CurveType())
	if err != nil {
		return nil, fmt.Errorf("ecdsa.NewPrivateKey: %v", err)
	}
	ecdhPrivateKey, err := curve.NewPrivateKey(privateKeyValue.Data(insecuresecretdataaccess.Token{}))
	if err != nil {
		return nil, fmt.Errorf("ecdsa.NewPrivateKey: point validation failed: %v", err)
	}
	publicPoint := ecdhPrivateKey.PublicKey().Bytes()
	publicKey, err := NewPublicKey(publicPoint, idRequirement, params)
	if err != nil {
		return nil, fmt.Errorf("ecdsa.NewPrivateKey: %v", err)
	}
	privKey := &PrivateKey{
		publicKey:       publicKey,
		privateKeyValue: privateKeyValue,
	}
	return privKey, nil
}

// validatePrivateKey checks that the private key value is valid with respect to
// the public key.
//
// It checks that an [ecdh.PrivateKey] can be constructed from the private key
// value and that the [ecdh.PublicKey] of that [ecdh.PrivateKey] is equal to the
// public key constructed from [PublicKey].
func validatePrivateKey(publicKey *PublicKey, privateKeyValue secretdata.Bytes) error {
	curve, err := ecdhCurveFromCurveType(publicKey.parameters.CurveType())
	if err != nil {
		return err
	}
	ecdhPrivateKey, err := curve.NewPrivateKey(privateKeyValue.Data(insecuresecretdataaccess.Token{}))
	if err != nil {
		return fmt.Errorf("point validation failed: %v", err)
	}
	ecdhPublicKeyFromPublicKey, err := curve.NewPublicKey(publicKey.publicPoint)
	if err != nil {
		// Should never happen.
		return fmt.Errorf("invalid public key point: %v", err)
	}
	if !ecdhPrivateKey.PublicKey().Equal(ecdhPublicKeyFromPublicKey) {
		return fmt.Errorf("invalid private key value")
	}
	return nil
}

// NewPrivateKeyFromPublicKey creates a new ECDSA PrivateKey object from a
// public key and private key value.
//
// The private key value must be octet encoded as per [SEC 1 v2.0, Section
// 2.3.5].
//
// [SEC 1 v2.0, Section 2.3.5]: https://www.secg.org/sec1-v2.pdf#page=17.08
func NewPrivateKeyFromPublicKey(publicKey *PublicKey, privateKeyValue secretdata.Bytes) (*PrivateKey, error) {
	// PublicKey can be either nil, PublicKey{} or a valid PublicKey created with
	// NewPublicKey.
	if publicKey == nil {
		return nil, fmt.Errorf("ecdsa.NewPrivateKeyFromPublicKey: publicKey is nil")
	}
	// This should suffice to rule out the empty PublicKey{} case.
	// If parameters are not nil, we know the public key is valid (e.g., it
	// contains a valid point).
	if publicKey.parameters == nil {
		return nil, fmt.Errorf("ecdsa.NewPrivateKeyFromPublicKey: invalid public key")
	}

	// Check that the private key value is valid with respect to the public key.
	if err := validatePrivateKey(publicKey, privateKeyValue); err != nil {
		return nil, fmt.Errorf("ecdsa.NewPrivateKeyFromPublicKey: %v", err)
	}

	privKey := &PrivateKey{
		publicKey:       publicKey,
		privateKeyValue: privateKeyValue,
	}
	return privKey, nil
}

// PrivateKeyValue returns the private key value as [secretdata.Bytes].
//
// The returned private key value has length equal to the size of the curve.
func (k *PrivateKey) PrivateKeyValue() secretdata.Bytes { return k.privateKeyValue }

// PublicKey returns the corresponding public key as [key.Key].
func (k *PrivateKey) PublicKey() (key.Key, error) { return k.publicKey, nil }

// Parameters returns the parameters of this key as [key.Parameters].
func (k *PrivateKey) Parameters() key.Parameters { return k.publicKey.Parameters() }

// IDRequirement tells whether the key ID and whether it is required.
func (k *PrivateKey) IDRequirement() (uint32, bool) { return k.publicKey.IDRequirement() }

// OutputPrefix returns the output prefix of this key.
func (k *PrivateKey) OutputPrefix() []byte { return k.publicKey.OutputPrefix() }

// Equal tells whether this key object is equal to other.
func (k *PrivateKey) Equal(other key.Key) bool {
	actualKey, ok := other.(*PrivateKey)
	return ok && k.publicKey.Equal(actualKey.publicKey) &&
		k.privateKeyValue.Equal(actualKey.privateKeyValue)
}

func createPrivateKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	ecdsaParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("ecdsa.createKey: invalid parameters type: %T", p)
	}
	curve, err := ecdhCurveFromCurveType(ecdsaParams.CurveType())
	if err != nil {
		return nil, fmt.Errorf("ecdsa.createKey: %v", err)
	}
	privateKey, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("ecdsa.createKey: %v", err)
	}
	privateKeyValue := secretdata.NewBytesFromData(privateKey.Bytes(), insecuresecretdataaccess.Token{})
	return NewPrivateKey(privateKeyValue, idRequirement, ecdsaParams)
}
