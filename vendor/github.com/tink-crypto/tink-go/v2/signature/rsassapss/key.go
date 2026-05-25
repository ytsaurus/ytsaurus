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

package rsassapss

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"math/big"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/outputprefix"
	"github.com/tink-crypto/tink-go/v2/internal/signature"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Variant is the prefix variant of an RSA-SSA-PSS key.
//
// It describes the format of the signature. For RSA-SSA-PSS there are
// four options:
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

// HashType is the curve type of the RSA-SSA-PSS key.
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

const (
	// f4 is the public exponent 65537.
	f4          = 65537
	maxExponent = 1<<31 - 1
)

// Parameters represents the parameters of an RSA-SSA-PSS key.
type Parameters struct {
	modulusSizeBits int
	sigHashType     HashType
	mgf1HashType    HashType
	publicExponent  int
	saltLengthBytes int
	variant         Variant
}

var _ key.Parameters = (*Parameters)(nil)

// SigHashType returns the signature hash type.
func (p *Parameters) SigHashType() HashType { return p.sigHashType }

// MGF1HashType returns the MGF1 hash type.
func (p *Parameters) MGF1HashType() HashType { return p.mgf1HashType }

// PublicExponent returns the public exponent.
func (p *Parameters) PublicExponent() int { return p.publicExponent }

// SaltLengthBytes returns the salt length in bytes.
func (p *Parameters) SaltLengthBytes() int { return p.saltLengthBytes }

// ModulusSizeBits returns the modulus size in bits.
func (p *Parameters) ModulusSizeBits() int { return p.modulusSizeBits }

// Variant returns the output prefix variant of the key.
func (p *Parameters) Variant() Variant { return p.variant }

func checkValidHash(hashType HashType) error {
	if hashType == SHA256 || hashType == SHA384 || hashType == SHA512 {
		return nil
	}
	return fmt.Errorf("unsupported hash type: %v", hashType)
}

// ParametersValues contains the values of a set of RSA-SSA-PSS parameters.
type ParametersValues struct {
	ModulusSizeBits int
	SigHashType     HashType
	MGF1HashType    HashType
	PublicExponent  int
	SaltLengthBytes int
}

// NewParameters creates a new RSA-SSA-PSS Parameters value.
func NewParameters(values ParametersValues, variant Variant) (*Parameters, error) {
	// These are consistent with the checks by tink-java and tink-cc.
	if values.ModulusSizeBits < 2048 {
		return nil, fmt.Errorf("rsassapss.NewParameters: invalid modulus size: %v, want >= 2048", values.ModulusSizeBits)
	}
	if values.PublicExponent < f4 {
		return nil, fmt.Errorf("rsassapss.NewParameters: invalid public exponent: %v, want >= %v", values.PublicExponent, f4)
	}
	// Similar check as in crypto/rsa.
	if values.PublicExponent > maxExponent {
		return nil, fmt.Errorf("rsassapss.NewParameters: invalid public exponent: %v, want <= %v", values.PublicExponent, maxExponent)
	}
	// These are consistent with the checks by tink-java and tink-cc.
	if values.PublicExponent%2 != 1 {
		return nil, fmt.Errorf("rsassapss.NewParameters: invalid public exponent: %v, want odd", values.PublicExponent)
	}
	if values.SaltLengthBytes < 0 {
		return nil, fmt.Errorf("rsassapss.NewParameters: invalid salt length bytes: %v, want >= 0", values.SaltLengthBytes)
	}
	if err := checkValidHash(values.SigHashType); err != nil {
		return nil, err
	}
	if values.MGF1HashType != values.SigHashType {
		return nil, fmt.Errorf("rsassapss.NewParameters: mismatched MGF1 hash type: %v, want %v", values.MGF1HashType, values.SigHashType)
	}
	if variant == VariantUnknown {
		return nil, fmt.Errorf("unsupported output prefix variant: %v", variant)
	}
	return &Parameters{
		modulusSizeBits: values.ModulusSizeBits,
		sigHashType:     values.SigHashType,
		mgf1HashType:    values.MGF1HashType,
		publicExponent:  values.PublicExponent,
		saltLengthBytes: values.SaltLengthBytes,
		variant:         variant,
	}, nil
}

// HasIDRequirement tells whether the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// Equal tells whether this parameters object is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	that, ok := other.(*Parameters)
	return ok && p.HasIDRequirement() == that.HasIDRequirement() &&
		p.modulusSizeBits == that.modulusSizeBits &&
		p.sigHashType == that.sigHashType &&
		p.mgf1HashType == that.mgf1HashType &&
		p.publicExponent == that.publicExponent &&
		p.saltLengthBytes == that.saltLengthBytes &&
		p.variant == that.variant
}

// PublicKey represents a function that can verify RSA-SSA-PSS signatures
// as defined in [RFC 3447, Section 8.1].
type PublicKey struct {
	modulus       []byte // Big integer value in big-endian encoding.
	idRequirement uint32
	outputPrefix  []byte
	parameters    *Parameters
}

var _ key.Key = (*PublicKey)(nil)

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

// NewPublicKey creates a new RSA-SSA-PSS PublicKey object from modulus,
// ID requirement and parameters.
//
// modulus is a Big integer value in big-endian encoding. Parameters must be
// non-nil.
func NewPublicKey(modulus []byte, idRequirement uint32, parameters *Parameters) (*PublicKey, error) {
	// This is set to UnknownHashType if the parameters is a struct literal.
	if parameters.SigHashType() == UnknownHashType {
		return nil, fmt.Errorf("rsassapss.NewPublicKey: invalid parameters")
	}
	modulusBigInt := new(big.Int).SetBytes(modulus)
	if modulusBigInt.BitLen() != parameters.ModulusSizeBits() {
		return nil, fmt.Errorf("rsassapss.NewPublicKey: invalid modulus bit-length: %v, want %v", modulusBigInt.BitLen(), parameters.ModulusSizeBits())
	}
	if parameters.Variant() == VariantNoPrefix && idRequirement != 0 {
		return nil, fmt.Errorf("rsassapss.NewPublicKey: key ID must be zero for VariantNoPrefix")
	}
	outputPrefix, err := calculateOutputPrefix(parameters.Variant(), idRequirement)
	if err != nil {
		return nil, fmt.Errorf("rsassapss.NewPublicKey: %v", err)
	}
	return &PublicKey{
		modulus:       modulusBigInt.Bytes(),
		idRequirement: idRequirement,
		outputPrefix:  outputPrefix,
		parameters:    parameters,
	}, nil
}

// Modulus returns the public key modulus.
func (k *PublicKey) Modulus() []byte { return bytes.Clone(k.modulus) }

// Parameters returns the parameters of this key.
func (k *PublicKey) Parameters() key.Parameters { return k.parameters }

// IDRequirement returns the key ID requirement and whether it is required.
func (k *PublicKey) IDRequirement() (uint32, bool) {
	return k.idRequirement, k.Parameters().HasIDRequirement()
}

// OutputPrefix returns the output prefix of this key.
func (k *PublicKey) OutputPrefix() []byte { return bytes.Clone(k.outputPrefix) }

// Equal tells whether this key object is equal to other.
func (k *PublicKey) Equal(other key.Key) bool {
	actualKey, ok := other.(*PublicKey)
	return ok && bytes.Equal(k.modulus, actualKey.modulus) &&
		k.idRequirement == actualKey.idRequirement &&
		k.parameters.Equal(actualKey.parameters)
}

// PrivateKey represents a function that can produce RSA-SSA-PSS
// signatures as defined in [RFC 3447, Section 8.1].
type PrivateKey struct {
	publicKey  *PublicKey
	privateKey *rsa.PrivateKey
}

// PrivateKeyValues contains the values of a private key.
type PrivateKeyValues struct {
	P, Q secretdata.Bytes
	D    secretdata.Bytes
	// dp, dq and QInv must be computed by the Go library.
	// See https://pkg.go.dev/crypto/rsa#PrivateKey.
}

// privateKeySelfCheck signs a test message with a private key and verifies a
// the signature with the corresponding public key.
//
// This is a security check to ensure that the private key is valid.
func privateKeySelfCheck(privateKey *PrivateKey) error {
	signer, err := NewSigner(privateKey, internalapi.Token{})
	if err != nil {
		return err
	}
	verifier, err := NewVerifier(privateKey.publicKey, internalapi.Token{})
	if err != nil {
		return err
	}
	testMessage := []byte("Tink and Wycheproof.")
	signature, err := signer.Sign(testMessage)
	if err != nil {
		return err
	}
	return verifier.Verify(signature, testMessage)
}

// NewPrivateKey creates a new RSA-SSA-PSS PrivateKey value from a public key
// and private key values.
//
// publicKey must be non-nil.
func NewPrivateKey(publicKey *PublicKey, opts PrivateKeyValues) (*PrivateKey, error) {
	if publicKey.parameters == nil {
		return nil, fmt.Errorf("rsassapss.NewPrivateKey: invalid public key")
	}
	privateKey := rsa.PrivateKey{
		PublicKey: rsa.PublicKey{
			N: new(big.Int).SetBytes(publicKey.Modulus()),
			E: publicKey.parameters.PublicExponent(),
		},
		D: new(big.Int).SetBytes(opts.D.Data(insecuresecretdataaccess.Token{})),
		Primes: []*big.Int{
			new(big.Int).SetBytes(opts.P.Data(insecuresecretdataaccess.Token{})),
			new(big.Int).SetBytes(opts.Q.Data(insecuresecretdataaccess.Token{})),
		},
	}
	if err := privateKey.Validate(); err != nil {
		return nil, fmt.Errorf("rsassapss.NewPrivateKey: %v", err)
	}
	privateKey.Precompute()
	pk := &PrivateKey{
		publicKey:  publicKey,
		privateKey: &privateKey,
	}
	if err := privateKeySelfCheck(pk); err != nil {
		return nil, fmt.Errorf("rsassapss.NewPrivateKey: %v", err)
	}
	return pk, nil
}

// P returns the prime P.
func (k *PrivateKey) P() secretdata.Bytes {
	return secretdata.NewBytesFromData(k.privateKey.Primes[0].Bytes(), insecuresecretdataaccess.Token{})
}

// Q returns the prime Q.
func (k *PrivateKey) Q() secretdata.Bytes {
	return secretdata.NewBytesFromData(k.privateKey.Primes[1].Bytes(), insecuresecretdataaccess.Token{})
}

// D returns the private exponent D.
func (k *PrivateKey) D() secretdata.Bytes {
	return secretdata.NewBytesFromData(k.privateKey.D.Bytes(), insecuresecretdataaccess.Token{})
}

// DP returns the private prime factor P-1.
func (k *PrivateKey) DP() secretdata.Bytes {
	return secretdata.NewBytesFromData(k.privateKey.Precomputed.Dp.Bytes(), insecuresecretdataaccess.Token{})
}

// DQ returns the private prime factor Q-1.
func (k *PrivateKey) DQ() secretdata.Bytes {
	return secretdata.NewBytesFromData(k.privateKey.Precomputed.Dq.Bytes(), insecuresecretdataaccess.Token{})
}

// QInv returns the inverse of Q.
func (k *PrivateKey) QInv() secretdata.Bytes {
	return secretdata.NewBytesFromData(k.privateKey.Precomputed.Qinv.Bytes(), insecuresecretdataaccess.Token{})
}

// PublicKey returns the corresponding public key.
func (k *PrivateKey) PublicKey() (key.Key, error) { return k.publicKey, nil }

// Parameters returns the parameters of this key.
func (k *PrivateKey) Parameters() key.Parameters { return k.publicKey.Parameters() }

// IDRequirement tells whether the key ID and whether it is required.
func (k *PrivateKey) IDRequirement() (uint32, bool) { return k.publicKey.IDRequirement() }

// OutputPrefix returns the output prefix of this key.
func (k *PrivateKey) OutputPrefix() []byte { return k.publicKey.OutputPrefix() }

// Equal tells whether this key object is equal to other.
func (k *PrivateKey) Equal(other key.Key) bool {
	that, ok := other.(*PrivateKey)
	return ok && k.publicKey.Equal(that.publicKey) && k.privateKey.Equal(that.privateKey)
}

func createPrivateKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	rsaSSAPSSParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: %T", p)
	}

	hashAlg := rsaSSAPSSParams.SigHashType().String()
	exp := new(big.Int).SetUint64(uint64(rsaSSAPSSParams.PublicExponent())).Bytes()
	if err := signature.ValidateRSAPublicKeyParams(hashAlg, rsaSSAPSSParams.ModulusSizeBits(), exp); err != nil {
		return nil, fmt.Errorf("invalid parameters: %w", err)
	}

	rsaKey, err := rsa.GenerateKey(rand.Reader, int(rsaSSAPSSParams.ModulusSizeBits()))
	if err != nil {
		return nil, fmt.Errorf("generating RSA key: %s", err)
	}

	rsaSSAPSSPublicKey, err := NewPublicKey(rsaKey.PublicKey.N.Bytes(), idRequirement, rsaSSAPSSParams)
	if err != nil {
		return nil, fmt.Errorf("creating public key: %s", err)
	}

	return NewPrivateKey(rsaSSAPSSPublicKey, PrivateKeyValues{
		P: secretdata.NewBytesFromData(rsaKey.Primes[0].Bytes(), insecuresecretdataaccess.Token{}),
		Q: secretdata.NewBytesFromData(rsaKey.Primes[1].Bytes(), insecuresecretdataaccess.Token{}),
		D: secretdata.NewBytesFromData(rsaKey.D.Bytes(), insecuresecretdataaccess.Token{}),
	})
}
