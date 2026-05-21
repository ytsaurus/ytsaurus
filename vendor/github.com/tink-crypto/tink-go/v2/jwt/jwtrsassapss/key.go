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

package jwtrsassapss

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// PublicKey represents a public key for JWT RSA-SSA-PSS signing.
type PublicKey struct {
	parameters    *Parameters
	modulus       []byte // Big integer value in big-endian encoding.
	idRequirement uint32
	kid           string
	hasKID        bool
}

var _ key.Key = (*PublicKey)(nil)

func computeKID(customKID *string, idRequirement uint32, parameters *Parameters) (string, bool, error) {
	switch parameters.KIDStrategy() {
	case Base64EncodedKeyIDAsKID:
		if customKID != nil {
			return "", false, fmt.Errorf("custom KID is not supported for KID strategy: %v", parameters.KIDStrategy())
		}
		// Serialize the ID requirement.
		idRequirementBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(idRequirementBytes, idRequirement)
		return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(idRequirementBytes), true, nil
	case IgnoredKID:
		if customKID != nil {
			return "", false, fmt.Errorf("custom KID is not supported for KID strategy: %v", parameters.KIDStrategy())
		}
		return "", false, nil
	case CustomKID:
		if customKID == nil {
			return "", false, fmt.Errorf("custom KID is required for KID strategy: %v", parameters.KIDStrategy())
		}
		return *customKID, true, nil
	default:
		return "", false, fmt.Errorf("invalid KID strategy: %v", parameters.KIDStrategy())
	}
}

// PublicKeyOpts are [PublicKey] options.
type PublicKeyOpts struct {
	Modulus       []byte
	IDRequirement uint32
	CustomKID     string
	HasCustomKID  bool
	Parameters    *Parameters
}

// NewPublicKey creates a new [PublicKey].
//
// The modulus is expected to be in big-endian encoding.
// The ID requirement must be 0 if the KID is not required.
func NewPublicKey(opts PublicKeyOpts) (*PublicKey, error) {
	if opts.Parameters == nil {
		return nil, fmt.Errorf("jwtrsassapss.NewPublicKey: parameters can't be nil")
	}
	if !opts.Parameters.HasIDRequirement() && opts.IDRequirement != 0 {
		return nil, fmt.Errorf("jwtrsassapss.NewPublicKey: ID requirement must be 0 if ID is not required")
	}

	modulusBigInt := new(big.Int).SetBytes(opts.Modulus)
	if modulusBigInt.BitLen() != opts.Parameters.ModulusSizeInBits() {
		return nil, fmt.Errorf("jwtrsassapss.NewPublicKey: invalid modulus bit-length: %v, want %v", modulusBigInt.BitLen(), opts.Parameters.ModulusSizeInBits())
	}

	var customKID *string = nil
	if opts.HasCustomKID {
		customKID = &opts.CustomKID
	}
	kid, hasKID, err := computeKID(customKID, opts.IDRequirement, opts.Parameters)
	if err != nil {
		return nil, fmt.Errorf("jwtrsassapss.NewPublicKey: %v", err)
	}
	return &PublicKey{
		parameters:    opts.Parameters,
		modulus:       opts.Modulus,
		idRequirement: opts.IDRequirement,
		kid:           kid,
		hasKID:        hasKID,
	}, nil
}

// Parameters returns the parameters of the key.
func (k *PublicKey) Parameters() key.Parameters { return k.parameters }

// Modulus returns the public key modulus.
func (k *PublicKey) Modulus() []byte { return bytes.Clone(k.modulus) }

// KID returns the KID for this key.
//
// If no kid is set, it returns ("", false).
func (k *PublicKey) KID() (string, bool) { return k.kid, k.hasKID }

// IDRequirement returns the ID requirement for this key.
func (k *PublicKey) IDRequirement() (uint32, bool) {
	return k.idRequirement, k.parameters.HasIDRequirement()
}

// Equal returns true if k and other are equal.
// Note that the comparison is not constant time.
func (k *PublicKey) Equal(other key.Key) bool {
	that, ok := other.(*PublicKey)
	return ok && k.parameters.Equal(that.parameters) &&
		bytes.Equal(k.modulus, that.modulus) &&
		k.idRequirement == that.idRequirement &&
		k.kid == that.kid && k.hasKID == that.hasKID
}

// PrivateKey represents a private key for JWT RSA-SSA-PSS signing.
type PrivateKey struct {
	publicKey  *PublicKey
	privateKey *rsa.PrivateKey
}

// PrivateKeyOpts are [PrivateKey] options.
type PrivateKeyOpts struct {
	PublicKey *PublicKey
	D         secretdata.Bytes
	P         secretdata.Bytes
	Q         secretdata.Bytes
	// dp, dq and QInv must be computed by the Go library.
	// See https://pkg.go.dev/crypto/rsa#PrivateKey.
}

// NewPrivateKey creates a new JWT RSA-SSA-PSS private key.
func NewPrivateKey(opts PrivateKeyOpts) (*PrivateKey, error) {
	if opts.PublicKey == nil {
		return nil, fmt.Errorf("jwtrsassapss.NewPrivateKey: public key cannot be nil")
	}
	privateKey := rsa.PrivateKey{
		PublicKey: rsa.PublicKey{
			N: new(big.Int).SetBytes(opts.PublicKey.Modulus()),
			E: opts.PublicKey.Parameters().(*Parameters).PublicExponent(),
		},
		D: new(big.Int).SetBytes(opts.D.Data(insecuresecretdataaccess.Token{})),
		Primes: []*big.Int{
			new(big.Int).SetBytes(opts.P.Data(insecuresecretdataaccess.Token{})),
			new(big.Int).SetBytes(opts.Q.Data(insecuresecretdataaccess.Token{})),
		},
	}
	if err := privateKey.Validate(); err != nil {
		return nil, fmt.Errorf("jwtrsassapss.NewPrivateKey: %v", err)
	}
	// We don't use opts.DP, opts.DQ, opts.QI directly, because rsa.PrivateKey.Validate()
	// does not check if they are correct. Instead, we call Precompute() to derive
	// them from P, Q and D, and then use the derived values. This ensures that the
	// precomputed values are correct.
	privateKey.Precompute()

	return &PrivateKey{
		publicKey:  opts.PublicKey,
		privateKey: &privateKey,
	}, nil
}

// Parameters returns the parameters of the key.
func (k *PrivateKey) Parameters() key.Parameters { return k.publicKey.Parameters() }

// PublicKey returns the public key.
func (k *PrivateKey) PublicKey() (key.Key, error) { return k.publicKey, nil }

// IDRequirement returns the ID requirement for this key.
func (k *PrivateKey) IDRequirement() (uint32, bool) { return k.publicKey.IDRequirement() }

// D returns the private exponent D.
func (k *PrivateKey) D() secretdata.Bytes {
	return secretdata.NewBytesFromData(k.privateKey.D.Bytes(), insecuresecretdataaccess.Token{})
}

// P returns the prime factor P.
func (k *PrivateKey) P() secretdata.Bytes {
	return secretdata.NewBytesFromData(k.privateKey.Primes[0].Bytes(), insecuresecretdataaccess.Token{})
}

// Q returns the prime factor Q.
func (k *PrivateKey) Q() secretdata.Bytes {
	return secretdata.NewBytesFromData(k.privateKey.Primes[1].Bytes(), insecuresecretdataaccess.Token{})
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

// Equal returns true if k and other are equal.
// Note that the comparison is not constant time.
func (k *PrivateKey) Equal(other key.Key) bool {
	that, ok := other.(*PrivateKey)
	if !ok {
		return false
	}
	return ok && k.publicKey.Equal(that.publicKey) && k.privateKey.Equal(that.privateKey)
}

func createPrivateKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	jwtRSASSAPKCS1Params, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("jwtrsassapss.createPrivateKey: invalid parameters type: want %T, got %T", (*Parameters)(nil), p)
	}
	if jwtRSASSAPKCS1Params.KIDStrategy() == CustomKID {
		return nil, fmt.Errorf("jwtrsassapss.createPrivateKey: key generation is not supported for strategy %v", jwtRSASSAPKCS1Params.KIDStrategy())
	}
	rsaKey, err := rsa.GenerateKey(rand.Reader, int(jwtRSASSAPKCS1Params.ModulusSizeInBits()))
	if err != nil {
		return nil, err
	}
	publicKey, err := NewPublicKey(PublicKeyOpts{
		Modulus:       rsaKey.PublicKey.N.Bytes(),
		IDRequirement: idRequirement,
		HasCustomKID:  false,
		Parameters:    jwtRSASSAPKCS1Params,
	})
	if err != nil {
		return nil, fmt.Errorf("jwtrsassapss.createPrivateKey: %v", err)
	}
	privateKey, err := NewPrivateKey(PrivateKeyOpts{
		PublicKey: publicKey,
		D:         secretdata.NewBytesFromData(rsaKey.D.Bytes(), insecuresecretdataaccess.Token{}),
		P:         secretdata.NewBytesFromData(rsaKey.Primes[0].Bytes(), insecuresecretdataaccess.Token{}),
		Q:         secretdata.NewBytesFromData(rsaKey.Primes[1].Bytes(), insecuresecretdataaccess.Token{}),
	})
	if err != nil {
		return nil, fmt.Errorf("jwtrsassapss.createPrivateKey: %v", err)
	}
	return privateKey, nil
}
