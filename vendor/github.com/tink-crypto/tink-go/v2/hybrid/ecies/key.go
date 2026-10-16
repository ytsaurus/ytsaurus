// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ecies

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

// PublicKey represents an ECIES public key.
type PublicKey struct {
	// A public point representing the public key. This can be either:
	//  - Uncompressed encoded EC point as per [SEC 1 v2.0, Section 2.3.3] if Nist*.
	//  - An X25519 public key bytes.
	publicKeyBytes []byte
	idRequirement  uint32
	outputPrefix   []byte
	parameters     *Parameters
}

var _ key.Key = (*PublicKey)(nil)

func calculateOutputPrefix(variant Variant, idRequirement uint32) ([]byte, error) {
	switch variant {
	case VariantTink:
		return outputprefix.Tink(idRequirement), nil
	case VariantCrunchy:
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
	case NISTP256:
		return ecdh.P256(), nil
	case NISTP384:
		return ecdh.P384(), nil
	case NISTP521:
		return ecdh.P521(), nil
	case X25519:
		return ecdh.X25519(), nil
	default:
		return nil, fmt.Errorf("invalid curve type: %v", ct)
	}
}

// NewPublicKey creates a new ECIES PublicKey.
//
// publicKeyBytes belongs to either a NIST Curve or Curve25519.
func NewPublicKey(publicKeyBytes []byte, idRequirement uint32, parameters *Parameters) (*PublicKey, error) {
	if parameters.Variant() == VariantNoPrefix && idRequirement != 0 {
		return nil, fmt.Errorf("ecies.NewPublicKey: key ID must be zero for VariantNoPrefix")
	}
	outputPrefix, err := calculateOutputPrefix(parameters.Variant(), idRequirement)
	if err != nil {
		return nil, fmt.Errorf("ecies.NewPublicKey: %v", err)
	}
	curve, err := ecdhCurveFromCurveType(parameters.CurveType())
	if err != nil {
		return nil, fmt.Errorf("ecies.NewPublicKey: %v", err)
	}
	// Validate the point.
	if _, err := curve.NewPublicKey(publicKeyBytes); err != nil {
		return nil, fmt.Errorf("ecies.NewPublicKey: point validation failed: %v", err)
	}
	return &PublicKey{
		publicKeyBytes: bytes.Clone(publicKeyBytes),
		idRequirement:  idRequirement,
		outputPrefix:   outputPrefix,
		parameters:     parameters,
	}, nil
}

// PublicKeyBytes returns the public key bytes.
func (k *PublicKey) PublicKeyBytes() []byte { return k.publicKeyBytes }

// Parameters returns the parameters of this key.
func (k *PublicKey) Parameters() key.Parameters { return k.parameters }

// IDRequirement returns the key ID and whether it is required.
func (k *PublicKey) IDRequirement() (uint32, bool) {
	return k.idRequirement, k.Parameters().HasIDRequirement()
}

// OutputPrefix returns the output prefix of this key.
func (k *PublicKey) OutputPrefix() []byte { return bytes.Clone(k.outputPrefix) }

// Equal tells whether this key value is equal to other.
func (k *PublicKey) Equal(other key.Key) bool {
	otherKey, ok := other.(*PublicKey)
	return ok && k.Parameters().Equal(otherKey.Parameters()) &&
		k.idRequirement == otherKey.idRequirement &&
		bytes.Equal(k.publicKeyBytes, otherKey.publicKeyBytes)
}

// PrivateKey represents an ECIES private key.
type PrivateKey struct {
	publicKey       *PublicKey
	privateKeyBytes secretdata.Bytes
}

var _ key.Key = (*PrivateKey)(nil)

// NewPrivateKey creates a new ECIES private key from privateKeyBytes,
// idRequirement and a [Parameters].
//
// If X25519 curve is used, the private key value must be 32 bytes.
// If NIST curve is used, the private key value must be octet encoded as per
// [SEC 1 v2.0, Section 2.3.5].
//
// [SEC 1 v2.0, Section 2.3.5]: https://www.secg.org/sec1-v2.pdf#page=17.08
func NewPrivateKey(privateKeyBytes secretdata.Bytes, idRequirement uint32, params *Parameters) (*PrivateKey, error) {
	curveType := params.CurveType()
	curve, err := ecdhCurveFromCurveType(curveType)
	if err != nil {
		return nil, err
	}
	ecdhPrivateKey, err := curve.NewPrivateKey(privateKeyBytes.Data(insecuresecretdataaccess.Token{}))
	if err != nil {
		return nil, fmt.Errorf("ecies.NewPrivateKey: private key validation failed: %v", err)
	}
	publicKey, err := NewPublicKey(ecdhPrivateKey.PublicKey().Bytes(), idRequirement, params)
	if err != nil {
		return nil, fmt.Errorf("ecies.NewPrivateKey: %v", err)
	}
	return &PrivateKey{
		publicKey:       publicKey,
		privateKeyBytes: privateKeyBytes,
	}, nil
}

// NewPrivateKeyFromPublicKey creates a new ECIES private key from
// privateKeyBytes and a [PublicKey].
//
// If X25519 curve is used, the private key value must be 32 bytes.
// If NIST curve is used, the private key value must be octet encoded as per
// [SEC 1 v2.0, Section 2.3.5].
//
// [SEC 1 v2.0, Section 2.3.5]: https://www.secg.org/sec1-v2.pdf#page=17.08
func NewPrivateKeyFromPublicKey(privateKeyBytes secretdata.Bytes, pubKey *PublicKey) (*PrivateKey, error) {
	curveType := pubKey.Parameters().(*Parameters).CurveType()
	curve, err := ecdhCurveFromCurveType(curveType)
	if err != nil {
		return nil, fmt.Errorf("ecies.NewPrivateKey: %v", err)
	}
	ecdhPrivateKey, err := curve.NewPrivateKey(privateKeyBytes.Data(insecuresecretdataaccess.Token{}))
	if err != nil {
		return nil, fmt.Errorf("ecies.NewPrivateKey: private key validation failed: %v", err)
	}
	ecdhPublicKeyFromPublicKey, err := curve.NewPublicKey(pubKey.publicKeyBytes)
	if err != nil {
		// Should never happen.
		return nil, fmt.Errorf("ecies.NewPrivateKey: invalid public key point: %v", err)
	}
	if !ecdhPrivateKey.PublicKey().Equal(ecdhPublicKeyFromPublicKey) {
		return nil, fmt.Errorf("ecies.NewPrivateKey: 	invalid private key value")
	}
	return &PrivateKey{
		publicKey:       pubKey,
		privateKeyBytes: privateKeyBytes,
	}, nil
}

// PrivateKeyBytes returns the private key bytes.
func (k *PrivateKey) PrivateKeyBytes() secretdata.Bytes { return k.privateKeyBytes }

// PublicKey returns the public key of the key.
//
// This implements the privateKey interface defined in handle.go.
func (k *PrivateKey) PublicKey() (key.Key, error) { return k.publicKey, nil }

// Parameters returns the parameters of the key.
func (k *PrivateKey) Parameters() key.Parameters { return k.publicKey.Parameters() }

// IDRequirement returns the ID requirement of the key, and whether it is
// required.
func (k *PrivateKey) IDRequirement() (uint32, bool) { return k.publicKey.IDRequirement() }

// OutputPrefix returns the output prefix of this key.
func (k *PrivateKey) OutputPrefix() []byte { return bytes.Clone(k.publicKey.outputPrefix) }

// Equal returns true if this key is equal to other.
func (k *PrivateKey) Equal(other key.Key) bool {
	otherKey, ok := other.(*PrivateKey)
	return ok && k.publicKey.Equal(otherKey.publicKey) &&
		k.privateKeyBytes.Equal(otherKey.privateKeyBytes)
}

func createPrivateKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	eciesParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: %T, want %T", p, (*Parameters)(nil))
	}
	curve, err := ecdhCurveFromCurveType(eciesParams.CurveType())
	if err != nil {
		return nil, err
	}
	privKeyBytes, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return NewPrivateKey(secretdata.NewBytesFromData(privKeyBytes.Bytes(), insecuresecretdataaccess.Token{}), idRequirement, eciesParams)
}
