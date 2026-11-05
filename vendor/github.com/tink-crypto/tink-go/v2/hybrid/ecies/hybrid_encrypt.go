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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or rawHybridEncryptied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ecies

import (
	"fmt"
	"math/big"
	"slices"

	"github.com/tink-crypto/tink-go/v2/hybrid/internal/ecies"
	"github.com/tink-crypto/tink-go/v2/hybrid/subtle"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

type hybridEncrypt struct {
	rawHybridEncrypt tink.HybridEncrypt
	prefix           []byte
	variant          Variant
}

// NewHybridEncrypt creates a new instance of [tink.HybridEncrypt] from a
// [PublicKey].
//
// This is an internal API.
func NewHybridEncrypt(publicKey *PublicKey, _ internalapi.Token) (tink.HybridEncrypt, error) {
	if publicKey == nil {
		return nil, fmt.Errorf("publicKey is nil")
	}
	params := publicKey.Parameters().(*Parameters)
	curve, err := subtle.GetCurve(params.CurveType().String())
	if err != nil {
		return nil, err
	}
	salt := params.Salt()
	hash := params.HashType().String()
	pointFormat := pointFormatToSubtleString(params.NISTCurvePointFormat())
	xy := publicKey.PublicKeyBytes()[1:]
	coordinateSize, err := coordinateSizeForCurve(params.CurveType())
	if err != nil {
		return nil, err
	}
	rDem, err := ecies.NewDEMHelper(params.DEMParameters())
	if err != nil {
		return nil, err
	}
	rawHybridEncrypt, err := subtle.NewECIESAEADHKDFHybridEncrypt(&subtle.ECPublicKey{
		Curve: curve,
		Point: subtle.ECPoint{
			X: new(big.Int).SetBytes(xy[:coordinateSize]),
			Y: new(big.Int).SetBytes(xy[coordinateSize:]),
		},
	}, salt, hash, pointFormat, rDem)
	if err != nil {
		return nil, err
	}
	return &hybridEncrypt{
		rawHybridEncrypt: rawHybridEncrypt,
		prefix:           publicKey.OutputPrefix(),
		variant:          publicKey.Parameters().(*Parameters).Variant(),
	}, nil
}

func (e *hybridEncrypt) Encrypt(plaintext, contextInfo []byte) ([]byte, error) {
	rawCiphertext, err := e.rawHybridEncrypt.Encrypt(plaintext, contextInfo)
	if err != nil {
		return nil, err
	}
	return slices.Concat(e.prefix, rawCiphertext), nil
}

func hybridEncryptConstructor(k key.Key) (any, error) {
	that, ok := k.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("invalid key type, got %T, want %T", k, (*PublicKey)(nil))
	}
	return NewHybridEncrypt(that, internalapi.Token{})
}
