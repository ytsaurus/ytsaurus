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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"math/big"

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	internalecdsa "github.com/tink-crypto/tink-go/v2/internal/signature/ecdsa"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// verifier implements the [tink.Verifier] interface for ECDSA (RFC6979).
//
// It accepts signature in both ASN.1 and IEEE_P1363 encoding.
type verifier struct {
	key        *ecdsa.PublicKey
	prefix     []byte
	parameters *Parameters
	hashFunc   func() hash.Hash
}

var _ tink.Verifier = (*verifier)(nil)

func curveFromTinkECDSACurveType(curveType CurveType) (elliptic.Curve, error) {
	switch curveType {
	case NistP256:
		return elliptic.P256(), nil
	case NistP384:
		return elliptic.P384(), nil
	case NistP521:
		return elliptic.P521(), nil
	default:
		// Should never happen.
		return nil, fmt.Errorf("unsupported curve: %v", curveType)
	}
}

func hashFunctionFromEnum(hash HashType) (func() hash.Hash, error) {
	switch hash {
	case SHA256:
		return sha256.New, nil
	case SHA384:
		return sha512.New384, nil
	case SHA512:
		return sha512.New, nil
	default:
		return nil, fmt.Errorf("invalid hash type: %s", hash)
	}
}

// NewVerifier creates a new ECDSA Verifier.
//
// This is an internal API.
func NewVerifier(publicKey *PublicKey, _ internalapi.Token) (tink.Verifier, error) {
	hashFunc, err := hashFunctionFromEnum(publicKey.parameters.HashType())
	if err != nil {
		return nil, err
	}
	curve, err := curveFromTinkECDSACurveType(publicKey.parameters.CurveType())
	if err != nil {
		return nil, err
	}
	publicPoint := publicKey.PublicPoint()
	xy := publicPoint[1:]
	ecdsaPublicKey := &ecdsa.PublicKey{
		Curve: curve,
		X:     new(big.Int).SetBytes(xy[:len(xy)/2]),
		Y:     new(big.Int).SetBytes(xy[len(xy)/2:]),
	}
	return &verifier{
		key:        ecdsaPublicKey,
		prefix:     publicKey.OutputPrefix(),
		parameters: publicKey.parameters,
		hashFunc:   hashFunc,
	}, nil
}

// Verify verifies whether the given signature is valid for the given data.
//
// The signature is expected to be of the form: prefix || signature, where
// prefix is the key's output prefix and can be empty, and signature is the
// signature in the encoding specified by the key's parameters.
func (e *verifier) Verify(signatureBytes, data []byte) error {
	if !bytes.HasPrefix(signatureBytes, e.prefix) {
		return fmt.Errorf("ecdsa_verifier: invalid signature prefix")
	}
	rawSignature := signatureBytes[len(e.prefix):]
	h := e.hashFunc()
	h.Write(data)
	if e.parameters.Variant() == VariantLegacy {
		h.Write([]byte{0})
	}
	hashed := h.Sum(nil)
	var asn1Signature []byte
	switch encoding := e.parameters.SignatureEncoding(); encoding {
	case DER:
		asn1Signature = rawSignature
	case IEEEP1363:
		decodedSig, err := internalecdsa.IEEEP1363Decode(rawSignature)
		if err != nil {
			return err
		}
		asn1Signature, err = internalecdsa.ASN1Encode(decodedSig)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("ecdsa_verifier: unsupported encoding: %s", encoding)
	}
	if ok := ecdsa.VerifyASN1(e.key, hashed, asn1Signature); !ok {
		return fmt.Errorf("ecdsa_verifier: invalid signature")
	}
	return nil
}

func verifierConstructor(key key.Key) (any, error) {
	that, ok := key.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("key is not a *ecdsa.PublicKey")
	}
	return NewVerifier(that, internalapi.Token{})
}
