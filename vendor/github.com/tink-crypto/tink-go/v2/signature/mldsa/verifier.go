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

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/signature/mldsa"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// verifier is an implementation of [tink.Verifier] for ML-DSA.
type verifier struct {
	publicKey *mldsa.PublicKey
	prefix    []byte
	variant   Variant
}

var _ tink.Verifier = (*verifier)(nil)

func mldsaPublicKeyFromPublicKey(publicKey *PublicKey) (*mldsa.PublicKey, error) {
	switch publicKey.params.instance {
	case MLDSA65:
		return mldsa.MLDSA65.DecodePublicKey(publicKey.KeyBytes())
	case MLDSA87:
		return mldsa.MLDSA87.DecodePublicKey(publicKey.KeyBytes())
	default:
		return &mldsa.PublicKey{}, fmt.Errorf("invalid instance: %v", publicKey.params.instance)
	}
}

// NewVerifier creates a new [tink.Verifier] for ML-DSA.
//
// This is an internal API.
func NewVerifier(publicKey *PublicKey, _ internalapi.Token) (tink.Verifier, error) {
	pubKey, err := mldsaPublicKeyFromPublicKey(publicKey)
	if err != nil {
		return nil, err
	}
	return &verifier{
		publicKey: pubKey,
		variant:   publicKey.params.Variant(),
		prefix:    publicKey.OutputPrefix(),
	}, nil
}

// Verify verifies whether the given signature is valid for the given data.
//
// It returns an error if the prefix is not valid or the signature is not
// valid.
func (v *verifier) Verify(signature, data []byte) error {
	if !bytes.HasPrefix(signature, v.prefix) {
		return fmt.Errorf("the signature does not have the expected prefix")
	}
	return v.publicKey.Verify(data, signature[len(v.prefix):], nil)
}

func verifierConstructor(key key.Key) (any, error) {
	publicKey, ok := key.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("key is not a %T", (*PublicKey)(nil))
	}
	return NewVerifier(publicKey, internalapi.Token{})
}
