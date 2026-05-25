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

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/signature/slhdsa"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// verifier is an implementation of [tink.Verifier] for SLH-DSA.
type verifier struct {
	publicKey *slhdsa.PublicKey
	prefix    []byte
	variant   Variant
}

var _ tink.Verifier = (*verifier)(nil)

// These checks are gated by NewParameters filtering out invalid parameters.
func slhdsaPublicKeyFromPublicKey(publicKey *PublicKey) (*slhdsa.PublicKey, error) {
	if publicKey.params.paramSet == slhDSASHA2128s() {
		return slhdsa.SLH_DSA_SHA2_128s.DecodePublicKey(publicKey.KeyBytes())
	}
	if publicKey.params.paramSet == slhDSASHAKE256f() {
		return slhdsa.SLH_DSA_SHAKE_256f.DecodePublicKey(publicKey.KeyBytes())
	}
	return nil, fmt.Errorf("invalid parameters: %v", publicKey.params)
}

// NewVerifier creates a new [tink.Verifier] for SLH-DSA.
//
// This is an internal API.
func NewVerifier(publicKey *PublicKey, _ internalapi.Token) (tink.Verifier, error) {
	pubKey, err := slhdsaPublicKeyFromPublicKey(publicKey)
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
