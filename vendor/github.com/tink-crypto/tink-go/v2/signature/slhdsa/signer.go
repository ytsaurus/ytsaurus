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
	"fmt"
	"slices"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/signature/slhdsa"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// signer is an implementation of [tink.Signer] for SLH-DSA.
type signer struct {
	secretKey *slhdsa.SecretKey
	prefix    []byte
	variant   Variant
}

var _ tink.Signer = (*signer)(nil)

// These checks are gated by NewParameters filtering out invalid parameters.
func slhdsaSecretKeyFromPrivateKey(privateKey *PrivateKey) (*slhdsa.SecretKey, error) {
	if privateKey.publicKey.params.paramSet == slhDSASHA2128s() {
		return slhdsa.SLH_DSA_SHA2_128s.DecodeSecretKey(privateKey.keyBytes.Data(insecuresecretdataaccess.Token{}))
	}
	if privateKey.publicKey.params.paramSet == slhDSASHAKE256f() {
		return slhdsa.SLH_DSA_SHAKE_256f.DecodeSecretKey(privateKey.keyBytes.Data(insecuresecretdataaccess.Token{}))
	}
	return nil, fmt.Errorf("invalid parameters: %v", privateKey.publicKey.params)
}

// NewSigner creates a new [tink.Signer] for SLH-DSA.
//
// This is an internal API.
func NewSigner(privateKey *PrivateKey, _ internalapi.Token) (tink.Signer, error) {
	secretKey, err := slhdsaSecretKeyFromPrivateKey(privateKey)
	if err != nil {
		return nil, err
	}
	return &signer{
		secretKey: secretKey,
		prefix:    privateKey.OutputPrefix(),
		variant:   privateKey.publicKey.params.Variant(),
	}, nil
}

// Sign computes a signature for the given data.
//
// If the key has a prefix, the signature will be prefixed with the output
// prefix.
func (e *signer) Sign(data []byte) ([]byte, error) {
	r, err := e.secretKey.Sign(data, nil)
	if err != nil {
		return nil, err
	}
	return slices.Concat(e.prefix, r), nil
}

func signerConstructor(key key.Key) (any, error) {
	that, ok := key.(*PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is not a %T", (*PrivateKey)(nil))
	}
	return NewSigner(that, internalapi.Token{})
}
