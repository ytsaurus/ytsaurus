// Copyright 2020 Google LLC
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

package ed25519

import (
	"crypto/ed25519"
	"fmt"
	"slices"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// signer is an implementation of [tink.Signer] for ED25519.
type signer struct {
	privateKey ed25519.PrivateKey
	prefix     []byte
	variant    Variant
}

var _ tink.Signer = (*signer)(nil)

// NewSigner creates a new [tink.Signer] for ED25519.
//
// This is an internal API.
func NewSigner(privateKey *PrivateKey, _ internalapi.Token) (tink.Signer, error) {
	return &signer{
		privateKey: ed25519.NewKeyFromSeed(privateKey.PrivateKeyBytes().Data(insecuresecretdataaccess.Token{})),
		prefix:     privateKey.OutputPrefix(),
		variant:    privateKey.publicKey.params.Variant(),
	}, nil
}

// Sign computes a signature for the given data.
//
// If the key has prefix, the signature will be prefixed with the output
// prefix.
func (e *signer) Sign(data []byte) ([]byte, error) {
	messageToSign := data
	if e.variant == VariantLegacy {
		messageToSign = slices.Concat(data, []byte{0})
	}
	r := ed25519.Sign(e.privateKey, messageToSign)
	if len(r) != ed25519.SignatureSize {
		return nil, fmt.Errorf("ed25519: invalid signature")
	}
	return slices.Concat(e.prefix, r), nil
}

func signerConstructor(key key.Key) (any, error) {
	that, ok := key.(*PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is not a *ed25519.PrivateKey")
	}
	return NewSigner(that, internalapi.Token{})
}
