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
	"bytes"
	"crypto/ed25519"
	"fmt"
	"slices"

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// verifier is an implementation of [tink.Verifier] for ED25519.
type verifier struct {
	publicKey ed25519.PublicKey
	prefix    []byte
	variant   Variant
}

var _ tink.Verifier = (*verifier)(nil)

// NewVerifier creates a new [tink.Verifier] for ED25519.
//
// This is an internal API.
func NewVerifier(publicKey *PublicKey, _ internalapi.Token) (tink.Verifier, error) {
	return &verifier{
		publicKey: publicKey.KeyBytes(),
		variant:   publicKey.params.Variant(),
		prefix:    publicKey.OutputPrefix(),
	}, nil
}

// Verify verifies whether the given signature is valid for the given data.
//
// It returns an error if the prefix is not valid or the signature is not
// valid.
func (e *verifier) Verify(signature, data []byte) error {
	if !bytes.HasPrefix(signature, e.prefix) {
		return fmt.Errorf("ed25519: the signature doesn't have the expected prefix")
	}
	signatureNoPrefix := signature[len(e.prefix):]
	if len(signatureNoPrefix) != ed25519.SignatureSize {
		return fmt.Errorf("ed25519: the length of the signature is not %d", ed25519.SignatureSize)
	}
	signedMessage := data
	if e.variant == VariantLegacy {
		signedMessage = slices.Concat(data, []byte{0})
	}
	if !ed25519.Verify(e.publicKey, signedMessage, signatureNoPrefix) {
		return fmt.Errorf("ed25519: invalid signature")
	}
	return nil
}

func verifierConstructor(key key.Key) (any, error) {
	that, ok := key.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("key is not a *ed25519.PublicKey")
	}
	return NewVerifier(that, internalapi.Token{})
}
