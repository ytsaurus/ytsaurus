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

package rsassapkcs1

import (
	"bytes"
	"crypto/rsa"
	"fmt"
	"math/big"
	"slices"

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/signature"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// verifier is an implementation of [tink.Verifier] for RSA-SSA-PKCS1.
type verifier struct {
	rawVerifier tink.Verifier
	variant     Variant
	prefix      []byte
}

var _ tink.Verifier = (*verifier)(nil)

// NewVerifier returns a new [tink.Verifier] that implements the primitive
// described by pubKey.
func NewVerifier(pubKey *PublicKey, _ internalapi.Token) (tink.Verifier, error) {
	v, err := signature.New_RSA_SSA_PKCS1_Verifier(pubKey.parameters.HashType().String(), &rsa.PublicKey{
		N: new(big.Int).SetBytes(pubKey.Modulus()),
		E: pubKey.parameters.PublicExponent(),
	})
	if err != nil {
		return nil, err
	}
	return &verifier{
		rawVerifier: v,
		prefix:      pubKey.OutputPrefix(),
		variant:     pubKey.parameters.Variant(),
	}, nil
}

// Verify verifies the signature on the given data.
func (v *verifier) Verify(sig, data []byte) error {
	if !bytes.HasPrefix(sig, v.prefix) {
		return fmt.Errorf("signature does not start with prefix")
	}
	toVerify := data
	if v.variant == VariantLegacy {
		toVerify = slices.Concat(data, []byte{0})
	}
	signatureWithoutPrefix := sig[len(v.prefix):]
	return v.rawVerifier.Verify(signatureWithoutPrefix, toVerify)
}

func verifierConstructor(key key.Key) (any, error) {
	that, ok := key.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("key is not a *rsassapkcs1.PublicKey")
	}
	return NewVerifier(that, internalapi.Token{})
}
