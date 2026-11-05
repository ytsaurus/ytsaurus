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

package rsassapss

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

// verifier is an implementation of Verifier for RSA-SSA-PSS.
type verifier struct {
	rawVerifier *signature.RSA_SSA_PSS_Verifier
	prefix      []byte
	variant     Variant
}

var _ tink.Verifier = (*verifier)(nil)

// NewVerifier creates a new [tink.Verifier] that implements a full RSA-SSA-PSS
// primitive from the given [PublicKey].
func NewVerifier(publicKey *PublicKey, _ internalapi.Token) (tink.Verifier, error) {
	rawVerifier, err := signature.New_RSA_SSA_PSS_Verifier(publicKey.parameters.SigHashType().String(), publicKey.parameters.SaltLengthBytes(), &rsa.PublicKey{
		N: new(big.Int).SetBytes(publicKey.Modulus()),
		E: publicKey.parameters.PublicExponent(),
	})
	if err != nil {
		return nil, err
	}
	return &verifier{
		rawVerifier: rawVerifier,
		prefix:      publicKey.OutputPrefix(),
		variant:     publicKey.parameters.Variant(),
	}, nil
}

// Verify verifies the signature on the given data.
func (v *verifier) Verify(signatureBytes, data []byte) error {
	if !bytes.HasPrefix(signatureBytes, v.prefix) {
		return fmt.Errorf("signature does not start with prefix %x", v.prefix)
	}
	toVerify := data
	if v.variant == VariantLegacy {
		toVerify = slices.Concat(data, []byte{0})
	}
	signatureWithoutPrefix := signatureBytes[len(v.prefix):]
	return v.rawVerifier.Verify(signatureWithoutPrefix, toVerify)
}

func verifierConstructor(key key.Key) (any, error) {
	that, ok := key.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("key is not a *rsassapss.PublicKey")
	}
	return NewVerifier(that, internalapi.Token{})
}
