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
	"fmt"
	"slices"

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/signature"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// signer is an implementation of [tink.Signer] for RSA-SSA-PKCS1.
type signer struct {
	rawSigner tink.Signer
	prefix    []byte
	variant   Variant
}

var _ tink.Signer = (*signer)(nil)

// NewSigner returns a new [tink.Signer] that implements the primitive
// described by privateKey.
func NewSigner(privateKey *PrivateKey, _ internalapi.Token) (tink.Signer, error) {
	rawPrimitive, err := signature.New_RSA_SSA_PKCS1_Signer(privateKey.publicKey.parameters.HashType().String(), privateKey.privateKey)
	if err != nil {
		return nil, err
	}
	return &signer{
		rawSigner: rawPrimitive,
		prefix:    privateKey.OutputPrefix(),
		variant:   privateKey.publicKey.parameters.Variant(),
	}, nil
}

// Sign computes a signature for the given data.
func (s *signer) Sign(data []byte) ([]byte, error) {
	toSign := data
	if s.variant == VariantLegacy {
		toSign = slices.Concat(data, []byte{0})
	}
	sig, err := s.rawSigner.Sign(toSign)
	if err != nil {
		return nil, err
	}
	return slices.Concat(s.prefix, sig), nil
}

func signerConstructor(key key.Key) (any, error) {
	that, ok := key.(*PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is not a *rsassapkcs1.PrivateKey")
	}
	return NewSigner(that, internalapi.Token{})
}
