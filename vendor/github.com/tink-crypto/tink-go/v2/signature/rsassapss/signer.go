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
	"fmt"
	"slices"

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/signature"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// signer is an implementation of Signer for RSA-SSA-PSS.
type signer struct {
	rawSigner *signature.RSA_SSA_PSS_Signer
	prefix    []byte
	variant   Variant
}

var _ tink.Signer = (*signer)(nil)

// NewSigner creates a new [tink.Signer] that implements a full RSA-SSA-PSS
// primitive from the given [PrivateKey].
func NewSigner(privateKey *PrivateKey, _ internalapi.Token) (tink.Signer, error) {
	params := privateKey.publicKey.parameters
	rawSigner, err := signature.New_RSA_SSA_PSS_Signer(params.SigHashType().String(), params.SaltLengthBytes(), privateKey.privateKey)
	if err != nil {
		return nil, err
	}
	return &signer{
		rawSigner: rawSigner,
		prefix:    privateKey.OutputPrefix(),
		variant:   privateKey.publicKey.parameters.Variant(),
	}, nil
}

// Sign computes a signature for the given data.
func (s *signer) Sign(data []byte) ([]byte, error) {
	toSign := data
	if s.variant == VariantLegacy {
		toSign = slices.Concat(data, []byte{0x00})
	}
	signature, err := s.rawSigner.Sign(toSign)
	if err != nil {
		return nil, err
	}
	return slices.Concat(s.prefix, signature), nil
}

func signerConstructor(key key.Key) (any, error) {
	that, ok := key.(*PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is not a *rsassapss.PrivateKey")
	}
	return NewSigner(that, internalapi.Token{})
}
