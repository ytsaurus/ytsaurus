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

package aessiv

import (
	"fmt"
	"slices"

	"github.com/tink-crypto/tink-go/v2/daead/subtle"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

type fullDAEAD struct {
	rawAESSIV    *subtle.AESSIV
	outputPrefix []byte
}

var _ tink.DeterministicAEAD = (*fullDAEAD)(nil)

// NewDeterministicAEAD creates a new [tink.DeterministicAEAD] from a [*Key].
//
// This is a "full" primitive, i.e., it applies the output prefix to the
// ciphertext.
func NewDeterministicAEAD(key *Key, _ internalapi.Token) (tink.DeterministicAEAD, error) {
	rawAESSIV, err := subtle.NewAESSIV(key.KeyBytes().Data(insecuresecretdataaccess.Token{}))
	if err != nil {
		return nil, err
	}
	return &fullDAEAD{
		rawAESSIV:    rawAESSIV,
		outputPrefix: key.OutputPrefix(),
	}, nil
}

func (a *fullDAEAD) EncryptDeterministically(plaintext, associatedData []byte) ([]byte, error) {
	ct, err := a.rawAESSIV.EncryptDeterministically(plaintext, associatedData)
	if err != nil {
		return nil, err
	}
	return slices.Concat(a.outputPrefix, ct), nil
}

func (a *fullDAEAD) DecryptDeterministically(ciphertext, associatedData []byte) ([]byte, error) {
	if len(ciphertext) < len(a.outputPrefix) {
		return nil, fmt.Errorf("ciphertext is too short")
	}
	if !slices.Equal(a.outputPrefix, ciphertext[:len(a.outputPrefix)]) {
		return nil, fmt.Errorf("output prefix mismatch")
	}
	return a.rawAESSIV.DecryptDeterministically(ciphertext[len(a.outputPrefix):], associatedData)
}

// primitiveConstructor creates a [fullDAEAD] from a [key.Key].
//
// The key must be of type [aessiv.Key].
func primitiveConstructor(k key.Key) (any, error) {
	that, ok := k.(*Key)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed %T", k, (*Key)(nil))
	}
	return NewDeterministicAEAD(that, internalapi.Token{})
}
