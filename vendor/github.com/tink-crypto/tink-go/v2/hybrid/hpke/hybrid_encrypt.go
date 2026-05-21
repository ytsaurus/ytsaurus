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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or rawHybridEncryptied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hpke

import (
	"fmt"
	"slices"

	internalhpke "github.com/tink-crypto/tink-go/v2/hybrid/internal/hpke"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

type hybridEncrypt struct {
	rawHybridEncrypt tink.HybridEncrypt
	prefix           []byte
	variant          Variant
}

// NewHybridEncrypt creates a new instance of [tink.HybridEncrypt] from a
// [PublicKey].
//
// This is an internal API.
func NewHybridEncrypt(publicKey *PublicKey, _ internalapi.Token) (tink.HybridEncrypt, error) {
	params := publicKey.Parameters().(*Parameters)
	kemID, err := kemIDFromParams(params)
	if err != nil {
		return nil, err
	}
	kdfID, err := kdfIDFromParams(params)
	if err != nil {
		return nil, err
	}
	aeadID, err := aeadIDFromParams(params)
	if err != nil {
		return nil, err
	}
	rawHybridEncrypt, err := internalhpke.NewEncrypt(publicKey.PublicKeyBytes(), kemID, kdfID, aeadID)
	if err != nil {
		return nil, err
	}
	return &hybridEncrypt{
		rawHybridEncrypt: rawHybridEncrypt,
		prefix:           publicKey.OutputPrefix(),
		variant:          params.Variant(),
	}, nil
}

func (e *hybridEncrypt) Encrypt(plaintext, contextInfo []byte) ([]byte, error) {
	rawCiphertext, err := e.rawHybridEncrypt.Encrypt(plaintext, contextInfo)
	if err != nil {
		return nil, err
	}
	return slices.Concat(e.prefix, rawCiphertext), nil
}

func hybridEncryptConstructor(k key.Key) (any, error) {
	that, ok := k.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("invalid key type, got %T, want %T", k, (*PublicKey)(nil))
	}
	return NewHybridEncrypt(that, internalapi.Token{})
}
