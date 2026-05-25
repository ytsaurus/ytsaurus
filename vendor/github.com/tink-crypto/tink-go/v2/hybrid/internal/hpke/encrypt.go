// Copyright 2022 Google LLC
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

package hpke

import (
	"errors"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/tink"
)

// Encrypt for HPKE implements interface HybridEncrypt.
type Encrypt struct {
	recipientPubKeyBytes []byte
	kem                  kem
	kdf                  kdf
	aead                 aead
}

var _ tink.HybridEncrypt = (*Encrypt)(nil)

// NewEncrypt constructs an Encrypt using HpkePublicKey.
func NewEncrypt(recipientPubKeyBytes []byte, kemID KEMID, kdfID KDFID, aeadID AEADID) (*Encrypt, error) {
	if len(recipientPubKeyBytes) == 0 {
		return nil, errors.New("empty recipient public key")
	}
	kem, kdf, aead, err := newPrimitives(kemID, kdfID, aeadID)
	if err != nil {
		return nil, err
	}
	return &Encrypt{recipientPubKeyBytes, kem, kdf, aead}, nil
}

// Encrypt encrypts plaintext, binding contextInfo to the resulting ciphertext.
func (e *Encrypt) Encrypt(plaintext, contextInfo []byte) ([]byte, error) {
	ctx, err := newSenderContext(e.recipientPubKeyBytes, e.kem, e.kdf, e.aead, contextInfo)
	if err != nil {
		return nil, fmt.Errorf("newSenderContext: %v", err)
	}

	ciphertext, err := ctx.seal(plaintext, emptyAssociatedData)
	if err != nil {
		return nil, fmt.Errorf("seal: %v", err)
	}
	output := make([]byte, 0, len(ctx.encapsulatedKey)+len(ciphertext))
	output = append(output, ctx.encapsulatedKey...)
	output = append(output, ciphertext...)
	return output, nil
}
