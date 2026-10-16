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

package subtle

import (
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
	"github.com/tink-crypto/tink-go/v2/internal/aead"
	"github.com/tink-crypto/tink-go/v2/subtle/random"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// ChaCha20Poly1305 is a ChaCha20-Poly1305 implementation of the [tink.AEAD]
// interface.
type ChaCha20Poly1305 struct {
	rawAEAD *aead.ChaCha20Poly1305InsecureNonce
}

var _ tink.AEAD = (*ChaCha20Poly1305)(nil)

// NewChaCha20Poly1305 returns an [ChaCha20Poly1305] instance.
//
// The key argument must be a 32-bytes key.
func NewChaCha20Poly1305(key []byte) (*ChaCha20Poly1305, error) {
	rawAEAD, err := aead.NewChaCha20Poly1305InsecureNonce(key)
	if err != nil {
		return nil, fmt.Errorf("chacha20_poly1305: %w", err)
	}

	return &ChaCha20Poly1305{rawAEAD: rawAEAD}, nil
}

// Encrypt encrypts plaintext with associatedData.
//
// The resulting ciphertext is of the form: | nonce | ciphertext | tag |.
func (ca *ChaCha20Poly1305) Encrypt(plaintext []byte, associatedData []byte) ([]byte, error) {
	nonce := random.GetRandomBytes(chacha20poly1305.NonceSize)
	ciphertext := make([]byte, 0, len(nonce)+len(plaintext)+aead.ChaCha20Poly1305InsecureTagSize)
	ciphertext = append(ciphertext, nonce...)
	ciphertext, err := ca.rawAEAD.Encrypt(ciphertext, nonce, plaintext, associatedData)
	if err != nil {
		return nil, fmt.Errorf("chacha20_poly1305: %w", err)
	}
	return ciphertext, nil
}

// Decrypt decrypts ciphertext with associatedData.
//
// The ciphertext must be of the form: | nonce | ciphertext | tag |.
func (ca *ChaCha20Poly1305) Decrypt(ciphertext []byte, associatedData []byte) ([]byte, error) {
	minCiphertextLength := aead.ChaCha20Poly1305InsecureNonceSize + aead.ChaCha20Poly1305InsecureTagSize
	if len(ciphertext) < minCiphertextLength {
		return nil, fmt.Errorf("chacha20_poly1305: ciphertext is too short: got %d, want at least %d", len(ciphertext), minCiphertextLength)
	}
	nonce := ciphertext[:aead.ChaCha20Poly1305InsecureNonceSize]
	ciphertextAndTag := ciphertext[aead.ChaCha20Poly1305InsecureNonceSize:]
	plaintext, err := ca.rawAEAD.Decrypt(nonce, ciphertextAndTag, associatedData)
	if err != nil {
		return nil, fmt.Errorf("chacha20_poly1305: %w", err)
	}
	return plaintext, nil
}
