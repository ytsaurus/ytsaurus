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

package chacha20poly1305

import (
	"bytes"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/aead"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/subtle/random"
	"github.com/tink-crypto/tink-go/v2/tink"
)

type fullAEAD struct {
	rawAEAD *aead.ChaCha20Poly1305InsecureNonce
	prefix  []byte
	variant Variant
}

var _ tink.AEAD = (*fullAEAD)(nil)

func newAEAD(key *Key) (tink.AEAD, error) {
	rawAEAD, err := aead.NewChaCha20Poly1305InsecureNonce(key.KeyBytes().Data(insecuresecretdataaccess.Token{}))
	if err != nil {
		return nil, fmt.Errorf("chacha20_poly1305: %w", err)
	}
	return &fullAEAD{
		rawAEAD: rawAEAD,
		prefix:  key.OutputPrefix(),
		variant: key.parameters.Variant(),
	}, nil
}

// Encrypt encrypts plaintext with associatedData.
func (ca *fullAEAD) Encrypt(plaintext []byte, associatedData []byte) ([]byte, error) {
	nonce := random.GetRandomBytes(aead.ChaCha20Poly1305InsecureNonceSize)
	ciphertext := make([]byte, 0, len(ca.prefix)+len(nonce)+len(plaintext)+aead.ChaCha20Poly1305InsecureTagSize)
	ciphertext = append(ciphertext, ca.prefix...)
	ciphertext = append(ciphertext, nonce...)
	ciphertext, err := ca.rawAEAD.Encrypt(ciphertext, nonce, plaintext, associatedData)
	if err != nil {
		return nil, fmt.Errorf("chacha20_poly1305: encryption failed: %w", err)
	}
	return ciphertext, nil
}

// Decrypt decrypts ciphertext with associatedData.
func (ca *fullAEAD) Decrypt(ciphertext []byte, associatedData []byte) ([]byte, error) {
	if !bytes.HasPrefix(ciphertext, ca.prefix) {
		return nil, fmt.Errorf("chacha20_poly1305: ciphertext has invalid prefix")
	}
	ciphertextNoPrefix := ciphertext[len(ca.prefix):]
	minCiphertextLength := aead.ChaCha20Poly1305InsecureNonceSize + aead.ChaCha20Poly1305InsecureTagSize
	if len(ciphertextNoPrefix) < minCiphertextLength {
		return nil, fmt.Errorf("chacha20_poly1305: ciphertext is too short: got %d, want at least %d", len(ciphertextNoPrefix), minCiphertextLength)
	}
	nonce := ciphertextNoPrefix[:aead.ChaCha20Poly1305InsecureNonceSize]
	ciphertextAndTag := ciphertextNoPrefix[aead.ChaCha20Poly1305InsecureNonceSize:]
	plaintext, err := ca.rawAEAD.Decrypt(nonce, ciphertextAndTag, associatedData)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}

func primitiveConstructor(key key.Key) (any, error) {
	that, ok := key.(*Key)
	if !ok {
		return nil, fmt.Errorf("key is not a *chacha20poly1305.Key")
	}
	return newAEAD(that)
}
