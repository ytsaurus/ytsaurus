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

package xchacha20poly1305

import (
	"bytes"
	"crypto/cipher"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/subtle/random"
	"github.com/tink-crypto/tink-go/v2/tink"
)

const (
	intSize           = 32 << (^uint(0) >> 63) // 32 or 64
	maxInt            = 1<<(intSize-1) - 1
	maxPlaintextSize  = maxInt - chacha20poly1305.NonceSizeX - chacha20poly1305.Overhead
	maxCiphertextSize = maxInt
)

// aead is a [tink.AEAD] implementation for XChaCha20Poly1305.
//
// See https://datatracker.ietf.org/doc/html/draft-irtf-cfrg-xchacha-03.
type aead struct {
	aead    cipher.AEAD
	prefix  []byte
	variant Variant
}

var _ tink.AEAD = (*aead)(nil)

func newAEAD(key *Key) (tink.AEAD, error) {
	c, err := chacha20poly1305.NewX(key.keyBytes.Data(insecuresecretdataaccess.Token{}))
	if err != nil {
		return nil, fmt.Errorf("xchacha20_poly1305: %s", err)
	}
	return &aead{
		aead:    c,
		prefix:  key.OutputPrefix(),
		variant: key.parameters.Variant(),
	}, nil
}

// Encrypt encrypts plaintext with associatedData.
func (a *aead) Encrypt(plaintext []byte, associatedData []byte) ([]byte, error) {
	if len(plaintext) > maxPlaintextSize {
		return nil, fmt.Errorf("xchacha20_poly1305: plaintext too long: got %d, want <= %d", len(plaintext), maxPlaintextSize)
	}
	nonce := random.GetRandomBytes(chacha20poly1305.NonceSizeX)
	ciphertextSize := len(a.prefix) + len(nonce) + len(plaintext) + chacha20poly1305.Overhead
	dst := make([]byte, 0, ciphertextSize)
	dst = append(dst, a.prefix...)
	dst = append(dst, nonce...)
	return a.aead.Seal(dst, nonce, plaintext, associatedData), nil
}

// Decrypt decrypts ciphertext with associatedData.
func (a *aead) Decrypt(ciphertext []byte, associatedData []byte) ([]byte, error) {
	minCiphertextSize := len(a.prefix) + chacha20poly1305.NonceSizeX + chacha20poly1305.Overhead
	if len(ciphertext) < minCiphertextSize {
		return nil, fmt.Errorf("xchacha20_poly1305: ciphertext too short: got %d, want >= %d", len(ciphertext), minCiphertextSize)
	}
	if len(ciphertext) > maxCiphertextSize {
		return nil, fmt.Errorf("xchacha20_poly1305: ciphertext too long: got %d, want <= %d", len(ciphertext), maxCiphertextSize)
	}
	if !bytes.HasPrefix(ciphertext, a.prefix) {
		return nil, fmt.Errorf("xchacha20_poly1305: ciphertext has invalid prefix")
	}
	ciphertextNoPrefix := ciphertext[len(a.prefix):]
	nonce := ciphertextNoPrefix[:chacha20poly1305.NonceSizeX]
	rawCiphertextAndTag := ciphertextNoPrefix[chacha20poly1305.NonceSizeX:]
	decrypted, err := a.aead.Open(nil, nonce, rawCiphertextAndTag, associatedData)
	if err != nil {
		return nil, fmt.Errorf("xchacha20_poly1305: %s", err)
	}
	return decrypted, nil
}

func primitiveConstructor(key key.Key) (any, error) {
	that, ok := key.(*Key)
	if !ok {
		return nil, fmt.Errorf("key is not a *xchacha20poly1305.Key")
	}
	return newAEAD(that)
}
