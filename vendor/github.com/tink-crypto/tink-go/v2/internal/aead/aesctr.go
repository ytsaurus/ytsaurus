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

package aead

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/subtle/random"
)

const (
	aesCTRMinIVSize = 12
)

// AESCTR is an implementation of IndCpa Interface.
type AESCTR struct {
	key    []byte
	ivSize int
}

// NewAESCTR returns an instance of [AESCTR] unauthenticated encryption.
func NewAESCTR(key []byte, ivSize int) (*AESCTR, error) {
	keySize := uint32(len(key))
	if err := ValidateAESKeySize(keySize); err != nil {
		return nil, fmt.Errorf("aes_ctr: %s", err)
	}
	if ivSize < aesCTRMinIVSize || ivSize > aes.BlockSize {
		return nil, fmt.Errorf("aes_ctr: invalid IV size: %d", ivSize)
	}
	return &AESCTR{key: key, ivSize: ivSize}, nil
}

// Encrypt encrypts plaintext using AES in CTR mode.
func (a *AESCTR) Encrypt(dst, plaintext []byte) ([]byte, error) {
	if len(plaintext) > maxInt-a.ivSize {
		return nil, fmt.Errorf("aes_ctr: plaintext too long")
	}
	ctSize := len(plaintext) + a.ivSize
	if len(dst) == 0 {
		dst = make([]byte, ctSize)
	}
	if len(dst) < ctSize {
		return nil, fmt.Errorf("aes_ctr: destination buffer too small (%d vs %d)", len(dst), ctSize)
	}

	iv := random.GetRandomBytes(uint32(a.ivSize))
	stream, err := newCipher(a.key, iv)
	if err != nil {
		return nil, err
	}
	if n := copy(dst, iv); n != a.ivSize {
		return nil, fmt.Errorf("aes_ctr: failed to copy IV (copied %d/%d bytes)", n, a.ivSize)
	}
	stream.XORKeyStream(dst[a.ivSize:], plaintext)
	return dst, nil
}

// Decrypt decrypts ciphertext in the format (prefix || iv || ciphertext).
func (a *AESCTR) Decrypt(dst, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < a.ivSize {
		return nil, fmt.Errorf("aes_ctr: ciphertext too short")
	}
	ptSize := len(ciphertext) - a.ivSize
	if len(dst) == 0 {
		dst = make([]byte, ptSize)
	}
	if len(dst) < ptSize {
		return nil, fmt.Errorf("aes_ctr: destination buffer too small (%d vs %d)", len(dst), ptSize)
	}
	stream, err := newCipher(a.key, ciphertext[:a.ivSize])
	if err != nil {
		return nil, err
	}
	stream.XORKeyStream(dst, ciphertext[a.ivSize:])
	return dst, nil
}

func newCipher(key, iv []byte) (cipher.Stream, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes_ctr: failed to create block cipher, error: %v", err)
	}

	// If the IV is less than BlockSize bytes we need to pad it with zeros
	// otherwise NewCTR will panic.
	if len(iv) < aes.BlockSize {
		paddedIV := make([]byte, aes.BlockSize)
		if n := copy(paddedIV, iv); n != len(iv) {
			return nil, fmt.Errorf("aes_ctr: failed to pad IV")
		}
		iv = paddedIV
	}
	return cipher.NewCTR(block, iv), nil
}
