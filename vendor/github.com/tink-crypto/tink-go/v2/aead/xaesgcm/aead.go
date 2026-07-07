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

package xaesgcm

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"slices"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/prf/subtle"
	"github.com/tink-crypto/tink-go/v2/subtle/random"
	"github.com/tink-crypto/tink-go/v2/tink"
)

const (
	ivSize  = 12
	tagSize = 16

	intSize = 32 << (^uint(0) >> 63) // 32 or 64
	maxInt  = 1<<(intSize-1) - 1
)

// aead is an implementation of [tink.AEAD] for X-AES-GCM.
//
// X-AES-GCM uses AES-CMAC to derive a per-message keys, as per
// NIST SP 800-108 Rev. 1, and uses AES-GCM to encrypt each message.
// (https://c2sp.org/XAES-256-GCM).
type aead struct {
	prefix          []byte
	prf             *subtle.AESCMACPRF
	saltSizeInBytes int
}

var _ tink.AEAD = (*aead)(nil)

// NewAEAD creates a new X-AES-GCM AEAD.
func NewAEAD(key *Key, _ internalapi.Token) (tink.AEAD, error) {
	prf, err := subtle.NewAESCMACPRF(key.KeyBytes().Data(insecuresecretdataaccess.Token{}))
	if err != nil {
		return nil, fmt.Errorf("xaesgcm.NewAEAD: failed to create AES-CMAC: %v", err)
	}
	return &aead{
		prefix:          key.OutputPrefix(),
		prf:             prf,
		saltSizeInBytes: key.parameters.SaltSizeInBytes(),
	}, nil
}

var (
	derivationBlock1Prefix = []byte{0x00, 0x01, 'X', 0x00}
	derivationBlock2Prefix = []byte{0x00, 0x02, 'X', 0x00}
)

func (a *aead) derivePerMessageKey(salt []byte) ([]byte, error) {
	paddedSalt := make([]byte, 12)
	copy(paddedSalt, salt)
	key1, err := a.prf.ComputePRF(slices.Concat(derivationBlock1Prefix, paddedSalt), 16)
	if err != nil {
		return nil, fmt.Errorf("failed to compute PRF: %v", err)
	}
	key2, err := a.prf.ComputePRF(slices.Concat(derivationBlock2Prefix, paddedSalt), 16)
	if err != nil {
		return nil, fmt.Errorf("failed to compute PRF: %v", err)
	}
	return slices.Concat(key1, key2), nil
}

func newAESGCMCipher(key []byte) (cipher.AEAD, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize cipher")
	}
	aesGCM, err := cipher.NewGCM(c)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher.AEAD")
	}
	return aesGCM, nil
}

func (a *aead) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	maxPlaintextSize := maxInt - (ivSize + tagSize + a.saltSizeInBytes + len(a.prefix))
	if len(plaintext) > maxPlaintextSize {
		return nil, fmt.Errorf("xaesgcm: plaintext with size %d is too large", len(plaintext))
	}

	saltAndIV := random.GetRandomBytes(uint32(a.saltSizeInBytes) + ivSize)
	salt := saltAndIV[:a.saltSizeInBytes]
	iv := saltAndIV[a.saltSizeInBytes:]
	perMessageKeyBytes, err := a.derivePerMessageKey(salt)
	if err != nil {
		return nil, fmt.Errorf("xaesgcm: failed to derive per-message key: %v", err)
	}
	aesGCM, err := newAESGCMCipher(perMessageKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("xaesgcm: failed to create cipher.AEAD")
	}
	dst := make([]byte, 0, len(a.prefix)+a.saltSizeInBytes+ivSize+len(plaintext)+tagSize)
	dst = append(dst, a.prefix...)
	dst = append(dst, saltAndIV...)
	return aesGCM.Seal(dst, iv, plaintext, associatedData), nil
}

func (a *aead) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	if ciphertextLen := len(ciphertext); ciphertextLen < len(a.prefix)+a.saltSizeInBytes+ivSize+tagSize {
		return nil, fmt.Errorf("xaesgcm: ciphertext with size %d is too short", ciphertextLen)
	}
	prefix := ciphertext[:len(a.prefix)]
	if !bytes.Equal(prefix, a.prefix) {
		return nil, fmt.Errorf("xaesgcm: ciphertext prefix does not match")
	}
	ciphertextNoPrefix := ciphertext[len(a.prefix):]
	salt := ciphertextNoPrefix[:a.saltSizeInBytes]
	iv := ciphertextNoPrefix[a.saltSizeInBytes : a.saltSizeInBytes+ivSize]
	ciphertextNoPrefixWithTag := ciphertextNoPrefix[a.saltSizeInBytes+ivSize:]
	perMessageKeyBytes, err := a.derivePerMessageKey(salt)
	if err != nil {
		return nil, fmt.Errorf("xaesgcm: failed to derive per-message key: %v", err)
	}
	aesGCM, err := newAESGCMCipher(perMessageKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("xaesgcm: failed to create cipher.AEAD")
	}
	dst := make([]byte, 0, len(ciphertextNoPrefixWithTag)-tagSize)
	return aesGCM.Open(dst, iv, ciphertextNoPrefixWithTag, associatedData)
}

// primitiveConstructor creates a [aead] from a [key.Key].
//
// The key must be of type [*Key].
func primitiveConstructor(k key.Key) (any, error) {
	that, ok := k.(*Key)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed *xaesgcm.Key", k)
	}
	return NewAEAD(that, internalapi.Token{})
}
