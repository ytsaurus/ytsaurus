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
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"

	internalaead "github.com/tink-crypto/tink-go/v2/internal/aead"
)

// aesGCMAEAD is an AES GCM HPKE AEAD variant that implements interface
// aead.
type aesGCMAEAD struct {
	// HPKE AEAD algorithm identifier.
	aeadID AEADID
	keyLen int
}

var _ aead = (*aesGCMAEAD)(nil)

// newAESGCMAEAD constructs an AES-GCM HPKE AEAD using keyLength.
func newAESGCMAEAD(keyLength int) (*aesGCMAEAD, error) {
	switch keyLength {
	case 16:
		return &aesGCMAEAD{aeadID: AES128GCM, keyLen: 16}, nil
	case 32:
		return &aesGCMAEAD{aeadID: AES256GCM, keyLen: 32}, nil
	default:
		return nil, fmt.Errorf("key length %d is not supported", keyLength)
	}
}

// newAESGCMCipher creates a [cipher.AEAD] using the given key.
//
// The key must be 16 or 32 bytes long.
func newAESGCMCipher(keyBytes []byte) (cipher.AEAD, error) {
	aesCipher, err := aes.NewCipher(keyBytes)
	if err != nil {
		return nil, errors.New("failed to initialize cipher")
	}
	gcmCipher, err := cipher.NewGCM(aesCipher)
	if err != nil {
		return nil, errors.New("failed to create cipher.AEAD")
	}
	return gcmCipher, nil
}

func (a *aesGCMAEAD) seal(key, nonce, plaintext, associatedData []byte) ([]byte, error) {
	if len(key) != a.keyLen {
		return nil, fmt.Errorf("unexpected key length: got %d, want %d", len(key), a.keyLen)
	}
	if len(nonce) != a.nonceLength() {
		return nil, fmt.Errorf("unexpected nonce length: got %d, want %d", len(nonce), a.nonceLength())
	}
	if err := internalaead.CheckPlaintextSize(uint64(len(plaintext))); err != nil {
		return nil, err
	}
	c, err := newAESGCMCipher(key)
	if err != nil {
		return nil, err
	}
	return c.Seal(nil, nonce, plaintext, associatedData), nil
}

func (a *aesGCMAEAD) open(key, nonce, ciphertext, associatedData []byte) ([]byte, error) {
	if len(key) != a.keyLen {
		return nil, fmt.Errorf("unexpected key length: got %d, want %d", len(key), a.keyLen)
	}
	if len(nonce) != a.nonceLength() {
		return nil, fmt.Errorf("unexpected nonce length: got %d, want %d", len(nonce), a.nonceLength())
	}
	c, err := newAESGCMCipher(key)
	if err != nil {
		return nil, err
	}
	return c.Open(nil, nonce, ciphertext, associatedData)
}

func (a *aesGCMAEAD) id() AEADID { return a.aeadID }

func (a *aesGCMAEAD) keyLength() int { return a.keyLen }

func (a *aesGCMAEAD) nonceLength() int { return internalaead.AESGCMIVSize }
