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
	"github.com/tink-crypto/tink-go/v2/internal/aead"

	// Placeholder for internal crypto/cipher allowlist, please ignore.
)

const (
	// AESCTRMinIVSize is the minimum IV size that this implementation supports.
	AESCTRMinIVSize = 12
)

// AESCTR is an implementation of AEAD interface.
type AESCTR struct {
	ctr    *aead.AESCTR
	IVSize int
}

// NewAESCTR returns an AESCTR instance.
// The key argument should be the AES key, either 16 or 32 bytes to select
// AES-128 or AES-256.
// ivSize specifies the size of the IV in bytes.
func NewAESCTR(key []byte, ivSize int) (*AESCTR, error) {
	ctr, err := aead.NewAESCTR(key, ivSize)
	if err != nil {
		return nil, err
	}
	return &AESCTR{ctr: ctr, IVSize: ivSize}, nil
}

// Encrypt encrypts plaintext using AES in CTR mode.
// The resulting ciphertext consists of two parts:
// (1) the IV used for encryption and (2) the actual ciphertext.
func (a *AESCTR) Encrypt(plaintext []byte) ([]byte, error) {
	return a.ctr.Encrypt(nil, plaintext)
}

// Decrypt decrypts ciphertext.
func (a *AESCTR) Decrypt(ciphertext []byte) ([]byte, error) {
	return a.ctr.Decrypt(nil, ciphertext)
}
