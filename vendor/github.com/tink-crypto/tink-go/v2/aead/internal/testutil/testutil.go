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

// Package testutil provides testing utilities for AEAD primitives.
package testutil

import (
	"bytes"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/subtle/random"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// EncryptDecrypt encrypts and decrypts random data using the given AEAD
// primitives.
func EncryptDecrypt(encryptor, decryptor tink.AEAD) error {
	// Try to encrypt and decrypt random data.
	pt := random.GetRandomBytes(32)
	aad := random.GetRandomBytes(32)
	ct, err := encryptor.Encrypt(pt, aad)
	if err != nil {
		return fmt.Errorf("encryptor.Encrypt() err = %v, want nil", err)
	}
	decrypted, err := decryptor.Decrypt(ct, aad)
	if err != nil {
		return fmt.Errorf("decryptor.Decrypt() err = %v, want nil", err)
	}
	if !bytes.Equal(decrypted, pt) {
		return fmt.Errorf("decryptor.Decrypt() = %v, want %v", decrypted, pt)
	}
	return nil
}
