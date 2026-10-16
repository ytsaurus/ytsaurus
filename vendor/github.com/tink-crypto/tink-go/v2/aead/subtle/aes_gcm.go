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

	"github.com/tink-crypto/tink-go/v2/aead/aesgcm"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	"github.com/tink-crypto/tink-go/v2/tink"
)

const (
	// AESGCMIVSize is the acceptable IV size defined by RFC 5116.
	AESGCMIVSize = 12
	// AESGCMTagSize is the acceptable tag size defined by RFC 5116.
	AESGCMTagSize = 16

	maxIntPlaintextSize = maxInt - AESGCMIVSize - AESGCMTagSize
)

// AESGCM is an implementation of the [tink.AEAD] interface.
//
// This primitive adds no prefix to the ciphertext.
type AESGCM struct {
	aeadImpl tink.AEAD
}

// NewAESGCM returns an [*AESGCM] value from the given key.
//
// The key must be of length 16 or 32 bytes. IV and TAG sizes are fixed to 12
// and 16 bytes respectively.
func NewAESGCM(key []byte) (*AESGCM, error) {
	opts := aesgcm.ParametersOpts{
		KeySizeInBytes: len(key),
		IVSizeInBytes:  AESGCMIVSize,
		TagSizeInBytes: AESGCMTagSize,
		Variant:        aesgcm.VariantNoPrefix,
	}
	params, err := aesgcm.NewParameters(opts)
	if err != nil {
		return nil, fmt.Errorf("subtle.NewAESGCM: %v", err)
	}
	k, err := aesgcm.NewKey(secretdata.NewBytesFromData(key, insecuresecretdataaccess.Token{}), 0, params)
	if err != nil {
		return nil, fmt.Errorf("subtle.NewAESGCM: %v", err)
	}
	aead, err := aesgcm.NewAEAD(k)
	if err != nil {
		return nil, fmt.Errorf("subtle.NewAESGCM: %v", err)
	}
	return &AESGCM{aeadImpl: aead}, nil
}

// Encrypt encrypts the plaintext with the associated data.
func (a *AESGCM) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	return a.aeadImpl.Encrypt(plaintext, associatedData)
}

// Decrypt decrypts the ciphertext with the associated data.
func (a *AESGCM) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	return a.aeadImpl.Decrypt(ciphertext, associatedData)
}
