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

package aesgcmhkdf

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/aead"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	"github.com/tink-crypto/tink-go/v2/streamingaead/subtle"
)

// Key represents an AES-GCM-HKDF streaming AEAD key.
type Key struct {
	keyBytes   secretdata.Bytes
	parameters *Parameters
}

var _ key.Key = (*Key)(nil)

// NewKey creates a new AES-GCM-HKDF key with the given key material and parameters.
//
// It returns an error if the parameters have an ID requirement.
func NewKey(parameters *Parameters, keyBytes secretdata.Bytes) (*Key, error) {
	// Make sure the parameters object is neither nil nor empty.
	if parameters == nil || parameters.KeySizeInBytes() == 0 {
		return nil, fmt.Errorf("aesgcmhkdf.NewKey: invalid input parameters")
	}
	if keyBytes.Len() != int(parameters.KeySizeInBytes()) {
		return nil, fmt.Errorf("aesgcmhkdf.NewKey: key length = %v, want %v", keyBytes.Len(), parameters.KeySizeInBytes())
	}
	return &Key{
		keyBytes:   keyBytes,
		parameters: parameters,
	}, nil
}

// KeyBytes returns the key material.
func (k *Key) KeyBytes() secretdata.Bytes { return k.keyBytes }

// Parameters returns the parameters of this key.
func (k *Key) Parameters() key.Parameters { return k.parameters }

// IDRequirement always returns (0, false), as ID requirements are not supported.
func (k *Key) IDRequirement() (uint32, bool) { return 0, false }

// Equal returns whether this key object is equal to other.
func (k *Key) Equal(other key.Key) bool {
	that, ok := other.(*Key)
	return ok &&
		k.parameters.Equal(that.parameters) &&
		k.keyBytes.Equal(that.keyBytes)
}

func primitiveConstructor(k key.Key) (any, error) {
	that, ok := k.(*Key)
	if !ok {
		return nil, fmt.Errorf("key is of type %T, want %T", k, (*Key)(nil))
	}

	if err := aead.ValidateAESKeySize(uint32(that.keyBytes.Len())); err != nil {
		return nil, fmt.Errorf("invalid key size: %v", err)
	}

	params := k.Parameters().(*Parameters)
	return subtle.NewAESGCMHKDF(
		that.keyBytes.Data(insecuresecretdataaccess.Token{}),
		params.HKDFHashType().String(),
		int(params.DerivedKeySizeInBytes()),
		int(params.SegmentSizeInBytes()),
		0, // no first segment offset
	)
}

func createKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	aesGCMHKDFParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("parameters is not a aesctrhmac.Parameters")
	}
	if idRequirement != 0 {
		return nil, fmt.Errorf("ID requirements are not supported")
	}

	if err := aead.ValidateAESKeySize(uint32(aesGCMHKDFParams.KeySizeInBytes())); err != nil {
		return nil, fmt.Errorf("invalid key size: %v", err)
	}

	keyBytes, err := secretdata.NewBytesFromRand(uint32(aesGCMHKDFParams.KeySizeInBytes()))
	if err != nil {
		return nil, fmt.Errorf("failed to generate random key material: %v", err)
	}
	return NewKey(aesGCMHKDFParams, keyBytes)
}
