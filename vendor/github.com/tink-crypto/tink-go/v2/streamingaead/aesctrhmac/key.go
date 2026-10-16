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

package aesctrhmac

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/aead"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	"github.com/tink-crypto/tink-go/v2/streamingaead/subtle"
)

// Key represents an AES-CTR-HMAC Streaming AEAD key.
type Key struct {
	parameters *Parameters
	keyBytes   secretdata.Bytes
}

// This ensures that the Key type implements the [key.Key] interface.
var _ key.Key = (*Key)(nil)

// NewKey creates a new AES-CTR-HMAC Streaming AEAD key.
func NewKey(parameters *Parameters, keyBytes secretdata.Bytes) (*Key, error) {
	if parameters == nil {
		return nil, fmt.Errorf("aesctrhmac.NewKey: Parameters must not be nil")
	}
	if keyBytes.Len() != parameters.KeySizeInBytes() {
		return nil, fmt.Errorf("aesctrhmac.NewKey: key has size %d, but must have size %d", keyBytes.Len(), parameters.KeySizeInBytes())
	}
	return &Key{
		parameters: parameters,
		keyBytes:   keyBytes,
	}, nil
}

// Parameters returns the parameters of the key.
func (k *Key) Parameters() key.Parameters { return k.parameters }

// KeyBytes returns the initial key material.
func (k *Key) KeyBytes() secretdata.Bytes { return k.keyBytes }

// IDRequirement always returns (0, false) for this key type.
func (k *Key) IDRequirement() (uint32, bool) { return 0, false }

// Equal returns true if k and other are equal.
func (k *Key) Equal(other key.Key) bool {
	that, ok := other.(*Key)
	return ok && k.parameters.Equal(that.parameters) && k.keyBytes.Equal(that.keyBytes)
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
	return subtle.NewAESCTRHMAC(
		that.keyBytes.Data(insecuresecretdataaccess.Token{}),
		params.HkdfHashType().String(),
		params.DerivedKeySizeInBytes(),
		params.HmacHashType().String(),
		params.HmacTagSizeInBytes(),
		int(params.SegmentSizeInBytes()),
		0)
}

// validateHashType checks if the given hash type is supported for key
// generation.
//
// We only support a subset of the hash types: SHA256 and SHA512.
func validateHashType(hashType HashType) error {
	switch hashType {
	case SHA256:
	case SHA512:
		// Do nothing.
	default:
		return fmt.Errorf("unsupported hash type: %v", hashType)
	}
	return nil
}

func createKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	aesCTRHMACParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("parameters is not a aesctrhmac.Parameters")
	}
	if idRequirement != 0 {
		return nil, fmt.Errorf("ID requirements are not supported")
	}

	if err := aead.ValidateAESKeySize(uint32(aesCTRHMACParams.KeySizeInBytes())); err != nil {
		return nil, fmt.Errorf("invalid key size: %v", err)
	}

	if err := validateHashType(aesCTRHMACParams.HkdfHashType()); err != nil {
		return nil, fmt.Errorf("invalid HKDF hash type: %v", err)
	}
	if err := validateHashType(aesCTRHMACParams.HmacHashType()); err != nil {
		return nil, fmt.Errorf("invalid HMAC hash type: %v", err)
	}

	keyBytes, err := secretdata.NewBytesFromRand(uint32(aesCTRHMACParams.KeySizeInBytes()))
	if err != nil {
		return nil, fmt.Errorf("failed to generate random key material: %v", err)
	}
	return NewKey(aesCTRHMACParams, keyBytes)
}
