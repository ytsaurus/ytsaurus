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

package hmacprf

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/prf/subtle"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Key represents an HKDF PRF key.
type Key struct {
	parameters *Parameters
	keyBytes   secretdata.Bytes
}

var _ key.Key = (*Key)(nil)

// NewKey creates a new [Key] from key bytes.
func NewKey(keyBytes secretdata.Bytes, parameters *Parameters) (*Key, error) {
	if parameters.KeySizeInBytes() != keyBytes.Len() {
		return nil, fmt.Errorf("key size %d does not match parameters key size %d", keyBytes.Len(), parameters.KeySizeInBytes())
	}
	return &Key{parameters: parameters, keyBytes: keyBytes}, nil
}

// KeyBytes returns the private key bytes.
func (k *Key) KeyBytes() secretdata.Bytes { return k.keyBytes }

// Parameters returns the parameters of the key.
func (k *Key) Parameters() key.Parameters { return k.parameters }

// IDRequirement returns the ID requirement of the key, and whether it is
// required.
//
// PRFs have no ID requirement, so this is always 0, false.
func (k *Key) IDRequirement() (uint32, bool) { return 0, false }

// Equal returns true if this key is equal to other.
func (k *Key) Equal(other key.Key) bool {
	that, ok := other.(*Key)
	return ok && k.keyBytes.Equal(that.keyBytes) && k.parameters.Equal(that.parameters)
}

func primitiveConstructor(key key.Key) (any, error) {
	actualKey, ok := key.(*Key)
	if !ok {
		return nil, fmt.Errorf("invalid key type: got %T, want %T", key, (*Key)(nil))
	}
	params := actualKey.Parameters().(*Parameters)
	if err := subtle.ValidateHMACPRFParams(params.HashType().String(), uint32(params.KeySizeInBytes())); err != nil {
		return nil, err
	}
	return subtle.NewHMACPRF(params.HashType().String(), actualKey.KeyBytes().Data(insecuresecretdataaccess.Token{}))
}

func createKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	hmacPRFarams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: %T", p)
	}
	err := subtle.ValidateHMACPRFParams(hmacPRFarams.HashType().String(), uint32(hmacPRFarams.KeySizeInBytes()))
	if err != nil {
		return nil, err
	}
	keyBytes, err := secretdata.NewBytesFromRand(uint32(hmacPRFarams.KeySizeInBytes()))
	if err != nil {
		return nil, err
	}
	return NewKey(keyBytes, hmacPRFarams)
}
