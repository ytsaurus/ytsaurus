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

package hmac

import (
	"bytes"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/outputprefix"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/mac/subtle"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Key is an HMAC key.
type Key struct {
	keyBytes      secretdata.Bytes
	idRequirement uint32
	outputPrefix  []byte
	params        *Parameters
}

var _ key.Key = (*Key)(nil)

// calculateOutputPrefix calculates the output prefix from keyID.
func calculateOutputPrefix(variant Variant, keyID uint32) ([]byte, error) {
	switch variant {
	case VariantTink:
		return outputprefix.Tink(keyID), nil
	case VariantCrunchy, VariantLegacy:
		return outputprefix.Legacy(keyID), nil
	case VariantNoPrefix:
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid output prefix variant: %v", variant)
	}
}

// NewKey creates a new HMAC key.
func NewKey(keyBytes secretdata.Bytes, params *Parameters, idRequirement uint32) (*Key, error) {
	if keyBytes.Len() != params.KeySizeInBytes() {
		return nil, fmt.Errorf("hmac.NewKey: invalid key size; want %d, got %d", keyBytes.Len(), params.KeySizeInBytes())
	}
	if params.Variant() == VariantNoPrefix && idRequirement != 0 {
		return nil, fmt.Errorf("hmac.NewKey: key ID must be zero for VariantNoPrefix")
	}
	outputPrefix, err := calculateOutputPrefix(params.Variant(), idRequirement)
	if err != nil {
		return nil, fmt.Errorf("hmac.NewKey: %v", err)
	}
	return &Key{
		keyBytes:      keyBytes,
		idRequirement: idRequirement,
		outputPrefix:  outputPrefix,
		params:        params,
	}, nil
}

// KeyBytes returns the key bytes.
func (k *Key) KeyBytes() secretdata.Bytes { return k.keyBytes }

// Parameters returns the parameters of this key.
func (k *Key) Parameters() key.Parameters { return k.params }

// IDRequirement returns required to indicate if this key requires an
// identifier. If it does, id will contain that identifier.
func (k *Key) IDRequirement() (uint32, bool) {
	return k.idRequirement, k.Parameters().HasIDRequirement()
}

// OutputPrefix returns the output prefix.
func (k *Key) OutputPrefix() []byte { return bytes.Clone(k.outputPrefix) }

// Equal returns whether this key object is equal to other.
func (k *Key) Equal(other key.Key) bool {
	that, ok := other.(*Key)
	return ok && k.Parameters().Equal(that.Parameters()) &&
		k.idRequirement == that.idRequirement &&
		k.keyBytes.Equal(that.keyBytes)
}

func primitiveConstructor(k key.Key) (any, error) {
	that, ok := k.(*Key)
	if !ok {
		return nil, fmt.Errorf("key is of type %T, want %T", k, (*Key)(nil))
	}
	return NewMAC(that, internalapi.Token{})
}

func createKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	hmacParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: %T", p)
	}
	err := subtle.ValidateHMACParams(hmacParams.HashType().String(), uint32(hmacParams.KeySizeInBytes()), uint32(hmacParams.tagSizeInBytes))
	if err != nil {
		return nil, err
	}
	keyBytes, err := secretdata.NewBytesFromRand(uint32(hmacParams.KeySizeInBytes()))
	if err != nil {
		return nil, err
	}
	return NewKey(keyBytes, hmacParams, idRequirement)
}
