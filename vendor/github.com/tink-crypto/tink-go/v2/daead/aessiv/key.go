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

package aessiv

import (
	"bytes"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/daead/subtle"
	"github.com/tink-crypto/tink-go/v2/internal/outputprefix"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Key represents an AES-SIV key and function that implements RFC 5297.
type Key struct {
	keyBytes      secretdata.Bytes
	idRequirement uint32
	outputPrefix  []byte
	parameters    *Parameters
}

var _ key.Key = (*Key)(nil)

// calculateOutputPrefix calculates the output prefix from keyID.
func calculateOutputPrefix(variant Variant, keyID uint32) ([]byte, error) {
	switch variant {
	case VariantTink:
		return outputprefix.Tink(keyID), nil
	case VariantCrunchy:
		return outputprefix.Legacy(keyID), nil
	case VariantNoPrefix:
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid output prefix variant: %v", variant)
	}
}

// NewKey creates a new AES-SIV key with key, idRequirement and parameters.
//
// The idRequirement is the ID requirement to be included in the output of the
// AES-SIV function. If parameters.HasIDRequirement() == false, idRequirement
// must be zero.
func NewKey(keyBytes secretdata.Bytes, idRequirement uint32, parameters *Parameters) (*Key, error) {
	if parameters == nil {
		return nil, fmt.Errorf("aessiv.NewKey: parameters is nil")
	}
	if err := validateParams(parameters); err != nil {
		return nil, fmt.Errorf("aessiv.NewKey: %v", err)
	}
	if !parameters.HasIDRequirement() && idRequirement != 0 {
		return nil, fmt.Errorf("aessiv.NewKey: idRequirement = %v and parameters.HasIDRequirement() = false, want 0", idRequirement)
	}
	if keyBytes.Len() != int(parameters.KeySizeInBytes()) {
		return nil, fmt.Errorf("aessiv.NewKey: key.Len() = %v, want %v", keyBytes.Len(), parameters.KeySizeInBytes())
	}
	outputPrefix, err := calculateOutputPrefix(parameters.Variant(), idRequirement)
	if err != nil {
		return nil, fmt.Errorf("aessiv.NewKey: %v", err)
	}
	return &Key{
		keyBytes:      keyBytes,
		idRequirement: idRequirement,
		outputPrefix:  outputPrefix,
		parameters:    parameters,
	}, nil
}

// KeyBytes returns the key material.
//
// This function provides access to partial key material. See
// https://developers.google.com/tink/design/access_control#access_of_parts_of_a_key
// for more information.
func (k *Key) KeyBytes() secretdata.Bytes { return k.keyBytes }

// Parameters returns the parameters of this key.
func (k *Key) Parameters() key.Parameters { return k.parameters }

// IDRequirement returns a tuple containing a boolean that indicates whether or
// not the key requires an identifier and the key identifier. The key identifier
// will equal 0 if an identifier is not required.
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
		k.keyBytes.Equal(that.keyBytes) &&
		bytes.Equal(k.outputPrefix, that.outputPrefix)
}

func createKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	if p == nil {
		return nil, fmt.Errorf("parameters is nil")
	}
	aesSIVParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed %T", p, (*Parameters)(nil))
	}

	if aesSIVParams.KeySizeInBytes() != subtle.AESSIVKeySize {
		return nil, fmt.Errorf("key size %d is not supported", aesSIVParams.KeySizeInBytes())
	}

	keyBytes, err := secretdata.NewBytesFromRand(uint32(aesSIVParams.KeySizeInBytes()))
	if err != nil {
		return nil, err
	}
	return NewKey(keyBytes, idRequirement, aesSIVParams)
}
