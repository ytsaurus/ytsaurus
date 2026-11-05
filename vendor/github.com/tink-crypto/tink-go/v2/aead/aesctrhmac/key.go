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

package aesctrhmac

import (
	"bytes"
	"fmt"

	internalaead "github.com/tink-crypto/tink-go/v2/internal/aead"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Key represents an AES-CTR-HMAC AEAD function.
type Key struct {
	aesKeyBytes   secretdata.Bytes
	hmacKeyBytes  secretdata.Bytes
	idRequirement uint32
	outputPrefix  []byte
	parameters    *Parameters
}

var _ key.Key = (*Key)(nil)

// KeyOpts specifies parameters to [NewKey].
type KeyOpts struct {
	AESKeyBytes, HMACKeyBytes secretdata.Bytes
	IDRequirement             uint32
	Parameters                *Parameters
}

// NewKey creates a new AES-CTR-HMAC key with key, idRequirement and parameters.
//
// The idRequirement is the ID requirement to be included in the output of the
// AES-CTR-HMAC function. If parameters.HasIDRequirement() == false, idRequirement
// must be zero.
func NewKey(params KeyOpts) (*Key, error) {
	// Make sure that parameters is not nil and that is not as struct literal
	// with default values.
	if params.Parameters == nil || params.Parameters.AESKeySizeInBytes() == 0 {
		return nil, fmt.Errorf("aesctrhmac.NewKey: invalid input params.Parameters")
	}
	if !params.Parameters.HasIDRequirement() && params.IDRequirement != 0 {
		return nil, fmt.Errorf("aesctrhmac.NewKey: params.IDRequirement = %v and params.Parameters.HasIDRequirement() = false, want 0", params.IDRequirement)
	}
	if params.AESKeyBytes.Len() != int(params.Parameters.AESKeySizeInBytes()) {
		return nil, fmt.Errorf("aesctrhmac.NewKey: AES key length = %v, want %v", params.AESKeyBytes.Len(), params.Parameters.AESKeySizeInBytes())
	}
	if params.HMACKeyBytes.Len() != int(params.Parameters.HMACKeySizeInBytes()) {
		return nil, fmt.Errorf("aesctrhmac.NewKey: HMAC key length = %v, want %v", params.HMACKeyBytes.Len(), params.Parameters.HMACKeySizeInBytes())
	}
	outputPrefix, err := calculateOutputPrefix(params.Parameters.Variant(), params.IDRequirement)
	if err != nil {
		return nil, fmt.Errorf("aesctrhmac.NewKey: %v", err)
	}
	return &Key{
		aesKeyBytes:   params.AESKeyBytes,
		hmacKeyBytes:  params.HMACKeyBytes,
		idRequirement: params.IDRequirement,
		outputPrefix:  outputPrefix,
		parameters:    params.Parameters,
	}, nil
}

// AESKeyBytes returns the key material.
//
// This function provides access to partial key material. See
// https://developers.google.com/tink/design/access_control#access_of_parts_of_a_key
// for more information.
func (k *Key) AESKeyBytes() secretdata.Bytes { return k.aesKeyBytes }

// HMACKeyBytes returns the key material.
//
// This function provides access to partial key material. See
// https://developers.google.com/tink/design/access_control#access_of_parts_of_a_key
// for more information.
func (k *Key) HMACKeyBytes() secretdata.Bytes { return k.hmacKeyBytes }

// Parameters returns the parameters of this key.
func (k *Key) Parameters() key.Parameters { return k.parameters }

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
		k.aesKeyBytes.Equal(that.aesKeyBytes) &&
		k.hmacKeyBytes.Equal(that.hmacKeyBytes) &&
		bytes.Equal(k.outputPrefix, that.outputPrefix)
}

func createKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	aesCTRHMACParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed %T", p, (*Parameters)(nil))
	}

	// Make sure AES key size is either 16 or 32 bytes for consistency with other Tink
	// implementations.
	if err := internalaead.ValidateAESKeySize(uint32(aesCTRHMACParams.AESKeySizeInBytes())); err != nil {
		return nil, err
	}

	aesKeyBytes, err := secretdata.NewBytesFromRand(uint32(aesCTRHMACParams.AESKeySizeInBytes()))
	if err != nil {
		return nil, err
	}
	macKeyBytes, err := secretdata.NewBytesFromRand(uint32(aesCTRHMACParams.HMACKeySizeInBytes()))
	if err != nil {
		return nil, err
	}
	return NewKey(KeyOpts{
		AESKeyBytes:   aesKeyBytes,
		HMACKeyBytes:  macKeyBytes,
		IDRequirement: idRequirement,
		Parameters:    aesCTRHMACParams,
	})
}
