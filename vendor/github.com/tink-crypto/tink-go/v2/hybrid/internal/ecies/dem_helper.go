// Copyright 2021 Google LLC
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

// Package ecies provides a helper for creating [tink.AEAD] or
// [tink.DeterministicAEAD] primitives for the specified [tink.Parameters] and
// key material.
package ecies

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/aead/aesctrhmac"
	"github.com/tink-crypto/tink-go/v2/aead/aesgcm"
	"github.com/tink-crypto/tink-go/v2/daead/aessiv"
	"github.com/tink-crypto/tink-go/v2/hybrid/subtle"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/registryconfig"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// DEMHelper provides a helper for creating [tink.AEAD] or
// [tink.DeterministicAEAD] primitives for the specified [tink.Parameters] and
// key material.
//
// Implements [subtle.EciesAEADHKDFDEMHelper].
type DEMHelper struct {
	params                     key.Parameters
	wantKeyMaterialSizeInBytes uint32
}

var _ subtle.EciesAEADHKDFDEMHelper = (*DEMHelper)(nil)

// NewDEMHelper initializes and returns a RegisterECIESAEADHKDFDemHelper
func NewDEMHelper(p key.Parameters) (*DEMHelper, error) {
	var wantKeyMaterialSizeInBytes uint32
	switch params := p.(type) {
	case *aesgcm.Parameters:
		wantKeyMaterialSizeInBytes = uint32(params.KeySizeInBytes())
	case *aessiv.Parameters:
		wantKeyMaterialSizeInBytes = uint32(params.KeySizeInBytes())
	case *aesctrhmac.Parameters:
		wantKeyMaterialSizeInBytes = uint32(params.AESKeySizeInBytes() + params.HMACKeySizeInBytes())
	default:
		return nil, fmt.Errorf("unsupported AEAD DEM key type: %T", p)
	}

	return &DEMHelper{
		params:                     p,
		wantKeyMaterialSizeInBytes: wantKeyMaterialSizeInBytes,
	}, nil
}

// GetSymmetricKeySize returns the symmetric key size
func (r *DEMHelper) GetSymmetricKeySize() uint32 { return r.wantKeyMaterialSizeInBytes }

// GetAEADOrDAEAD returns the AEAD or deterministic AEAD primitive from the DEM
func (r *DEMHelper) GetAEADOrDAEAD(symmetricKeyValue []byte) (any, error) {
	if uint32(len(symmetricKeyValue)) != r.GetSymmetricKeySize() {
		return nil, fmt.Errorf("symmetric key has incorrect length: got %d, want %d", len(symmetricKeyValue), r.GetSymmetricKeySize())
	}

	var k key.Key
	var err error
	switch params := r.params.(type) {
	case *aesgcm.Parameters:
		k, err = aesgcm.NewKey(secretdata.NewBytesFromData(symmetricKeyValue, insecuresecretdataaccess.Token{}), 0, params)
		if err != nil {
			return nil, err
		}
	case *aessiv.Parameters:
		k, err = aessiv.NewKey(secretdata.NewBytesFromData(symmetricKeyValue, insecuresecretdataaccess.Token{}), 0, params)
		if err != nil {
			return nil, err
		}
	case *aesctrhmac.Parameters:
		k, err = aesctrhmac.NewKey(aesctrhmac.KeyOpts{
			AESKeyBytes:   secretdata.NewBytesFromData(symmetricKeyValue[:params.AESKeySizeInBytes()], insecuresecretdataaccess.Token{}),
			HMACKeyBytes:  secretdata.NewBytesFromData(symmetricKeyValue[params.AESKeySizeInBytes():], insecuresecretdataaccess.Token{}),
			IDRequirement: 0,
			Parameters:    params,
		})
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported AEAD DEM key type: %T", r.params)
	}

	p, err := (&registryconfig.RegistryConfig{}).PrimitiveFromKey(k, internalapi.Token{})
	if err != nil {
		return nil, err
	}
	switch p.(type) {
	case tink.AEAD, tink.DeterministicAEAD:
		return p, nil
	default:
		return nil, fmt.Errorf("Unexpected primitive type returned by the registry for the DEM: %T", p)
	}
}
