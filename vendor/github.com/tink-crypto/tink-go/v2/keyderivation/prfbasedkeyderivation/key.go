// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prfbasedkeyderivation

import (
	"fmt"
	"reflect"

	"github.com/tink-crypto/tink-go/v2/internal/keygenregistry"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/keyderivation/internal/keyderivers"
	"github.com/tink-crypto/tink-go/v2/prf/aescmacprf"
	"github.com/tink-crypto/tink-go/v2/prf/hkdfprf"
	"github.com/tink-crypto/tink-go/v2/prf/hmacprf"
)

// Key represents a PRF-based key derivation function.
type Key struct {
	parameters    *Parameters
	prfKey        key.Key
	idRequirement uint32
}

var _ key.Key = (*Key)(nil)

// NewKey creates a new Key object.
//
// Inputs are such that:
//  1. prfKey must be one of the following types:
//     - [aescmacprf.Key]
//     - [hkdfprf.Key]
//     - [hmacprf.Key]
//  2. prfKey.Parameters() must be equal to parameters.PRFParameters()
//  3. If parameters.HasIDRequirement() is false, idRequirement must be 0.
func NewKey(parameters *Parameters, prfKey key.Key, idRequirement uint32) (*Key, error) {
	if parameters == nil {
		return nil, fmt.Errorf("prfbasedkeyderivation.NewKey: parameters must not be nil")
	}
	if prfKey == nil {
		return nil, fmt.Errorf("prfbasedkeyderivation.NewKey: prfKey must not be nil")
	}

	// 1.
	switch prfKey.(type) {
	case *aescmacprf.Key:
	case *hkdfprf.Key:
	case *hmacprf.Key:
		// Do nothing.
	default:
		return nil, fmt.Errorf("prfbasedkeyderivation.NewKey: unknown PRF key type: %T", prfKey)
	}
	// 2.
	if !parameters.PRFParameters().Equal(prfKey.Parameters()) {
		return nil, fmt.Errorf("prfbasedkeyderivation.NewKey: prfKey.Parameters() is not equal to parameters.PrfParameters()")
	}
	// 3.
	if !parameters.HasIDRequirement() && idRequirement != 0 {
		return nil, fmt.Errorf("prfbasedkeyderivation.NewKey: idRequirement = %v and parameters.HasIDRequirement() = false, want 0", idRequirement)
	}

	return &Key{
		parameters:    parameters,
		prfKey:        prfKey,
		idRequirement: idRequirement,
	}, nil
}

// PRFKey returns the PRF key.
func (k *Key) PRFKey() key.Key { return k.prfKey }

// Parameters returns the parameters of this key.
func (k *Key) Parameters() key.Parameters { return k.parameters }

// IDRequirement returns required to indicate if this key requires an
// identifier. If it does, id will contain that identifier.
func (k *Key) IDRequirement() (uint32, bool) {
	return k.idRequirement, k.parameters.HasIDRequirement()
}

// Equal returns whether this key object is equal to other.
func (k *Key) Equal(other key.Key) bool {
	that, ok := other.(*Key)
	return ok && k.parameters.Equal(that.parameters) &&
		k.prfKey.Equal(that.prfKey) &&
		k.idRequirement == that.idRequirement
}

func createKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	prfbasedKeyDerivationParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: %T", p)
	}

	switch prfKeyTemplateType := prfbasedKeyDerivationParams.PRFParameters().(type) {
	case *hkdfprf.Parameters:
		// Only HKDF PRF is supported.
	default:
		return nil, fmt.Errorf("invalid PRF key template type: got %T, want %T", prfKeyTemplateType, (*hkdfprf.Parameters)(nil))
	}

	// We are sure this is a PRF-based key derivation parameters object, as
	// this is checked in the constructor.
	prfKey, err := keygenregistry.CreateKey(prfbasedKeyDerivationParams.PRFParameters(), idRequirement)
	if err != nil {
		return nil, err
	}

	// We reject key templates for which we cannot derive a key.
	if !keyderivers.CanDeriveKey(reflect.TypeOf(prfbasedKeyDerivationParams.DerivedKeyParameters())) {
		return nil, fmt.Errorf("PRF key parameters are not supported: %T", prfbasedKeyDerivationParams.DerivedKeyParameters())
	}

	return NewKey(prfbasedKeyDerivationParams, prfKey, idRequirement)
}
