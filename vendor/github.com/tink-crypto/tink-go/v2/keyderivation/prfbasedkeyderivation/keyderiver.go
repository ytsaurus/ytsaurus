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

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/keyderivation/internal/keyderiver"
	"github.com/tink-crypto/tink-go/v2/keyderivation/internal/keyderivers"
	"github.com/tink-crypto/tink-go/v2/keyderivation/internal/streamingprf"
	"github.com/tink-crypto/tink-go/v2/prf/hkdfprf"
)

type keyDeriver struct {
	key          *Key
	streamingPRF streamingprf.StreamingPRF
}

// NewKeyDeriver creates a new KeyDeriver.
//
// It constructs a [keyderiver.KeyDeriver] from the PRF key in the provided Key.
//
// This is an internal API.
func NewKeyDeriver(key *Key, _ internalapi.Token) (keyderiver.KeyDeriver, error) {
	if key == nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: key must not be nil")
	}
	prfKey, ok := key.PRFKey().(*hkdfprf.Key)
	if !ok {
		return nil, fmt.Errorf("prfbasedkeyderivation: unsupported PRF key type: got %T, want %T", key.PRFKey(), (*hkdfprf.Key)(nil))
	}
	prfKeyParams := prfKey.Parameters().(*hkdfprf.Parameters)
	prf, err := streamingprf.NewHKDFStreamingPRF(prfKeyParams.HashType().String(), prfKey.KeyBytes().Data(insecuresecretdataaccess.Token{}), prfKeyParams.Salt())
	if err != nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: %v", err)
	}
	return &keyDeriver{
		key:          key,
		streamingPRF: prf,
	}, nil
}

// DeriveKey derives a single key from the PRF-Based Deriver key.
//
// It produces a single key.Key.
func (k *keyDeriver) DeriveKey(salt []byte) (key.Key, error) {
	randomness, err := k.streamingPRF.Compute(salt)
	if err != nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: compute randomness from PRF failed: %v", err)
	}

	derivedKeyParameters := k.key.Parameters().(*Parameters).DerivedKeyParameters()
	idRequirement, _ := k.key.IDRequirement()
	return keyderivers.DeriveKey(derivedKeyParameters, idRequirement, randomness, insecuresecretdataaccess.Token{})
}

func primitiveConstructor(key key.Key) (any, error) {
	k, ok := key.(*Key)
	if !ok {
		return nil, fmt.Errorf("prfbasedkeyderivation: key is not a PRF-Based Deriver key")
	}
	return NewKeyDeriver(k, internalapi.Token{})
}
