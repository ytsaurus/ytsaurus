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

	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/prf/aescmacprf"
	"github.com/tink-crypto/tink-go/v2/prf/hkdfprf"
	"github.com/tink-crypto/tink-go/v2/prf/hmacprf"
)

// Parameters represents the parameters of a PRF-based key derivation function.
type Parameters struct {
	prfParameters        key.Parameters
	derivedKeyParameters key.Parameters
}

var _ key.Parameters = (*Parameters)(nil)

// NewParameters creates a new Parameters struct.
//
// The PRF parameters must be one of the following types:
//   - [aescmacprf.Parameters]
//   - [hkdfprf.Parameters]
//   - [hmacprf.Parameters]
func NewParameters(prfParameters key.Parameters, derivedKeyParameters key.Parameters) (*Parameters, error) {
	if prfParameters == nil {
		return nil, fmt.Errorf("prfParameters must not be nil")
	}
	if derivedKeyParameters == nil {
		return nil, fmt.Errorf("derivedKeyParameters must not be nil")
	}
	switch prfParametersType := prfParameters.(type) {
	case *aescmacprf.Parameters:
	case *hkdfprf.Parameters:
	case *hmacprf.Parameters:
		// Do nothing.
	default:
		return nil, fmt.Errorf("invalid PRF parameters type: %T", prfParametersType)
	}
	return &Parameters{
		prfParameters:        prfParameters,
		derivedKeyParameters: derivedKeyParameters,
	}, nil
}

// PRFParameters returns the parameters of the PRF.
func (p *Parameters) PRFParameters() key.Parameters { return p.prfParameters }

// DerivedKeyParameters returns the parameters of the derived key.
func (p *Parameters) DerivedKeyParameters() key.Parameters { return p.derivedKeyParameters }

// HasIDRequirement implements key.Parameters.
func (p *Parameters) HasIDRequirement() bool {
	return p.DerivedKeyParameters().HasIDRequirement()
}

// Equal implements key.Parameters.
func (p *Parameters) Equal(other key.Parameters) bool {
	otherParams, ok := other.(*Parameters)
	return ok && p.prfParameters.Equal(otherParams.prfParameters) &&
		p.derivedKeyParameters.Equal(otherParams.derivedKeyParameters)
}
