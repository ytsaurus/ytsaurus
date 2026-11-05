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

package aescmacprf

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/key"
)

// Parameters represents the parameters of an AES-CMAC PRF key.
type Parameters struct {
	keySizeInBytes int
}

var _ key.Parameters = (*Parameters)(nil)

// NewParameters creates a new [Parameters] value.
func NewParameters(keySizeInBytes int) (Parameters, error) {
	if keySizeInBytes != 16 && keySizeInBytes != 32 {
		return Parameters{}, fmt.Errorf("keySizeInBytes must be 16 or 32, got %d", keySizeInBytes)
	}
	return Parameters{keySizeInBytes: keySizeInBytes}, nil
}

// KeySizeInBytes the key size.
func (p *Parameters) KeySizeInBytes() int { return p.keySizeInBytes }

// HasIDRequirement tells whether the key has an ID requirement.
//
// PRFs have no output prefix, so this is always false.
func (p *Parameters) HasIDRequirement() bool { return false }

// Equal tells whether this parameters value is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	otherParams, ok := other.(*Parameters)
	return ok && p.keySizeInBytes == otherParams.keySizeInBytes
}
