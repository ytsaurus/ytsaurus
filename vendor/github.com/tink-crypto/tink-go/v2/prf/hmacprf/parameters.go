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

	"github.com/tink-crypto/tink-go/v2/key"
)

// HashType is the hash type of the HMAC key.
type HashType int

const (
	// UnknownHashType is the default value of HashType.
	UnknownHashType HashType = iota
	// SHA1 is the SHA1 hash type.
	SHA1
	// SHA224 is the SHA224 hash type.
	SHA224
	// SHA256 is the SHA256 hash type.
	SHA256
	// SHA384 is the SHA384 hash type.
	SHA384
	// SHA512 is the SHA512 hash type.
	SHA512
)

func (ht HashType) String() string {
	switch ht {
	case SHA1:
		return "SHA1"
	case SHA224:
		return "SHA224"
	case SHA256:
		return "SHA256"
	case SHA384:
		return "SHA384"
	case SHA512:
		return "SHA512"
	default:
		return "UNKNOWN"
	}
}

// Parameters represents the parameters of an HKDF PRF key.
type Parameters struct {
	keySizeInBytes int
	hashType       HashType
}

var _ key.Parameters = (*Parameters)(nil)

// NewParameters creates a new [Parameters] value.
func NewParameters(keySizeInBytes int, hashType HashType) (*Parameters, error) {
	if keySizeInBytes < 16 {
		return nil, fmt.Errorf("keySizeInBytes must be >= 16, got %d", keySizeInBytes)
	}
	if hashType == UnknownHashType {
		return nil, fmt.Errorf("hashType must be specified")
	}
	return &Parameters{keySizeInBytes: keySizeInBytes, hashType: hashType}, nil
}

// KeySizeInBytes returns the key size.
func (p *Parameters) KeySizeInBytes() int { return p.keySizeInBytes }

// HashType returns the hash type of the HMAC key.
func (p *Parameters) HashType() HashType { return p.hashType }

// HasIDRequirement tells whether the key has an ID requirement.
//
// PRFs have no output prefix, so this is always false.
func (p *Parameters) HasIDRequirement() bool { return false }

// Equal tells whether this parameters value is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	otherParams, ok := other.(*Parameters)
	return ok && p.keySizeInBytes == otherParams.keySizeInBytes &&
		p.hashType == otherParams.hashType
}
