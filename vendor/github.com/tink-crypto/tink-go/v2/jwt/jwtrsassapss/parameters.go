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

package jwtrsassapss

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/key"
)

// KIDStrategy is the strategy for handling the "kid" header.
// See https://datatracker.ietf.org/doc/html/rfc7515#section-4.1.4
// The available options are:
//   - Base64EncodedKeyID: The `kid` is the URL safe (RFC 4648 Section 5) base64-encoded big-endian `key_id` in the keyset.
//   - Ignored: The `kid` header is ignored.
//   - Custom: The `kid` is fixed. It can be obtained by calling `key.GetKid()`.
type KIDStrategy int

const (
	// UnknownKIDStrategy indicates that the key ID strategy is unknown.
	UnknownKIDStrategy KIDStrategy = iota
	// Base64EncodedKeyIDAsKID indicates that the a Base64 encoded key ID is
	// used as the kid.
	//
	// Using this strategy means that:
	//  - [jwt.Signer]'s SignAndEncode always adds the `kid`.
	//  - [jwt.Verifyer]'s VerifyAndDecode checks that the `kid` is
	//    present and equal to the key ID.
	//
	// NOTE: This strategy is recommended by Tink.
	Base64EncodedKeyIDAsKID
	// IgnoredKID indicates that the kid is ignored.
	//
	// Using this strategy means that:
	//  - [jwt.Signer]'s SignAndEncode does not write the kid header
	//  - [jwt.Verifyer]'s VerifyAndDecode ignores the kid header
	IgnoredKID
	// CustomKID indicates that the kid has a custom fixed value.
	//
	// Using this strategy means that:
	//  - [jwt.Signer]'s SignAndEncode writes the KID header to the value
	//    given by key.KID()
	//  - [jwt.Verifyer]'s VerifyAndDecode if the kid is present then it
	//    must match key.KID(); if the kid is not present, it will be
	//    accepted.
	//
	// NOTE: Tink doesn't allow creation of random JWT RSA-SSA-PKCS1 keys from
	// parameters when parameters.KIDStrategy() == CustomKID.
	CustomKID
)

func (k KIDStrategy) String() string {
	switch k {
	case Base64EncodedKeyIDAsKID:
		return "Base64EncodedKeyIDAsKID"
	case IgnoredKID:
		return "IgnoredKID"
	case CustomKID:
		return "CustomKID"
	default:
		return "UnknownKIDStrategy"
	}
}

// Algorithm is the signature algorithm.
// See https://datatracker.ietf.org/doc/html/rfc7518#section-3.5
// The available options are:
//   - PS256: RSASSA-PSS using SHA-256
//   - PS384: RSASSA-PSS using SHA-384
//   - PS512: RSASSA-PSS using SHA-512
type Algorithm int

const (
	// UnknownAlgorithm indicates that the algorithm is unknown.
	UnknownAlgorithm Algorithm = iota
	// PS256 is RSASSA-PSS using SHA-256.
	PS256 Algorithm = iota + 1
	// PS384 is RSASSA-PSS using SHA-384.
	PS384
	// PS512 is RSASSA-PSS using SHA-512.
	PS512
)

func (a Algorithm) String() string {
	switch a {
	case PS256:
		return "PS256"
	case PS384:
		return "PS384"
	case PS512:
		return "PS512"
	default:
		return "UnknownAlgorithm"
	}
}

// Parameters represents the parameters of a JWT RSA SSA PSS key.
type Parameters struct {
	kidStrategy       KIDStrategy
	algorithm         Algorithm
	modulusSizeInBits int
	publicExponent    int
}

var _ key.Parameters = (*Parameters)(nil)

const (
	// f4 is the public exponent 65537.
	f4 = 65537
	// Max exponent for RSA keys used in
	// https://cs.opensource.google/go/go/+/master:src/crypto/internal/fips140/rsa/rsa.go;l=370;drc=a76cc5a4ecb004616404cac5bb756da293818469
	maxExponent = 1<<31 - 1
)

// ParametersOpts represents the options for creating a [Parameters] instance.
type ParametersOpts struct {
	ModulusSizeInBits int
	PublicExponent    int
	Algorithm         Algorithm
	KidStrategy       KIDStrategy
}

// NewParameters creates a new Parameters instance.
func NewParameters(opts ParametersOpts) (*Parameters, error) {
	if opts.ModulusSizeInBits < 2048 {
		return nil, fmt.Errorf("invalid modulus size: %v, want >= 2048", opts.ModulusSizeInBits)
	}
	if opts.Algorithm == UnknownAlgorithm {
		return nil, fmt.Errorf("unsupported algorithm: %v", opts.Algorithm)
	}
	if opts.KidStrategy == UnknownKIDStrategy {
		return nil, fmt.Errorf("unsupported kid strategy: %v", opts.KidStrategy)
	}
	if opts.PublicExponent < f4 || opts.PublicExponent > maxExponent {
		return nil, fmt.Errorf("invalid public exponent: %v, want >= %v and <= %v", opts.PublicExponent, f4, maxExponent)
	}
	// To be consistent with tink-java and tink-cc.
	if opts.PublicExponent%2 != 1 {
		return nil, fmt.Errorf("invalid public exponent: %v, want odd", opts.PublicExponent)
	}
	return &Parameters{
		kidStrategy:       opts.KidStrategy,
		algorithm:         opts.Algorithm,
		modulusSizeInBits: opts.ModulusSizeInBits,
		publicExponent:    opts.PublicExponent,
	}, nil
}

// KIDStrategy returns the KIDStrategy.
func (p *Parameters) KIDStrategy() KIDStrategy { return p.kidStrategy }

// Algorithm returns the Algorithm.
func (p *Parameters) Algorithm() Algorithm { return p.algorithm }

// ModulusSizeInBits returns the ModulusSizeInBits.
func (p *Parameters) ModulusSizeInBits() int { return p.modulusSizeInBits }

// PublicExponent returns the PublicExponent.
func (p *Parameters) PublicExponent() int { return p.publicExponent }

// HasIDRequirement returns true if the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool {
	return p.kidStrategy == Base64EncodedKeyIDAsKID
}

// Equal returns true if the two Parameters are equal.
func (p *Parameters) Equal(other key.Parameters) bool {
	otherParams, ok := other.(*Parameters)
	if !ok {
		return false
	}
	return p.kidStrategy == otherParams.kidStrategy &&
		p.algorithm == otherParams.algorithm &&
		p.modulusSizeInBits == otherParams.modulusSizeInBits &&
		p.publicExponent == otherParams.publicExponent
}
