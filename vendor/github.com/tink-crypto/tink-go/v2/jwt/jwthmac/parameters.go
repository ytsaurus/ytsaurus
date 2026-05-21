// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/Lycense-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package jwthmac

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/key"
)

// KIDStrategy defines how the key ID is generated.
type KIDStrategy int

const (
	// UnknownKIDStrategy indicates that the key ID strategy is unknown.
	UnknownKIDStrategy KIDStrategy = iota
	// Base64EncodedKeyIDAsKID indicates that the a Base64 encoded key ID is
	// used as the kid.
	//
	// Using this strategy means that:
	//  - [jwt.MAC]'s ComputeMACAndEncode always adds the `kid`.
	//  - [jwt.MAC]'s VerifyMACAndDecode checks that the `kid` is
	//    present and equal to the key ID.
	//
	// NOTE: This strategy is recommended by Tink.
	Base64EncodedKeyIDAsKID
	// IgnoredKID indicates that the kid is ignored.
	//
	// Using this strategy means that:
	//  - [jwt.MAC]'s ComputeMACAndEncodedoes not write the kid header
	//  - [jwt.MAC]'s VerifyMACAndDecode ignores the kid header
	IgnoredKID
	// CustomKID indicates that the kid has a custom fixed value.
	//
	// Using this strategy means that:
	//  - [jwt.MAC]'s ComputeMACAndEncode writes the KID header to the value
	//    given by key.KID()
	//  - In [jwt.MAC]'s VerifyMACAndDecode if the kid is present then it
	//    must match key.KID(); if the kid is not present, it will be
	//    accepted.
	//
	// NOTE: Tink doesn't allow creation of random JWT HMAC keys
	// from parameters when parameters.KIDStrategy() == CustomKID.
	CustomKID
)

// String returns the string representation of the KIDStrategy.
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

// Algorithm is the HMAC algorithm.
// See https://datatracker.ietf.org/doc/html/rfc7518#section-3.2
// The available options are:
//   - HS256: HMAC with SHA-256
//   - HS384: HMAC with SHA-384
//   - HS512: HMAC with SHA-512
type Algorithm int

const (
	// UnknownAlgorithm indicates that the algorithm is unknown.
	UnknownAlgorithm Algorithm = iota
	// HS256 is HMAC using SHA-256.
	HS256
	// HS384 is HMAC using SHA-384.
	HS384
	// HS512 is HMAC using SHA-512.
	HS512
)

// String returns the string representation of the Algorithm.
func (a Algorithm) String() string {
	switch a {
	case HS256:
		return "HS256"
	case HS384:
		return "HS384"
	case HS512:
		return "HS512"
	default:
		return "UnknownAlgorithm"
	}
}

// Parameters contains the parameters for JWT HMAC.
type Parameters struct {
	keySizeInBytes int
	kidStrategy    KIDStrategy
	algorithm      Algorithm
}

var _ key.Parameters = (*Parameters)(nil)

// minKeySizeInBytes returns the minimum key size in bytes for a given algorithm.
//
// See https://datatracker.ietf.org/doc/html/rfc7518#section-3.2.
func minKeySizeInBytes(algorithm Algorithm) (int, error) {
	switch algorithm {
	case HS256:
		return 32, nil
	case HS384:
		return 48, nil
	case HS512:
		return 64, nil
	default:
		return 0, fmt.Errorf("invalid algorithm: %v", algorithm)
	}
}

// NewParameters creates a new [Parameters] value.
func NewParameters(keySizeInBytes int, kidStrategy KIDStrategy, algorithm Algorithm) (*Parameters, error) {
	minKeySize, err := minKeySizeInBytes(algorithm)
	if err != nil {
		return nil, err
	}
	if keySizeInBytes < minKeySize {
		return nil, fmt.Errorf("keySizeInBytes must be at least %d bytes", minKeySize)
	}
	if kidStrategy == UnknownKIDStrategy {
		return nil, fmt.Errorf("kidStrategy must be set")
	}
	return &Parameters{
		keySizeInBytes: keySizeInBytes,
		kidStrategy:    kidStrategy,
		algorithm:      algorithm,
	}, nil
}

// KIDStrategy returns the key ID strategy.
func (p *Parameters) KIDStrategy() KIDStrategy { return p.kidStrategy }

// KeySizeInBytes returns the key size in bytes.
func (p *Parameters) KeySizeInBytes() int { return p.keySizeInBytes }

// Algorithm returns the algorithm used for signing.
func (p *Parameters) Algorithm() Algorithm { return p.algorithm }

// HasIDRequirement tells whether the key has an ID requirement, that is, if a
// key generated with these parameters must have a given ID.
func (p *Parameters) HasIDRequirement() bool { return p.KIDStrategy() == Base64EncodedKeyIDAsKID }

// Equal compares this parameters value with other.
func (p *Parameters) Equal(other key.Parameters) bool {
	o, ok := other.(*Parameters)
	return ok && p.KIDStrategy() == o.KIDStrategy() &&
		p.Algorithm() == o.Algorithm() &&
		p.KeySizeInBytes() == o.KeySizeInBytes()
}
