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

package jwtecdsa

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
	// NOTE: Tink doesn't allow creation of random JWT ECDSA keys from
	// parameters when parameters.KIDStrategy() == CustomKID.
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

// Algorithm defines the algorithm used for signing.
type Algorithm int

const (
	// UnknownAlgorithm indicates that the algorithm is unknown.
	UnknownAlgorithm Algorithm = iota
	// ES256 indicates the ECDSA algorithm with SHA-256.
	ES256
	// ES384 indicates the ECDSA algorithm with SHA-384.
	ES384
	// ES512 indicates the ECDSA algorithm with SHA-512.
	ES512
)

// String returns the string representation of the Algorithm.
func (a Algorithm) String() string {
	switch a {
	case ES256:
		return "ES256"
	case ES384:
		return "ES384"
	case ES512:
		return "ES512"
	default:
		return "UnknownAlgorithm"
	}
}

// Parameters contains the parameters for JWT ECDSA signing.
type Parameters struct {
	kidStrategy KIDStrategy
	algorithm   Algorithm
}

var _ key.Parameters = (*Parameters)(nil)

// NewParameters creates a new Parameters object.
func NewParameters(kidStrategy KIDStrategy, algorithm Algorithm) (*Parameters, error) {
	if kidStrategy == UnknownKIDStrategy {
		return nil, fmt.Errorf("kidStrategy must be set")
	}
	if algorithm == UnknownAlgorithm {
		return nil, fmt.Errorf("algorithm must be set")
	}
	return &Parameters{kidStrategy: kidStrategy, algorithm: algorithm}, nil
}

// KIDStrategy returns the key ID strategy.
func (p *Parameters) KIDStrategy() KIDStrategy { return p.kidStrategy }

// Algorithm returns the algorithm used for signing.
func (p *Parameters) Algorithm() Algorithm { return p.algorithm }

// HasIDRequirement tells whether the key has an ID requirement, that is, if a
// key generated with these parameters must have a given ID.
func (p *Parameters) HasIDRequirement() bool { return p.KIDStrategy() == Base64EncodedKeyIDAsKID }

// Equal compares this parameters object with other.
func (p *Parameters) Equal(other key.Parameters) bool {
	o, ok := other.(*Parameters)
	return ok && p.KIDStrategy() == o.KIDStrategy() && p.Algorithm() == o.Algorithm()
}
