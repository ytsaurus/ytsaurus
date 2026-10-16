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
	"encoding/base64"
	"encoding/binary"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
)

// Key represents a JWT HMAC key.
type Key struct {
	parameters    *Parameters
	keyBytes      secretdata.Bytes
	idRequirement uint32
	kid           string
	hasKID        bool // True iif a custom KID was given or parameters.KIDStrategy() == Base64EncodedKeyIDAsKID
}

var _ key.Key = (*Key)(nil)

// KeyOpts are [Key] options.
type KeyOpts struct {
	KeyBytes      secretdata.Bytes
	IDRequirement uint32
	CustomKID     string
	HasCustomKID  bool
	Parameters    *Parameters
}

func computeKID(customKID *string, idRequirement uint32, parameters *Parameters) (string, bool, error) {
	switch parameters.KIDStrategy() {
	case Base64EncodedKeyIDAsKID:
		if customKID != nil {
			return "", false, fmt.Errorf("custom KID is not supported for KID strategy: %v", parameters.KIDStrategy())
		}
		// Serialize the ID requirement.
		idRequirementBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(idRequirementBytes, idRequirement)
		return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(idRequirementBytes), true, nil
	case IgnoredKID:
		if customKID != nil {
			return "", false, fmt.Errorf("custom KID is not supported for KID strategy: %v", parameters.KIDStrategy())
		}
		return "", false, nil
	case CustomKID:
		if customKID == nil {
			return "", false, fmt.Errorf("custom KID is required for KID strategy: %v", parameters.KIDStrategy())
		}
		return *customKID, true, nil
	default:
		return "", false, fmt.Errorf("invalid KID strategy: %v", parameters.KIDStrategy())
	}
}

// NewKey creates a new JWT HMAC key.
func NewKey(opts KeyOpts) (*Key, error) {
	if opts.Parameters == nil {
		return nil, fmt.Errorf("jwthmac.NewKey: parameters can't be nil")
	}
	if !opts.Parameters.HasIDRequirement() && opts.IDRequirement != 0 {
		return nil, fmt.Errorf("jwthmac.NewKey: ID requirement must be 0 if ID is not required")
	}
	if opts.KeyBytes.Len() != opts.Parameters.KeySizeInBytes() {
		return nil, fmt.Errorf("jwthmac.NewKey: key size mismatch: got %d, want %d", opts.KeyBytes.Len(), opts.Parameters.KeySizeInBytes())
	}

	var customKID *string = nil
	if opts.HasCustomKID {
		customKID = &opts.CustomKID
	}
	kid, hasKID, err := computeKID(customKID, opts.IDRequirement, opts.Parameters)
	if err != nil {
		return nil, fmt.Errorf("jwthmac.NewKey: %v", err)
	}
	return &Key{
		parameters:    opts.Parameters,
		keyBytes:      opts.KeyBytes,
		idRequirement: opts.IDRequirement,
		kid:           kid,
		hasKID:        hasKID,
	}, nil
}

// Parameters returns the parameters of the key.
func (k *Key) Parameters() key.Parameters { return k.parameters }

// KeyBytes returns the key bytes.
func (k *Key) KeyBytes() secretdata.Bytes { return k.keyBytes }

// KID returns the KID for this key.
//
// If no kid is set, it returns ("", false).
func (k *Key) KID() (string, bool) { return k.kid, k.hasKID }

// IDRequirement returns the ID requirement for this key.
func (k *Key) IDRequirement() (uint32, bool) { return k.idRequirement, k.parameters.HasIDRequirement() }

// Equal returns true if k and other are equal.
func (k *Key) Equal(other key.Key) bool {
	that, ok := other.(*Key)
	return ok && k.parameters.Equal(that.parameters) &&
		k.keyBytes.Equal(that.keyBytes) &&
		k.idRequirement == that.idRequirement &&
		k.kid == that.kid && k.hasKID == that.hasKID
}

func createKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	jwtHMACParams, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("jwthmac.createKey: invalid parameters type: want %T, got %T", (*Parameters)(nil), p)
	}
	if jwtHMACParams.KIDStrategy() == CustomKID {
		return nil, fmt.Errorf("jwthmac.createKey: key generation is not supported for strategy %v", jwtHMACParams.KIDStrategy())
	}

	secretKeyBytes, err := secretdata.NewBytesFromRand(uint32(jwtHMACParams.KeySizeInBytes()))
	if err != nil {
		return nil, fmt.Errorf("jwthmac.createKey: failed to generate secret key bytes: %v", err)
	}

	return NewKey(KeyOpts{
		KeyBytes:      secretKeyBytes,
		IDRequirement: idRequirement,
		Parameters:    jwtHMACParams,
	})
}
