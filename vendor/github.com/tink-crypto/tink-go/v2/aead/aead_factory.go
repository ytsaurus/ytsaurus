// Copyright 2018 Google LLC
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

package aead

import (
	"fmt"
	"slices"

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/internalregistry"
	"github.com/tink-crypto/tink-go/v2/internal/monitoringutil"
	"github.com/tink-crypto/tink-go/v2/internal/prefixmap"
	"github.com/tink-crypto/tink-go/v2/internal/primitiveset"
	"github.com/tink-crypto/tink-go/v2/internal/registryconfig"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/monitoring"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// New returns an AEAD primitive from the given keyset handle.
func New(handle *keyset.Handle) (tink.AEAD, error) {
	ps, err := keyset.Primitives[tink.AEAD](handle, &registryconfig.RegistryConfig{}, internalapi.Token{})
	if err != nil {
		return nil, fmt.Errorf("aead_factory: cannot obtain primitive set: %s", err)
	}
	return newWrappedAead(ps)
}

// NewWithConfig creates an AEAD primitive from the given [keyset.Handle] using
// the provided [Config].
func NewWithConfig(handle *keyset.Handle, config keyset.Config) (tink.AEAD, error) {
	ps, err := keyset.Primitives[tink.AEAD](handle, config, internalapi.Token{})
	if err != nil {
		return nil, fmt.Errorf("aead_factory: cannot obtain primitive set with config: %s", err)
	}
	return newWrappedAead(ps)
}

// wrappedAead is an AEAD implementation that uses the underlying primitive set for encryption
// and decryption.
type wrappedAead struct {
	primary    aeadAndKeyID
	primitives *prefixmap.PrefixMap[aeadAndKeyID]

	encLogger monitoring.Logger
	decLogger monitoring.Logger
}

type aeadAndKeyID struct {
	primitive tink.AEAD
	keyID     uint32
}

func (a *aeadAndKeyID) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	return a.primitive.Encrypt(plaintext, associatedData)
}

func (a *aeadAndKeyID) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	return a.primitive.Decrypt(ciphertext, associatedData)
}

// aeadPrimitiveAdapter is an adapter that turns a non-full [tink.AEAD]
// primitive into a full [tink.AEAD] primitive.
type fullAEADPrimitiveAdapter struct {
	primitive tink.AEAD
	prefix    []byte
}

func (a *fullAEADPrimitiveAdapter) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	ct, err := a.primitive.Encrypt(plaintext, associatedData)
	if err != nil {
		return nil, err
	}
	return slices.Concat(a.prefix, ct), nil
}

func (a *fullAEADPrimitiveAdapter) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	return a.primitive.Decrypt(ciphertext[len(a.prefix):], associatedData)
}

// extractFullAEAD returns a full aeadAndKeyID primitive from the given
// [primitiveset.Entry[tink.AEAD]].
func extractFullAEAD(entry *primitiveset.Entry[tink.AEAD]) (*aeadAndKeyID, error) {
	if entry.FullPrimitive != nil {
		return &aeadAndKeyID{primitive: entry.FullPrimitive, keyID: entry.KeyID}, nil
	}
	return &aeadAndKeyID{
		primitive: &fullAEADPrimitiveAdapter{primitive: entry.Primitive, prefix: entry.OutputPrefix()},
		keyID:     entry.KeyID,
	}, nil
}

func newWrappedAead(ps *primitiveset.PrimitiveSet[tink.AEAD]) (*wrappedAead, error) {
	primary, err := extractFullAEAD(ps.Primary)
	if err != nil {
		return nil, err
	}
	primitives := prefixmap.New[aeadAndKeyID]()
	for _, entries := range ps.Entries {
		for _, e := range entries {
			p, err := extractFullAEAD(e)
			if err != nil {
				return nil, err
			}
			primitives.Insert(string(e.OutputPrefix()), *p)
		}
	}
	encLogger, decLogger, err := createLoggers(ps)
	if err != nil {
		return nil, err
	}
	return &wrappedAead{
		primary:    *primary,
		primitives: primitives,
		encLogger:  encLogger,
		decLogger:  decLogger,
	}, nil
}

func createLoggers(ps *primitiveset.PrimitiveSet[tink.AEAD]) (monitoring.Logger, monitoring.Logger, error) {
	if len(ps.Annotations) == 0 {
		return &monitoringutil.DoNothingLogger{}, &monitoringutil.DoNothingLogger{}, nil
	}
	client := internalregistry.GetMonitoringClient()
	keysetInfo, err := monitoringutil.KeysetInfoFromPrimitiveSet(ps)
	if err != nil {
		return nil, nil, err
	}
	encLogger, err := client.NewLogger(&monitoring.Context{
		Primitive:   "aead",
		APIFunction: "encrypt",
		KeysetInfo:  keysetInfo,
	})
	if err != nil {
		return nil, nil, err
	}
	decLogger, err := client.NewLogger(&monitoring.Context{
		Primitive:   "aead",
		APIFunction: "decrypt",
		KeysetInfo:  keysetInfo,
	})
	if err != nil {
		return nil, nil, err
	}
	return encLogger, decLogger, nil
}

// Encrypt encrypts the given plaintext with the given associatedData.
// It returns the concatenation of the primary's identifier and the ciphertext.
func (a *wrappedAead) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	ct, err := a.primary.Encrypt(plaintext, associatedData)
	if err != nil {
		a.encLogger.LogFailure()
		return nil, err
	}
	a.encLogger.Log(a.primary.keyID, len(plaintext))
	return ct, nil
}

// Decrypt decrypts the given ciphertext and authenticates it with the given
// associatedData. It returns the corresponding plaintext if the
// ciphertext is authenticated.
func (a *wrappedAead) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	it := a.primitives.PrimitivesMatchingPrefix(ciphertext)
	for primitive, ok := it.Next(); ok; primitive, ok = it.Next() {
		pt, err := primitive.Decrypt(ciphertext, associatedData)
		if err != nil {
			continue
		}
		a.decLogger.Log(primitive.keyID, len(ciphertext))
		return pt, nil
	}
	// Nothing worked.
	a.decLogger.LogFailure()
	return nil, fmt.Errorf("aead_factory: decryption failed")
}
