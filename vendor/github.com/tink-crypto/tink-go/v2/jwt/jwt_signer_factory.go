// Copyright 2022 Google LLC
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

package jwt

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/internalregistry"
	"github.com/tink-crypto/tink-go/v2/internal/monitoringutil"
	"github.com/tink-crypto/tink-go/v2/internal/primitiveset"
	"github.com/tink-crypto/tink-go/v2/internal/registryconfig"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/monitoring"
)

// NewSigner generates a new [jwt.Signer] primitive with the
// global registry.
func NewSigner(handle *keyset.Handle) (Signer, error) {
	return NewSignerWithConfig(handle, &registryconfig.RegistryConfig{})
}

// NewSignerWithConfig generates a new [jwt.Signer] primitive
// with the provided [keyset.Config].
//
// NOTE: This is currently not usable in OSS because [keyset.Config]
// is not user-implementable.
func NewSignerWithConfig(handle *keyset.Handle, config keyset.Config) (Signer, error) {
	if handle == nil {
		return nil, fmt.Errorf("keyset handle can't be nil")
	}
	ps, err := keyset.Primitives[Signer](handle, config, internalapi.Token{})
	if err != nil {
		return nil, fmt.Errorf("jwt_signer_factory: cannot obtain primitive set: %v", err)
	}
	return newWrappedSigner(ps)
}

func newWrappedSigner(ps *primitiveset.PrimitiveSet[Signer]) (Signer, error) {
	logger, err := createSignerLogger(ps)
	if err != nil {
		return nil, err
	}
	if ps.Primary.FullPrimitive == nil {
		// Something is wrong, this should not happen.
		return nil, fmt.Errorf("jwt_signer_factory: primary full primitive is nil")
	}
	return &wrappedSigner{
		primaryFullPrimitive: ps.Primary.FullPrimitive,
		keyID:                ps.Primary.KeyID,
		logger:               logger,
	}, nil
}

// wrappedSigner is a JWT Signer implementation that uses the underlying
// primary full primitive for signing. It logs success/failure of the signing
// operation.
type wrappedSigner struct {
	primaryFullPrimitive Signer
	keyID                uint32
	logger               monitoring.Logger
}

var _ Signer = (*wrappedSigner)(nil)

func createSignerLogger[T any](ps *primitiveset.PrimitiveSet[T]) (monitoring.Logger, error) {
	// Only keysets with annotations are monitored.
	if len(ps.Annotations) == 0 {
		return &monitoringutil.DoNothingLogger{}, nil
	}
	keysetInfo, err := monitoringutil.KeysetInfoFromPrimitiveSet(ps)
	if err != nil {
		return nil, err
	}
	return internalregistry.GetMonitoringClient().NewLogger(&monitoring.Context{
		KeysetInfo:  keysetInfo,
		Primitive:   "jwtsign",
		APIFunction: "sign",
	})
}

func (w *wrappedSigner) SignAndEncode(rawJWT *RawJWT) (string, error) {
	token, err := w.primaryFullPrimitive.SignAndEncode(rawJWT)
	if err != nil {
		w.logger.LogFailure()
		return "", err
	}
	w.logger.Log(w.keyID, 1)
	return token, nil
}
