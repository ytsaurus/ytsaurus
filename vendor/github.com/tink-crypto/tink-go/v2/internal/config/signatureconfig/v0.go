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

// Package signatureconfig provides an instance of the Config for signature primitives.
package signatureconfig

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/config"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/signature/ecdsa"
	"github.com/tink-crypto/tink-go/v2/signature/ed25519"
	"github.com/tink-crypto/tink-go/v2/signature/mldsa"
	"github.com/tink-crypto/tink-go/v2/signature/rsassapkcs1"
	"github.com/tink-crypto/tink-go/v2/signature/rsassapss"
	"github.com/tink-crypto/tink-go/v2/signature/slhdsa"
)

var configV0 = mustCreateConfigV0()

func mustCreateConfigV0() config.Config {
	builder := config.NewBuilder()

	if err := ecdsa.RegisterPrimitiveConstructor(builder, internalapi.Token{}); err != nil {
		panic(fmt.Sprintf("mustCreateConfigV0() failed to register ECDSA: %v", err))
	}
	if err := ed25519.RegisterPrimitiveConstructor(builder, internalapi.Token{}); err != nil {
		panic(fmt.Sprintf("mustCreateConfigV0() failed to register Ed25519: %v", err))
	}
	if err := mldsa.RegisterPrimitiveConstructor(builder, internalapi.Token{}); err != nil {
		panic(fmt.Sprintf("mustCreateConfigV0() failed to register ML-DSA: %v", err))
	}
	if err := rsassapkcs1.RegisterPrimitiveConstructor(builder, internalapi.Token{}); err != nil {
		panic(fmt.Sprintf("mustCreateConfigV0() failed to register RSA-SSA-PKCS1: %v", err))
	}
	if err := rsassapss.RegisterPrimitiveConstructor(builder, internalapi.Token{}); err != nil {
		panic(fmt.Sprintf("mustCreateConfigV0() failed to register RSA-SSA-PSS: %v", err))
	}
	if err := slhdsa.RegisterPrimitiveConstructors(builder, internalapi.Token{}); err != nil {
		panic(fmt.Sprintf("mustCreateConfigV0() failed to register SLH-DSA: %v", err))
	}

	return builder.Build()
}

// V0 returns an instance of the ConfigV0.
func V0() config.Config { return configV0 }
