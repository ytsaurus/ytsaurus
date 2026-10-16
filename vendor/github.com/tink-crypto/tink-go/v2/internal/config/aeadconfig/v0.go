// Copyright 2024 Google LLC
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

// Package aeadconfig provides an instance of the Config for AEAD primitives.
package aeadconfig

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/aead/aesctrhmac"
	"github.com/tink-crypto/tink-go/v2/aead/aesgcm"
	"github.com/tink-crypto/tink-go/v2/aead/aesgcmsiv"
	"github.com/tink-crypto/tink-go/v2/aead/chacha20poly1305"
	"github.com/tink-crypto/tink-go/v2/aead/xchacha20poly1305"
	"github.com/tink-crypto/tink-go/v2/internal/config"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
)

var configV0 = mustCreateConfigV0()

func mustCreateConfigV0() config.Config {
	builder := config.NewBuilder()

	if err := aesctrhmac.RegisterPrimitiveConstructor(builder, internalapi.Token{}); err != nil {
		panic(fmt.Sprintf("mustCreateConfigV0() failed to register AES-CTR-HMAC: %v", err))
	}

	if err := aesgcm.RegisterPrimitiveConstructor(builder, internalapi.Token{}); err != nil {
		panic(fmt.Sprintf("mustCreateConfigV0() failed to register AES-GCM: %v", err))
	}

	if err := aesgcmsiv.RegisterPrimitiveConstructor(builder, internalapi.Token{}); err != nil {
		panic(fmt.Sprintf("mustCreateConfigV0() failed to register AES-GCM: %v", err))
	}

	if err := chacha20poly1305.RegisterPrimitiveConstructor(builder, internalapi.Token{}); err != nil {
		panic(fmt.Sprintf("mustCreateConfigV0() failed to register CHACHA20-POLY1305: %v", err))
	}

	if err := xchacha20poly1305.RegisterPrimitiveConstructor(builder, internalapi.Token{}); err != nil {
		panic(fmt.Sprintf("mustCreateConfigV0() failed to register XCHACHA20-POLY1305: %v", err))
	}

	return builder.Build()
}

// V0 returns an instance of the ConfigV0.
func V0() config.Config { return configV0 }
