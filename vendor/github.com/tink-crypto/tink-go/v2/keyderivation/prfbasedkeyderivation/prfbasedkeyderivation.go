// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package prfbasedkeyderivation provides a key derivation function that is based on a PRF.
package prfbasedkeyderivation

import (
	"fmt"
	"reflect"

	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/internal/config"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/keygenregistry"
	"github.com/tink-crypto/tink-go/v2/internal/primitiveregistry"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
)

// RegisterPrimitiveConstructor accepts a config object and registers the
// PRF-based key derivation primitive constructor to the provided config.
//
// It is *NOT* part of the public API.
func RegisterPrimitiveConstructor(b *config.Builder, t internalapi.Token) error {
	return b.RegisterPrimitiveConstructor(reflect.TypeFor[*Key](), primitiveConstructor, t)
}

func init() {
	if err := protoserialization.RegisterKeyParser(typeURL, new(keyParser)); err != nil {
		panic(fmt.Sprintf("prfbasedkeyderivation.init() failed: %v", err))
	}
	if err := protoserialization.RegisterKeySerializer[*Key](new(keySerializer)); err != nil {
		panic(fmt.Sprintf("prfbasedkeyderivation.init() failed: %v", err))
	}
	if err := primitiveregistry.RegisterPrimitiveConstructor[*Key](primitiveConstructor); err != nil {
		panic(fmt.Sprintf("prfbasedkeyderivation.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersSerializer[*Parameters](new(parametersSerializer)); err != nil {
		panic(fmt.Sprintf("prfbasedkeyderivation.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersParser(typeURL, &parametersParser{}); err != nil {
		panic(fmt.Sprintf("prfbasedkeyderivation.init() failed: %v", err))
	}
	if err := registry.RegisterKeyManager(new(keyManager)); err != nil {
		panic(fmt.Sprintf("prfbasedkeyderivation.init() failed: %v", err))
	}
	if err := keygenregistry.RegisterKeyCreator[*Parameters](createKey); err != nil {
		panic(fmt.Sprintf("prfbasedkeyderivation.init() failed: %v", err))
	}
}
