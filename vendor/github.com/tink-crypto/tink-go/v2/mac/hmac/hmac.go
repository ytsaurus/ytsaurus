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

// Package hmac provides the key manager for HMAC.
package hmac

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/internal/config"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/keygenregistry"
	"github.com/tink-crypto/tink-go/v2/internal/legacykeymanager"
	"github.com/tink-crypto/tink-go/v2/internal/primitiveregistry"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	hmpb "github.com/tink-crypto/tink-go/v2/proto/hmac_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

func newKeyManager() registry.KeyManager {
	return legacykeymanager.New(typeURL, primitiveConstructor, tinkpb.KeyData_SYMMETRIC, func(b []byte) (proto.Message, error) {
		protoKey := &hmpb.HmacKey{}
		if err := proto.Unmarshal(b, protoKey); err != nil {
			return nil, err
		}
		return protoKey, nil
	})
}

// RegisterPrimitiveConstructor accepts a config object and registers the HMAC
// primitive constructor to the provided config.
//
// It is *NOT* part of the public API.
func RegisterPrimitiveConstructor(c *config.Builder, t internalapi.Token) error {
	return c.RegisterPrimitiveConstructor(reflect.TypeFor[*Key](), primitiveConstructor, t)
}

func init() {
	if err := protoserialization.RegisterKeyParser(typeURL, new(keyParser)); err != nil {
		panic(fmt.Sprintf("hmac.init() failed: %v", err))
	}
	if err := protoserialization.RegisterKeySerializer[*Key](new(keySerializer)); err != nil {
		panic(fmt.Sprintf("hmac.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersSerializer[*Parameters](new(parametersSerializer)); err != nil {
		panic(fmt.Sprintf("hmac.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersParser(typeURL, new(parametersParser)); err != nil {
		panic(fmt.Sprintf("hmac.init() failed: %v", err))
	}
	if err := primitiveregistry.RegisterPrimitiveConstructor[*Key](primitiveConstructor); err != nil {
		panic(fmt.Sprintf("hmac.init() failed: %v", err))
	}
	if err := keygenregistry.RegisterKeyCreator[*Parameters](createKey); err != nil {
		panic(fmt.Sprintf("hmac.init() failed: %v", err))
	}

	if err := registry.RegisterKeyManager(newKeyManager()); err != nil {
		panic(fmt.Sprintf("hmac.init() failed: %v", err))
	}
}
