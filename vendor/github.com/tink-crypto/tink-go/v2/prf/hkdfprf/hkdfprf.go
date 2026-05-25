// Copyright 2020 Google LLC
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

// Package hkdfprf provides the HKDF PRF key manager, key and parameters.
package hkdfprf

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
	hkdfpb "github.com/tink-crypto/tink-go/v2/proto/hkdf_prf_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

func init() {
	if err := protoserialization.RegisterKeySerializer[*Key](new(keySerializer)); err != nil {
		panic(fmt.Sprintf("hkdfprf.init() failed: %v", err))
	}
	if err := protoserialization.RegisterKeyParser(typeURL, new(keyParser)); err != nil {
		panic(fmt.Sprintf("hkdfprf.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersSerializer[*Parameters](new(parametersSerializer)); err != nil {
		panic(fmt.Sprintf("hkdfprf.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersParser(typeURL, new(parametersParser)); err != nil {
		panic(fmt.Sprintf("hkdfprf.init() failed: %v", err))
	}
	if err := primitiveregistry.RegisterPrimitiveConstructor[*Key](primitiveConstructor); err != nil {
		panic(fmt.Sprintf("hkdfprf.init() failed: %v", err))
	}
	if err := keygenregistry.RegisterKeyCreator[*Parameters](createKey); err != nil {
		panic(fmt.Sprintf("hkdfprf.init() failed: %v", err))
	}
	if err := registry.RegisterKeyManager(legacykeymanager.New(typeURL, primitiveConstructor, tinkpb.KeyData_SYMMETRIC, func(b []byte) (proto.Message, error) {
		protoKey := &hkdfpb.HkdfPrfKey{}
		if err := proto.Unmarshal(b, protoKey); err != nil {
			return nil, err
		}
		return protoKey, nil
	})); err != nil {
		panic(fmt.Sprintf("hkdfprf.init() failed: %v", err))
	}
}

// RegisterPrimitiveConstructor registers the HKDF PRF primitive constructor
// to the provided config.
//
// It is *NOT* part of the public API.
func RegisterPrimitiveConstructor(c *config.Builder, t internalapi.Token) error {
	return c.RegisterPrimitiveConstructor(reflect.TypeFor[*Key](), primitiveConstructor, t)
}
