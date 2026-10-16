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

// Package aesctrhmac provides a key manager for AES-CTR-HMAC streaming AEADs.
package aesctrhmac

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
	streamaeadpb "github.com/tink-crypto/tink-go/v2/proto/aes_ctr_hmac_streaming_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

// RegisterPrimitiveConstructor accepts a config object and registers the
// AES-CTR-HMAC primitive constructors to the provided config.
//
// It is *NOT* part of the public API.
func RegisterPrimitiveConstructor(b *config.Builder, t internalapi.Token) error {
	if err := b.RegisterPrimitiveConstructor(reflect.TypeFor[*Key](), primitiveConstructor, t); err != nil {
		return err
	}
	return nil
}

func init() {
	if err := protoserialization.RegisterKeySerializer[*Key](&keySerializer{}); err != nil {
		panic(fmt.Sprintf("aesctrhmac.init() failed: %v", err))
	}
	if err := protoserialization.RegisterKeyParser(typeURL, &keyParser{}); err != nil {
		panic(fmt.Sprintf("aesctrhmac.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersSerializer[*Parameters](&parametersSerializer{}); err != nil {
		panic(fmt.Sprintf("aesctrhmac.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersParser(typeURL, &parametersParser{}); err != nil {
		panic(fmt.Sprintf("aesctrhmac.init() failed: %v", err))
	}
	if err := primitiveregistry.RegisterPrimitiveConstructor[*Key](primitiveConstructor); err != nil {
		panic(fmt.Sprintf("aesctrhmac.init() failed: %v", err))
	}
	if err := keygenregistry.RegisterKeyCreator[*Parameters](createKey); err != nil {
		panic(fmt.Sprintf("aesctrhmac.init() failed: %v", err))
	}
	if err := registry.RegisterKeyManager(legacykeymanager.New(typeURL, primitiveConstructor, tinkpb.KeyData_SYMMETRIC, func(b []byte) (proto.Message, error) {
		protoKey := &streamaeadpb.AesCtrHmacStreamingKey{}
		if err := proto.Unmarshal(b, protoKey); err != nil {
			return nil, err
		}
		return protoKey, nil
	})); err != nil {
		panic(fmt.Sprintf("aesctrhmac.init() failed: %v", err))
	}
}
