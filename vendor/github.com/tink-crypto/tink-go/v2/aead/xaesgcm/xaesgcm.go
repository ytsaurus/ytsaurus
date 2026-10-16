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

// Package xaesgcm provides a key and parameters definition for X-AES-GCM,
// which is specified in https://c2sp.org/XAES-256-GCM.
package xaesgcm

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/internal/keygenregistry"
	"github.com/tink-crypto/tink-go/v2/internal/legacykeymanager"
	"github.com/tink-crypto/tink-go/v2/internal/primitiveregistry"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
	xaesgcmpb "github.com/tink-crypto/tink-go/v2/proto/x_aes_gcm_go_proto"
)

func newKeyManager() registry.KeyManager {
	return legacykeymanager.New(typeURL, primitiveConstructor, tinkpb.KeyData_SYMMETRIC, func(b []byte) (proto.Message, error) {
		protoKey := &xaesgcmpb.XAesGcmKey{}
		if err := proto.Unmarshal(b, protoKey); err != nil {
			return nil, err
		}
		return protoKey, nil
	})
}

func init() {
	if err := protoserialization.RegisterKeySerializer[*Key](&keySerializer{}); err != nil {
		panic(fmt.Sprintf("xaesgcm.init() failed: %v", err))
	}
	if err := protoserialization.RegisterKeyParser(typeURL, &keyParser{}); err != nil {
		panic(fmt.Sprintf("xaesgcm.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersSerializer[*Parameters](&parametersSerializer{}); err != nil {
		panic(fmt.Sprintf("xaesgcm.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersParser(typeURL, &parametersParser{}); err != nil {
		panic(fmt.Sprintf("xaesgcm.init() failed: %v", err))
	}
	if err := registry.RegisterKeyManager(newKeyManager()); err != nil {
		panic(fmt.Sprintf("xaesgcm.init() failed: %v", err))
	}
	if err := primitiveregistry.RegisterPrimitiveConstructor[*Key](primitiveConstructor); err != nil {
		panic(fmt.Sprintf("xaesgcm.init() failed: %v", err))
	}
	if err := keygenregistry.RegisterKeyCreator[*Parameters](createKey); err != nil {
		panic(fmt.Sprintf("xaesgcm.init() failed: %v", err))
	}
}
