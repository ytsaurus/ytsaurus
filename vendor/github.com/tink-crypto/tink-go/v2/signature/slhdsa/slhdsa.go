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

// Package slhdsa provides SLH-DSA keys and parameters definitions, and key
// managers.
package slhdsa

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
	slhdsapb "github.com/tink-crypto/tink-go/v2/proto/slh_dsa_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

func init() {
	if err := registry.RegisterKeyManager(legacykeymanager.NewPrivateKeyManager(signerTypeURL, signerConstructor, tinkpb.KeyData_ASYMMETRIC_PRIVATE, func(b []byte) (proto.Message, error) {
		protoKey := &slhdsapb.SlhDsaPrivateKey{}
		if err := proto.Unmarshal(b, protoKey); err != nil {
			return nil, err
		}
		return protoKey, nil
	})); err != nil {
		panic(fmt.Sprintf("slhdsa.init() failed: %v", err))
	}
	if err := registry.RegisterKeyManager(legacykeymanager.New(verifierTypeURL, verifierConstructor, tinkpb.KeyData_ASYMMETRIC_PUBLIC, func(b []byte) (proto.Message, error) {
		protoKey := &slhdsapb.SlhDsaPublicKey{}
		if err := proto.Unmarshal(b, protoKey); err != nil {
			return nil, err
		}
		return protoKey, nil
	})); err != nil {
		panic(fmt.Sprintf("slhdsa.init() failed: %v", err))
	}
	if err := protoserialization.RegisterKeySerializer[*PublicKey](&publicKeySerializer{}); err != nil {
		panic(fmt.Sprintf("slhdsa.init() failed: %v", err))
	}
	if err := protoserialization.RegisterKeyParser(verifierTypeURL, &publicKeyParser{}); err != nil {
		panic(fmt.Sprintf("slhdsa.init() failed: %v", err))
	}
	if err := protoserialization.RegisterKeySerializer[*PrivateKey](&privateKeySerializer{}); err != nil {
		panic(fmt.Sprintf("slhdsa.init() failed: %v", err))
	}
	if err := protoserialization.RegisterKeyParser(signerTypeURL, &privateKeyParser{}); err != nil {
		panic(fmt.Sprintf("slhdsa.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersSerializer[*Parameters](&parametersSerializer{}); err != nil {
		panic(fmt.Sprintf("slhdsa.init() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersParser(signerTypeURL, &parametersParser{}); err != nil {
		panic(fmt.Sprintf("slhdsa.init() failed: %v", err))
	}
	if err := primitiveregistry.RegisterPrimitiveConstructor[*PublicKey](verifierConstructor); err != nil {
		panic(fmt.Sprintf("slhdsa.init() failed: %v", err))
	}
	if err := primitiveregistry.RegisterPrimitiveConstructor[*PrivateKey](signerConstructor); err != nil {
		panic(fmt.Sprintf("slhdsa.init() failed: %v", err))
	}
	if err := keygenregistry.RegisterKeyCreator[*Parameters](createPrivateKey); err != nil {
		panic(fmt.Sprintf("slhdsa.init() failed: %v", err))
	}
}

// RegisterPrimitiveConstructors accepts a config object and registers the
// SLH-DSA primitive constructors to the provided config.
//
// It is *NOT* part of the public API.
func RegisterPrimitiveConstructors(c *config.Builder, t internalapi.Token) error {
	if err := c.RegisterPrimitiveConstructor(reflect.TypeFor[*PrivateKey](), signerConstructor, t); err != nil {
		return err
	}
	return c.RegisterPrimitiveConstructor(reflect.TypeFor[*PublicKey](), verifierConstructor, t)
}
