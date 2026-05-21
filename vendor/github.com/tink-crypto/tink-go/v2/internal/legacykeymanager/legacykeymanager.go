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

// Package legacykeymanager provides a legacy implementation of the
// [registry.KeyManager] interface.
package legacykeymanager

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/keygenregistry"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

// config is a subset of [keyset.Config]. Defined here to avoid potential
// circular dependencies.
type config interface {
	PrimitiveFromKey(key key.Key, _ internalapi.Token) (any, error)
}

type primitiveConstructor func(key key.Key) (any, error)

// KeyManager implements the [registry.KeyManager] interface.
type KeyManager struct {
	typeURL              string
	protoKeyUnmashaller  func([]byte) (proto.Message, error)
	keyMaterialType      tinkpb.KeyData_KeyMaterialType
	primitiveConstructor primitiveConstructor
}

var _ registry.KeyManager = (*KeyManager)(nil)

// New creates a new [LegacyKeyManager].
//
// Assumes parameters are not nil.
func New(typeURL string, primitiveConstructor primitiveConstructor, keyMaterialType tinkpb.KeyData_KeyMaterialType, protoKeyUnmashaller func([]byte) (proto.Message, error)) *KeyManager {
	return &KeyManager{
		typeURL:              typeURL,
		protoKeyUnmashaller:  protoKeyUnmashaller,
		keyMaterialType:      keyMaterialType,
		primitiveConstructor: primitiveConstructor,
	}
}

// Primitive creates a primitive from the given serialized key.
func (m *KeyManager) Primitive(serializedKey []byte) (any, error) {
	keySerialization, err := protoserialization.NewKeySerialization(&tinkpb.KeyData{
		TypeUrl:         m.typeURL,
		Value:           serializedKey,
		KeyMaterialType: m.keyMaterialType,
	}, tinkpb.OutputPrefixType_RAW, 0)
	if err != nil {
		return nil, err
	}
	key, err := protoserialization.ParseKey(keySerialization)
	if err != nil {
		return nil, err
	}
	return m.primitiveConstructor(key)
}

// NewKey creates a new key from the given serialized key format.
//
// Deprecated: Tink always used [NewKeyData] to create new keys. This is
// implemented only to support:
// - the deprecated function [registry.NewKey]
// - direct usage of the [registry.KeyManager] via [registry.GetKeyManager]
func (m *KeyManager) NewKey(serializedKeyFormat []byte) (proto.Message, error) {
	keyData, err := m.NewKeyData(serializedKeyFormat)
	if err != nil {
		return nil, err
	}
	return m.protoKeyUnmashaller(keyData.GetValue())
}

// DoesSupport implements the [registry.KeyManager] interface.
func (m *KeyManager) DoesSupport(typeURL string) bool { return m.typeURL == typeURL }

// TypeURL implements the [registry.KeyManager] interface.
func (m *KeyManager) TypeURL() string { return m.typeURL }

// NewKeyData generates a new KeyData according to specification in serializedkeyFormat.
func (m *KeyManager) NewKeyData(serializedKeyFormat []byte) (*tinkpb.KeyData, error) {
	parameters, err := protoserialization.ParseParameters(&tinkpb.KeyTemplate{
		TypeUrl:          m.typeURL,
		Value:            serializedKeyFormat,
		OutputPrefixType: tinkpb.OutputPrefixType_RAW, // Always RAW for key managers.
	})
	if err != nil {
		return nil, err
	}
	k, err := keygenregistry.CreateKey(parameters, 0)
	if err != nil {
		return nil, err
	}
	keySerialization, err := protoserialization.SerializeKey(k)
	if err != nil {
		return nil, err
	}
	return keySerialization.KeyData(), nil
}

// privateKey represents a key with a public key. Matches [keyset.privateKey].
type privateKey interface {
	PublicKey() (key.Key, error)
}

// PrivateKeyManager is a type of [KeyManager] that understands private key
// types. It implements the [registry.PrivateKeyManager] interface.
type PrivateKeyManager struct {
	KeyManager
}

var _ registry.PrivateKeyManager = (*PrivateKeyManager)(nil)

// NewPrivateKeyManager creates a new [PrivateKeyManager].
func NewPrivateKeyManager(typeURL string, primitiveConstructor primitiveConstructor, keyMaterialType tinkpb.KeyData_KeyMaterialType, protoKeyUnmashaller func([]byte) (proto.Message, error)) *PrivateKeyManager {
	return &PrivateKeyManager{
		KeyManager: *New(typeURL, primitiveConstructor, keyMaterialType, protoKeyUnmashaller),
	}
}

// PublicKeyData extracts the public key as [tinkpb.KeyData] from the private
// key.
func (m *PrivateKeyManager) PublicKeyData(serializedPrivKey []byte) (*tinkpb.KeyData, error) {
	keySerialization, err := protoserialization.NewKeySerialization(&tinkpb.KeyData{
		TypeUrl:         m.typeURL,
		Value:           serializedPrivKey,
		KeyMaterialType: m.keyMaterialType,
	}, tinkpb.OutputPrefixType_RAW, 0)
	if err != nil {
		return nil, err
	}
	key, err := protoserialization.ParseKey(keySerialization)
	if err != nil {
		return nil, err
	}
	privateKey, ok := key.(privateKey)
	if !ok {
		return nil, fmt.Errorf("invalid key type: got %T, want a key implementing PublicKey() (key.Key, error)", key)
	}
	publicKey, err := privateKey.PublicKey()
	if err != nil {
		return nil, err
	}
	publicKeySerialization, err := protoserialization.SerializeKey(publicKey)
	if err != nil {
		return nil, err
	}
	return publicKeySerialization.KeyData(), nil
}
