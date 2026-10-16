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

// Package protoserialization defines interfaces for proto key to key objects parsers, and provides
// a global registry that maps key type URLs to key parsers. The package also provides a fallback
// proto key struct that wraps a proto keyset key.
package protoserialization

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/internal/outputprefix"
	"github.com/tink-crypto/tink-go/v2/key"

	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

var (
	keyParsersMu            sync.RWMutex
	keyParsers              = make(map[string]KeyParser) // TypeURL -> KeyParser
	keySerializersMu        sync.RWMutex
	keySerializers          = make(map[reflect.Type]KeySerializer) // KeyType -> KeySerializer
	parametersSerializersMu sync.RWMutex
	parameterSerializers    = make(map[reflect.Type]ParametersSerializer) // ParameterType -> ParametersSerializer
	parametersParsersMu     sync.RWMutex
	parameterParsers        = make(map[string]ParametersParser) // TypeURL -> ParametersParser
)

type fallbackProtoKeyParams struct {
	hasIDRequirement bool
}

func (p *fallbackProtoKeyParams) HasIDRequirement() bool { return p.hasIDRequirement }

func (p *fallbackProtoKeyParams) Equal(parameters key.Parameters) bool {
	_, ok := parameters.(*fallbackProtoKeyParams)
	return ok && p.hasIDRequirement == parameters.HasIDRequirement()
}

// KeySerialization represents a Protobuf serialization of a [key.Key].
type KeySerialization struct {
	keyData          *tinkpb.KeyData
	outputPrefixType tinkpb.OutputPrefixType
	idRequirement    uint32
}

// NewKeySerialization creates a new KeySerialization.
//
// idRequirement must be zero if outputPrefixType is RAW.
func NewKeySerialization(keyData *tinkpb.KeyData, outputPrefixType tinkpb.OutputPrefixType, idRequirement uint32) (*KeySerialization, error) {
	if outputPrefixType == tinkpb.OutputPrefixType_RAW && idRequirement != 0 {
		return nil, fmt.Errorf("idRequirement must be zero if hasIDRequirement is false")
	}
	return &KeySerialization{
		keyData:          keyData,
		outputPrefixType: outputPrefixType,
		idRequirement:    idRequirement,
	}, nil
}

// KeyData returns the proto key data.
func (k *KeySerialization) KeyData() *tinkpb.KeyData { return k.keyData }

// OutputPrefixType returns the output prefix type of the key.
func (k *KeySerialization) OutputPrefixType() tinkpb.OutputPrefixType { return k.outputPrefixType }

// HasIDRequirement returns whether the key has an ID requirement.
func (k *KeySerialization) HasIDRequirement() bool {
	return k.OutputPrefixType() != tinkpb.OutputPrefixType_RAW
}

// IDRequirement returns the key ID and whether it is required.
//
// If the key ID is not required, the returned ID is zero.
func (k *KeySerialization) IDRequirement() (uint32, bool) {
	return k.idRequirement, k.HasIDRequirement()
}

// Equal reports whether k is equal to other.
func (k *KeySerialization) Equal(other *KeySerialization) bool {
	return proto.Equal(k.keyData, other.keyData) &&
		k.outputPrefixType == other.outputPrefixType &&
		k.idRequirement == other.idRequirement
}

// clone returns a copy of the key serialization.
func (k *KeySerialization) clone() *KeySerialization {
	return &KeySerialization{
		keyData:          proto.Clone(k.keyData).(*tinkpb.KeyData),
		outputPrefixType: k.outputPrefixType,
		idRequirement:    k.idRequirement,
	}
}

// FallbackProtoKey is a key that wraps a proto key serialization.
//
// FallbackProtoKey implements the [key.Key] interface. This is a fallback key
// type that is used to wrap individual keyset keys when no concrete key type
// is available; it is purposely internal and does not allow accessing the
// internal proto representation to avoid premature use of this type.
type FallbackProtoKey struct {
	protoKeySerialization *KeySerialization
	parameters            *fallbackProtoKeyParams
	outputPrefix          []byte
}

// Parameters returns the parameters of this key.
func (k *FallbackProtoKey) Parameters() key.Parameters { return k.parameters }

// Equal reports whether k is equal to other.
func (k *FallbackProtoKey) Equal(other key.Key) bool {
	otherFallbackProtoKey, ok := other.(*FallbackProtoKey)
	return ok && k.parameters.Equal(other.Parameters()) &&
		k.protoKeySerialization.Equal(otherFallbackProtoKey.protoKeySerialization)
}

// IDRequirement returns the key ID and whether it is required.
func (k *FallbackProtoKey) IDRequirement() (uint32, bool) {
	return k.protoKeySerialization.IDRequirement()
}

// OutputPrefix returns the output prefix of the key.
func (k *FallbackProtoKey) OutputPrefix() []byte { return bytes.Clone(k.outputPrefix) }

// calculateOutputPrefix calculates the output prefix from keyID.
func calculateOutputPrefix(outputPrefixType tinkpb.OutputPrefixType, keyID uint32) ([]byte, error) {
	switch outputPrefixType {
	case tinkpb.OutputPrefixType_TINK:
		return outputprefix.Tink(keyID), nil
	case tinkpb.OutputPrefixType_LEGACY, tinkpb.OutputPrefixType_CRUNCHY:
		return outputprefix.Legacy(keyID), nil
	case tinkpb.OutputPrefixType_RAW:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown output prefix type: %v", outputPrefixType)
	}
}

// NewFallbackProtoKey creates a new FallbackProtoKey.
func NewFallbackProtoKey(protoKeySerialization *KeySerialization) (*FallbackProtoKey, error) {
	outputPrefix, err := calculateOutputPrefix(protoKeySerialization.OutputPrefixType(), protoKeySerialization.idRequirement)
	if err != nil {
		return nil, err
	}
	return &FallbackProtoKey{
		protoKeySerialization: protoKeySerialization,
		parameters: &fallbackProtoKeyParams{
			hasIDRequirement: protoKeySerialization.HasIDRequirement(),
		},
		outputPrefix: outputPrefix,
	}, nil
}

// FallbackProtoPrivateKey represents a fallback private key that wraps a proto
// keyset key.
type FallbackProtoPrivateKey struct {
	FallbackProtoKey
}

var _ key.Key = (*FallbackProtoPrivateKey)(nil)

// Equal returns whether k is equal to other.
func (k *FallbackProtoPrivateKey) Equal(other key.Key) bool {
	that, ok := other.(*FallbackProtoPrivateKey)
	return ok && k.FallbackProtoKey.Equal(&that.FallbackProtoKey)
}

// NewFallbackProtoPrivateKey creates a new FallbackProtoPrivateKey.
func NewFallbackProtoPrivateKey(protoKeySerialization *KeySerialization) (*FallbackProtoPrivateKey, error) {
	if protoKeySerialization.KeyData().GetKeyMaterialType() != tinkpb.KeyData_ASYMMETRIC_PRIVATE {
		return nil, fmt.Errorf("the key is not a private key")
	}
	fallbackProtoKey, err := NewFallbackProtoKey(protoKeySerialization)
	if err != nil {
		return nil, err
	}
	return &FallbackProtoPrivateKey{
		FallbackProtoKey: *fallbackProtoKey,
	}, nil
}

// PublicKey returns the public key of the private key.
func (k *FallbackProtoPrivateKey) PublicKey() (key.Key, error) {
	privKeyData := k.protoKeySerialization.KeyData()
	keyManager, err := registry.GetKeyManager(privKeyData.GetTypeUrl())
	if err != nil {
		return nil, err
	}
	privateKeyManager, ok := keyManager.(registry.PrivateKeyManager)
	if !ok {
		return nil, fmt.Errorf("%s does not correspond to a PrivateKeyManager", privKeyData.GetTypeUrl())
	}
	publicKeyData, err := privateKeyManager.PublicKeyData(privKeyData.GetValue())
	if err != nil {
		return nil, err
	}
	// idRequirement is zero if the key doesn't have a key requirement.
	idRequirement, _ := k.protoKeySerialization.IDRequirement()
	keySerialization, err := NewKeySerialization(publicKeyData, k.protoKeySerialization.OutputPrefixType(), idRequirement)
	if err != nil {
		return nil, err
	}
	return ParseKey(keySerialization)
}

// GetKeySerialization returns the fallbackProtoKey's proto key serialization.
func GetKeySerialization(fallbackProtoKey *FallbackProtoKey) *KeySerialization {
	return fallbackProtoKey.protoKeySerialization
}

// KeyParser is an interface for parsing a key serialization into a key.
type KeyParser interface {
	// ParseKey parses the given key serialization into a key.
	ParseKey(keysetKey *KeySerialization) (key.Key, error)
}

// KeySerializer is an interface for serializing a key into a proto key
// serialization.
type KeySerializer interface {
	// SerializeKey serializes the given key into a proto key serialization.
	SerializeKey(key key.Key) (*KeySerialization, error)
}

// ParametersSerializer is an interface for serializing [key.Parameters] into
// [*tinkpb.KeyTemplate].
type ParametersSerializer interface {
	// Serialize serializes the given parameters into a proto key template.
	Serialize(parameters key.Parameters) (*tinkpb.KeyTemplate, error)
}

// ParametersParser is an interface for parsing key templates into a
// [key.Parameters].
type ParametersParser interface {
	// Parse parses the given [*tinkpb.KeyTemplate] into a [key.Parameters].
	Parse(template *tinkpb.KeyTemplate) (key.Parameters, error)
}

// RegisterKeySerializer registers the given key serializer for keys of type K.
//
// It doesn't allow replacing existing serializers.
func RegisterKeySerializer[K key.Key](keySerializer KeySerializer) error {
	keySerializersMu.Lock()
	defer keySerializersMu.Unlock()
	keyType := reflect.TypeFor[K]()
	if _, found := keySerializers[keyType]; found {
		return fmt.Errorf("protoserialization.RegisterKeySerializer: type %v already registered", keyType)
	}
	keySerializers[keyType] = keySerializer
	return nil
}

// RegisterParametersSerializer registers the given parameter serializer for
// parameters of type P.
//
// It doesn't allow replacing existing serializers.
func RegisterParametersSerializer[P key.Parameters](parameterSerializer ParametersSerializer) error {
	parametersSerializersMu.Lock()
	defer parametersSerializersMu.Unlock()
	parameterType := reflect.TypeOf((*P)(nil)).Elem()
	if _, found := parameterSerializers[parameterType]; found {
		return fmt.Errorf("protoserialization.RegisterParametersSerializer: type %v already registered", parameterType)
	}
	parameterSerializers[parameterType] = parameterSerializer
	return nil
}

// RegisterParametersParser registers the given parameter parser for
// a given type URL.
//
// It doesn't allow replacing existing serializers.
func RegisterParametersParser(keyTypeURL string, parameterParser ParametersParser) error {
	parametersParsersMu.Lock()
	defer parametersParsersMu.Unlock()
	if _, found := parameterParsers[keyTypeURL]; found {
		return fmt.Errorf("protoserialization.RegisterParametersParser: type %v already registered", keyTypeURL)
	}
	parameterParsers[keyTypeURL] = parameterParser
	return nil
}

// SerializeKey serializes the given key into a proto keyset key.
func SerializeKey(key key.Key) (*KeySerialization, error) {
	keyType := reflect.TypeOf(key)
	serializer, ok := keySerializers[keyType]
	if !ok {
		return nil, fmt.Errorf("protoserialization.SerializeKey: no serializer for type %v", keyType)
	}
	return serializer.SerializeKey(key)
}

// SerializeParameters serializes the given parameters into a proto key template.
func SerializeParameters(parameters key.Parameters) (*tinkpb.KeyTemplate, error) {
	if parameters == nil {
		return nil, fmt.Errorf("protoserialization.SerializeParameters: parameters is nil")
	}
	parametersType := reflect.TypeOf(parameters)
	serializer, ok := parameterSerializers[parametersType]
	if !ok {
		return nil, fmt.Errorf("protoserialization.SerializeParameters: no serializer for type %v", parametersType)
	}
	return serializer.Serialize(parameters)
}

// RegisterKeyParser registers the given key parser.
//
// It doesn't allow replacing existing parsers.
func RegisterKeyParser(keyTypeURL string, keyParser KeyParser) error {
	keyParsersMu.Lock()
	defer keyParsersMu.Unlock()
	if _, found := keyParsers[keyTypeURL]; found {
		return fmt.Errorf("protoserialization.RegisterKeyParser: type %s already registered", keyTypeURL)
	}
	keyParsers[keyTypeURL] = keyParser
	return nil
}

// ParseKey parses the given keyset key into a key.
//
// If no parser is registered for the given type URL, a fallback key is returned.
func ParseKey(keySerialization *KeySerialization) (key.Key, error) {
	parser, found := keyParsers[keySerialization.KeyData().GetTypeUrl()]
	if !found {
		if keySerialization.KeyData().GetKeyMaterialType() == tinkpb.KeyData_ASYMMETRIC_PRIVATE {
			return NewFallbackProtoPrivateKey(keySerialization)
		}
		return NewFallbackProtoKey(keySerialization)
	}
	return parser.ParseKey(keySerialization)
}

// ParseParameters parses the given keyset key into a [key.Parameters].
//
// If no parser is registered for the given type URL, returns an error.
func ParseParameters(keyTemplate *tinkpb.KeyTemplate) (key.Parameters, error) {
	parser, found := parameterParsers[keyTemplate.GetTypeUrl()]
	if !found {
		return nil, fmt.Errorf("protoserialization.ParseParameters: no parser for type %s", keyTemplate.GetTypeUrl())
	}
	return parser.Parse(keyTemplate)
}

type fallbackProtoKeySerializer struct{}

func (s *fallbackProtoKeySerializer) SerializeKey(key key.Key) (*KeySerialization, error) {
	fallbackKey, ok := key.(*FallbackProtoKey)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed *FallbackProtoKey", fallbackKey)
	}
	// Make a copy of the proto key serialization. This is to avoid the caller
	// modifying the key data field of the key.
	return fallbackKey.protoKeySerialization.clone(), nil
}

type fallbackProtoPrivateKeySerializer struct{}

func (s *fallbackProtoPrivateKeySerializer) SerializeKey(key key.Key) (*KeySerialization, error) {
	fallbackKey, ok := key.(*FallbackProtoPrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed *FallbackProtoPrivateKey", fallbackKey)
	}
	// Make a copy of the proto key serialization. This is to avoid the caller
	// modifying the key data field of the key.
	return fallbackKey.protoKeySerialization.clone(), nil
}

// UnregisterKeyParser removes the key parser for the given type URL from the
// global key parsers registry.
//
// This function is intended to be used in tests only.
func UnregisterKeyParser(keyTypeURL string) {
	keyParsersMu.Lock()
	defer keyParsersMu.Unlock()
	delete(keyParsers, keyTypeURL)
}

// UnregisterKeySerializer removes the serializer for the given key type from
// the global registry. If no serializer is registered for the given type, this
// function does nothing.
//
// This function is intended to be used in tests only.
func UnregisterKeySerializer[K key.Key]() {
	keySerializersMu.Lock()
	defer keySerializersMu.Unlock()
	keyType := reflect.TypeFor[K]()
	delete(keySerializers, keyType)
}

// ClearParametersSerializers clears the global parameters serializers registry.
//
// This function is intended to be used in tests only.
func ClearParametersSerializers() {
	parametersSerializersMu.Lock()
	defer parametersSerializersMu.Unlock()
	clear(parameterSerializers)
}

// UnregisterParametersParser removes the parameters parser for the given type
// URL from the global registry.
//
// This function is intended to be used in tests only.
func UnregisterParametersParser(keyTypeURL string) {
	parametersParsersMu.Lock()
	defer parametersParsersMu.Unlock()
	delete(parameterParsers, keyTypeURL)
}

func init() {
	RegisterKeySerializer[*FallbackProtoKey](&fallbackProtoKeySerializer{})
	RegisterKeySerializer[*FallbackProtoPrivateKey](&fallbackProtoPrivateKeySerializer{})
}
