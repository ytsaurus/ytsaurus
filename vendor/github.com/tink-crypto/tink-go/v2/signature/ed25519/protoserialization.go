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

package ed25519

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	ed25519pb "github.com/tink-crypto/tink-go/v2/proto/ed25519_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

const (
	// publicKeyProtoVersion is the accepted [ed25519pb.Ed25519PublicKey] proto
	// version.
	//
	// Currently, only version 0 is supported; other versions are rejected.
	publicKeyProtoVersion = 0
	// privateKeyProtoVersion is the accepted [ed25519pb.Ed25519PrivateKey] proto
	// version.
	//
	// Currently, only version 0 is supported; other versions are rejected.
	privateKeyProtoVersion = 0

	signerTypeURL   = "type.googleapis.com/google.crypto.tink.Ed25519PrivateKey"
	verifierTypeURL = "type.googleapis.com/google.crypto.tink.Ed25519PublicKey"
)

type publicKeySerializer struct{}

var _ protoserialization.KeySerializer = (*publicKeySerializer)(nil)

func protoOutputPrefixTypeFromVariant(variant Variant) (tinkpb.OutputPrefixType, error) {
	switch variant {
	case VariantTink:
		return tinkpb.OutputPrefixType_TINK, nil
	case VariantCrunchy:
		return tinkpb.OutputPrefixType_CRUNCHY, nil
	case VariantLegacy:
		return tinkpb.OutputPrefixType_LEGACY, nil
	case VariantNoPrefix:
		return tinkpb.OutputPrefixType_RAW, nil
	default:
		return tinkpb.OutputPrefixType_UNKNOWN_PREFIX, fmt.Errorf("unknown output prefix variant: %v", variant)
	}
}

func (s *publicKeySerializer) SerializeKey(key key.Key) (*protoserialization.KeySerialization, error) {
	ed25519PubKey, ok := key.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("invalid key type: %T, want *ed25519.PublicKey", key)
	}
	outputPrefixType, err := protoOutputPrefixTypeFromVariant(ed25519PubKey.params.Variant())
	if err != nil {
		return nil, err
	}
	protoKey := &ed25519pb.Ed25519PublicKey{
		KeyValue: ed25519PubKey.KeyBytes(),
		Version:  publicKeyProtoVersion,
	}
	serializedKey, err := proto.Marshal(protoKey)
	if err != nil {
		return nil, err
	}
	// idRequirement is zero if the key doesn't have a key requirement.
	idRequirement, _ := ed25519PubKey.IDRequirement()
	keyData := &tinkpb.KeyData{
		TypeUrl:         verifierTypeURL,
		Value:           serializedKey,
		KeyMaterialType: tinkpb.KeyData_ASYMMETRIC_PUBLIC,
	}
	return protoserialization.NewKeySerialization(keyData, outputPrefixType, idRequirement)
}

type privateKeySerializer struct{}

var _ protoserialization.KeySerializer = (*privateKeySerializer)(nil)

func (s *privateKeySerializer) SerializeKey(key key.Key) (*protoserialization.KeySerialization, error) {
	ed25519PrivKey, ok := key.(*PrivateKey)
	if !ok {
		return nil, fmt.Errorf("invalid key type: %T, want *ed25519.PrivateKey", key)
	}
	if ed25519PrivKey.publicKey == nil {
		return nil, fmt.Errorf("invalid key: public key is nil")
	}
	params := ed25519PrivKey.publicKey.params
	outputPrefixType, err := protoOutputPrefixTypeFromVariant(params.Variant())
	if err != nil {
		return nil, err
	}
	protoKey := &ed25519pb.Ed25519PrivateKey{
		KeyValue: ed25519PrivKey.PrivateKeyBytes().Data(insecuresecretdataaccess.Token{}),
		PublicKey: &ed25519pb.Ed25519PublicKey{
			KeyValue: ed25519PrivKey.publicKey.KeyBytes(),
			Version:  publicKeyProtoVersion,
		},
		Version: privateKeyProtoVersion,
	}
	serializedKey, err := proto.Marshal(protoKey)
	if err != nil {
		return nil, err
	}
	// idRequirement is zero if the key doesn't have a key requirement.
	idRequirement, _ := ed25519PrivKey.IDRequirement()
	keyData := &tinkpb.KeyData{
		TypeUrl:         signerTypeURL,
		Value:           serializedKey,
		KeyMaterialType: tinkpb.KeyData_ASYMMETRIC_PRIVATE,
	}
	return protoserialization.NewKeySerialization(keyData, outputPrefixType, idRequirement)
}

type publicKeyParser struct{}

var _ protoserialization.KeyParser = (*publicKeyParser)(nil)

func variantFromProto(prefixType tinkpb.OutputPrefixType) (Variant, error) {
	switch prefixType {
	case tinkpb.OutputPrefixType_TINK:
		return VariantTink, nil
	case tinkpb.OutputPrefixType_CRUNCHY:
		return VariantCrunchy, nil
	case tinkpb.OutputPrefixType_LEGACY:
		return VariantLegacy, nil
	case tinkpb.OutputPrefixType_RAW:
		return VariantNoPrefix, nil
	default:
		return VariantUnknown, fmt.Errorf("unsupported output prefix type: %v", prefixType)
	}
}

func (s *publicKeyParser) ParseKey(keySerialization *protoserialization.KeySerialization) (key.Key, error) {
	if keySerialization == nil {
		return nil, fmt.Errorf("key serialization is nil")
	}
	keyData := keySerialization.KeyData()
	if keyData.GetTypeUrl() != verifierTypeURL {
		return nil, fmt.Errorf("invalid key type URL: %v", keyData.GetTypeUrl())
	}
	if keyData.GetKeyMaterialType() != tinkpb.KeyData_ASYMMETRIC_PUBLIC {
		return nil, fmt.Errorf("invalid key material type: %v", keyData.GetKeyMaterialType())
	}
	protoKey := new(ed25519pb.Ed25519PublicKey)
	if err := proto.Unmarshal(keyData.GetValue(), protoKey); err != nil {
		return nil, err
	}
	if protoKey.GetVersion() != publicKeyProtoVersion {
		return nil, fmt.Errorf("public key has unsupported version: %v", protoKey.GetVersion())
	}
	variant, err := variantFromProto(keySerialization.OutputPrefixType())
	if err != nil {
		return nil, err
	}
	params, err := NewParameters(variant)
	if err != nil {
		return nil, err
	}
	// keySerialization.IDRequirement() returns zero if the key doesn't have a key requirement.
	keyID, _ := keySerialization.IDRequirement()
	return NewPublicKey(protoKey.GetKeyValue(), keyID, params)
}

type privateKeyParser struct{}

var _ protoserialization.KeyParser = (*privateKeyParser)(nil)

func (s *privateKeyParser) ParseKey(keySerialization *protoserialization.KeySerialization) (key.Key, error) {
	if keySerialization == nil {
		return nil, fmt.Errorf("key serialization is nil")
	}
	keyData := keySerialization.KeyData()
	if keyData.GetTypeUrl() != signerTypeURL {
		return nil, fmt.Errorf("invalid key type URL: %v", keyData.GetTypeUrl())
	}
	if keyData.GetKeyMaterialType() != tinkpb.KeyData_ASYMMETRIC_PRIVATE {
		return nil, fmt.Errorf("invalid key material type: %v", keyData.GetKeyMaterialType())
	}
	protoKey := new(ed25519pb.Ed25519PrivateKey)
	if err := proto.Unmarshal(keyData.GetValue(), protoKey); err != nil {
		return nil, err
	}
	if protoKey.GetVersion() != privateKeyProtoVersion {
		return nil, fmt.Errorf("private key has unsupported version: %v", protoKey.GetVersion())
	}
	variant, err := variantFromProto(keySerialization.OutputPrefixType())
	if err != nil {
		return nil, err
	}
	params, err := NewParameters(variant)
	if err != nil {
		return nil, err
	}
	if protoKey.GetPublicKey().GetVersion() != publicKeyProtoVersion {
		return nil, fmt.Errorf("public key has unsupported version: %v", protoKey.GetPublicKey().GetVersion())
	}
	// keySerialization.IDRequirement() returns zero if the key doesn't have a key requirement.
	keyID, _ := keySerialization.IDRequirement()
	publicKey, err := NewPublicKey(protoKey.GetPublicKey().GetKeyValue(), keyID, params)
	if err != nil {
		return nil, err
	}
	privateKeyBytes := secretdata.NewBytesFromData(protoKey.GetKeyValue(), insecuresecretdataaccess.Token{})
	return NewPrivateKeyWithPublicKey(privateKeyBytes, publicKey)
}

type parametersSerializer struct{}

var _ protoserialization.ParametersSerializer = (*parametersSerializer)(nil)

func (s *parametersSerializer) Serialize(parameters key.Parameters) (*tinkpb.KeyTemplate, error) {
	ed25519Parameters, ok := parameters.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: got %T, want *ed25519.Parameters", parameters)
	}
	outputPrefixType, err := protoOutputPrefixTypeFromVariant(ed25519Parameters.Variant())
	if err != nil {
		return nil, err
	}
	format := &ed25519pb.Ed25519KeyFormat{
		Version: 0,
	}
	serializedFormat, err := proto.Marshal(format)
	if err != nil {
		return nil, err
	}
	return &tinkpb.KeyTemplate{
		TypeUrl:          signerTypeURL,
		OutputPrefixType: outputPrefixType,
		Value:            serializedFormat,
	}, nil
}

type parametersParser struct{}

var _ protoserialization.ParametersParser = (*parametersParser)(nil)

func (s *parametersParser) Parse(keyTemplate *tinkpb.KeyTemplate) (key.Parameters, error) {
	if keyTemplate.GetTypeUrl() != signerTypeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", keyTemplate.GetTypeUrl(), signerTypeURL)
	}
	format := new(ed25519pb.Ed25519KeyFormat)
	if err := proto.Unmarshal(keyTemplate.GetValue(), format); err != nil {
		return nil, err
	}

	if format.GetVersion() != 0 {
		return nil, fmt.Errorf("unsupported key version: got %d, want %d", format.GetVersion(), 0)
	}

	variant, err := variantFromProto(keyTemplate.GetOutputPrefixType())
	if err != nil {
		return nil, err
	}
	params, err := NewParameters(variant)
	if err != nil {
		return nil, err
	}
	return &params, nil
}
