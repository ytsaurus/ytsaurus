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

package prfbasedkeyderivation

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	prfderpb "github.com/tink-crypto/tink-go/v2/proto/prf_based_deriver_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

const (
	typeURL = "type.googleapis.com/google.crypto.tink.PrfBasedDeriverKey"
)

type keySerializer struct{}

var _ protoserialization.KeySerializer = (*keySerializer)(nil)

// SerializeKey converts a [prfbasedkeyderivation.Key] into its proto serialized form.
// It assumes that the input k is a *[prfbasedkeyderivation.Key] and that this
// struct provides a method like `toProto()` to access the underlying *prfderpb.PrfBasedDeriverKey.
func (s *keySerializer) SerializeKey(k key.Key) (*protoserialization.KeySerialization, error) {
	pbdKey, ok := k.(*Key)
	if !ok {
		return nil, fmt.Errorf("prfbasedkeyderivation: unexpected key type: got %T, want %T", k, (*Key)(nil))
	}

	derivedKeyTemplate, err := protoserialization.SerializeParameters(pbdKey.Parameters().(*Parameters).DerivedKeyParameters())
	if err != nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: failed to serialize parameters: %w", err)
	}

	serializedPrfKey, err := protoserialization.SerializeKey(pbdKey.PRFKey())
	if err != nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: failed to serialize PRF key: %w", err)
	}

	protoKey := &prfderpb.PrfBasedDeriverKey{
		Version: 0,
		PrfKey:  serializedPrfKey.KeyData(),
		Params: &prfderpb.PrfBasedDeriverParams{
			DerivedKeyTemplate: derivedKeyTemplate,
		},
	}

	serializedProtoKey, err := proto.Marshal(protoKey)
	if err != nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: failed to marshal key proto: %w", err)
	}

	idRequirement, _ := pbdKey.IDRequirement()

	keyData := &tinkpb.KeyData{
		TypeUrl:         typeURL,
		Value:           serializedProtoKey,
		KeyMaterialType: tinkpb.KeyData_SYMMETRIC, // PRF-based deriver keys are considered symmetric.
	}

	return protoserialization.NewKeySerialization(keyData, derivedKeyTemplate.GetOutputPrefixType(), idRequirement)
}

type keyParser struct{}

var _ protoserialization.KeyParser = (*keyParser)(nil)

// ParseKey converts a proto serialized key into a [prfbasedkeyderivation.Key] object.
func (p *keyParser) ParseKey(keySerialization *protoserialization.KeySerialization) (key.Key, error) {
	if keySerialization.KeyData().GetTypeUrl() != typeURL {
		return nil, fmt.Errorf("prfbasedkeyderivation: unexpected type URL: got %q, want %q", keySerialization.KeyData().GetTypeUrl(), typeURL)
	}
	if keySerialization.KeyData().GetKeyMaterialType() != tinkpb.KeyData_SYMMETRIC {
		return nil, fmt.Errorf("prfbasedkeyderivation: invalid key material type: got %v, want %v", keySerialization.KeyData().GetKeyMaterialType(), tinkpb.KeyData_SYMMETRIC)
	}

	protoKey := new(prfderpb.PrfBasedDeriverKey)
	if err := proto.Unmarshal(keySerialization.KeyData().GetValue(), protoKey); err != nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: failed to unmarshal key proto: %w", err)
	}
	// Version check for the key proto itself.
	if protoKey.GetVersion() != 0 {
		return nil, fmt.Errorf("prfbasedkeyderivation: unsupported key version %d", protoKey.GetVersion())
	}
	if protoKey.GetParams().GetDerivedKeyTemplate().GetOutputPrefixType() != keySerialization.OutputPrefixType() {
		return nil, fmt.Errorf("prfbasedkeyderivation: inconsistent output prefix type: got %v, want %v", protoKey.GetParams().GetDerivedKeyTemplate().GetOutputPrefixType(), keySerialization.OutputPrefixType())
	}

	derivedKeyParameters, err := protoserialization.ParseParameters(protoKey.GetParams().GetDerivedKeyTemplate())
	if err != nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: failed to parse derived key parameters: %w", err)
	}

	prfKeyProtoSerialization, err := protoserialization.NewKeySerialization(protoKey.GetPrfKey(), tinkpb.OutputPrefixType_RAW, 0)
	if err != nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: failed to create PRF key proto serialization: %w", err)
	}
	prfKey, err := protoserialization.ParseKey(prfKeyProtoSerialization)
	if err != nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: failed to parse PRF key: %w", err)
	}

	params, err := NewParameters(prfKey.Parameters(), derivedKeyParameters)
	if err != nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: failed to create parameters: %w", err)
	}
	keyID, _ := keySerialization.IDRequirement()
	return NewKey(params, prfKey, keyID)
}

type parametersSerializer struct{}

var _ protoserialization.ParametersSerializer = (*parametersSerializer)(nil)

// Serialize converts a [prfbasedkeyderivation.Parameters] into its proto serialized form.
func (s *parametersSerializer) Serialize(params key.Parameters) (*tinkpb.KeyTemplate, error) {
	p, ok := params.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("unexpected parameters type: got %T, want %T", params, (*Parameters)(nil))
	}
	prfKeyTemplate, err := protoserialization.SerializeParameters(p.prfParameters)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize prf parameters: %w", err)
	}
	derivedKeyTemplate, err := protoserialization.SerializeParameters(p.derivedKeyParameters)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize derived key parameters: %w", err)
	}

	format := &prfderpb.PrfBasedDeriverKeyFormat{
		PrfKeyTemplate: prfKeyTemplate,
		Params: &prfderpb.PrfBasedDeriverParams{
			DerivedKeyTemplate: derivedKeyTemplate,
		},
	}

	serializedFormat, err := proto.Marshal(format)
	if err != nil {
		return nil, fmt.Errorf("prfbasedkeyderivation: failed to marshal parameters proto: %w", err)
	}
	return &tinkpb.KeyTemplate{
		TypeUrl:          typeURL,
		Value:            serializedFormat,
		OutputPrefixType: derivedKeyTemplate.GetOutputPrefixType(),
	}, nil
}

type parametersParser struct{}

var _ protoserialization.ParametersParser = (*parametersParser)(nil)

// Parse converts a proto serialized parameters into a [prfbasedkeyderivation.Parameters] object.
func (p *parametersParser) Parse(keyTemplate *tinkpb.KeyTemplate) (key.Parameters, error) {
	if keyTemplate.GetTypeUrl() != typeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", keyTemplate.GetTypeUrl(), typeURL)
	}

	format := new(prfderpb.PrfBasedDeriverKeyFormat)
	if err := proto.Unmarshal(keyTemplate.GetValue(), format); err != nil {
		return nil, fmt.Errorf("failed to unmarshal parameters proto: %w", err)
	}

	// Force the output prefix type to be consistent with the one of the derived
	// key template.
	if got, want := keyTemplate.GetOutputPrefixType(), format.GetParams().GetDerivedKeyTemplate().GetOutputPrefixType(); got != want {
		return nil, fmt.Errorf("invalid output prefix type for the key derivation key template: got %v, want %v", got, want)
	}

	prfParams, err := protoserialization.ParseParameters(format.GetPrfKeyTemplate())
	if err != nil {
		return nil, fmt.Errorf("failed to parse prf parameters: %w", err)
	}
	derivedKeyParams, err := protoserialization.ParseParameters(format.GetParams().GetDerivedKeyTemplate())
	if err != nil {
		return nil, fmt.Errorf("failed to parse derived key parameters: %w", err)
	}
	return NewParameters(prfParams, derivedKeyParams)
}
