// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/Lycense-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package jwthmac

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"

	jwthmacpb "github.com/tink-crypto/tink-go/v2/proto/jwt_hmac_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

type parametersSerializer struct{}

var _ protoserialization.ParametersSerializer = (*parametersSerializer)(nil)

func algorithmToProto(a Algorithm) jwthmacpb.JwtHmacAlgorithm {
	switch a {
	case HS256:
		return jwthmacpb.JwtHmacAlgorithm_HS256
	case HS384:
		return jwthmacpb.JwtHmacAlgorithm_HS384
	case HS512:
		return jwthmacpb.JwtHmacAlgorithm_HS512
	}
	return jwthmacpb.JwtHmacAlgorithm_HS_UNKNOWN
}

func algorithmFromProto(a jwthmacpb.JwtHmacAlgorithm) (Algorithm, error) {
	switch a {
	case jwthmacpb.JwtHmacAlgorithm_HS256:
		return HS256, nil
	case jwthmacpb.JwtHmacAlgorithm_HS384:
		return HS384, nil
	case jwthmacpb.JwtHmacAlgorithm_HS512:
		return HS512, nil
	}
	return UnknownAlgorithm, fmt.Errorf("unknown algorithm: %v", a)
}

func outputPrefixTypeFromKIDStrategy(s KIDStrategy) (tinkpb.OutputPrefixType, error) {
	switch s {
	case CustomKID:
		return tinkpb.OutputPrefixType_RAW, nil
	case IgnoredKID:
		return tinkpb.OutputPrefixType_RAW, nil
	case Base64EncodedKeyIDAsKID:
		return tinkpb.OutputPrefixType_TINK, nil
	}
	return tinkpb.OutputPrefixType_UNKNOWN_PREFIX, fmt.Errorf("unknown KID strategy: %v", s)
}

func kidStrategyFromOutputPrefixType(s tinkpb.OutputPrefixType, hasCustomKID bool) (KIDStrategy, error) {
	switch s {
	case tinkpb.OutputPrefixType_RAW:
		if hasCustomKID {
			return CustomKID, nil
		}
		return IgnoredKID, nil
	case tinkpb.OutputPrefixType_TINK:
		return Base64EncodedKeyIDAsKID, nil
	}
	return UnknownKIDStrategy, fmt.Errorf("unknown output prefix type: %v", s)
}

func (s *parametersSerializer) Serialize(p key.Parameters) (*tinkpb.KeyTemplate, error) {
	if p == nil {
		return nil, fmt.Errorf("parameters can't be nil")
	}
	params, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: got %T, want %T", p, (*Parameters)(nil))
	}
	keyFormat := &jwthmacpb.JwtHmacKeyFormat{
		Algorithm: algorithmToProto(params.Algorithm()),
		KeySize:   uint32(params.KeySizeInBytes()),
		Version:   0,
	}
	serializedKeyFormat, err := proto.Marshal(keyFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JwtHmacKeyFormat: %v", err)
	}
	outputPrefixType, err := outputPrefixTypeFromKIDStrategy(params.kidStrategy)
	if err != nil {
		return nil, err
	}
	return &tinkpb.KeyTemplate{
		TypeUrl:          keyTypeURL,
		Value:            serializedKeyFormat,
		OutputPrefixType: outputPrefixType,
	}, nil
}

type parametersParser struct{}

var _ protoserialization.ParametersParser = (*parametersParser)(nil)

func (s *parametersParser) Parse(kt *tinkpb.KeyTemplate) (key.Parameters, error) {
	if kt == nil {
		return nil, fmt.Errorf("key template can't be nil")
	}
	if kt.GetTypeUrl() != keyTypeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", kt.GetTypeUrl(), keyTypeURL)
	}
	keyFormat := &jwthmacpb.JwtHmacKeyFormat{}
	if err := proto.Unmarshal(kt.GetValue(), keyFormat); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtHmacKeyFormat: %v", err)
	}
	if keyFormat.GetVersion() != 0 {
		return nil, fmt.Errorf("invalid version: got %d, want 0", keyFormat.GetVersion())
	}
	kidStrategy, err := kidStrategyFromOutputPrefixType(kt.GetOutputPrefixType(), false)
	if err != nil {
		return nil, err
	}
	algorithm, err := algorithmFromProto(keyFormat.GetAlgorithm())
	if err != nil {
		return nil, err
	}
	return NewParameters(int(keyFormat.GetKeySize()), kidStrategy, algorithm)
}

type keySerializer struct{}

var _ protoserialization.KeySerializer = (*keySerializer)(nil)

func (s *keySerializer) SerializeKey(k key.Key) (*protoserialization.KeySerialization, error) {
	jwtHMACKey, ok := k.(*Key)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed *Key", k)
	}
	protoKey := &jwthmacpb.JwtHmacKey{
		Version:   0,
		Algorithm: algorithmToProto(jwtHMACKey.parameters.Algorithm()),
		KeyValue:  jwtHMACKey.keyBytes.Data(insecuresecretdataaccess.Token{}),
	}
	if jwtHMACKey.parameters.KIDStrategy() == CustomKID {
		kid, ok := jwtHMACKey.KID()
		if !ok {
			return nil, fmt.Errorf("custom_kid is not set")
		}
		protoKey.CustomKid = &jwthmacpb.JwtHmacKey_CustomKid{
			Value: kid,
		}
	}
	serializedKey, err := proto.Marshal(protoKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JwtHmacKey: %v", err)
	}
	outputPrefixType, err := outputPrefixTypeFromKIDStrategy(jwtHMACKey.parameters.KIDStrategy())
	if err != nil {
		return nil, err
	}
	idRequirement, _ := jwtHMACKey.IDRequirement()
	return protoserialization.NewKeySerialization(&tinkpb.KeyData{
		TypeUrl:         keyTypeURL,
		Value:           serializedKey,
		KeyMaterialType: tinkpb.KeyData_SYMMETRIC,
	}, outputPrefixType, idRequirement)
}

type keyParser struct{}

var _ protoserialization.KeyParser = (*keyParser)(nil)

func (s *keyParser) ParseKey(keySerialization *protoserialization.KeySerialization) (key.Key, error) {
	if keySerialization == nil {
		return nil, fmt.Errorf("key serialization can't be nil")
	}
	if keySerialization.KeyData().GetTypeUrl() != keyTypeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", keySerialization.KeyData().GetTypeUrl(), keyTypeURL)
	}
	if keySerialization.KeyData().GetKeyMaterialType() != tinkpb.KeyData_SYMMETRIC {
		return nil, fmt.Errorf("invalid key material type: got %v, want %v", keySerialization.KeyData().GetKeyMaterialType(), tinkpb.KeyData_SYMMETRIC)
	}

	keyProto := &jwthmacpb.JwtHmacKey{}
	if err := proto.Unmarshal(keySerialization.KeyData().GetValue(), keyProto); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtHmacKey: %v", err)
	}

	if keyProto.GetVersion() != 0 {
		return nil, fmt.Errorf("invalid key version: got %d, want 0", keyProto.GetVersion())
	}

	kidStrategy, err := kidStrategyFromOutputPrefixType(keySerialization.OutputPrefixType(), keyProto.GetCustomKid() != nil)
	if err != nil {
		return nil, err
	}
	algorithm, err := algorithmFromProto(keyProto.GetAlgorithm())
	if err != nil {
		return nil, err
	}
	params, err := NewParameters(len(keyProto.GetKeyValue()), kidStrategy, algorithm)
	if err != nil {
		return nil, err
	}
	keyID, _ := keySerialization.IDRequirement()

	opts := KeyOpts{
		Parameters:    params,
		KeyBytes:      secretdata.NewBytesFromData(keyProto.GetKeyValue(), insecuresecretdataaccess.Token{}),
		IDRequirement: keyID,
	}
	if keyProto.GetCustomKid() != nil {
		opts.HasCustomKID = true
		opts.CustomKID = keyProto.GetCustomKid().GetValue()
	}

	return NewKey(opts)
}
