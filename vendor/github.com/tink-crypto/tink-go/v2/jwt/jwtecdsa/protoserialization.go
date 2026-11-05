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

package jwtecdsa

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/ec"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"

	jwtecdsapb "github.com/tink-crypto/tink-go/v2/proto/jwt_ecdsa_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

const (
	privateKeyTypeURL = "type.googleapis.com/google.crypto.tink.JwtEcdsaPrivateKey"
	publicKeyTypeURL  = "type.googleapis.com/google.crypto.tink.JwtEcdsaPublicKey"
)

type parametersSerializer struct{}

var _ protoserialization.ParametersSerializer = (*parametersSerializer)(nil)

func algorithmToProto(a Algorithm) jwtecdsapb.JwtEcdsaAlgorithm {
	switch a {
	case ES256:
		return jwtecdsapb.JwtEcdsaAlgorithm_ES256
	case ES384:
		return jwtecdsapb.JwtEcdsaAlgorithm_ES384
	case ES512:
		return jwtecdsapb.JwtEcdsaAlgorithm_ES512
	}
	return jwtecdsapb.JwtEcdsaAlgorithm_ES_UNKNOWN
}

func algorithmFromProto(a jwtecdsapb.JwtEcdsaAlgorithm) Algorithm {
	switch a {
	case jwtecdsapb.JwtEcdsaAlgorithm_ES256:
		return ES256
	case jwtecdsapb.JwtEcdsaAlgorithm_ES384:
		return ES384
	case jwtecdsapb.JwtEcdsaAlgorithm_ES512:
		return ES512
	}
	return UnknownAlgorithm
}

func outputPrefixTypeFromKIDStrategy(s KIDStrategy) tinkpb.OutputPrefixType {
	switch s {
	case CustomKID:
		return tinkpb.OutputPrefixType_RAW
	case IgnoredKID:
		return tinkpb.OutputPrefixType_RAW
	case Base64EncodedKeyIDAsKID:
		return tinkpb.OutputPrefixType_TINK
	}
	return tinkpb.OutputPrefixType_UNKNOWN_PREFIX
}

func kidStrategyFromOutputPrefixType(s tinkpb.OutputPrefixType, hasCustomKID bool) KIDStrategy {
	switch s {
	case tinkpb.OutputPrefixType_RAW:
		if hasCustomKID {
			return CustomKID
		}
		return IgnoredKID
	case tinkpb.OutputPrefixType_TINK:
		return Base64EncodedKeyIDAsKID
	}
	return UnknownKIDStrategy
}

func (s *parametersSerializer) Serialize(p key.Parameters) (*tinkpb.KeyTemplate, error) {
	if p == nil {
		return nil, fmt.Errorf("parameters can't be nil")
	}
	params, ok := p.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: got %T, want %T", p, (*Parameters)(nil))
	}
	keyFormat := &jwtecdsapb.JwtEcdsaKeyFormat{
		Algorithm: algorithmToProto(params.Algorithm()),
		Version:   0,
	}
	serializedKeyFormat, err := proto.Marshal(keyFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JwtEcdsaKeyFormat: %v", err)
	}
	return &tinkpb.KeyTemplate{
		TypeUrl:          privateKeyTypeURL,
		Value:            serializedKeyFormat,
		OutputPrefixType: outputPrefixTypeFromKIDStrategy(params.kidStrategy),
	}, nil
}

type parametersParser struct{}

var _ protoserialization.ParametersParser = (*parametersParser)(nil)

func (s *parametersParser) Parse(kt *tinkpb.KeyTemplate) (key.Parameters, error) {
	if kt == nil {
		return nil, fmt.Errorf("key template can't be nil")
	}
	if kt.GetTypeUrl() != privateKeyTypeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", kt.GetTypeUrl(), privateKeyTypeURL)
	}
	keyFormat := &jwtecdsapb.JwtEcdsaKeyFormat{}
	if err := proto.Unmarshal(kt.GetValue(), keyFormat); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtEcdsaKeyFormat: %v", err)
	}
	if keyFormat.GetVersion() != 0 {
		return nil, fmt.Errorf("invalid version: got %d, want 0", keyFormat.GetVersion())
	}
	kidStrategy := kidStrategyFromOutputPrefixType(kt.GetOutputPrefixType(), false)
	return NewParameters(kidStrategy, algorithmFromProto(keyFormat.GetAlgorithm()))
}

func publicKeyToProto(k *PublicKey) (*jwtecdsapb.JwtEcdsaPublicKey, error) {
	xy := k.PublicPoint()[1:]
	coordinateSize, err := coordinateSizeFromAlgorithm(k.parameters.Algorithm())
	if err != nil {
		return nil, err
	}
	paddedX, err := ec.BigIntBytesToFixedSizeBuffer(xy[:coordinateSize], coordinateSize+1)
	if err != nil {
		return nil, err
	}
	paddedY, err := ec.BigIntBytesToFixedSizeBuffer(xy[coordinateSize:], coordinateSize+1)
	if err != nil {
		return nil, err
	}
	protoPublicKey := &jwtecdsapb.JwtEcdsaPublicKey{
		Version:   0,
		Algorithm: algorithmToProto(k.parameters.Algorithm()),
		X:         paddedX,
		Y:         paddedY,
	}
	if k.parameters.KIDStrategy() == CustomKID {
		protoPublicKey.CustomKid = &jwtecdsapb.JwtEcdsaPublicKey_CustomKid{
			Value: k.kid,
		}
	}
	return protoPublicKey, nil
}

type publicKeySerializer struct{}

var _ protoserialization.KeySerializer = (*publicKeySerializer)(nil)

func coordinateSizeFromAlgorithm(a Algorithm) (int, error) {
	switch a {
	case ES256:
		return 32, nil
	case ES384:
		return 48, nil
	case ES512:
		return 66, nil
	}
	return 0, fmt.Errorf("unknown algorithm: %v", a)
}

func (s *publicKeySerializer) SerializeKey(key key.Key) (*protoserialization.KeySerialization, error) {
	jwtECDSAPublicKey, ok := key.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed *PublicKey", key)
	}
	protoPublicKey, err := publicKeyToProto(jwtECDSAPublicKey)
	if err != nil {
		return nil, err
	}
	serializedPublicKey, err := proto.Marshal(protoPublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JwtEcdsaPublicKey: %v", err)
	}

	idRequirement, _ := jwtECDSAPublicKey.IDRequirement()
	return protoserialization.NewKeySerialization(&tinkpb.KeyData{
		TypeUrl:         publicKeyTypeURL,
		Value:           serializedPublicKey,
		KeyMaterialType: tinkpb.KeyData_ASYMMETRIC_PUBLIC,
	}, outputPrefixTypeFromKIDStrategy(jwtECDSAPublicKey.parameters.KIDStrategy()), idRequirement)
}

func publicKeyFromProto(protoPublicKey *jwtecdsapb.JwtEcdsaPublicKey, keySerialization *protoserialization.KeySerialization) (*PublicKey, error) {
	if protoPublicKey.GetVersion() != 0 {
		return nil, fmt.Errorf("invalid public key version: got %d, want 0", protoPublicKey.GetVersion())
	}
	coordinateSize, err := coordinateSizeFromAlgorithm(algorithmFromProto(protoPublicKey.GetAlgorithm()))
	if err != nil {
		return nil, err
	}
	// Tolerate arbitrary leading zeros in the coordinates.
	// This is to support the case where the curve size in bytes + 1 is the
	// length of the coordinate. This happens when Tink adds an extra leading
	// 0x00 byte (see b/264525021).
	x, err := ec.BigIntBytesToFixedSizeBuffer(protoPublicKey.GetX(), coordinateSize)
	if err != nil {
		return nil, err
	}
	y, err := ec.BigIntBytesToFixedSizeBuffer(protoPublicKey.GetY(), coordinateSize)
	if err != nil {
		return nil, err
	}
	if len(x) != coordinateSize || len(y) != coordinateSize {
		return nil, fmt.Errorf("invalid coordinate size: got (%d, %d) want (%d, %d)", len(x), len(y), coordinateSize, coordinateSize)
	}
	kidStrategy := kidStrategyFromOutputPrefixType(keySerialization.OutputPrefixType(), protoPublicKey.GetCustomKid() != nil)
	params, err := NewParameters(kidStrategy, algorithmFromProto(protoPublicKey.GetAlgorithm()))
	if err != nil {
		return nil, err
	}
	uncompressedPoint := slices.Concat([]byte{0x04}, x, y)
	keyID, _ := keySerialization.IDRequirement()
	return NewPublicKey(PublicKeyOpts{
		PublicPoint:   uncompressedPoint,
		IDRequirement: keyID,
		HasCustomKID:  protoPublicKey.GetCustomKid() != nil,
		CustomKID:     protoPublicKey.GetCustomKid().GetValue(),
		Parameters:    params,
	})
}

type publicKeyParser struct{}

var _ protoserialization.KeyParser = (*publicKeyParser)(nil)

func (s *publicKeyParser) ParseKey(keySerialization *protoserialization.KeySerialization) (key.Key, error) {
	if keySerialization == nil {
		return nil, fmt.Errorf("key serialization can't be nil")
	}
	if keySerialization.KeyData().GetTypeUrl() != publicKeyTypeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", keySerialization.KeyData().GetTypeUrl(), publicKeyTypeURL)
	}
	if keySerialization.KeyData().GetKeyMaterialType() != tinkpb.KeyData_ASYMMETRIC_PUBLIC {
		return nil, fmt.Errorf("invalid key material type: got %v, want %v", keySerialization.KeyData().GetKeyMaterialType(), tinkpb.KeyData_ASYMMETRIC_PUBLIC)
	}

	publicKeyProto := &jwtecdsapb.JwtEcdsaPublicKey{}
	if err := proto.Unmarshal(keySerialization.KeyData().GetValue(), publicKeyProto); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtEcdsaPublicKey: %v", err)
	}
	return publicKeyFromProto(publicKeyProto, keySerialization)
}

type privateKeySerializer struct{}

var _ protoserialization.KeySerializer = (*privateKeySerializer)(nil)

func (s *privateKeySerializer) SerializeKey(k key.Key) (*protoserialization.KeySerialization, error) {
	jwtECDSAPrivateKey, ok := k.(*PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed *PrivateKey", k)
	}
	publicKey, err := jwtECDSAPrivateKey.PublicKey()
	if err != nil {
		return nil, err
	}
	jwtECDSAPublicKey, ok := publicKey.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("public key is of type %T; needed *PublicKey", publicKey)
	}
	protoPublicKey, err := publicKeyToProto(jwtECDSAPublicKey)
	if err != nil {
		return nil, err
	}
	coordinateSize, err := coordinateSizeFromAlgorithm(jwtECDSAPublicKey.parameters.Algorithm())
	if err != nil {
		return nil, err
	}
	paddedPrivateKey, err := ec.BigIntBytesToFixedSizeBuffer(jwtECDSAPrivateKey.privateKeyBytes.Data(insecuresecretdataaccess.Token{}), coordinateSize+1)
	if err != nil {
		return nil, err
	}
	protoPrivateKey := &jwtecdsapb.JwtEcdsaPrivateKey{
		Version:   0,
		PublicKey: protoPublicKey,
		KeyValue:  paddedPrivateKey,
	}
	serializedPrivateKey, err := proto.Marshal(protoPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JwtEcdsaPrivateKey: %v", err)
	}
	idRequirement, _ := jwtECDSAPrivateKey.IDRequirement()
	return protoserialization.NewKeySerialization(&tinkpb.KeyData{
		TypeUrl:         privateKeyTypeURL,
		Value:           serializedPrivateKey,
		KeyMaterialType: tinkpb.KeyData_ASYMMETRIC_PRIVATE,
	}, outputPrefixTypeFromKIDStrategy(jwtECDSAPublicKey.parameters.KIDStrategy()), idRequirement)
}

type privateKeyParser struct{}

var _ protoserialization.KeyParser = (*privateKeyParser)(nil)

func (s *privateKeyParser) ParseKey(keySerialization *protoserialization.KeySerialization) (key.Key, error) {
	if keySerialization == nil {
		return nil, fmt.Errorf("key serialization can't be nil")
	}
	if keySerialization.KeyData().GetTypeUrl() != privateKeyTypeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", keySerialization.KeyData().GetTypeUrl(), privateKeyTypeURL)
	}
	if keySerialization.KeyData().GetKeyMaterialType() != tinkpb.KeyData_ASYMMETRIC_PRIVATE {
		return nil, fmt.Errorf("invalid key material type: got %v, want %v", keySerialization.KeyData().GetKeyMaterialType(), tinkpb.KeyData_ASYMMETRIC_PRIVATE)
	}
	privateKeyProto := &jwtecdsapb.JwtEcdsaPrivateKey{}
	if err := proto.Unmarshal(keySerialization.KeyData().GetValue(), privateKeyProto); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtEcdsaPrivateKey: %v", err)
	}
	if privateKeyProto.GetVersion() != 0 {
		return nil, fmt.Errorf("invalid version: got %d, want 0", privateKeyProto.GetVersion())
	}
	publicKey, err := publicKeyFromProto(privateKeyProto.GetPublicKey(), keySerialization)
	if err != nil {
		return nil, err
	}
	alg := algorithmFromProto(privateKeyProto.GetPublicKey().GetAlgorithm())
	coordinateSize, err := coordinateSizeFromAlgorithm(alg)
	if err != nil {
		return nil, err
	}
	// Tolerate arbitrary leading zeros in the private key.
	privateKeyBytes, err := ec.BigIntBytesToFixedSizeBuffer(privateKeyProto.GetKeyValue(), coordinateSize)
	if err != nil {
		return nil, err
	}
	return NewPrivateKeyFromPublicKey(
		secretdata.NewBytesFromData(privateKeyBytes, insecuresecretdataaccess.Token{}),
		publicKey,
	)
}
