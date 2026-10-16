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

package jwtrsassapkcs1

import (
	"fmt"
	"math/big"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"

	jwtrsapb "github.com/tink-crypto/tink-go/v2/proto/jwt_rsa_ssa_pkcs1_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

const (
	privateKeyTypeURL = "type.googleapis.com/google.crypto.tink.JwtRsaSsaPkcs1PrivateKey"
	publicKeyTypeURL  = "type.googleapis.com/google.crypto.tink.JwtRsaSsaPkcs1PublicKey"
)

type parametersSerializer struct{}

var _ protoserialization.ParametersSerializer = (*parametersSerializer)(nil)

func algorithmToProto(a Algorithm) jwtrsapb.JwtRsaSsaPkcs1Algorithm {
	switch a {
	case RS256:
		return jwtrsapb.JwtRsaSsaPkcs1Algorithm_RS256
	case RS384:
		return jwtrsapb.JwtRsaSsaPkcs1Algorithm_RS384
	case RS512:
		return jwtrsapb.JwtRsaSsaPkcs1Algorithm_RS512
	}
	// Should never happen.
	return jwtrsapb.JwtRsaSsaPkcs1Algorithm_RS_UNKNOWN
}

func algorithmFromProto(a jwtrsapb.JwtRsaSsaPkcs1Algorithm) Algorithm {
	switch a {
	case jwtrsapb.JwtRsaSsaPkcs1Algorithm_RS256:
		return RS256
	case jwtrsapb.JwtRsaSsaPkcs1Algorithm_RS384:
		return RS384
	case jwtrsapb.JwtRsaSsaPkcs1Algorithm_RS512:
		return RS512
	}
	return UnknownAlgorithm
}

func outputPrefixTypeFromKIDStrategy(s KIDStrategy) tinkpb.OutputPrefixType {
	switch s {
	case CustomKID, IgnoredKID:
		return tinkpb.OutputPrefixType_RAW
	case Base64EncodedKeyIDAsKID:
		return tinkpb.OutputPrefixType_TINK
	}
	// Should never happen.
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
	keyFormat := &jwtrsapb.JwtRsaSsaPkcs1KeyFormat{
		Algorithm:         algorithmToProto(params.Algorithm()),
		ModulusSizeInBits: uint32(params.ModulusSizeInBits()),
		PublicExponent:    []byte{0x01, 0x00, 0x01}, // F4
		Version:           0,
	}
	serializedKeyFormat, err := proto.Marshal(keyFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal key format: %v", err)
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
	keyFormat := &jwtrsapb.JwtRsaSsaPkcs1KeyFormat{}
	if err := proto.Unmarshal(kt.GetValue(), keyFormat); err != nil {
		return nil, fmt.Errorf("failed to unmarshal key format: %v", err)
	}
	if keyFormat.GetVersion() != 0 {
		return nil, fmt.Errorf("invalid version: got %d, want 0", keyFormat.GetVersion())
	}
	// Key format cannot specify a CustomKID.
	publicExponent := new(big.Int).SetBytes(keyFormat.GetPublicExponent())
	if !publicExponent.IsInt64() {
		return nil, fmt.Errorf("public exponent cannot be represented as int64")
	}

	kidStrategy := kidStrategyFromOutputPrefixType(kt.GetOutputPrefixType(), false)
	return NewParameters(ParametersOpts{
		KidStrategy:       kidStrategy,
		Algorithm:         algorithmFromProto(keyFormat.GetAlgorithm()),
		ModulusSizeInBits: int(keyFormat.GetModulusSizeInBits()),
		PublicExponent:    int(publicExponent.Int64()),
	})
}

// publicKeyToProto converts a [PublicKey] to a [jwtrsapb.JwtRsaSsaPkcs1PublicKey]
// proto.
func publicKeyToProto(k *PublicKey) (*jwtrsapb.JwtRsaSsaPkcs1PublicKey, error) {
	if k.parameters == nil {
		return nil, fmt.Errorf("parameters can't be nil")
	}
	protoPublicKey := &jwtrsapb.JwtRsaSsaPkcs1PublicKey{
		Version:   0,
		Algorithm: algorithmToProto(k.parameters.Algorithm()),
		N:         k.Modulus(),
		E:         new(big.Int).SetInt64(int64(k.parameters.PublicExponent())).Bytes(),
	}
	if k.parameters.KIDStrategy() == CustomKID {
		kid, hasKID := k.KID()
		if !hasKID {
			return nil, fmt.Errorf("CustomKID strategy requires a KID")
		}
		protoPublicKey.CustomKid = &jwtrsapb.JwtRsaSsaPkcs1PublicKey_CustomKid{
			Value: kid,
		}
	}
	return protoPublicKey, nil
}

type publicKeySerializer struct{}

var _ protoserialization.KeySerializer = (*publicKeySerializer)(nil)

func (s *publicKeySerializer) SerializeKey(key key.Key) (*protoserialization.KeySerialization, error) {
	jwtRSAPublicKey, ok := key.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed %T", key, (*PublicKey)(nil))
	}
	protoPublicKey, err := publicKeyToProto(jwtRSAPublicKey)
	if err != nil {
		return nil, err
	}
	serializedPublicKey, err := proto.Marshal(protoPublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal %T: %v", protoPublicKey, err)
	}

	idRequirement, _ := jwtRSAPublicKey.IDRequirement()
	return protoserialization.NewKeySerialization(&tinkpb.KeyData{
		TypeUrl:         publicKeyTypeURL,
		Value:           serializedPublicKey,
		KeyMaterialType: tinkpb.KeyData_ASYMMETRIC_PUBLIC,
	}, outputPrefixTypeFromKIDStrategy(jwtRSAPublicKey.parameters.KIDStrategy()), idRequirement)
}

// publicKeyFromProto converts a [jwtrsapb.JwtRsaSsaPkcs1PublicKey] proto to a
// [PublicKey].
func publicKeyFromProto(protoPublicKey *jwtrsapb.JwtRsaSsaPkcs1PublicKey, outputPrefixType tinkpb.OutputPrefixType, idRequirement uint32) (*PublicKey, error) {
	if protoPublicKey.GetVersion() != 0 {
		return nil, fmt.Errorf("invalid public key version: got %d, want 0", protoPublicKey.GetVersion())
	}
	kidStrategy := kidStrategyFromOutputPrefixType(outputPrefixType, protoPublicKey.GetCustomKid() != nil)

	modulusSizeInBits := new(big.Int).SetBytes(protoPublicKey.GetN()).BitLen()

	exponent := new(big.Int).SetBytes(protoPublicKey.GetE()).Int64()
	params, err := NewParameters(ParametersOpts{
		KidStrategy:       kidStrategy,
		Algorithm:         algorithmFromProto(protoPublicKey.GetAlgorithm()),
		ModulusSizeInBits: modulusSizeInBits,
		PublicExponent:    int(exponent),
	})
	if err != nil {
		return nil, err
	}
	return NewPublicKey(PublicKeyOpts{
		Modulus:       protoPublicKey.GetN(),
		IDRequirement: idRequirement,
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

	publicKeyProto := &jwtrsapb.JwtRsaSsaPkcs1PublicKey{}
	if err := proto.Unmarshal(keySerialization.KeyData().GetValue(), publicKeyProto); err != nil {
		return nil, fmt.Errorf("failed to unmarshal %T: %v", err, publicKeyProto)
	}
	idRequirement, _ := keySerialization.IDRequirement()
	return publicKeyFromProto(publicKeyProto, keySerialization.OutputPrefixType(), idRequirement)
}

type privateKeySerializer struct{}

var _ protoserialization.KeySerializer = (*privateKeySerializer)(nil)

func (s *privateKeySerializer) SerializeKey(k key.Key) (*protoserialization.KeySerialization, error) {
	jwtRSAPrivateKey, ok := k.(*PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed %T", k, (*PrivateKey)(nil))
	}
	if jwtRSAPrivateKey.publicKey == nil {
		return nil, fmt.Errorf("public key can't be nil")
	}
	publicKeyProto, err := publicKeyToProto(jwtRSAPrivateKey.publicKey)
	if err != nil {
		return nil, err
	}
	protoPrivateKey := &jwtrsapb.JwtRsaSsaPkcs1PrivateKey{
		Version:   0,
		PublicKey: publicKeyProto,
		D:         jwtRSAPrivateKey.D().Data(insecuresecretdataaccess.Token{}),
		P:         jwtRSAPrivateKey.P().Data(insecuresecretdataaccess.Token{}),
		Q:         jwtRSAPrivateKey.Q().Data(insecuresecretdataaccess.Token{}),
		Dp:        jwtRSAPrivateKey.DP().Data(insecuresecretdataaccess.Token{}),
		Dq:        jwtRSAPrivateKey.DQ().Data(insecuresecretdataaccess.Token{}),
		Crt:       jwtRSAPrivateKey.QInv().Data(insecuresecretdataaccess.Token{}),
	}
	serializedPrivateKey, err := proto.Marshal(protoPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal %T: %v", protoPrivateKey, err)
	}
	idRequirement, _ := jwtRSAPrivateKey.IDRequirement()
	return protoserialization.NewKeySerialization(&tinkpb.KeyData{
		TypeUrl:         privateKeyTypeURL,
		Value:           serializedPrivateKey,
		KeyMaterialType: tinkpb.KeyData_ASYMMETRIC_PRIVATE,
	}, outputPrefixTypeFromKIDStrategy(jwtRSAPrivateKey.publicKey.parameters.KIDStrategy()), idRequirement)
}

func removeLeadingZeros(keyBytes []byte) []byte {
	return new(big.Int).SetBytes(keyBytes).Bytes()
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
	privateKeyProto := &jwtrsapb.JwtRsaSsaPkcs1PrivateKey{}
	if err := proto.Unmarshal(keySerialization.KeyData().GetValue(), privateKeyProto); err != nil {
		return nil, fmt.Errorf("failed to unmarshal %T: %v", privateKeyProto, err)
	}
	idRequirement, _ := keySerialization.IDRequirement()
	outputPrefixType := keySerialization.OutputPrefixType()

	if privateKeyProto.GetVersion() != 0 {
		return nil, fmt.Errorf("invalid private key version: got %d, want 0", privateKeyProto.GetVersion())
	}
	publicKey, err := publicKeyFromProto(privateKeyProto.GetPublicKey(), outputPrefixType, idRequirement)
	if err != nil {
		return nil, err
	}
	privateKey, err := NewPrivateKey(PrivateKeyOpts{
		PublicKey: publicKey,
		D:         secretdata.NewBytesFromData(removeLeadingZeros(privateKeyProto.GetD()), insecuresecretdataaccess.Token{}),
		P:         secretdata.NewBytesFromData(removeLeadingZeros(privateKeyProto.GetP()), insecuresecretdataaccess.Token{}),
		Q:         secretdata.NewBytesFromData(removeLeadingZeros(privateKeyProto.GetQ()), insecuresecretdataaccess.Token{}),
	})
	if err != nil {
		return nil, err
	}

	// Make sure the computed values match the ones in the proto.
	if !privateKey.DP().Equal(secretdata.NewBytesFromData(removeLeadingZeros(privateKeyProto.GetDp()), insecuresecretdataaccess.Token{})) {
		return nil, fmt.Errorf("private key DP doesn't match")
	}
	if !privateKey.DQ().Equal(secretdata.NewBytesFromData(removeLeadingZeros(privateKeyProto.GetDq()), insecuresecretdataaccess.Token{})) {
		return nil, fmt.Errorf("private key DQ doesn't match")
	}
	if !privateKey.QInv().Equal(secretdata.NewBytesFromData(removeLeadingZeros(privateKeyProto.GetCrt()), insecuresecretdataaccess.Token{})) {
		return nil, fmt.Errorf("private key QInv doesn't match")
	}
	return privateKey, nil
}
