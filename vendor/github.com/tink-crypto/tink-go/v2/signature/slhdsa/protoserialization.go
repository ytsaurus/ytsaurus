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

package slhdsa

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	slhdsapb "github.com/tink-crypto/tink-go/v2/proto/slh_dsa_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

const (
	// publicKeyProtoVersion is the accepted [slhdsapb.SlhDsaPublicKey] proto
	// version.
	//
	// Currently, only version 0 is supported; other versions are rejected.
	publicKeyProtoVersion = 0
	// privateKeyProtoVersion is the accepted [slhdsapb.SlhDsaPrivateKey] proto
	// version.
	//
	// Currently, only version 0 is supported; other versions are rejected.
	privateKeyProtoVersion = 0

	signerTypeURL   = "type.googleapis.com/google.crypto.tink.SlhDsaPrivateKey"
	verifierTypeURL = "type.googleapis.com/google.crypto.tink.SlhDsaPublicKey"
)

type publicKeySerializer struct{}

var _ protoserialization.KeySerializer = (*publicKeySerializer)(nil)

func protoOutputPrefixTypeFromVariant(variant Variant) (tinkpb.OutputPrefixType, error) {
	switch variant {
	case VariantTink:
		return tinkpb.OutputPrefixType_TINK, nil
	case VariantNoPrefix:
		return tinkpb.OutputPrefixType_RAW, nil
	default:
		return tinkpb.OutputPrefixType_UNKNOWN_PREFIX, fmt.Errorf("unknown output prefix variant: %v", variant)
	}
}

func protoSlhDsaHashTypeFromHashType(hashType HashType) (slhdsapb.SlhDsaHashType, error) {
	switch hashType {
	case SHA2:
		return slhdsapb.SlhDsaHashType_SHA2, nil
	case SHAKE:
		return slhdsapb.SlhDsaHashType_SHAKE, nil
	default:
		return slhdsapb.SlhDsaHashType_SLH_DSA_HASH_TYPE_UNSPECIFIED, fmt.Errorf("unknown hash type: %v", hashType)
	}
}

func protoSlhDsaSignatureTypeFromSignatureType(sigType SignatureType) (slhdsapb.SlhDsaSignatureType, error) {
	switch sigType {
	case FastSigning:
		return slhdsapb.SlhDsaSignatureType_FAST_SIGNING, nil
	case SmallSignature:
		return slhdsapb.SlhDsaSignatureType_SMALL_SIGNATURE, nil
	default:
		return slhdsapb.SlhDsaSignatureType_SLH_DSA_SIGNATURE_TYPE_UNSPECIFIED, fmt.Errorf("unknown signature type: %v", sigType)
	}
}

func (s *publicKeySerializer) SerializeKey(key key.Key) (*protoserialization.KeySerialization, error) {
	slhdsaPubKey, ok := key.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("invalid key type: %T, want *slhdsa.PublicKey", key)
	}
	if slhdsaPubKey.params == nil {
		return nil, fmt.Errorf("invalid key: parameters are nil")
	}
	outputPrefixType, err := protoOutputPrefixTypeFromVariant(slhdsaPubKey.params.Variant())
	if err != nil {
		return nil, err
	}
	slhdsaHashType, err := protoSlhDsaHashTypeFromHashType(slhdsaPubKey.params.HashType())
	if err != nil {
		return nil, err
	}
	slhdsaSigType, err := protoSlhDsaSignatureTypeFromSignatureType(slhdsaPubKey.params.SignatureType())
	if err != nil {
		return nil, err
	}
	protoKey := &slhdsapb.SlhDsaPublicKey{
		KeyValue: slhdsaPubKey.KeyBytes(),
		Params: &slhdsapb.SlhDsaParams{
			KeySize:  int32(slhdsaPubKey.params.KeySize()),
			HashType: slhdsaHashType,
			SigType:  slhdsaSigType,
		},
		Version: publicKeyProtoVersion,
	}
	serializedKey, err := proto.Marshal(protoKey)
	if err != nil {
		return nil, err
	}
	// idRequirement is zero if the key doesn't have a key requirement.
	idRequirement, _ := slhdsaPubKey.IDRequirement()
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
	slhdsaPrivKey, ok := key.(*PrivateKey)
	if !ok {
		return nil, fmt.Errorf("invalid key type: %T, want *slhdsa.PrivateKey", key)
	}
	if slhdsaPrivKey.publicKey == nil {
		return nil, fmt.Errorf("invalid key: public key is nil")
	}
	params := slhdsaPrivKey.publicKey.params
	if params == nil {
		return nil, fmt.Errorf("invalid key: public key parameters are nil")
	}
	outputPrefixType, err := protoOutputPrefixTypeFromVariant(params.Variant())
	if err != nil {
		return nil, err
	}
	slhdsaHashType, err := protoSlhDsaHashTypeFromHashType(params.HashType())
	if err != nil {
		return nil, err
	}
	slhdsaSigType, err := protoSlhDsaSignatureTypeFromSignatureType(params.SignatureType())
	if err != nil {
		return nil, err
	}
	protoKey := &slhdsapb.SlhDsaPrivateKey{
		KeyValue: slhdsaPrivKey.PrivateKeyBytes().Data(insecuresecretdataaccess.Token{}),
		PublicKey: &slhdsapb.SlhDsaPublicKey{
			KeyValue: slhdsaPrivKey.publicKey.KeyBytes(),
			Params: &slhdsapb.SlhDsaParams{
				KeySize:  int32(params.KeySize()),
				HashType: slhdsaHashType,
				SigType:  slhdsaSigType,
			},
			Version: publicKeyProtoVersion,
		},
		Version: privateKeyProtoVersion,
	}
	serializedKey, err := proto.Marshal(protoKey)
	if err != nil {
		return nil, err
	}
	// idRequirement is zero if the key doesn't have a key requirement.
	idRequirement, _ := slhdsaPrivKey.IDRequirement()
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
	case tinkpb.OutputPrefixType_RAW:
		return VariantNoPrefix, nil
	default:
		return VariantUnknown, fmt.Errorf("unsupported output prefix type: %v", prefixType)
	}
}

func hashTypeFromProto(hashType slhdsapb.SlhDsaHashType) (HashType, error) {
	switch hashType {
	case slhdsapb.SlhDsaHashType_SHA2:
		return SHA2, nil
	case slhdsapb.SlhDsaHashType_SHAKE:
		return SHAKE, nil
	default:
		return UnknownHashType, fmt.Errorf("unsupported hash type: %v", hashType)
	}
}

func signatureTypeFromProto(sigType slhdsapb.SlhDsaSignatureType) (SignatureType, error) {
	switch sigType {
	case slhdsapb.SlhDsaSignatureType_FAST_SIGNING:
		return FastSigning, nil
	case slhdsapb.SlhDsaSignatureType_SMALL_SIGNATURE:
		return SmallSignature, nil
	default:
		return UnknownSignatureType, fmt.Errorf("unsupported signature type: %v", sigType)
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
	protoKey := new(slhdsapb.SlhDsaPublicKey)
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
	hashType, err := hashTypeFromProto(protoKey.GetParams().GetHashType())
	if err != nil {
		return nil, err
	}
	sigType, err := signatureTypeFromProto(protoKey.GetParams().GetSigType())
	if err != nil {
		return nil, err
	}
	params, err := NewParameters(hashType, int(protoKey.GetParams().GetKeySize()), sigType, variant)
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
	protoKey := new(slhdsapb.SlhDsaPrivateKey)
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
	hashType, err := hashTypeFromProto(protoKey.GetPublicKey().GetParams().GetHashType())
	if err != nil {
		return nil, err
	}
	sigType, err := signatureTypeFromProto(protoKey.GetPublicKey().GetParams().GetSigType())
	if err != nil {
		return nil, err
	}
	params, err := NewParameters(hashType, int(protoKey.GetPublicKey().GetParams().GetKeySize()), sigType, variant)
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
	slhdsaParameters, ok := parameters.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: got %T, want *slhdsa.Parameters", parameters)
	}
	outputPrefixType, err := protoOutputPrefixTypeFromVariant(slhdsaParameters.Variant())
	if err != nil {
		return nil, err
	}
	slhdsaHashType, err := protoSlhDsaHashTypeFromHashType(slhdsaParameters.HashType())
	if err != nil {
		return nil, err
	}
	slhdsaSigType, err := protoSlhDsaSignatureTypeFromSignatureType(slhdsaParameters.SignatureType())
	if err != nil {
		return nil, err
	}
	format := &slhdsapb.SlhDsaKeyFormat{
		Params: &slhdsapb.SlhDsaParams{
			KeySize:  int32(slhdsaParameters.KeySize()),
			HashType: slhdsaHashType,
			SigType:  slhdsaSigType,
		},
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
	format := new(slhdsapb.SlhDsaKeyFormat)
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
	hashType, err := hashTypeFromProto(format.GetParams().GetHashType())
	if err != nil {
		return nil, err
	}
	sigType, err := signatureTypeFromProto(format.GetParams().GetSigType())
	if err != nil {
		return nil, err
	}
	params, err := NewParameters(hashType, int(format.GetParams().GetKeySize()), sigType, variant)
	if err != nil {
		return nil, err
	}
	return params, nil
}
