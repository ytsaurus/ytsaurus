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

package aesctrhmac

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	aesctrpb "github.com/tink-crypto/tink-go/v2/proto/aes_ctr_go_proto"
	aesctrhmacpb "github.com/tink-crypto/tink-go/v2/proto/aes_ctr_hmac_aead_go_proto"
	commonpb "github.com/tink-crypto/tink-go/v2/proto/common_go_proto"
	hmacpb "github.com/tink-crypto/tink-go/v2/proto/hmac_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

const typeURL = "type.googleapis.com/google.crypto.tink.AesCtrHmacAeadKey"

type keySerializer struct{}

var _ protoserialization.KeySerializer = (*keySerializer)(nil)

func protoOutputPrefixTypeFromVariant(variant Variant) (tinkpb.OutputPrefixType, error) {
	switch variant {
	case VariantTink:
		return tinkpb.OutputPrefixType_TINK, nil
	case VariantCrunchy:
		return tinkpb.OutputPrefixType_CRUNCHY, nil
	case VariantNoPrefix:
		return tinkpb.OutputPrefixType_RAW, nil
	default:
		return tinkpb.OutputPrefixType_UNKNOWN_PREFIX, fmt.Errorf("unknown output prefix variant: %v", variant)
	}
}

func hashTypeToProto(ht HashType) (commonpb.HashType, error) {
	switch ht {
	case SHA1:
		return commonpb.HashType_SHA1, nil
	case SHA224:
		return commonpb.HashType_SHA224, nil
	case SHA256:
		return commonpb.HashType_SHA256, nil
	case SHA384:
		return commonpb.HashType_SHA384, nil
	case SHA512:
		return commonpb.HashType_SHA512, nil
	default:
		return commonpb.HashType_UNKNOWN_HASH, fmt.Errorf("unknown hash type: %v", ht)
	}
}

func (s *keySerializer) SerializeKey(key key.Key) (*protoserialization.KeySerialization, error) {
	actualKey, ok := key.(*Key)
	if !ok {
		return nil, fmt.Errorf("key is not a Key")
	}
	actualParameters, ok := actualKey.Parameters().(*Parameters)
	if !ok {
		return nil, fmt.Errorf("key parameters is not a Parameters")
	}
	outputPrefixType, err := protoOutputPrefixTypeFromVariant(actualParameters.Variant())
	if err != nil {
		return nil, err
	}
	hash, err := hashTypeToProto(actualParameters.HashType())
	if err != nil {
		return nil, err
	}
	protoKey := &aesctrhmacpb.AesCtrHmacAeadKey{
		AesCtrKey: &aesctrpb.AesCtrKey{
			Version: 0,
			Params: &aesctrpb.AesCtrParams{
				IvSize: uint32(actualParameters.IVSizeInBytes()),
			},
			KeyValue: actualKey.AESKeyBytes().Data(insecuresecretdataaccess.Token{}),
		},
		HmacKey: &hmacpb.HmacKey{
			Version: 0,
			Params: &hmacpb.HmacParams{
				Hash:    hash,
				TagSize: uint32(actualParameters.TagSizeInBytes()),
			},
			KeyValue: actualKey.HMACKeyBytes().Data(insecuresecretdataaccess.Token{}),
		},
		Version: 0,
	}
	serializedKey, err := proto.Marshal(protoKey)
	if err != nil {
		return nil, err
	}
	// idRequirement is zero if the key doesn't have a key requirement.
	idRequirement, _ := actualKey.IDRequirement()
	keyData := &tinkpb.KeyData{
		TypeUrl:         typeURL,
		Value:           serializedKey,
		KeyMaterialType: tinkpb.KeyData_SYMMETRIC,
	}
	return protoserialization.NewKeySerialization(keyData, outputPrefixType, idRequirement)
}

type keyParser struct{}

var _ protoserialization.KeyParser = (*keyParser)(nil)

func variantFromProto(prefixType tinkpb.OutputPrefixType) (Variant, error) {
	switch prefixType {
	case tinkpb.OutputPrefixType_TINK:
		return VariantTink, nil
	case tinkpb.OutputPrefixType_CRUNCHY, tinkpb.OutputPrefixType_LEGACY:
		return VariantCrunchy, nil
	case tinkpb.OutputPrefixType_RAW:
		return VariantNoPrefix, nil
	default:
		return VariantUnknown, fmt.Errorf("unsupported output prefix type: %v", prefixType)
	}
}

func hashTypeFromProto(ht commonpb.HashType) (HashType, error) {
	switch ht {
	case commonpb.HashType_SHA1:
		return SHA1, nil
	case commonpb.HashType_SHA224:
		return SHA224, nil
	case commonpb.HashType_SHA256:
		return SHA256, nil
	case commonpb.HashType_SHA384:
		return SHA384, nil
	case commonpb.HashType_SHA512:
		return SHA512, nil
	default:
		return UnknownHashType, fmt.Errorf("unknown hash type: %v", ht)
	}
}

func (s *keyParser) ParseKey(keySerialization *protoserialization.KeySerialization) (key.Key, error) {
	if keySerialization == nil {
		return nil, fmt.Errorf("key serialization is nil")
	}
	keyData := keySerialization.KeyData()
	if keyData.GetTypeUrl() != typeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", keyData.GetTypeUrl(), typeURL)
	}
	if keyData.GetKeyMaterialType() != tinkpb.KeyData_SYMMETRIC {
		return nil, fmt.Errorf("key is not a SYMMETRIC key")
	}
	protoKey := new(aesctrhmacpb.AesCtrHmacAeadKey)
	if err := proto.Unmarshal(keyData.GetValue(), protoKey); err != nil {
		return nil, err
	}
	if protoKey.GetVersion() != 0 {
		return nil, fmt.Errorf("unsupported aesctrhmacpb.AesCtrHmacAeadKey version: got %q, want %q", protoKey.GetVersion(), 0)
	}
	if protoKey.GetAesCtrKey().GetVersion() != 0 {
		return nil, fmt.Errorf("unsupported aesctrpb.AesCtrKey version: got %q, want %q", protoKey.GetAesCtrKey().GetVersion(), 0)
	}
	if protoKey.GetHmacKey().GetVersion() != 0 {
		return nil, fmt.Errorf("unsupported hmacpb.HmacKey version: got %q, want %q", protoKey.GetHmacKey().GetVersion(), 0)
	}
	variant, err := variantFromProto(keySerialization.OutputPrefixType())
	if err != nil {
		return nil, err
	}
	aesKeySizeInBytes := len(protoKey.GetAesCtrKey().GetKeyValue())
	hmacKeySizeInBytes := len(protoKey.GetHmacKey().GetKeyValue())
	hashType, err := hashTypeFromProto(protoKey.GetHmacKey().GetParams().GetHash())
	if err != nil {
		return nil, err
	}
	params, err := NewParameters(ParametersOpts{
		AESKeySizeInBytes:  aesKeySizeInBytes,
		HMACKeySizeInBytes: hmacKeySizeInBytes,
		IVSizeInBytes:      int(protoKey.GetAesCtrKey().GetParams().GetIvSize()),
		TagSizeInBytes:     int(protoKey.GetHmacKey().GetParams().GetTagSize()),
		HashType:           hashType,
		Variant:            variant,
	})
	if err != nil {
		return nil, err
	}
	aesKeyMaterial := secretdata.NewBytesFromData(protoKey.GetAesCtrKey().GetKeyValue(), insecuresecretdataaccess.Token{})
	hmacKeyMaterial := secretdata.NewBytesFromData(protoKey.GetHmacKey().GetKeyValue(), insecuresecretdataaccess.Token{})
	// keySerialization.IDRequirement() returns zero if the key doesn't have a
	// key requirement.
	keyID, _ := keySerialization.IDRequirement()
	return NewKey(KeyOpts{
		AESKeyBytes:   aesKeyMaterial,
		HMACKeyBytes:  hmacKeyMaterial,
		IDRequirement: keyID,
		Parameters:    params})
}

type parametersSerializer struct{}

var _ protoserialization.ParametersSerializer = (*parametersSerializer)(nil)

func (s *parametersSerializer) Serialize(parameters key.Parameters) (*tinkpb.KeyTemplate, error) {
	actualParameters, ok := parameters.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: got %T, want *aesctrhmac.Parameters", parameters)
	}
	outputPrefixType, err := protoOutputPrefixTypeFromVariant(actualParameters.Variant())
	if err != nil {
		return nil, err
	}

	hashType, err := hashTypeToProto(actualParameters.HashType())
	if err != nil {
		return nil, err
	}

	format := &aesctrhmacpb.AesCtrHmacAeadKeyFormat{
		AesCtrKeyFormat: &aesctrpb.AesCtrKeyFormat{
			KeySize: uint32(actualParameters.AESKeySizeInBytes()),
			Params: &aesctrpb.AesCtrParams{
				IvSize: uint32(actualParameters.IVSizeInBytes()),
			},
		},
		HmacKeyFormat: &hmacpb.HmacKeyFormat{
			KeySize: uint32(actualParameters.HMACKeySizeInBytes()),
			Params: &hmacpb.HmacParams{
				Hash:    hashType,
				TagSize: uint32(actualParameters.TagSizeInBytes()),
			},
			Version: 0,
		},
	}
	serializedFormat, err := proto.Marshal(format)
	if err != nil {
		return nil, err
	}
	return &tinkpb.KeyTemplate{
		TypeUrl:          typeURL,
		OutputPrefixType: outputPrefixType,
		Value:            serializedFormat,
	}, nil
}

type parametersParser struct{}

var _ protoserialization.ParametersParser = (*parametersParser)(nil)

func (s *parametersParser) Parse(keyTemplate *tinkpb.KeyTemplate) (key.Parameters, error) {
	if keyTemplate.GetTypeUrl() != typeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", keyTemplate.GetTypeUrl(), typeURL)
	}
	format := new(aesctrhmacpb.AesCtrHmacAeadKeyFormat)
	if err := proto.Unmarshal(keyTemplate.GetValue(), format); err != nil {
		return nil, err
	}
	if format.GetHmacKeyFormat().GetVersion() != 0 {
		return nil, fmt.Errorf("unsupported hmacpb.HmacKeyFormat version: got %q, want %q", format.GetHmacKeyFormat().GetVersion(), 0)
	}

	variant, err := variantFromProto(keyTemplate.GetOutputPrefixType())
	if err != nil {
		return nil, err
	}
	hashType, err := hashTypeFromProto(format.GetHmacKeyFormat().GetParams().GetHash())
	if err != nil {
		return nil, err
	}
	return NewParameters(ParametersOpts{
		AESKeySizeInBytes:  int(format.GetAesCtrKeyFormat().GetKeySize()),
		HMACKeySizeInBytes: int(format.GetHmacKeyFormat().GetKeySize()),
		IVSizeInBytes:      int(format.GetAesCtrKeyFormat().GetParams().GetIvSize()),
		TagSizeInBytes:     int(format.GetHmacKeyFormat().GetParams().GetTagSize()),
		HashType:           hashType,
		Variant:            variant,
	})
}
