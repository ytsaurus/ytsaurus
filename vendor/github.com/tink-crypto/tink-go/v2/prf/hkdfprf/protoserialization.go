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

package hkdfprf

import (
	"bytes"
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	commonpb "github.com/tink-crypto/tink-go/v2/proto/common_go_proto"
	hkdfprfpb "github.com/tink-crypto/tink-go/v2/proto/hkdf_prf_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

const typeURL = "type.googleapis.com/google.crypto.tink.HkdfPrfKey"

type keySerializer struct{}

var _ protoserialization.KeySerializer = (*keySerializer)(nil)

func toProtoHashType(hashType HashType) (commonpb.HashType, error) {
	switch hashType {
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
		return commonpb.HashType_UNKNOWN_HASH, fmt.Errorf("unsupported hash type: %v", hashType)
	}
}

func (s *keySerializer) SerializeKey(key key.Key) (*protoserialization.KeySerialization, error) {
	actualKey, ok := key.(*Key)
	if !ok {
		return nil, fmt.Errorf("invalid key type: got %T, want %T", key, (*Key)(nil))
	}
	params, ok := actualKey.Parameters().(*Parameters)
	if !ok {
		return nil, fmt.Errorf("key parameters is not a Parameters")
	}
	protoHashType, err := toProtoHashType(params.HashType())
	if err != nil {
		return nil, err
	}
	keyBytes := actualKey.KeyBytes()
	protoKey := &hkdfprfpb.HkdfPrfKey{
		KeyValue: keyBytes.Data(insecuresecretdataaccess.Token{}),
		Version:  0,
		Params: &hkdfprfpb.HkdfPrfParams{
			Hash: protoHashType,
			Salt: bytes.Clone(params.Salt()),
		},
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
	return protoserialization.NewKeySerialization(keyData, tinkpb.OutputPrefixType_RAW, idRequirement)
}

type keyParser struct{}

var _ protoserialization.KeyParser = (*keyParser)(nil)

func fromProtoHashType(hashType commonpb.HashType) (HashType, error) {
	switch hashType {
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
		return UnknownHashType, fmt.Errorf("unsupported proto hash type: %v", hashType)
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
	if keySerialization.OutputPrefixType() != tinkpb.OutputPrefixType_RAW {
		return nil, fmt.Errorf("unsupported output prefix type: %v", keySerialization.OutputPrefixType())
	}
	// Do not check key material type for compatibility with other Tink implementations.
	// TODO - b/403459737: Consider adding the check.
	protoKey := new(hkdfprfpb.HkdfPrfKey)
	if err := proto.Unmarshal(keyData.GetValue(), protoKey); err != nil {
		return nil, err
	}
	if protoKey.GetVersion() != 0 {
		return nil, fmt.Errorf("key has unsupported version: %v", protoKey.GetVersion())
	}
	keyMaterial := secretdata.NewBytesFromData(protoKey.GetKeyValue(), insecuresecretdataaccess.Token{})
	hashType, err := fromProtoHashType(protoKey.GetParams().GetHash())
	if err != nil {
		return nil, err
	}
	params, err := NewParameters(keyMaterial.Len(), hashType, bytes.Clone(protoKey.GetParams().GetSalt()))
	if err != nil {
		return nil, err
	}
	return NewKey(keyMaterial, params)
}

type parametersSerializer struct{}

var _ protoserialization.ParametersSerializer = (*parametersSerializer)(nil)

func (s *parametersSerializer) Serialize(parameters key.Parameters) (*tinkpb.KeyTemplate, error) {
	actualParameters, ok := parameters.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: got %T, want %T", parameters, (*Parameters)(nil))
	}
	hashType, err := toProtoHashType(actualParameters.HashType())
	if err != nil {
		return nil, err
	}
	format := &hkdfprfpb.HkdfPrfKeyFormat{
		Version: 0,
		KeySize: uint32(actualParameters.KeySizeInBytes()),
		Params: &hkdfprfpb.HkdfPrfParams{
			Hash: hashType,
			Salt: bytes.Clone(actualParameters.Salt()),
		},
	}
	serializedFormat, err := proto.Marshal(format)
	if err != nil {
		return nil, err
	}
	return &tinkpb.KeyTemplate{
		TypeUrl:          typeURL,
		OutputPrefixType: tinkpb.OutputPrefixType_RAW,
		Value:            serializedFormat,
	}, nil
}

type parametersParser struct{}

var _ protoserialization.ParametersParser = (*parametersParser)(nil)

func (s *parametersParser) Parse(keyTemplate *tinkpb.KeyTemplate) (key.Parameters, error) {
	if keyTemplate.GetTypeUrl() != typeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", keyTemplate.GetTypeUrl(), typeURL)
	}
	if keyTemplate.GetOutputPrefixType() != tinkpb.OutputPrefixType_RAW {
		return nil, fmt.Errorf("unsupported output prefix type: %v", keyTemplate.GetOutputPrefixType())
	}
	format := new(hkdfprfpb.HkdfPrfKeyFormat)
	if err := proto.Unmarshal(keyTemplate.GetValue(), format); err != nil {
		return nil, err
	}
	if format.GetVersion() != 0 {
		return nil, fmt.Errorf("key has unsupported version: %v", format.GetVersion())
	}
	hashType, err := fromProtoHashType(format.GetParams().GetHash())
	if err != nil {
		return nil, err
	}
	return NewParameters(int(format.GetKeySize()), hashType, bytes.Clone(format.GetParams().GetSalt()))
}
