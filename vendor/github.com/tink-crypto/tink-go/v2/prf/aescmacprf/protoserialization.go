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

package aescmacprf

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	aescmacprfpb "github.com/tink-crypto/tink-go/v2/proto/aes_cmac_prf_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

type keySerializer struct{}

const typeURL = "type.googleapis.com/google.crypto.tink.AesCmacPrfKey"

var _ protoserialization.KeySerializer = (*keySerializer)(nil)

func (s *keySerializer) SerializeKey(key key.Key) (*protoserialization.KeySerialization, error) {
	actualKey, ok := key.(*Key)
	if !ok {
		return nil, fmt.Errorf("invalid key type: got %T, want %T", key, (*Key)(nil))
	}
	if _, ok := actualKey.Parameters().(*Parameters); !ok {
		return nil, fmt.Errorf("key parameters is not a Parameters")
	}
	keyBytes := actualKey.KeyBytes()
	protoKey := &aescmacprfpb.AesCmacPrfKey{
		KeyValue: keyBytes.Data(insecuresecretdataaccess.Token{}),
		Version:  0,
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
	protoKey := new(aescmacprfpb.AesCmacPrfKey)
	if err := proto.Unmarshal(keyData.GetValue(), protoKey); err != nil {
		return nil, err
	}
	if protoKey.GetVersion() != 0 {
		return nil, fmt.Errorf("key has unsupported version: %v", protoKey.GetVersion())
	}
	keyMaterial := secretdata.NewBytesFromData(protoKey.GetKeyValue(), insecuresecretdataaccess.Token{})
	return NewKey(keyMaterial)
}

type parametersSerializer struct{}

var _ protoserialization.ParametersSerializer = (*parametersSerializer)(nil)

func (s *parametersSerializer) Serialize(parameters key.Parameters) (*tinkpb.KeyTemplate, error) {
	actualParameters, ok := parameters.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: got %T, want %T", parameters, (*Parameters)(nil))
	}
	format := &aescmacprfpb.AesCmacPrfKeyFormat{
		Version: 0,
		KeySize: uint32(actualParameters.KeySizeInBytes()),
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
	format := new(aescmacprfpb.AesCmacPrfKeyFormat)
	if err := proto.Unmarshal(keyTemplate.GetValue(), format); err != nil {
		return nil, err
	}
	if format.GetVersion() != 0 {
		return nil, fmt.Errorf("key has unsupported version: %v", format.GetVersion())
	}
	parameters, err := NewParameters(int(format.GetKeySize()))
	if err != nil {
		return nil, err
	}
	return &parameters, nil
}
