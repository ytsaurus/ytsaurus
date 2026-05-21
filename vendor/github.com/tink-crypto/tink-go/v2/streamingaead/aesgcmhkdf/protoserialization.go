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

package aesgcmhkdf

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	streamaeadpb "github.com/tink-crypto/tink-go/v2/proto/aes_gcm_hkdf_streaming_go_proto"
	commonpb "github.com/tink-crypto/tink-go/v2/proto/common_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

const typeURL = "type.googleapis.com/google.crypto.tink.AesGcmHkdfStreamingKey"

type keySerializer struct{}

var _ protoserialization.KeySerializer = (*keySerializer)(nil)

func hashTypeToProto(ht HashType) (commonpb.HashType, error) {
	switch ht {
	case SHA1:
		return commonpb.HashType_SHA1, nil
	case SHA256:
		return commonpb.HashType_SHA256, nil
	case SHA512:
		return commonpb.HashType_SHA512, nil
	default:
		return commonpb.HashType_UNKNOWN_HASH, fmt.Errorf("unknown hash type: %v", ht)
	}
}

func (s *keySerializer) SerializeKey(k key.Key) (*protoserialization.KeySerialization, error) {
	actualKey, ok := k.(*Key)
	if !ok {
		return nil, fmt.Errorf("invalid key type: got %T, want %T", k, (*Key)(nil))
	}
	// Case where the key is empty.
	if actualKey.KeyBytes().Len() == 0 {
		return nil, fmt.Errorf("invalid key")
	}
	actualParameters, ok := actualKey.Parameters().(*Parameters)
	if !ok {
		return nil, fmt.Errorf("key parameters is not a aesgcmhkdf.Parameters")
	}
	hkdfHashType, err := hashTypeToProto(actualParameters.HKDFHashType())
	if err != nil {
		return nil, err
	}
	protoKey := &streamaeadpb.AesGcmHkdfStreamingKey{
		Version:  0,
		KeyValue: actualKey.KeyBytes().Data(insecuresecretdataaccess.Token{}),
		Params: &streamaeadpb.AesGcmHkdfStreamingParams{
			HkdfHashType:          hkdfHashType,
			DerivedKeySize:        uint32(actualParameters.DerivedKeySizeInBytes()),
			CiphertextSegmentSize: uint32(actualParameters.SegmentSizeInBytes()),
		},
	}
	serializedKey, err := proto.Marshal(protoKey)
	if err != nil {
		return nil, err
	}
	keyData := &tinkpb.KeyData{
		TypeUrl:         typeURL,
		Value:           serializedKey,
		KeyMaterialType: tinkpb.KeyData_SYMMETRIC,
	}
	return protoserialization.NewKeySerialization(keyData, tinkpb.OutputPrefixType_RAW, 0)
}

type keyParser struct{}

var _ protoserialization.KeyParser = (*keyParser)(nil)

func hashTypeFromProto(ht commonpb.HashType) (HashType, error) {
	switch ht {
	case commonpb.HashType_SHA1:
		return SHA1, nil
	case commonpb.HashType_SHA256:
		return SHA256, nil
	case commonpb.HashType_SHA512:
		return SHA512, nil
	default:
		return HashTypeUnknown, fmt.Errorf("unknown hash type: %v", ht)
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
	protoKey := new(streamaeadpb.AesGcmHkdfStreamingKey)
	if err := proto.Unmarshal(keyData.GetValue(), protoKey); err != nil {
		return nil, err
	}
	if protoKey.GetVersion() != 0 {
		return nil, fmt.Errorf("unsupported key version: got %q, want %q", protoKey.GetVersion(), 0)
	}
	paramsProto := protoKey.GetParams()
	hkdfHashType, err := hashTypeFromProto(paramsProto.GetHkdfHashType())
	if err != nil {
		return nil, err
	}
	params, err := NewParameters(ParametersOpts{
		KeySizeInBytes:        len(protoKey.GetKeyValue()),
		DerivedKeySizeInBytes: int(paramsProto.GetDerivedKeySize()),
		HKDFHashType:          hkdfHashType,
		SegmentSizeInBytes:    int32(paramsProto.GetCiphertextSegmentSize()),
	})
	if err != nil {
		return nil, err
	}
	keyBytes := secretdata.NewBytesFromData(protoKey.GetKeyValue(), insecuresecretdataaccess.Token{})
	return NewKey(params, keyBytes)
}

type parametersSerializer struct{}

var _ protoserialization.ParametersSerializer = (*parametersSerializer)(nil)

func (s *parametersSerializer) Serialize(params key.Parameters) (*tinkpb.KeyTemplate, error) {
	actualParams, ok := params.(*Parameters)
	if !ok {
		return nil, fmt.Errorf("parameters is not a %T", (*Parameters)(nil))
	}
	hkdfHashType, err := hashTypeToProto(actualParams.HKDFHashType())
	if err != nil {
		return nil, fmt.Errorf("invalid HKDF hash type: %v", err)
	}
	keyFormat := &streamaeadpb.AesGcmHkdfStreamingKeyFormat{
		Version: 0,
		Params: &streamaeadpb.AesGcmHkdfStreamingParams{
			HkdfHashType:          hkdfHashType,
			DerivedKeySize:        uint32(actualParams.DerivedKeySizeInBytes()),
			CiphertextSegmentSize: uint32(actualParams.SegmentSizeInBytes()),
		},
		KeySize: uint32(actualParams.KeySizeInBytes()),
	}
	serializedParams, err := proto.Marshal(keyFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal key format: %v", err)
	}
	return &tinkpb.KeyTemplate{
		TypeUrl:          typeURL,
		Value:            serializedParams,
		OutputPrefixType: tinkpb.OutputPrefixType_RAW,
	}, nil
}

type parametersParser struct{}

var _ protoserialization.ParametersParser = (*parametersParser)(nil)

func (s *parametersParser) Parse(kt *tinkpb.KeyTemplate) (key.Parameters, error) {
	if kt == nil {
		return nil, fmt.Errorf("parameters serialization is nil")
	}
	if kt.GetTypeUrl() != typeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", kt.GetTypeUrl(), typeURL)
	}
	if kt.GetOutputPrefixType() != tinkpb.OutputPrefixType_RAW {
		return nil, fmt.Errorf("output prefix type is not RAW")
	}
	keyFormat := new(streamaeadpb.AesGcmHkdfStreamingKeyFormat)
	if err := proto.Unmarshal(kt.GetValue(), keyFormat); err != nil {
		return nil, fmt.Errorf("failed to unmarshal key format: %v", err)
	}
	if keyFormat.GetVersion() != 0 {
		return nil, fmt.Errorf("unsupported key version: got %d, want %d", keyFormat.GetVersion(), 0)
	}
	protoParams := keyFormat.GetParams()
	hkdfHashType, err := hashTypeFromProto(protoParams.GetHkdfHashType())
	if err != nil {
		return nil, fmt.Errorf("invalid HKDF hash type: %v", err)
	}
	return NewParameters(ParametersOpts{
		KeySizeInBytes:        int(keyFormat.GetKeySize()),
		DerivedKeySizeInBytes: int(protoParams.GetDerivedKeySize()),
		HKDFHashType:          hkdfHashType,
		SegmentSizeInBytes:    int32(protoParams.GetCiphertextSegmentSize()),
	})
}
