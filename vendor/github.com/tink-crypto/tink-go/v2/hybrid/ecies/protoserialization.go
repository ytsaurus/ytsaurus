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

package ecies

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/ec"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/secretdata"

	commonpb "github.com/tink-crypto/tink-go/v2/proto/common_go_proto"
	eciespb "github.com/tink-crypto/tink-go/v2/proto/ecies_aead_hkdf_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

const (
	publicKeyTypeURL  = "type.googleapis.com/google.crypto.tink.EciesAeadHkdfPublicKey"
	privateKeyTypeURL = "type.googleapis.com/google.crypto.tink.EciesAeadHkdfPrivateKey"
)

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

type publicKeySerializer struct{}

var _ protoserialization.KeySerializer = (*publicKeySerializer)(nil)

func protoCurveFromCurveType(curveType CurveType) (commonpb.EllipticCurveType, error) {
	switch curveType {
	case NISTP256:
		return commonpb.EllipticCurveType_NIST_P256, nil
	case NISTP384:
		return commonpb.EllipticCurveType_NIST_P384, nil
	case NISTP521:
		return commonpb.EllipticCurveType_NIST_P521, nil
	case X25519:
		return commonpb.EllipticCurveType_CURVE25519, nil
	default:
		return commonpb.EllipticCurveType_UNKNOWN_CURVE, fmt.Errorf("unknown curve type: %v", curveType)
	}
}

func protoHashTypeFromHashType(hashType HashType) (commonpb.HashType, error) {
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
		return commonpb.HashType_UNKNOWN_HASH, fmt.Errorf("unknown hash type: %v", hashType)
	}
}

func protoEcPointFormatFromPointFormat(pointFormat PointFormat) (commonpb.EcPointFormat, error) {
	switch pointFormat {
	case CompressedPointFormat:
		return commonpb.EcPointFormat_COMPRESSED, nil
	case UncompressedPointFormat:
		return commonpb.EcPointFormat_UNCOMPRESSED, nil
	case LegacyUncompressedPointFormat:
		return commonpb.EcPointFormat_DO_NOT_USE_CRUNCHY_UNCOMPRESSED, nil
	case UnspecifiedPointFormat:
		// This is unspecified only for X25519, so we set it to COMPRESSED.
		return commonpb.EcPointFormat_COMPRESSED, nil
	default:
		return commonpb.EcPointFormat_UNKNOWN_FORMAT, fmt.Errorf("unknown point format: %v ", pointFormat)
	}
}

func createProtoECIESParams(p *Parameters) (*eciespb.EciesAeadHkdfParams, error) {
	curveType, err := protoCurveFromCurveType(p.CurveType())
	if err != nil {
		return nil, err
	}

	protoHashType, err := protoHashTypeFromHashType(p.HashType())
	if err != nil {
		return nil, err
	}

	protoDEMParams, err := protoserialization.SerializeParameters(p.DEMParameters())
	if err != nil {
		return nil, err
	}
	// NOTE: Always set the output prefix type to TINK for backward compatibility.
	protoDEMParams.OutputPrefixType = tinkpb.OutputPrefixType_TINK

	pointFormat, err := protoEcPointFormatFromPointFormat(p.NISTCurvePointFormat())
	if err != nil {
		return nil, err
	}

	return &eciespb.EciesAeadHkdfParams{
		KemParams: &eciespb.EciesHkdfKemParams{
			CurveType:    curveType,
			HkdfHashType: protoHashType,
			HkdfSalt:     p.Salt(),
		},
		DemParams: &eciespb.EciesAeadDemParams{
			AeadDem: protoDEMParams,
		},
		EcPointFormat: pointFormat,
	}, nil
}

func coordinateSizeForCurve(curveType CurveType) (int, error) {
	switch curveType {
	case NISTP256:
		return 32, nil
	case NISTP384:
		return 48, nil
	case NISTP521:
		return 66, nil
	default:
		return 0, fmt.Errorf("unsupported curve: %v", curveType)
	}
}

func publicKeyToProtoPublicKey(publicKey *PublicKey) (*eciespb.EciesAeadHkdfPublicKey, error) {
	if publicKey == nil {
		return nil, fmt.Errorf("public key is nil")
	}

	eciesParams := publicKey.Parameters().(*Parameters)
	// This is nil if PublicKey was created as a struct literal.
	if eciesParams == nil {
		return nil, fmt.Errorf("key has nil parameters")
	}

	protoECIESParams, err := createProtoECIESParams(eciesParams)
	if err != nil {
		return nil, err
	}

	protoPublicKey := &eciespb.EciesAeadHkdfPublicKey{
		Version: 0,
		Params:  protoECIESParams,
	}

	switch eciesParams.CurveType() {
	case NISTP256, NISTP384, NISTP521:
		// Encoding must be as per [SEC 1 v2.0, Section 2.3.3]. This function adds
		// an extra leading 0x00 byte to the coordinates for compatibility with
		// other Tink implementations (see b/264525021).
		coordinateSize, err := coordinateSizeForCurve(eciesParams.CurveType())
		if err != nil {
			return nil, err
		}
		if len(publicKey.PublicKeyBytes()) != 2*coordinateSize+1 {
			return nil, fmt.Errorf("public key point has invalid coordinate size: got %v, want %v", len(publicKey.PublicKeyBytes()), 2*coordinateSize+1)
		}
		if publicKey.PublicKeyBytes()[0] != 0x04 {
			return nil, fmt.Errorf("public key has invalid 1st byte: got %x, want %x", publicKey.PublicKeyBytes()[0], 0x04)
		}
		xy := publicKey.PublicKeyBytes()[1:]
		protoPublicKey.X, err = ec.BigIntBytesToFixedSizeBuffer(xy[:coordinateSize], coordinateSize+1)
		if err != nil {
			return nil, err
		}
		protoPublicKey.Y, err = ec.BigIntBytesToFixedSizeBuffer(xy[coordinateSize:], coordinateSize+1)
		if err != nil {
			return nil, err
		}
	case X25519:
		protoPublicKey.X = publicKey.PublicKeyBytes()
	default:
		return nil, fmt.Errorf("unsupported curve type: %v", eciesParams.CurveType())
	}
	return protoPublicKey, nil
}

func (s *publicKeySerializer) SerializeKey(key key.Key) (*protoserialization.KeySerialization, error) {
	eciesPublicKey, ok := key.(*PublicKey)
	if !ok {
		return nil, fmt.Errorf("key is of type %T, want %T", key, (*PublicKey)(nil))
	}

	protoPublicKey, err := publicKeyToProtoPublicKey(eciesPublicKey)
	if err != nil {
		return nil, err
	}

	serializedECIESPubKey, err := proto.Marshal(protoPublicKey)
	if err != nil {
		return nil, err
	}

	outputPrefixType, err := protoOutputPrefixTypeFromVariant(eciesPublicKey.Parameters().(*Parameters).Variant())
	if err != nil {
		return nil, err
	}

	// idRequirement is zero if the key doesn't have a key requirement.
	idRequirement, _ := eciesPublicKey.IDRequirement()
	keyData := &tinkpb.KeyData{
		TypeUrl:         publicKeyTypeURL,
		Value:           serializedECIESPubKey,
		KeyMaterialType: tinkpb.KeyData_ASYMMETRIC_PUBLIC,
	}
	return protoserialization.NewKeySerialization(keyData, outputPrefixType, idRequirement)
}

type publicKeyParser struct{}

var _ protoserialization.KeyParser = (*publicKeyParser)(nil)

func curveTypeFromProto(curveType commonpb.EllipticCurveType) (CurveType, error) {
	switch curveType {
	case commonpb.EllipticCurveType_NIST_P256:
		return NISTP256, nil
	case commonpb.EllipticCurveType_NIST_P384:
		return NISTP384, nil
	case commonpb.EllipticCurveType_NIST_P521:
		return NISTP521, nil
	case commonpb.EllipticCurveType_CURVE25519:
		return X25519, nil
	default:
		return UnknownCurveType, fmt.Errorf("unknown curve type: %v", curveType)
	}
}

func hashTypeFromProto(hashType commonpb.HashType) (HashType, error) {
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
		return UnknownHashType, fmt.Errorf("unknown hash type: %v", hashType)
	}
}

func variantFromProto(outputPrefixType tinkpb.OutputPrefixType) (Variant, error) {
	switch outputPrefixType {
	case tinkpb.OutputPrefixType_TINK:
		return VariantTink, nil
	case tinkpb.OutputPrefixType_CRUNCHY, tinkpb.OutputPrefixType_LEGACY:
		return VariantCrunchy, nil
	case tinkpb.OutputPrefixType_RAW:
		return VariantNoPrefix, nil
	default:
		return VariantUnknown, fmt.Errorf("unknown output prefix: %v", outputPrefixType)
	}
}

func pointFormatFromProtoPointFormat(pointFormat commonpb.EcPointFormat) (PointFormat, error) {
	switch pointFormat {
	case commonpb.EcPointFormat_COMPRESSED:
		return CompressedPointFormat, nil
	case commonpb.EcPointFormat_UNCOMPRESSED:
		return UncompressedPointFormat, nil
	case commonpb.EcPointFormat_DO_NOT_USE_CRUNCHY_UNCOMPRESSED:
		return LegacyUncompressedPointFormat, nil
	default:
		return UnspecifiedPointFormat, fmt.Errorf("unknown point format: %v ", pointFormat)
	}
}

func parseParameters(protoParams *eciespb.EciesAeadHkdfParams, outputPrefixType tinkpb.OutputPrefixType) (*Parameters, error) {
	curveType, err := curveTypeFromProto(protoParams.GetKemParams().GetCurveType())
	if err != nil {
		return nil, err
	}
	hashType, err := hashTypeFromProto(protoParams.GetKemParams().GetHkdfHashType())
	if err != nil {
		return nil, err
	}
	variant, err := variantFromProto(outputPrefixType)
	if err != nil {
		return nil, err
	}
	pointFormat, err := pointFormatFromProtoPointFormat(protoParams.GetEcPointFormat())
	if err != nil {
		return nil, err
	}

	if protoParams.GetDemParams() == nil {
		return nil, fmt.Errorf("nil DEM params")
	}
	if protoParams.GetDemParams().GetAeadDem() == nil {
		return nil, fmt.Errorf("nil AEAD DEM")
	}
	// NOTE: Ignore the the output prefix type and use RAW for backward compatibility.
	demTemplate := proto.Clone(protoParams.GetDemParams().GetAeadDem()).(*tinkpb.KeyTemplate)
	demTemplate.OutputPrefixType = tinkpb.OutputPrefixType_RAW
	demParams, err := protoserialization.ParseParameters(demTemplate)
	if err != nil {
		return nil, err
	}
	if curveType == X25519 {
		if pointFormat != CompressedPointFormat {
			return nil, fmt.Errorf("for X25519, point format must be COMPRESSED, got %v", pointFormat)
		}
		// Leave unspecified for X25519.
		pointFormat = UnspecifiedPointFormat
	}

	return NewParameters(ParametersOpts{
		CurveType:            curveType,
		HashType:             hashType,
		Variant:              variant,
		NISTCurvePointFormat: pointFormat,
		DEMParameters:        demParams,
		Salt:                 protoParams.GetKemParams().GetHkdfSalt(),
	})
}

func parsePublicKey(publicKey *eciespb.EciesAeadHkdfPublicKey, outputPrefixType tinkpb.OutputPrefixType, keyID uint32) (*PublicKey, error) {
	if publicKey.GetVersion() != 0 {
		return nil, fmt.Errorf("invalid key version: %v, want 0", publicKey.GetVersion())
	}

	params, err := parseParameters(publicKey.GetParams(), outputPrefixType)
	if err != nil {
		return nil, err
	}

	var publicKeyBytes []byte
	if params.CurveType() == X25519 {
		publicKeyBytes = publicKey.GetX()
	} else {
		coordinateSize, err := coordinateSizeForCurve(params.CurveType())
		if err != nil {
			return nil, err
		}
		// Tolerate arbitrary leading zeros in the coordinates.
		// This is to support the case where the curve size in bytes + 1 is the
		// length of the coordinate. This happens when Tink adds an extra leading
		// 0x00 byte (see b/264525021).
		x, err := ec.BigIntBytesToFixedSizeBuffer(publicKey.GetX(), coordinateSize)
		if err != nil {
			return nil, err
		}
		y, err := ec.BigIntBytesToFixedSizeBuffer(publicKey.GetY(), coordinateSize)
		if err != nil {
			return nil, err
		}
		publicKeyBytes = slices.Concat([]byte{0x04}, x, y)
	}
	return NewPublicKey(publicKeyBytes, keyID, params)
}

func (s *publicKeyParser) ParseKey(keySerialization *protoserialization.KeySerialization) (key.Key, error) {
	if keySerialization == nil {
		return nil, fmt.Errorf("key serialization is nil")
	}
	keyData := keySerialization.KeyData()
	if keyData.GetTypeUrl() != publicKeyTypeURL {
		return nil, fmt.Errorf("invalid key type URL %v, want %v", keyData.GetTypeUrl(), publicKeyTypeURL)
	}
	if keyData.GetKeyMaterialType() != tinkpb.KeyData_ASYMMETRIC_PUBLIC {
		return nil, fmt.Errorf("invalid key material type: %v", keyData.GetKeyMaterialType())
	}
	protoECIESKey := new(eciespb.EciesAeadHkdfPublicKey)
	if err := proto.Unmarshal(keyData.GetValue(), protoECIESKey); err != nil {
		return nil, err
	}

	// keySerialization.IDRequirement() returns zero if the key doesn't have a key requirement.
	keyID, _ := keySerialization.IDRequirement()
	return parsePublicKey(protoECIESKey, keySerialization.OutputPrefixType(), keyID)
}

type privateKeySerializer struct{}

var _ protoserialization.KeySerializer = (*privateKeySerializer)(nil)

func (s *privateKeySerializer) SerializeKey(key key.Key) (*protoserialization.KeySerialization, error) {
	if key == nil {
		return nil, fmt.Errorf("key is nil")
	}
	eciesPrivateKey, ok := key.(*PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is of type %T, want %T", key, (*PrivateKey)(nil))
	}

	publicKey, err := eciesPrivateKey.PublicKey()
	if err != nil {
		return nil, err
	}
	protoPublicKey, err := publicKeyToProtoPublicKey(publicKey.(*PublicKey))
	if err != nil {
		return nil, err
	}

	privateKeyValue := eciesPrivateKey.PrivateKeyBytes().Data(insecuresecretdataaccess.Token{})
	if eciesPrivateKey.Parameters().(*Parameters).CurveType() != X25519 {
		// This must be padded with at least one leading zero (see b/264525021).
		coordinateSize, err := coordinateSizeForCurve(eciesPrivateKey.Parameters().(*Parameters).CurveType())
		if err != nil {
			return nil, err
		}
		privateKeyValue, err = ec.BigIntBytesToFixedSizeBuffer(eciesPrivateKey.PrivateKeyBytes().Data(insecuresecretdataaccess.Token{}), coordinateSize+1)
		if err != nil {
			return nil, err
		}
	}

	protoPrivateKey := &eciespb.EciesAeadHkdfPrivateKey{
		Version:   0,
		PublicKey: protoPublicKey,
		KeyValue:  privateKeyValue,
	}
	serializedECIESPrivKey, err := proto.Marshal(protoPrivateKey)
	if err != nil {
		return nil, err
	}

	outputPrefixType, err := protoOutputPrefixTypeFromVariant(eciesPrivateKey.Parameters().(*Parameters).Variant())
	if err != nil {
		return nil, err
	}

	// idRequirement is zero if the key doesn't have a key ID requirement.
	idRequirement, _ := eciesPrivateKey.IDRequirement()
	keyData := &tinkpb.KeyData{
		TypeUrl:         privateKeyTypeURL,
		Value:           serializedECIESPrivKey,
		KeyMaterialType: tinkpb.KeyData_ASYMMETRIC_PRIVATE,
	}
	return protoserialization.NewKeySerialization(keyData, outputPrefixType, idRequirement)
}

type privateKeyParser struct{}

var _ protoserialization.KeyParser = (*privateKeyParser)(nil)

func (s *privateKeyParser) ParseKey(keySerialization *protoserialization.KeySerialization) (key.Key, error) {
	if keySerialization == nil {
		return nil, fmt.Errorf("key serialization is nil")
	}
	keyData := keySerialization.KeyData()
	if keyData.GetTypeUrl() != privateKeyTypeURL {
		return nil, fmt.Errorf("invalid key type URL %v, want %v", keyData.GetTypeUrl(), privateKeyTypeURL)
	}
	if keyData.GetKeyMaterialType() != tinkpb.KeyData_ASYMMETRIC_PRIVATE {
		return nil, fmt.Errorf("invalid key material type: %v", keyData.GetKeyMaterialType())
	}
	protoECIESKey := new(eciespb.EciesAeadHkdfPrivateKey)
	if err := proto.Unmarshal(keyData.GetValue(), protoECIESKey); err != nil {
		return nil, err
	}
	if protoECIESKey.GetVersion() != 0 {
		return nil, fmt.Errorf("invalid key version: %v, want 0", protoECIESKey.GetVersion())
	}
	// keySerialization.IDRequirement() returns zero if the key doesn't have a key requirement.
	keyID, _ := keySerialization.IDRequirement()

	publicKey, err := parsePublicKey(protoECIESKey.GetPublicKey(), keySerialization.OutputPrefixType(), keyID)
	if err != nil {
		return nil, err
	}

	privateKeyBytes := protoECIESKey.GetKeyValue()
	if protoECIESKey.GetPublicKey().GetParams().GetKemParams().GetCurveType() != commonpb.EllipticCurveType_CURVE25519 {
		// Tolerate arbitrary leading zeros in the private key.
		coordinateSize, err := coordinateSizeForCurve(publicKey.Parameters().(*Parameters).CurveType())
		if err != nil {
			return nil, err
		}
		privateKeyBytes, err = ec.BigIntBytesToFixedSizeBuffer(privateKeyBytes, coordinateSize)
		if err != nil {
			return nil, err
		}
	}
	return NewPrivateKeyFromPublicKey(secretdata.NewBytesFromData(privateKeyBytes, insecuresecretdataaccess.Token{}), publicKey)
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

	params, err := createProtoECIESParams(actualParameters)
	if err != nil {
		return nil, err
	}

	format := &eciespb.EciesAeadHkdfKeyFormat{
		Params: params,
	}
	serializedFormat, err := proto.Marshal(format)
	if err != nil {
		return nil, err
	}
	return &tinkpb.KeyTemplate{
		TypeUrl:          privateKeyTypeURL,
		OutputPrefixType: outputPrefixType,
		Value:            serializedFormat,
	}, nil
}

type parametersParser struct{}

var _ protoserialization.ParametersParser = (*parametersParser)(nil)

func (s *parametersParser) Parse(keyTemplate *tinkpb.KeyTemplate) (key.Parameters, error) {
	if keyTemplate.GetTypeUrl() != privateKeyTypeURL {
		return nil, fmt.Errorf("invalid type URL: got %q, want %q", keyTemplate.GetTypeUrl(), privateKeyTypeURL)
	}
	format := new(eciespb.EciesAeadHkdfKeyFormat)
	if err := proto.Unmarshal(keyTemplate.GetValue(), format); err != nil {
		return nil, err
	}
	return parseParameters(format.GetParams(), keyTemplate.GetOutputPrefixType())
}
