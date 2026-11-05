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

package jwt

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/jwt/jwtecdsa"
	"github.com/tink-crypto/tink-go/v2/jwt/jwtrsassapkcs1"
	"github.com/tink-crypto/tink-go/v2/jwt/jwtrsassapss"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/signature/ecdsa"
	"github.com/tink-crypto/tink-go/v2/signature/rsassapkcs1"
	"github.com/tink-crypto/tink-go/v2/signature/rsassapss"
	"github.com/tink-crypto/tink-go/v2/tink"
)

func ecdsaCurveAndHashFromJWTAlgorithm(algorithm jwtecdsa.Algorithm) (ecdsa.CurveType, ecdsa.HashType, error) {
	switch algorithm {
	case jwtecdsa.ES256:
		return ecdsa.NistP256, ecdsa.SHA256, nil
	case jwtecdsa.ES384:
		return ecdsa.NistP384, ecdsa.SHA384, nil
	case jwtecdsa.ES512:
		return ecdsa.NistP521, ecdsa.SHA512, nil
	default:
		return ecdsa.UnknownCurveType, ecdsa.UnknownHashType, fmt.Errorf("unsupported algorithm: %s", algorithm)
	}
}

// Implementation of the [Signer] interface as a full primitive.
//
// A full primitive is bound to a specific key ID.
type fullSigner struct {
	sKID *signerWithKID
	// keyID is set only for TINK keys. Keys with strategy CustomKID or IgnoreKID
	// are RAW, thus this is nil.
	keyID *string
}

var _ Signer = (*fullSigner)(nil)

func (s *fullSigner) SignAndEncode(rawJWT *RawJWT) (string, error) {
	return s.sKID.SignAndEncodeWithKID(rawJWT, s.keyID)
}

func newFullSigner(hasIDRequirement, isCustomKID bool, kid string, algorithm string, signer tink.Signer) (any, error) {
	var err error
	var sKID *signerWithKID
	if isCustomKID {
		// In this case we do have a KID, but it is custom, so we pass it to
		// newSignerWithKID, and set a nil kid.
		sKID, err = newSignerWithKID(signer, algorithm, &kid)
	} else {
		// No custom KID, the key is either TINK or RAW.
		sKID, err = newSignerWithKID(signer, algorithm, nil)
	}
	if err != nil {
		return nil, err
	}

	if hasIDRequirement {
		// TINK.
		return &fullSigner{
			sKID:  sKID,
			keyID: &kid,
		}, nil
	}
	// RAW.
	return &fullSigner{
		sKID:  sKID,
		keyID: nil,
	}, nil
}

func createJWTECDSASigner(key key.Key) (any, error) {
	privateKey, ok := key.(*jwtecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("expected %T, got %T", (*jwtecdsa.PrivateKey)(nil), key)
	}
	publicKey, err := privateKey.PublicKey()
	if err != nil {
		return nil, err
	}
	jwtPublicKey := publicKey.(*jwtecdsa.PublicKey)
	jwtParams := privateKey.Parameters().(*jwtecdsa.Parameters)
	curveType, hashType, err := ecdsaCurveAndHashFromJWTAlgorithm(jwtParams.Algorithm())
	if err != nil {
		return nil, err
	}
	ecdsaParams, err := ecdsa.NewParameters(curveType, hashType, ecdsa.IEEEP1363, ecdsa.VariantNoPrefix)
	if err != nil {
		return nil, err
	}
	ecdsaPrivateKey, err := ecdsa.NewPrivateKey(privateKey.PrivateKeyValue(), 0, ecdsaParams)
	if err != nil {
		return nil, err
	}
	ecdsaSigner, err := ecdsa.NewSigner(ecdsaPrivateKey, internalapi.Token{})
	if err != nil {
		return nil, err
	}

	kid, _ := jwtPublicKey.KID()
	return newFullSigner(jwtParams.HasIDRequirement(), jwtParams.KIDStrategy() == jwtecdsa.CustomKID, kid, jwtParams.Algorithm().String(), ecdsaSigner)
}

func hashTypeFromJWTRSAlgorithm(algorithm jwtrsassapkcs1.Algorithm) (rsassapkcs1.HashType, error) {
	switch algorithm {
	case jwtrsassapkcs1.RS256:
		return rsassapkcs1.SHA256, nil
	case jwtrsassapkcs1.RS384:
		return rsassapkcs1.SHA384, nil
	case jwtrsassapkcs1.RS512:
		return rsassapkcs1.SHA512, nil
	default:
		return rsassapkcs1.UnknownHashType, fmt.Errorf("unsupported algorithm: %s", algorithm)
	}
}

func createJWTRSASSAPKCS1Signer(key key.Key) (any, error) {
	privateKey, ok := key.(*jwtrsassapkcs1.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("expected %T, got %T", (*jwtrsassapkcs1.PrivateKey)(nil), key)
	}
	publicKey, err := privateKey.PublicKey()
	if err != nil {
		return nil, err
	}
	jwtPublicKey := publicKey.(*jwtrsassapkcs1.PublicKey)
	jwtParams := privateKey.Parameters().(*jwtrsassapkcs1.Parameters)

	hashType, err := hashTypeFromJWTRSAlgorithm(jwtParams.Algorithm())
	if err != nil {
		return nil, err
	}

	rsaSSAPKCS1Params, err := rsassapkcs1.NewParameters(jwtParams.ModulusSizeInBits(), hashType, jwtParams.PublicExponent(), rsassapkcs1.VariantNoPrefix)
	if err != nil {
		return nil, err
	}
	rsaSSAPKCS1PublicKey, err := rsassapkcs1.NewPublicKey(jwtPublicKey.Modulus(), 0, rsaSSAPKCS1Params)
	if err != nil {
		return nil, err
	}
	rsaSSAPKCS1PrivateKey, err := rsassapkcs1.NewPrivateKey(rsaSSAPKCS1PublicKey, rsassapkcs1.PrivateKeyValues{
		P: privateKey.P(),
		Q: privateKey.Q(),
		D: privateKey.D(),
	})
	if err != nil {
		return nil, err
	}
	rsaSSAPKCS1Signer, err := rsassapkcs1.NewSigner(rsaSSAPKCS1PrivateKey, internalapi.Token{})
	if err != nil {
		return nil, err
	}

	kid, _ := jwtPublicKey.KID()
	return newFullSigner(jwtParams.HasIDRequirement(), jwtParams.KIDStrategy() == jwtrsassapkcs1.CustomKID, kid, jwtParams.Algorithm().String(), rsaSSAPKCS1Signer)
}

func hashTypeAndSaltLengthFromJWTPSAlgorithm(algorithm jwtrsassapss.Algorithm) (rsassapss.HashType, int, error) {
	switch algorithm {
	case jwtrsassapss.PS256:
		return rsassapss.SHA256, 32, nil
	case jwtrsassapss.PS384:
		return rsassapss.SHA384, 48, nil
	case jwtrsassapss.PS512:
		return rsassapss.SHA512, 64, nil
	default:
		return rsassapss.UnknownHashType, 0, fmt.Errorf("unsupported algorithm: %s", algorithm)
	}
}

func createJWTRSASSAPSSSigner(key key.Key) (any, error) {
	privateKey, ok := key.(*jwtrsassapss.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("expected %T, got %T", (*jwtrsassapss.PrivateKey)(nil), key)
	}
	publicKey, err := privateKey.PublicKey()
	if err != nil {
		return nil, err
	}
	jwtPublicKey := publicKey.(*jwtrsassapss.PublicKey)
	jwtParams := privateKey.Parameters().(*jwtrsassapss.Parameters)

	hashType, saltLen, err := hashTypeAndSaltLengthFromJWTPSAlgorithm(jwtParams.Algorithm())
	if err != nil {
		return nil, err
	}

	rsaSSAPSSParams, err := rsassapss.NewParameters(rsassapss.ParametersValues{
		ModulusSizeBits: jwtParams.ModulusSizeInBits(),
		PublicExponent:  jwtParams.PublicExponent(),
		SigHashType:     hashType,
		MGF1HashType:    hashType,
		SaltLengthBytes: saltLen,
	}, rsassapss.VariantNoPrefix)
	if err != nil {
		return nil, err
	}
	rsaSSAPSSPublicKey, err := rsassapss.NewPublicKey(jwtPublicKey.Modulus(), 0, rsaSSAPSSParams)
	if err != nil {
		return nil, err
	}
	rsaSSAPSSPrivateKey, err := rsassapss.NewPrivateKey(rsaSSAPSSPublicKey, rsassapss.PrivateKeyValues{
		P: privateKey.P(),
		Q: privateKey.Q(),
		D: privateKey.D(),
	})
	if err != nil {
		return nil, err
	}
	rsaSSAPSSSigner, err := rsassapss.NewSigner(rsaSSAPSSPrivateKey, internalapi.Token{})
	if err != nil {
		return nil, err
	}

	kid, _ := jwtPublicKey.KID()
	return newFullSigner(jwtParams.HasIDRequirement(), jwtParams.KIDStrategy() == jwtrsassapss.CustomKID, kid, jwtParams.Algorithm().String(), rsaSSAPSSSigner)
}

// Implementation of the [Verifier] interface as a full primitive.
//
// A full primitive is bound to a specific key ID.
type fullVerifier struct {
	vKID *verifierWithKID
	// keyID is set only for TINK keys. Keys with strategy CustomKID or IgnoreKID
	// are RAW, thus this is nil.
	keyID *string
}

var _ Verifier = (*fullVerifier)(nil)

func (s *fullVerifier) VerifyAndDecode(compact string, validator *Validator) (*VerifiedJWT, error) {
	return s.vKID.VerifyAndDecodeWithKID(compact, validator, s.keyID)
}

func newFullVerifier(hasIDRequirement, isCustomKID bool, kid string, algorithm string, signer tink.Verifier) (any, error) {
	var err error
	var vKID *verifierWithKID
	if isCustomKID {
		// In this case we do have a KID, but it is custom, so we pass it to
		// newVerifierWithKID, and set a nil kid.
		vKID, err = newVerifierWithKID(signer, algorithm, &kid)
	} else {
		// No custom KID, the key is either TINK or RAW.
		vKID, err = newVerifierWithKID(signer, algorithm, nil)
	}
	if err != nil {
		return nil, err
	}

	if hasIDRequirement {
		// TINK.
		return &fullVerifier{
			vKID:  vKID,
			keyID: &kid,
		}, nil
	}
	// RAW.
	return &fullVerifier{
		vKID:  vKID,
		keyID: nil,
	}, nil
}

func createJWTECDSAVerifier(key key.Key) (any, error) {
	jwtPublicKey, ok := key.(*jwtecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("expected %T, got %T", (*jwtecdsa.PublicKey)(nil), key)
	}
	jwtParams := jwtPublicKey.Parameters().(*jwtecdsa.Parameters)
	curveType, hashType, err := ecdsaCurveAndHashFromJWTAlgorithm(jwtParams.Algorithm())
	if err != nil {
		return nil, err
	}
	ecdsaParams, err := ecdsa.NewParameters(curveType, hashType, ecdsa.IEEEP1363, ecdsa.VariantNoPrefix)
	if err != nil {
		return nil, err
	}
	ecdsaPublicKey, err := ecdsa.NewPublicKey(jwtPublicKey.PublicPoint(), 0, ecdsaParams)
	if err != nil {
		return nil, err
	}
	ecdsaVerifier, err := ecdsa.NewVerifier(ecdsaPublicKey, internalapi.Token{})
	if err != nil {
		return nil, err
	}
	kid, _ := jwtPublicKey.KID()
	return newFullVerifier(jwtParams.HasIDRequirement(), jwtParams.KIDStrategy() == jwtecdsa.CustomKID, kid, jwtParams.Algorithm().String(), ecdsaVerifier)
}

func createJWTRSASSAPKCS1Verifier(key key.Key) (any, error) {
	jwtPublicKey, ok := key.(*jwtrsassapkcs1.PublicKey)
	if !ok {
		return nil, fmt.Errorf("expected %T, got %T", (*jwtrsassapkcs1.PublicKey)(nil), key)
	}
	jwtParams := jwtPublicKey.Parameters().(*jwtrsassapkcs1.Parameters)

	hashType, err := hashTypeFromJWTRSAlgorithm(jwtParams.Algorithm())
	if err != nil {
		return nil, err
	}

	rsaSSAPKCS1Params, err := rsassapkcs1.NewParameters(jwtParams.ModulusSizeInBits(), hashType, jwtParams.PublicExponent(), rsassapkcs1.VariantNoPrefix)
	if err != nil {
		return nil, err
	}
	rsaSSAPKCS1PublicKey, err := rsassapkcs1.NewPublicKey(jwtPublicKey.Modulus(), 0, rsaSSAPKCS1Params)
	if err != nil {
		return nil, err
	}
	rsaSSAPKCS1Verifier, err := rsassapkcs1.NewVerifier(rsaSSAPKCS1PublicKey, internalapi.Token{})
	if err != nil {
		return nil, err
	}

	kid, _ := jwtPublicKey.KID()
	return newFullVerifier(jwtParams.HasIDRequirement(), jwtParams.KIDStrategy() == jwtrsassapkcs1.CustomKID, kid, jwtParams.Algorithm().String(), rsaSSAPKCS1Verifier)
}

func createJWTRSASSAPSSVerifier(key key.Key) (any, error) {
	jwtPublicKey, ok := key.(*jwtrsassapss.PublicKey)
	if !ok {
		return nil, fmt.Errorf("expected %T, got %T", (*jwtrsassapss.PublicKey)(nil), key)
	}
	jwtParams := jwtPublicKey.Parameters().(*jwtrsassapss.Parameters)

	hashType, saltLen, err := hashTypeAndSaltLengthFromJWTPSAlgorithm(jwtParams.Algorithm())
	if err != nil {
		return nil, err
	}

	rsaSSAPSSParams, err := rsassapss.NewParameters(rsassapss.ParametersValues{
		ModulusSizeBits: jwtParams.ModulusSizeInBits(),
		PublicExponent:  jwtParams.PublicExponent(),
		SigHashType:     hashType,
		MGF1HashType:    hashType,
		SaltLengthBytes: saltLen,
	}, rsassapss.VariantNoPrefix)
	if err != nil {
		return nil, err
	}
	rsaSSAPSSPublicKey, err := rsassapss.NewPublicKey(jwtPublicKey.Modulus(), 0, rsaSSAPSSParams)
	if err != nil {
		return nil, err
	}
	rsaSSAPSSVerifier, err := rsassapss.NewVerifier(rsaSSAPSSPublicKey, internalapi.Token{})
	if err != nil {
		return nil, err
	}

	kid, _ := jwtPublicKey.KID()
	return newFullVerifier(jwtParams.HasIDRequirement(), jwtParams.KIDStrategy() == jwtrsassapss.CustomKID, kid, jwtParams.Algorithm().String(), rsaSSAPSSVerifier)
}
