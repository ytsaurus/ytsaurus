// Copyright 2022 Google LLC
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

// Package jwt implements a subset of JSON Web Token (JWT) as defined by RFC 7519 (https://tools.ietf.org/html/rfc7519) that is considered safe and most often used.
package jwt

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/internal/legacykeymanager"
	"github.com/tink-crypto/tink-go/v2/internal/primitiveregistry"
	"github.com/tink-crypto/tink-go/v2/jwt/jwtecdsa"             // Also registers jwtecdsa keys and proto serialization.
	"github.com/tink-crypto/tink-go/v2/jwt/jwthmac"               // Also registers jwthmac keys and proto serialization.
	"github.com/tink-crypto/tink-go/v2/jwt/jwtrsassapkcs1" // Also registers jwtrsassapkcs1 keys and proto serialization.
	"github.com/tink-crypto/tink-go/v2/jwt/jwtrsassapss"     // Also registers jwtrsassapss keys and proto serialization.
	"github.com/tink-crypto/tink-go/v2/key"
	jepb "github.com/tink-crypto/tink-go/v2/proto/jwt_ecdsa_go_proto"
	jwtmacpb "github.com/tink-crypto/tink-go/v2/proto/jwt_hmac_go_proto"
	jrsppb "github.com/tink-crypto/tink-go/v2/proto/jwt_rsa_ssa_pkcs1_go_proto"
	jpsppb "github.com/tink-crypto/tink-go/v2/proto/jwt_rsa_ssa_pss_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

// A generic error returned when something went wrong before validation
var errJwtVerification = errors.New("verification failed")
var errJwtExpired = errors.New("token has expired")

// IsExpirationErr returns true if err was returned by a JWT verification for a token
// with a valid signature that is expired.
//
// Note that if the corresponding verification key has been removed from the keyset,
// verification will not return an expiration error even if the token is expired, because
// the expiration is only verified if the signature is valid.
func IsExpirationErr(err error) bool {
	return err == errJwtExpired
}

const (
	jwtECDSAVerifierTypeURL          = "type.googleapis.com/google.crypto.tink.JwtEcdsaPublicKey"
	jwtJWTRSASSAPKCS1VerifierTypeURL = "type.googleapis.com/google.crypto.tink.JwtRsaSsaPkcs1PublicKey"
	jwtJWTRSASSAPSSVerifierTypeURL   = "type.googleapis.com/google.crypto.tink.JwtRsaSsaPssPublicKey"
	jwtHMACTypeURL                   = "type.googleapis.com/google.crypto.tink.JwtHmacKey"
	jwtECDSASignerTypeURL            = "type.googleapis.com/google.crypto.tink.JwtEcdsaPrivateKey"
	jwtJWTRSASSAPKCS1SignerTypeURL   = "type.googleapis.com/google.crypto.tink.JwtRsaSsaPkcs1PrivateKey"
	jwtJWTRSASSAPSSSignerTypeURL     = "type.googleapis.com/google.crypto.tink.JwtRsaSsaPssPrivateKey"
)

func jwtHMACPrimitive(_ key.Key) (any, error) {
	return nil, fmt.Errorf("the key manager should not be used to obtain a new primitive from a JWT HMAC key")
}

func jwtECDSASignerPrimitive(_ key.Key) (any, error) {
	return nil, fmt.Errorf("the key manager should not be used to obtain a new primitive from a JWT ECDSA key")
}

func jwtRSASSAPKCS1Primitive(_ key.Key) (any, error) {
	return nil, fmt.Errorf("the key manager should not be used to obtain a new primitive from a JWT RSA SSA PKCS1 key")
}

func jwtRSASSAPSSPrimitive(_ key.Key) (any, error) {
	return nil, fmt.Errorf("the key manager should not be used to obtain a new primitive from a JWT RSA SSA PSS key")
}

func unmarshalJWTHMACKey(serializedKey []byte) (proto.Message, error) {
	key := &jwtmacpb.JwtHmacKey{}
	if err := proto.Unmarshal(serializedKey, key); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtHmacKey: %v", err)
	}
	return key, nil
}

func unmarshalJWTECDSAPrivateKey(serializedKey []byte) (proto.Message, error) {
	privKey := &jepb.JwtEcdsaPrivateKey{}
	if err := proto.Unmarshal(serializedKey, privKey); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtEcdsaPrivateKey: %v", err)
	}
	return privKey, nil
}

func unmarshalJWTRSASSAPKCS1PrivateKey(serializedKey []byte) (proto.Message, error) {
	privKey := &jrsppb.JwtRsaSsaPkcs1PrivateKey{}
	if err := proto.Unmarshal(serializedKey, privKey); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtRsaSsaPkcs1PrivateKey: %v", err)
	}
	return privKey, nil
}

func unmarshalJWTRSASSAPSSPrivateKey(serializedKey []byte) (proto.Message, error) {
	privKey := &jpsppb.JwtRsaSsaPssPrivateKey{}
	if err := proto.Unmarshal(serializedKey, privKey); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtRsaSsaPssPrivateKey: %v", err)
	}
	return privKey, nil
}

func unmarshalJWTECDSAPublicKey(serializedKey []byte) (proto.Message, error) {
	privKey := &jepb.JwtEcdsaPublicKey{}
	if err := proto.Unmarshal(serializedKey, privKey); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtEcdsaPublicKey: %v", err)
	}
	return privKey, nil
}

func unmarshalJWTRSASSAPKCS1PublicKey(serializedKey []byte) (proto.Message, error) {
	privKey := &jrsppb.JwtRsaSsaPkcs1PublicKey{}
	if err := proto.Unmarshal(serializedKey, privKey); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtRsaSsaPkcs1PublicKey: %v", err)
	}
	return privKey, nil
}

func unmarshalJWTRSASSAPSSPublicKey(serializedKey []byte) (proto.Message, error) {
	privKey := &jpsppb.JwtRsaSsaPssPublicKey{}
	if err := proto.Unmarshal(serializedKey, privKey); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JwtRsaSsaPssPublicKey: %v", err)
	}
	return privKey, nil
}

func init() {
	if err := registry.RegisterKeyManager(legacykeymanager.New(jwtHMACTypeURL, jwtHMACPrimitive, tinkpb.KeyData_SYMMETRIC, unmarshalJWTHMACKey)); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT HMAC key manager: %v", err))
	}

	if err := registry.RegisterKeyManager(legacykeymanager.NewPrivateKeyManager(jwtECDSASignerTypeURL, jwtECDSASignerPrimitive, tinkpb.KeyData_ASYMMETRIC_PRIVATE, unmarshalJWTECDSAPrivateKey)); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT ECDSA signer key manager: %v", err))
	}
	if err := registry.RegisterKeyManager(legacykeymanager.NewPrivateKeyManager(jwtJWTRSASSAPKCS1SignerTypeURL, jwtRSASSAPKCS1Primitive, tinkpb.KeyData_ASYMMETRIC_PRIVATE, unmarshalJWTRSASSAPKCS1PrivateKey)); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT RSA SSA PKCS1 signer key manager: %v", err))
	}
	if err := registry.RegisterKeyManager(legacykeymanager.NewPrivateKeyManager(jwtJWTRSASSAPSSSignerTypeURL, jwtRSASSAPSSPrimitive, tinkpb.KeyData_ASYMMETRIC_PRIVATE, unmarshalJWTRSASSAPSSPrivateKey)); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT RSA SSA PSS signer key manager: %v", err))
	}

	// Verifier key managers.
	if err := registry.RegisterKeyManager(legacykeymanager.New(jwtECDSAVerifierTypeURL, jwtECDSASignerPrimitive, tinkpb.KeyData_ASYMMETRIC_PUBLIC, unmarshalJWTECDSAPublicKey)); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT ECDSA verifier key manager: %v", err))
	}
	if err := registry.RegisterKeyManager(legacykeymanager.New(jwtJWTRSASSAPKCS1VerifierTypeURL, jwtRSASSAPKCS1Primitive, tinkpb.KeyData_ASYMMETRIC_PUBLIC, unmarshalJWTRSASSAPKCS1PublicKey)); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT RSA SSA PKCS1 verifier key manager: %v", err))
	}
	if err := registry.RegisterKeyManager(legacykeymanager.New(jwtJWTRSASSAPSSVerifierTypeURL, jwtRSASSAPSSPrimitive, tinkpb.KeyData_ASYMMETRIC_PUBLIC, unmarshalJWTRSASSAPSSPublicKey)); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT RSA SSA PSS verifier key manager: %v", err))
	}

	// MAC primitive constructors.
	if err := primitiveregistry.RegisterPrimitiveConstructor[*jwthmac.Key](createJWTHMAC); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT HMAC primitive constructor: %v", err))
	}

	// Signer primitive constructors.
	if err := primitiveregistry.RegisterPrimitiveConstructor[*jwtecdsa.PrivateKey](createJWTECDSASigner); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT ECDSA signer primitive constructor: %v", err))
	}
	if err := primitiveregistry.RegisterPrimitiveConstructor[*jwtrsassapkcs1.PrivateKey](createJWTRSASSAPKCS1Signer); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT RSA SSA PKCS1 signer primitive constructor: %v", err))
	}
	if err := primitiveregistry.RegisterPrimitiveConstructor[*jwtrsassapss.PrivateKey](createJWTRSASSAPSSSigner); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT RSA SSA PSS signer primitive constructor: %v", err))
	}

	// Verifier primitive constructors.
	if err := primitiveregistry.RegisterPrimitiveConstructor[*jwtecdsa.PublicKey](createJWTECDSAVerifier); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT ECDSA verifier primitive constructor: %v", err))
	}
	if err := primitiveregistry.RegisterPrimitiveConstructor[*jwtrsassapkcs1.PublicKey](createJWTRSASSAPKCS1Verifier); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT RSA SSA PKCS1 verifier primitive constructor: %v", err))
	}
	if err := primitiveregistry.RegisterPrimitiveConstructor[*jwtrsassapss.PublicKey](createJWTRSASSAPSSVerifier); err != nil {
		panic(fmt.Sprintf("jwt.init() failed registering JWT RSA SSA PSS verifier primitive constructor: %v", err))
	}
}
