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
	"github.com/tink-crypto/tink-go/v2/jwt/jwthmac"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/mac/hmac"
)

type fullMac struct {
	m *macWithKID
	// kid is set only for TINK keys. Keys with Custom KID or that ignores
	// it are RAW, thus this is nil.
	kid *string
}

var _ MAC = (*fullMac)(nil)

func (s *fullMac) ComputeMACAndEncode(token *RawJWT) (string, error) {
	return s.m.ComputeMACAndEncodeWithKID(token, s.kid)
}

func (s *fullMac) VerifyMACAndDecode(compact string, validator *Validator) (*VerifiedJWT, error) {
	return s.m.VerifyMACAndDecodeWithKID(compact, validator, s.kid)
}

func createJWTHMAC(key key.Key) (any, error) {
	jwthmacKey, ok := key.(*jwthmac.Key)
	if !ok {
		return nil, fmt.Errorf("createJWTHMAC: key is of type %T, want %T", key, (*jwthmac.Key)(nil))
	}
	jwtParams := jwthmacKey.Parameters().(*jwthmac.Parameters)

	var hashType hmac.HashType
	var tagSizeInBytes int
	switch jwtParams.Algorithm() {
	case jwthmac.HS256:
		hashType = hmac.SHA256
		tagSizeInBytes = 32
	case jwthmac.HS384:
		hashType = hmac.SHA384
		tagSizeInBytes = 48
	case jwthmac.HS512:
		hashType = hmac.SHA512
		tagSizeInBytes = 64
	default:
		return nil, fmt.Errorf("createJWTHMAC: unsupported algorithm: %s", jwtParams.Algorithm())
	}

	hmacParams, err := hmac.NewParameters(hmac.ParametersOpts{
		KeySizeInBytes: jwtParams.KeySizeInBytes(),
		TagSizeInBytes: tagSizeInBytes,
		HashType:       hashType,
		Variant:        hmac.VariantNoPrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("createJWTHMAC: %v", err)
	}
	hmacKey, err := hmac.NewKey(jwthmacKey.KeyBytes(), hmacParams, 0)
	if err != nil {
		return nil, fmt.Errorf("createJWTHMAC: %v", err)
	}
	mac, err := hmac.NewMAC(hmacKey, internalapi.Token{})
	if err != nil {
		return nil, fmt.Errorf("createJWTHMAC: %v", err)
	}

	var kid, customKID *string
	kidValue, hasKID := jwthmacKey.KID()
	switch jwtParams.KIDStrategy() {
	case jwthmac.CustomKID:
		if !hasKID {
			// This should never happen.
			return nil, fmt.Errorf("createJWTHMAC: CustomKID strategy requires a KID")
		}
		customKID = &kidValue
		kid = nil
	case jwthmac.Base64EncodedKeyIDAsKID:
		if !hasKID {
			// This should never happen.
			return nil, fmt.Errorf("createJWTHMAC: Base64EncodedKeyIDAsKID strategy requires a KID")
		}
		kid = &kidValue
		customKID = nil
	case jwthmac.IgnoredKID:
		kid = nil
		customKID = nil
	default:
		// This should never happen.
		return nil, fmt.Errorf("createJWTHMAC: unsupported KID strategy: %s", jwtParams.KIDStrategy())
	}
	macWithKID, err := newMACWithKID(mac, jwtParams.Algorithm().String(), customKID)
	if err != nil {
		return nil, fmt.Errorf("createJWTHMAC: %v", err)
	}
	return &fullMac{m: macWithKID, kid: kid}, nil
}
