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

package jwt

import (
	"github.com/tink-crypto/tink-go/v2/internal/jwk"
	"github.com/tink-crypto/tink-go/v2/keyset"
)

// JWKSetToPublicKeysetHandle converts a Json Web Key (JWK) set into a Tink KeysetHandle.
// It requires that all keys in the set have the "alg" field set. Currently, only
// public keys for algorithms ES256, ES384, ES512, RS256, RS384, and RS512 are supported.
// JWK is defined in https://www.rfc-editor.org/rfc/rfc7517.txt.
func JWKSetToPublicKeysetHandle(jwkSet []byte) (*keyset.Handle, error) {
	return jwk.ToPublicKeysetHandle(jwkSet, jwk.Ed25519SupportNone)
}

// JWKSetFromPublicKeysetHandle converts a Tink KeysetHandle with JWT keys into a Json Web Key (JWK) set.
// Currently only public keys for algorithms ES256, ES384, ES512, RS256, RS384, and RS512 are supported.
// JWK is defined in https://www.rfc-editor.org/rfc/rfc7517.html.
func JWKSetFromPublicKeysetHandle(kh *keyset.Handle) ([]byte, error) {
	return jwk.FromPublicKeysetHandle(kh, jwk.Ed25519SupportNone)
}
