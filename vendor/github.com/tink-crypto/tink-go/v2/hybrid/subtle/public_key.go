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

package subtle

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/hybrid/hpke"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/keyset"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

// SerializePrimaryPublicKey serializes a public keyset handle's primary key if
// the primary key is a public key and matches both the template argument and a
// supported template.
//
// Supported templates are the same as KeysetHandleFromSerializedPublicKey's:
//   - DHKEM_X25519_HKDF_SHA256_HKDF_SHA256_CHACHA20_POLY1305_Raw_Key_Template,
//     which returns the KEM-encoding of the public key, i.e. SerializePublicKey
//     in https://www.rfc-editor.org/rfc/rfc9180.html#section-7.1.1.
func SerializePrimaryPublicKey(handle *keyset.Handle, template *tinkpb.KeyTemplate) ([]byte, error) {
	params, err := protoserialization.ParseParameters(template)
	if err != nil {
		return nil, fmt.Errorf("failed to parse parameters: %v", err)
	}
	paramsHPKE, ok := params.(*hpke.Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: %T, want %T", params, (*hpke.Parameters)(nil))
	}
	if err := validateParameters(paramsHPKE); err != nil {
		return nil, fmt.Errorf("invalid parameters: %v", err)
	}
	primaryEntry, err := handle.Primary()
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key: %v", err)
	}
	switch publicKey := primaryEntry.Key().(type) {
	case *hpke.PublicKey:
		if !paramsHPKE.Equal(publicKey.Parameters()) {
			return nil, fmt.Errorf("invalid template: %v, want %v", paramsHPKE, publicKey.Parameters())
		}
		return publicKey.PublicKeyBytes(), nil
	default:
		return nil, fmt.Errorf("invalid key type: %T, want %T", publicKey, (*hpke.PublicKey)(nil))
	}
}

// KeysetHandleFromSerializedPublicKey returns a keyset handle containing a
// primary key that has the specified pubKeyBytes and matches template.
//
// Supported templates are the same as PublicKeyFromPrimaryKey's:
//   - DHKEM_X25519_HKDF_SHA256_HKDF_SHA256_CHACHA20_POLY1305_Raw_Key_Template,
//     which requires pubKeyBytes to be the KEM-encoding of the public key, i.e.
//     SerializePublicKey in
//     https://www.rfc-editor.org/rfc/rfc9180.html#section-7.1.1.
func KeysetHandleFromSerializedPublicKey(pubKeyBytes []byte, template *tinkpb.KeyTemplate) (*keyset.Handle, error) {
	params, err := protoserialization.ParseParameters(template)
	if err != nil {
		return nil, fmt.Errorf("failed to parse parameters: %v", err)
	}
	paramsHPKE, ok := params.(*hpke.Parameters)
	if !ok {
		return nil, fmt.Errorf("invalid parameters type: %T, want %T", params, (*hpke.Parameters)(nil))
	}
	if err := validateParameters(paramsHPKE); err != nil {
		return nil, fmt.Errorf("invalid parameters: %v", err)
	}
	pubKey, err := hpke.NewPublicKey(pubKeyBytes, 0, paramsHPKE)
	if err != nil {
		return nil, fmt.Errorf("failed to create HPKE public key: %v", err)
	}
	manager := keyset.NewManager()
	keyID := uint32(1)
	opts := []keyset.KeyOpts{keyset.WithFixedID(keyID), keyset.AsPrimary()}
	if _, err := manager.AddKeyWithOpts(pubKey, internalapi.Token{}, opts...); err != nil {
		return nil, fmt.Errorf("failed to add key: %v", err)
	}
	return manager.Handle()
}

func validateParameters(params *hpke.Parameters) error {
	if params.AEADID() != hpke.ChaCha20Poly1305 {
		return fmt.Errorf("invalid AEAD ID: %v, want %v", params.AEADID(), hpke.ChaCha20Poly1305)
	}
	if params.KEMID() != hpke.DHKEM_X25519_HKDF_SHA256 {
		return fmt.Errorf("invalid KEM ID: %v, want %v", params.KEMID(), hpke.DHKEM_X25519_HKDF_SHA256)
	}
	if params.Variant() != hpke.VariantNoPrefix {
		return fmt.Errorf("invalid variant: %v, want %v", params.Variant(), hpke.VariantNoPrefix)
	}
	if params.KDFID() != hpke.HKDFSHA256 {
		return fmt.Errorf("invalid KDF ID: %v, want %v", params.KDFID(), hpke.HKDFSHA256)
	}
	if params.Variant() != hpke.VariantNoPrefix {
		return fmt.Errorf("invalid variant: %v, want %v", params.Variant(), hpke.VariantNoPrefix)
	}
	return nil
}
