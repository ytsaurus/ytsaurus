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

package hpke

import (
	"bytes"
	"fmt"

	internalhpke "github.com/tink-crypto/tink-go/v2/hybrid/internal/hpke"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

type hybridDecrypt struct {
	rawHybridDecrypt tink.HybridDecrypt
	prefix           []byte
	variant          Variant
}

func kemIDFromParams(params *Parameters) (internalhpke.KEMID, error) {
	switch params.KEMID() {
	case DHKEM_P256_HKDF_SHA256:
		return internalhpke.P256HKDFSHA256, nil
	case DHKEM_P384_HKDF_SHA384:
		return internalhpke.P384HKDFSHA384, nil
	case DHKEM_P521_HKDF_SHA512:
		return internalhpke.P521HKDFSHA512, nil
	case DHKEM_X25519_HKDF_SHA256:
		return internalhpke.X25519HKDFSHA256, nil
	default:
		return 0, fmt.Errorf("unsupported variant: %v", params.KEMID())
	}
}

func kdfIDFromParams(params *Parameters) (internalhpke.KDFID, error) {
	switch params.KDFID() {
	case HKDFSHA256:
		return internalhpke.HKDFSHA256, nil
	case HKDFSHA384:
		return internalhpke.HKDFSHA384, nil
	case HKDFSHA512:
		return internalhpke.HKDFSHA512, nil
	default:
		return 0, fmt.Errorf("unsupported variant: %v", params.KDFID())
	}
}

func aeadIDFromParams(params *Parameters) (internalhpke.AEADID, error) {
	switch params.AEADID() {
	case AES128GCM:
		return internalhpke.AES128GCM, nil
	case AES256GCM:
		return internalhpke.AES256GCM, nil
	case ChaCha20Poly1305:
		return internalhpke.ChaCha20Poly1305, nil
	default:
		return 0, fmt.Errorf("unsupported variant: %v", params.AEADID())
	}
}

// NewHybridDecrypt creates a new instance of [tink.HybridDecrypt] from a
// [PrivateKey].
//
// This is an internal API.
func NewHybridDecrypt(privateKey *PrivateKey, _ internalapi.Token) (tink.HybridDecrypt, error) {
	params := privateKey.Parameters().(*Parameters)
	kemID, err := kemIDFromParams(params)
	if err != nil {
		return nil, err
	}
	kdfID, err := kdfIDFromParams(params)
	if err != nil {
		return nil, err
	}
	aeadID, err := aeadIDFromParams(params)
	if err != nil {
		return nil, err
	}
	rawHybridDecrypt, err := internalhpke.NewDecrypt(privateKey.PrivateKeyBytes(), kemID, kdfID, aeadID)
	if err != nil {
		return nil, err
	}
	return &hybridDecrypt{
		rawHybridDecrypt: rawHybridDecrypt,
		prefix:           privateKey.OutputPrefix(),
		variant:          params.Variant(),
	}, nil
}

func (e *hybridDecrypt) Decrypt(ciphertext, contextInfo []byte) ([]byte, error) {
	if len(ciphertext) < len(e.prefix) {
		return nil, fmt.Errorf("ciphertext too short")
	}
	if !bytes.Equal(e.prefix, ciphertext[:len(e.prefix)]) {
		return nil, fmt.Errorf("ciphertext does not start with the expected prefix")
	}
	return e.rawHybridDecrypt.Decrypt(ciphertext[len(e.prefix):], contextInfo)
}

func hybridDecryptConstructor(k key.Key) (any, error) {
	that, ok := k.(*PrivateKey)
	if !ok {
		return nil, fmt.Errorf("invalid key type, got %T, want %T", k, (*PrivateKey)(nil))
	}
	return NewHybridDecrypt(that, internalapi.Token{})
}
