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

package hpke

import (
	"fmt"
)

// newPrimitives constructs new KEM, KDF, AEADs.
func newPrimitives(kemID KEMID, kdfID KDFID, aeadID AEADID) (kem, kdf, aead, error) {
	kem, err := newKEM(kemID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("newKEM(%d): %v", kemID, err)
	}
	kdf, err := newKDF(kdfID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("newKDF(%d): %v", kdfID, err)
	}
	aead, err := newAEAD(aeadID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("newAEAD(%d): %v", aeadID, err)
	}
	return kem, kdf, aead, nil
}

// newKEM constructs a HPKE KEM using kemID, which are specified at
// https://www.rfc-editor.org/rfc/rfc9180.html#section-7.1.
func newKEM(kemID KEMID) (kem, error) {
	switch kemID {
	case P256HKDFSHA256:
		return newNISTCurvesKEM(P256HKDFSHA256)
	case P384HKDFSHA384:
		return newNISTCurvesKEM(P384HKDFSHA384)
	case P521HKDFSHA512:
		return newNISTCurvesKEM(P521HKDFSHA512)
	case X25519HKDFSHA256:
		return newX25519KEM(SHA256)
	default:
		return nil, fmt.Errorf("KEM ID %d is not supported", kemID)
	}
}

// newKDF constructs a HPKE KDF using kdfID, which are specified at
// https://www.rfc-editor.org/rfc/rfc9180.html#section-7.2.
func newKDF(kdfID KDFID) (kdf, error) {
	switch kdfID {
	case HKDFSHA256:
		return newHKDFKDF(SHA256)
	case HKDFSHA384:
		return newHKDFKDF(SHA384)
	case HKDFSHA512:
		return newHKDFKDF(SHA512)
	default:
		return nil, fmt.Errorf("KDF ID %d is not supported", kdfID)
	}
}

// newAEAD constructs a HPKE AEAD using aeadID, which are specified at
// https://www.rfc-editor.org/rfc/rfc9180.html#section-7.3.
func newAEAD(aeadID AEADID) (aead, error) {
	switch aeadID {
	case AES128GCM:
		return newAESGCMAEAD(16)
	case AES256GCM:
		return newAESGCMAEAD(32)
	case ChaCha20Poly1305:
		return &chaCha20Poly1305AEAD{}, nil
	default:
		return nil, fmt.Errorf("AEAD ID %d is not supported", aeadID)
	}
}
