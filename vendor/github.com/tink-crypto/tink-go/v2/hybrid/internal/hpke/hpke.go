// Copyright 2021 Google LLC
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

// Package hpke provides implementations of Hybrid Public Key Encryption.
package hpke

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Mode identifiers.
const (
	// BaseMode is the base mode identifier.
	baseMode uint8 = 0x00
)

// KEMID is the key encapsulation mechanism identifier.
type KEMID uint16

// All identifier values are specified in
// https://www.rfc-editor.org/rfc/rfc9180.html.

// KEM algorithm identifiers.
const (
	// UnknownKEMID is the KEM identifier for unknown KEM algorithms.
	UnknownKEMID KEMID = 0
	// P256HKDFSHA256 is the KEM identifier for NIST P-256 with HKDF-SHA-256.
	P256HKDFSHA256 = 0x0010
	// P384HKDFSHA384 is the KEM identifier for NIST P-384 with HKDF-SHA-384.
	P384HKDFSHA384 = 0x0011
	// P521HKDFSHA512 is the KEM identifier for NIST P-521 with HKDF-SHA-512.
	P521HKDFSHA512 = 0x0012
	// X25519HKDFSHA256 is the KEM identifier for Curve25519 with HKDF-SHA-256.
	X25519HKDFSHA256 = 0x0020
)

// KDFID is the key derivation function identifier.
type KDFID uint16

// KDF algorithm identifiers.
const (
	// UnknownKDFID is the KDF identifier for unknown KDF algorithms.
	UnknownKDFID KDFID = 0
	// HKDFSHA256 is the KDF identifier for HKDF-SHA-256.
	HKDFSHA256 = 0x0001
	// HKDFSHA384 is the KDF identifier for HKDF-SHA-384.
	HKDFSHA384 = 0x0002
	// HKDFSHA512 is the KDF identifier for HKDF-SHA-512.
	HKDFSHA512 = 0x0003
)

// AEADID is the authenticated encryption with associated data identifier.
type AEADID uint16

// AEAD algorithm identifiers.
const (
	// UnknownAEADID is the AEAD identifier for unknown AEAD algorithms.
	UnknownAEADID AEADID = 0
	// AES128GCM is the AEAD identifier for AES-128-GCM.
	AES128GCM = 0x0001
	// AES256GCM is the AEAD identifier for AES-256-GCM.
	AES256GCM = 0x0002
	// ChaCha20Poly1305 is the AEAD identifier for ChaCha20-Poly1305.
	ChaCha20Poly1305 = 0x0003
)

// String returns a string representation of the AEAD ID.
func (a AEADID) String() string {
	switch a {
	case AES128GCM:
		return "AES128GCM"
	case AES256GCM:
		return "AES256GCM"
	case ChaCha20Poly1305:
		return "ChaCha20Poly1305"
	default:
		return fmt.Sprintf("unknown AEADID: %d", a)
	}
}

// HashType is the hash function identifier.
type HashType int

// Hash function identifiers.
const (
	// UnknownHashType is the hash function identifier for unknown hash functions.
	UnknownHashType HashType = iota
	// SHA256 is the identifier for the SHA-256 hash function.
	SHA256
	// SHA384 is the identifier for the SHA-384 hash function.
	SHA384
	// SHA512 is the identifier for the SHA-512 hash function.
	SHA512
)

// String returns a string representation of the hash type.
func (h HashType) String() string {
	switch h {
	case SHA256:
		return "SHA256"
	case SHA384:
		return "SHA384"
	case SHA512:
		return "SHA512"
	default:
		return fmt.Sprintf("unknown HashType: %d", h)
	}
}

const hpkeV1 = "HPKE-v1"

var (
	// KEM lengths from https://www.rfc-editor.org/rfc/rfc9180.html#section-7.1
	kemLengths = map[KEMID]struct {
		nSecret, nEnc, nPK, nSK int
	}{
		P256HKDFSHA256:   {nSecret: 32, nEnc: 65, nPK: 65, nSK: 32},
		P384HKDFSHA384:   {nSecret: 48, nEnc: 97, nPK: 97, nSK: 48},
		P521HKDFSHA512:   {nSecret: 64, nEnc: 133, nPK: 133, nSK: 66},
		X25519HKDFSHA256: {nSecret: 32, nEnc: 32, nPK: 32, nSK: 32},
	}

	errInvalidHPKEParams           = errors.New("invalid HPKE parameters")
	errInvalidHPKEPrivateKeyLength = errors.New("invalid HPKE private key length")
	errInvalidHPKEPublicKeyLength  = errors.New("invalid HPKE public key length")

	emptySalt           = []byte{}
	emptyIKM            = []byte{}
	emptyAssociatedData = []byte{}
)

// kemSuiteID generates the KEM suite ID from kemID according to
// https://www.rfc-editor.org/rfc/rfc9180.html#section-4.1-5.
func kemSuiteID(kemID KEMID) []byte {
	return binary.BigEndian.AppendUint16([]byte("KEM"), uint16(kemID))
}

// hpkeSuiteID generates the HPKE suite ID according to
// https://www.rfc-editor.org/rfc/rfc9180.html#section-5.1-8.
func hpkeSuiteID(kemID KEMID, kdfID KDFID, aeadID AEADID) []byte {
	// Allocate memory for the return value with the exact amount of bytes needed.
	res := make([]byte, 0, 4+2+2+2)
	res = append(res, "HPKE"...)
	res = binary.BigEndian.AppendUint16(res, uint16(kemID))
	res = binary.BigEndian.AppendUint16(res, uint16(kdfID))
	res = binary.BigEndian.AppendUint16(res, uint16(aeadID))
	return res
}

// keyScheduleContext creates the key_schedule_context defined at
// https://www.rfc-editor.org/rfc/rfc9180.html#section-5.1-10.
func keyScheduleContext(mode uint8, pskIDHash, infoHash []byte) []byte {
	// Allocate memory for the return value with the exact amount of bytes needed.
	res := make([]byte, 0, 1+len(pskIDHash)+len(infoHash))
	res = append(res, mode)
	res = append(res, pskIDHash...)
	res = append(res, infoHash...)
	return res
}

// labelIKM returns a labeled IKM according to LabeledExtract() defined at
// https://www.rfc-editor.org/rfc/rfc9180.html#section-4.
func labelIKM(label string, ikm, suiteID []byte) []byte {
	// Allocate memory for the return value with the exact amount of bytes needed.
	res := make([]byte, 0, len(hpkeV1)+len(suiteID)+len(label)+len(ikm))
	res = append(res, hpkeV1...)
	res = append(res, suiteID...)
	res = append(res, label...)
	res = append(res, ikm...)
	return res
}

// labelInfo returns a labeled info according to LabeledExpand() defined at
// https://www.rfc-editor.org/rfc/rfc9180.html#section-4.
func labelInfo(label string, info, suiteID []byte, length int) ([]byte, error) {
	length16 := uint16(length)
	if int(length16) != length {
		return nil, fmt.Errorf("length %d must be a valid uint16 value", length)
	}

	// Allocate memory for the return value with the exact amount of bytes needed.
	res := make([]byte, 0, 2+len(hpkeV1)+len(suiteID)+len(label)+len(info))
	res = binary.BigEndian.AppendUint16(res, length16)
	res = append(res, hpkeV1...)
	res = append(res, suiteID...)
	res = append(res, label...)
	res = append(res, info...)
	return res, nil
}
