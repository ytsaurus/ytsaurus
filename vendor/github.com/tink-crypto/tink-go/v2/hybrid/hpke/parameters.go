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
	"fmt"

	"github.com/tink-crypto/tink-go/v2/key"
)

// Variant is the prefix variant of an HPKE key.
//
// It describes the format of the ciphertext. For HPKE, there are three options:
//
//   - TINK: prepends '0x01<big endian key id>' to the ciphertext.
//   - CRUNCHY: prepends '0x00<big endian key id>' to the ciphertext.
//   - NO_PREFIX: adds no prefix to the ciphertext.
type Variant int

const (
	// VariantUnknown is the default value of Variant.
	VariantUnknown Variant = iota
	// VariantTink prefixes '0x01<big endian key id>' to the ciphertext.
	VariantTink
	// VariantCrunchy prefixes '0x00<big endian key id>' to the ciphertext.
	VariantCrunchy
	// VariantNoPrefix does not prefix the ciphertext with the key id.
	VariantNoPrefix
)

func (variant Variant) String() string {
	switch variant {
	case VariantTink:
		return "TINK"
	case VariantCrunchy:
		return "CRUNCHY"
	case VariantNoPrefix:
		return "NO_PREFIX"
	default:
		return "UNKNOWN"
	}
}

// KEMID is an HPKE KEM identifier specified in
// https://www.rfc-editor.org/rfc/rfc9180.html#section-7.1.
type KEMID int

const (
	// UnknownKEMID is the default value of KEMID.
	UnknownKEMID KEMID = iota
	// DHKEM_P256_HKDF_SHA256 implements DHKEM-P256-HKDF-SHA256.
	DHKEM_P256_HKDF_SHA256
	// DHKEM_P384_HKDF_SHA384 implements DHKEM-P384-HKDF-SHA384.
	DHKEM_P384_HKDF_SHA384
	// DHKEM_P521_HKDF_SHA512 implements DHKEM-P521-HKDF-SHA512.
	DHKEM_P521_HKDF_SHA512
	// DHKEM_X25519_HKDF_SHA256 implements DHKEM-X25519-HKDF-SHA256.
	DHKEM_X25519_HKDF_SHA256
)

func (kemID KEMID) String() string {
	switch kemID {
	case DHKEM_P256_HKDF_SHA256:
		return "DHKEM-P256-HKDF-SHA256"
	case DHKEM_P384_HKDF_SHA384:
		return "DHKEM-P384-HKDF-SHA384"
	case DHKEM_P521_HKDF_SHA512:
		return "DHKEM-P521-HKDF-SHA512"
	case DHKEM_X25519_HKDF_SHA256:
		return "DHKEM-X25519-HKDF-SHA256"
	default:
		return "UNKNOWN"
	}
}

// KDFID is an HPKE KDF identifier specified in
// https://www.rfc-editor.org/rfc/rfc9180.html#section-7.2.
type KDFID int

const (
	// UnknownKDFID is the default value of KDFID.
	UnknownKDFID KDFID = iota
	// HKDFSHA256 implements HKDF-SHA256.
	HKDFSHA256
	// HKDFSHA384 implements HKDF-SHA384.
	HKDFSHA384
	// HKDFSHA512 implements HKDF-SHA512.
	HKDFSHA512
)

func (kdfID KDFID) String() string {
	switch kdfID {
	case HKDFSHA256:
		return "HKDF-SHA256"
	case HKDFSHA384:
		return "HKDF-SHA384"
	case HKDFSHA512:
		return "HKDF-SHA512"
	default:
		return "UNKNOWN"
	}
}

// AEADID is an HPKE AEAD identifier specified in
// https://www.rfc-editor.org/rfc/rfc9180.html#section-7.3.
type AEADID int

const (
	// UnknownAEADID is the default value of AEADID.
	UnknownAEADID AEADID = iota
	// AES128GCM implements AES-128-GCM.
	AES128GCM
	// AES256GCM implements AES-256-GCM.
	AES256GCM
	// ChaCha20Poly1305 implements ChaCha20-Poly1305.
	ChaCha20Poly1305
)

func (aeadID AEADID) String() string {
	switch aeadID {
	case AES128GCM:
		return "AES-128-GCM"
	case AES256GCM:
		return "AES-256-GCM"
	case ChaCha20Poly1305:
		return "ChaCha20-Poly1305"
	default:
		return "UNKNOWN"
	}
}

// ParametersOpts is the options for creating parameters.
type ParametersOpts struct {
	KEMID   KEMID
	KDFID   KDFID
	AEADID  AEADID
	Variant Variant
}

func (paramsOpts ParametersOpts) String() string {
	return fmt.Sprintf("KEMID: %s, KDFID: %s, AEADID: %s, Variant: %s", paramsOpts.KEMID, paramsOpts.KDFID, paramsOpts.AEADID, paramsOpts.Variant)
}

// Parameters represents the parameters of an HPKE key.
type Parameters struct {
	kemID   KEMID
	kdfID   KDFID
	aeadID  AEADID
	variant Variant
}

var _ key.Parameters = (*Parameters)(nil)

// NewParameters creates a new [hpke.Parameters] value.
func NewParameters(opts ParametersOpts) (*Parameters, error) {
	if opts.KEMID == UnknownKEMID || opts.KDFID == UnknownKDFID || opts.AEADID == UnknownAEADID || opts.Variant == VariantUnknown {
		return nil, fmt.Errorf("invalid parameters: %v", opts)
	}
	return &Parameters{
		kemID:   opts.KEMID,
		kdfID:   opts.KDFID,
		aeadID:  opts.AEADID,
		variant: opts.Variant,
	}, nil
}

// KEMID the KEM ID.
func (p *Parameters) KEMID() KEMID { return p.kemID }

// KDFID the KDF ID.
func (p *Parameters) KDFID() KDFID { return p.kdfID }

// AEADID the AEAD ID.
func (p *Parameters) AEADID() AEADID { return p.aeadID }

// Variant returns the output prefix variant of the key.
func (p *Parameters) Variant() Variant { return p.variant }

// HasIDRequirement tells whether the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// Equal tells whether this parameters value is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	actualParams, ok := other.(*Parameters)
	return ok && p.kemID == actualParams.kemID &&
		p.kdfID == actualParams.kdfID &&
		p.aeadID == actualParams.aeadID &&
		p.variant == actualParams.variant
}
