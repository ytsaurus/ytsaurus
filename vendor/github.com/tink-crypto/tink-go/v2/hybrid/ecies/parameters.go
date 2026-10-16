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
	"bytes"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/aead/aesctrhmac"
	"github.com/tink-crypto/tink-go/v2/aead/aesgcm"
	"github.com/tink-crypto/tink-go/v2/aead/xchacha20poly1305"
	"github.com/tink-crypto/tink-go/v2/daead/aessiv"
	"github.com/tink-crypto/tink-go/v2/key"
)

// Variant is the prefix variant of an ECIES key.
//
// It describes the format of the ciphertext. For ECIES, there are four options:
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

// CurveType is the curve type for the KEM.
type CurveType int

const (
	// UnknownCurveType is the default value of CurveType.
	UnknownCurveType CurveType = iota
	// NISTP256 is the NIST P-256 curve.
	NISTP256
	// NISTP384 is the NIST P-384 curve.
	NISTP384
	// NISTP521 is the NIST P-521 curve.
	NISTP521
	// X25519 is the X25519 curve.
	X25519
)

func (ct CurveType) String() string {
	switch ct {
	case NISTP256:
		return "NIST_P256"
	case NISTP384:
		return "NIST_P384"
	case NISTP521:
		return "NIST_P521"
	case X25519:
		return "X25519"
	default:
		return "UNKNOWN"
	}
}

// HashType is the hash type of the KEM.
type HashType int

const (
	// UnknownHashType is the default value of HashType.
	UnknownHashType HashType = iota
	// SHA1 is the SHA1 hash type.
	SHA1
	// SHA224 is the SHA224 hash type.
	SHA224
	// SHA256 is the SHA256 hash type.
	SHA256
	// SHA384 is the SHA384 hash type.
	SHA384
	// SHA512 is the SHA512 hash type.
	SHA512
)

func (ht HashType) String() string {
	switch ht {
	case SHA1:
		return "SHA1"
	case SHA224:
		return "SHA224"
	case SHA256:
		return "SHA256"
	case SHA384:
		return "SHA384"
	case SHA512:
		return "SHA512"
	default:
		return "UNKNOWN"
	}
}

// PointFormat is the elliptic curve point format for encoding the public key
// in the KEM prefix added to the ciphertext.
type PointFormat int

const (
	// UnspecifiedPointFormat is the default value of PointFormat.
	UnspecifiedPointFormat PointFormat = iota
	// CompressedPointFormat is the compressed point format.
	CompressedPointFormat
	// UncompressedPointFormat is the uncompressed point format.
	UncompressedPointFormat
	// LegacyUncompressedPointFormat is the same as UncompressedPointFormat, but
	// without the leading '\x04' prefix byte. Only used by Crunchy.
	//
	// DO NOT USE unless you are a Crunchy user migrating to Tink.
	LegacyUncompressedPointFormat
)

func (pf PointFormat) String() string {
	switch pf {
	case CompressedPointFormat:
		return "CompressedPointFormat"
	case UncompressedPointFormat:
		return "UncompressedPointFormat"
	case LegacyUncompressedPointFormat:
		return "LegacyUncompressedPointFormat"
	default:
		return "UnspecifiedPointFormat"
	}
}

// Parameters represents the parameters of an ECIES key.
//
// These are parameters for keys that implement ECIES ISO 18033-2 standard
// (Elliptic Curve Integrated Encryption Scheme, see http://www.shoup.net/iso/std6.pdf).
type Parameters struct {
	curveType            CurveType
	hashType             HashType
	nistCurvePointFormat PointFormat
	demParameters        key.Parameters
	salt                 []byte
	variant              Variant
}

var (
	allowedDEMParameters []key.Parameters = mustCreateAllowedDEMParameters()
)

func mustCreateAllowedDEMParameters() []key.Parameters {
	aes128GCMParams, err := aesgcm.NewParameters(aesgcm.ParametersOpts{
		KeySizeInBytes: 16,
		IVSizeInBytes:  12,
		TagSizeInBytes: 16,
		Variant:        aesgcm.VariantNoPrefix,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to create AES128-GCM parameters: %v", err))
	}
	aes256GCMParams, err := aesgcm.NewParameters(aesgcm.ParametersOpts{
		KeySizeInBytes: 32,
		IVSizeInBytes:  12,
		TagSizeInBytes: 16,
		Variant:        aesgcm.VariantNoPrefix,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to create AES256-GCM parameters: %v", err))
	}
	aes256SIVParams, err := aessiv.NewParameters(64, aessiv.VariantNoPrefix)
	if err != nil {
		panic(fmt.Sprintf("failed to create AES-SIV parameters: %v", err))
	}
	xchacha20poly1305Params, err := xchacha20poly1305.NewParameters(xchacha20poly1305.VariantNoPrefix)
	if err != nil {
		panic(fmt.Sprintf("failed to create XChaCha20Poly1305 parameters: %v", err))
	}
	aes128CTRHMACSHA256Params, err := aesctrhmac.NewParameters(aesctrhmac.ParametersOpts{
		AESKeySizeInBytes:  16,
		HMACKeySizeInBytes: 32,
		IVSizeInBytes:      16,
		HashType:           aesctrhmac.SHA256,
		TagSizeInBytes:     16,
		Variant:            aesctrhmac.VariantNoPrefix,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to create AES-CTR-HMAC parameters: %v", err))
	}
	aes256CTRHMACSHA256Params, err := aesctrhmac.NewParameters(aesctrhmac.ParametersOpts{
		AESKeySizeInBytes:  32,
		HMACKeySizeInBytes: 32,
		IVSizeInBytes:      16,
		HashType:           aesctrhmac.SHA256,
		TagSizeInBytes:     32,
		Variant:            aesctrhmac.VariantNoPrefix,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to create AES-CTR-HMAC parameters: %v", err))
	}
	return []key.Parameters{
		aes128GCMParams,
		aes256GCMParams,
		aes256SIVParams,
		xchacha20poly1305Params,
		aes128CTRHMACSHA256Params,
		aes256CTRHMACSHA256Params,
	}
}

func isAllowedDEMParameters(demParameters key.Parameters) error {
	for _, allowedDEMParameters := range allowedDEMParameters {
		if demParameters.Equal(allowedDEMParameters) {
			return nil
		}
	}
	// TODO: b/388807656 - Include supported DEM parameters in error message.
	return fmt.Errorf("unsupported DEM parameters %s", demParameters)
}

func isNISTCurve(curveType CurveType) bool {
	switch curveType {
	case NISTP256, NISTP384, NISTP521:
		return true
	default:
		return false
	}
}

// ParametersOpts is the options for creating a new ECIES Parameters value.
type ParametersOpts struct {
	CurveType            CurveType
	HashType             HashType
	NISTCurvePointFormat PointFormat // Must be UnspecifiedPointFormat for X25519.
	DEMParameters        key.Parameters
	Salt                 []byte
	Variant              Variant
}

// NewParameters creates a new ECDSA Parameters value.
func NewParameters(opts ParametersOpts) (*Parameters, error) {
	if opts.CurveType == UnknownCurveType {
		return nil, fmt.Errorf("ecies.NewParameters: curve type must not be %v", UnknownCurveType)
	}
	if opts.HashType == UnknownHashType {
		return nil, fmt.Errorf("ecies.NewParameters: hash type must not be %v", UnknownHashType)
	}
	if opts.Variant == VariantUnknown {
		return nil, fmt.Errorf("ecies.NewParameters: variant must not be %v", VariantUnknown)
	}
	if isNISTCurve(opts.CurveType) && opts.NISTCurvePointFormat == UnspecifiedPointFormat {
		return nil, fmt.Errorf("ecies.NewParameters: point format must not be %v when curve type is a NIST curve", UnspecifiedPointFormat)
	}
	if !isNISTCurve(opts.CurveType) && opts.NISTCurvePointFormat != UnspecifiedPointFormat {
		return nil, fmt.Errorf("ecies.NewParameters: point format must be %v when curve type is not a NIST curve", UnspecifiedPointFormat)
	}
	if err := isAllowedDEMParameters(opts.DEMParameters); err != nil {
		return nil, fmt.Errorf("ecies.NewParameters: %v", err)
	}
	p := &Parameters{
		curveType:            opts.CurveType,
		hashType:             opts.HashType,
		nistCurvePointFormat: opts.NISTCurvePointFormat,
		demParameters:        opts.DEMParameters,
		salt:                 bytes.Clone(opts.Salt),
		variant:              opts.Variant,
	}
	return p, nil
}

var _ key.Parameters = (*Parameters)(nil)

// CurveType the curve type.
func (p *Parameters) CurveType() CurveType { return p.curveType }

// HashType returns the hash type.
func (p *Parameters) HashType() HashType { return p.hashType }

// NISTCurvePointFormat returns the point format.
func (p *Parameters) NISTCurvePointFormat() PointFormat { return p.nistCurvePointFormat }

// DEMParameters returns the Data Encapsulation Mechanism (DEM) key parameters.
func (p *Parameters) DEMParameters() key.Parameters { return p.demParameters }

// Salt returns the salt.
func (p *Parameters) Salt() []byte { return bytes.Clone(p.salt) }

// Variant returns the output prefix variant of the key.
func (p *Parameters) Variant() Variant { return p.variant }

// HasIDRequirement tells whether the key has an ID requirement.
func (p *Parameters) HasIDRequirement() bool { return p.variant != VariantNoPrefix }

// Equal tells whether this parameters value is equal to other.
func (p *Parameters) Equal(other key.Parameters) bool {
	actualParams, ok := other.(*Parameters)
	return ok && p.curveType == actualParams.curveType &&
		p.hashType == actualParams.hashType &&
		p.demParameters.Equal(actualParams.demParameters) &&
		p.nistCurvePointFormat == actualParams.nistCurvePointFormat &&
		bytes.Equal(p.salt, actualParams.salt) &&
		p.variant == actualParams.variant
}
