// Copyright 2020 Google LLC
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

// Package ecdsa provides internal ECDSA utility functions.
package ecdsa

import (
	"bytes"
	"crypto/elliptic"
	"encoding/asn1"
	"fmt"
	"math/big"
)

// Signature is an ECDSA signature.
type Signature struct {
	R, S *big.Int
}

// ASN1Encode encodes the given ECDSA signature using ASN.1 encoding.
func ASN1Encode(s *Signature) ([]byte, error) {
	ret, err := asn1.Marshal(*s)
	if err != nil {
		return nil, fmt.Errorf("asn.1 encoding failed")
	}
	return ret, nil
}

var errAsn1Decoding = fmt.Errorf("asn.1 decoding failed")

// ASN1Decode verifies the given ECDSA signature and decodes it if it is valid.
//
// Since asn1.Unmarshal() doesn't do a strict verification on its input, it will
// accept signatures with trailing data. Thus, we add an additional check to make sure
// that the input follows strict DER encoding: after unmarshalling the signature bytes,
// we marshal the obtained signature object again. Since DER encoding is deterministic,
// we expect that the obtained bytes would be equal to the input.
func ASN1Decode(b []byte) (*Signature, error) {
	// parse the signature
	sig := new(Signature)
	_, err := asn1.Unmarshal(b, sig)
	if err != nil {
		return nil, errAsn1Decoding
	}
	// encode the signature again
	encoded, err := asn1.Marshal(*sig)
	if err != nil {
		return nil, errAsn1Decoding
	}
	if !bytes.Equal(b, encoded) {
		return nil, errAsn1Decoding
	}
	return sig, nil
}

func ieeeSignatureSize(curveName string) (int, error) {
	switch curveName {
	case elliptic.P256().Params().Name:
		return 64, nil
	case elliptic.P384().Params().Name:
		return 96, nil
	case elliptic.P521().Params().Name:
		return 132, nil
	default:
		return 0, fmt.Errorf("ieeeP1363 unsupported curve name: %q", curveName)
	}
}

// IEEEP1363Encode converts the signature to the IEEE_P1363 encoding format.
func IEEEP1363Encode(sig *Signature, curveName string) ([]byte, error) {
	sigSize, err := ieeeSignatureSize(curveName)
	if err != nil {
		return nil, err
	}
	// Bounds checking for the FillBytes() calls.
	scalarSize := sigSize / 2
	if sig.R.BitLen() > scalarSize*8 || sig.S.BitLen() > scalarSize*8 {
		return nil, fmt.Errorf("ecdsa: invalid signature")
	}
	enc := make([]byte, sigSize)
	sig.R.FillBytes(enc[:sigSize/2])
	sig.S.FillBytes(enc[sigSize/2:])
	return enc, nil
}

// IEEEP1363Decode decodes the given ECDSA signature using IEEE_P1363 encoding.
func IEEEP1363Decode(encodedBytes []byte) (*Signature, error) {
	if len(encodedBytes) == 0 || len(encodedBytes) > 132 || len(encodedBytes)%2 != 0 {
		return nil, fmt.Errorf("ecdsa: Invalid IEEE_P1363 encoded bytes")
	}
	r := new(big.Int).SetBytes(encodedBytes[:len(encodedBytes)/2])
	s := new(big.Int).SetBytes(encodedBytes[len(encodedBytes)/2:])
	return &Signature{R: r, S: s}, nil
}
