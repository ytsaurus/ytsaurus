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

package hpke

import (
	"crypto/ecdh"
	"crypto/rand"
	"fmt"
	"io"
	"slices"

	"github.com/tink-crypto/tink-go/v2/subtle"
)

// nistCurvesKEM implements the `kem` interface for the NIST-curve HPKE KEMs from RFC 9180.
type nistCurvesKEM struct {
	// kemID is an HPKE KEM algorithm identifier.
	kemID KEMID
	// hmacHashAlg is an HMAC hash algorithm name.
	hmacHashAlg HashType
	// curve is a NIST curve.
	curve ecdh.Curve
	// generatePrivateKey is a function to generate a random private key for the NIST curve.
	generatePrivateKey func(io.Reader) (*ecdh.PrivateKey, error)
}

var _ kem = (*nistCurvesKEM)(nil)

// newNISTCurvesKEM constructs a NIST-curve HPKE KEM.
func newNISTCurvesKEM(kemID KEMID) (*nistCurvesKEM, error) {
	switch kemID {
	case P256HKDFSHA256:
		return &nistCurvesKEM{
			kemID:              P256HKDFSHA256,
			hmacHashAlg:        SHA256,
			curve:              ecdh.P256(),
			generatePrivateKey: ecdh.P256().GenerateKey,
		}, nil
	case P384HKDFSHA384:
		return &nistCurvesKEM{
			kemID:              P384HKDFSHA384,
			hmacHashAlg:        SHA384,
			curve:              ecdh.P384(),
			generatePrivateKey: ecdh.P384().GenerateKey,
		}, nil
	case P521HKDFSHA512:
		return &nistCurvesKEM{
			kemID:              P521HKDFSHA512,
			hmacHashAlg:        SHA512,
			curve:              ecdh.P521(),
			generatePrivateKey: ecdh.P521().GenerateKey,
		}, nil
	default:
		return nil, fmt.Errorf("KEM ID %d is not supported", kemID)
	}
}

func (x *nistCurvesKEM) encapsulate(recipientPubKeyBytes []byte) (sharedSecret, senderPubKeyBytes []byte, err error) {
	senderPrivKey, err := x.generatePrivateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	recipientPubKey, err := x.curve.NewPublicKey(recipientPubKeyBytes)
	if err != nil {
		return nil, nil, err
	}
	dh, err := senderPrivKey.ECDH(recipientPubKey)
	if err != nil {
		return nil, nil, err
	}
	senderPubKeyBytes = senderPrivKey.PublicKey().Bytes()
	sharedSecret, err = x.deriveKEMSharedSecret(dh, senderPubKeyBytes, recipientPubKeyBytes)
	if err != nil {
		return nil, nil, err
	}
	return sharedSecret, senderPubKeyBytes, nil
}

func (x *nistCurvesKEM) decapsulate(senderPubKeyBytes, recipientPrivKeyBytes []byte) ([]byte, error) {
	recipientPrivKey, err := x.curve.NewPrivateKey(recipientPrivKeyBytes)
	if err != nil {
		return nil, err
	}
	senderPubKey, err := x.curve.NewPublicKey(senderPubKeyBytes)
	if err != nil {
		return nil, err
	}
	dh, err := recipientPrivKey.ECDH(senderPubKey)
	if err != nil {
		return nil, err
	}
	recipientPubKeyBytes := recipientPrivKey.PublicKey().Bytes()
	return x.deriveKEMSharedSecret(dh, senderPubKeyBytes, recipientPubKeyBytes)
}

func (x *nistCurvesKEM) id() KEMID { return x.kemID }

func (x *nistCurvesKEM) encapsulatedKeyLength() int { return kemLengths[x.kemID].nEnc }

// deriveKEMSharedSecret returns a pseudorandom key obtained via the HKDF.
func (x *nistCurvesKEM) deriveKEMSharedSecret(dh, senderPubKey, recipientPubKey []byte) ([]byte, error) {
	ctx := slices.Concat(senderPubKey, recipientPubKey)
	suiteID := kemSuiteID(x.kemID)
	hmacHashLength, err := subtle.GetHashDigestSize(x.hmacHashAlg.String())
	if err != nil {
		return nil, err
	}
	hkdfKDF, err := newHKDFKDF(x.hmacHashAlg)
	if err != nil {
		return nil, err
	}
	return hkdfKDF.extractAndExpand(
		nil, /*=salt*/
		dh,
		"eae_prk",
		ctx,
		"shared_secret",
		suiteID,
		int(hmacHashLength))
}
