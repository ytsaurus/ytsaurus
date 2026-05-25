// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package keyderivers provides functions to register and use key derivers.
package keyderivers

import (
	"fmt"
	"io"
	"reflect"

	"golang.org/x/crypto/ed25519"
	"github.com/tink-crypto/tink-go/v2/aead/aesgcm"
	"github.com/tink-crypto/tink-go/v2/aead/xchacha20poly1305"
	"github.com/tink-crypto/tink-go/v2/daead/aessiv"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/mac/hmac"
	"github.com/tink-crypto/tink-go/v2/prf/hkdfprf"
	"github.com/tink-crypto/tink-go/v2/prf/hmacprf"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	tinked25519 "github.com/tink-crypto/tink-go/v2/signature/ed25519"
	"github.com/tink-crypto/tink-go/v2/streamingaead/aesgcmhkdf"
)

var (
	keyDerivers = make(map[reflect.Type]keyDeriver)
)

type keyDeriver func(parameters key.Parameters, idRequirement uint32, reader io.Reader, token insecuresecretdataaccess.Token) (key.Key, error)

// DeriveKey derives a new [key.Key] from the given [key.Parameters].
//
// It looks up the appropriate key deriver from the registry based on the type
// of params.
func DeriveKey(params key.Parameters, idRequirement uint32, reader io.Reader, token insecuresecretdataaccess.Token) (key.Key, error) {
	pType := reflect.TypeOf(params)
	deriver, ok := keyDerivers[pType]
	if !ok {
		return nil, fmt.Errorf("no key deriver found for %v", pType)
	}
	return deriver(params, idRequirement, reader, token)
}

// CanDeriveKey returns true if the given parameters type can be used to derive
// a key.
func CanDeriveKey(paramsType reflect.Type) bool {
	_, ok := keyDerivers[paramsType]
	return ok
}

func addAESGCMKeyDeriver() {
	parametersType := reflect.TypeFor[*aesgcm.Parameters]()
	keyDerivers[parametersType] = func(p key.Parameters, idRequirement uint32, reader io.Reader, token insecuresecretdataaccess.Token) (key.Key, error) {
		params, ok := p.(*aesgcm.Parameters)
		if !ok {
			return nil, fmt.Errorf("key is of type %T; needed %T", p, (*aesgcm.Parameters)(nil))
		}
		keyBytes := make([]byte, params.KeySizeInBytes())
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, fmt.Errorf("insufficient pseudorandomness")
		}
		return aesgcm.NewKey(secretdata.NewBytesFromData(keyBytes, token), idRequirement, params)
	}
}

func addXChaCha20Poly1305KeyDeriver() {
	parametersType := reflect.TypeFor[*xchacha20poly1305.Parameters]()
	keyDerivers[parametersType] = func(p key.Parameters, idRequirement uint32, reader io.Reader, token insecuresecretdataaccess.Token) (key.Key, error) {
		params, ok := p.(*xchacha20poly1305.Parameters)
		if !ok {
			return nil, fmt.Errorf("key is of type %T; needed %T", p, (*xchacha20poly1305.Parameters)(nil))
		}
		keyBytes := make([]byte, 32)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, fmt.Errorf("insufficient pseudorandomness")
		}
		return xchacha20poly1305.NewKey(secretdata.NewBytesFromData(keyBytes, token), idRequirement, params)
	}
}

func addAESSIVKeyDeriver() {
	parametersType := reflect.TypeFor[*aessiv.Parameters]()
	keyDerivers[parametersType] = func(p key.Parameters, idRequirement uint32, reader io.Reader, token insecuresecretdataaccess.Token) (key.Key, error) {
		params, ok := p.(*aessiv.Parameters)
		if !ok {
			return nil, fmt.Errorf("key is of type %T; needed %T", p, (*aessiv.Parameters)(nil))
		}
		keyBytes := make([]byte, params.KeySizeInBytes())
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, fmt.Errorf("insufficient pseudorandomness")
		}
		return aessiv.NewKey(secretdata.NewBytesFromData(keyBytes, token), idRequirement, params)
	}
}

func addHMACKeyDeriver() {
	parametersType := reflect.TypeFor[*hmac.Parameters]()
	keyDerivers[parametersType] = func(p key.Parameters, idRequirement uint32, reader io.Reader, token insecuresecretdataaccess.Token) (key.Key, error) {
		params, ok := p.(*hmac.Parameters)
		if !ok {
			return nil, fmt.Errorf("key is of type %T; needed %T", p, (*hmac.Parameters)(nil))
		}
		keyBytes := make([]byte, params.KeySizeInBytes())
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, fmt.Errorf("insufficient pseudorandomness")
		}
		return hmac.NewKey(secretdata.NewBytesFromData(keyBytes, token), params, idRequirement)
	}
}

func addHKDFPRFKeyDeriver() {
	parametersType := reflect.TypeFor[*hkdfprf.Parameters]()
	keyDerivers[parametersType] = func(p key.Parameters, _ uint32, reader io.Reader, token insecuresecretdataaccess.Token) (key.Key, error) {
		params, ok := p.(*hkdfprf.Parameters)
		if !ok {
			return nil, fmt.Errorf("key is of type %T; needed %T", p, (*hkdfprf.Parameters)(nil))
		}
		keyBytes := make([]byte, params.KeySizeInBytes())
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, fmt.Errorf("insufficient pseudorandomness")
		}
		return hkdfprf.NewKey(secretdata.NewBytesFromData(keyBytes, token), params)
	}
}

func addHMACPRFKeyDeriver() {
	parametersType := reflect.TypeFor[*hmacprf.Parameters]()
	keyDerivers[parametersType] = func(p key.Parameters, _ uint32, reader io.Reader, token insecuresecretdataaccess.Token) (key.Key, error) {
		params, ok := p.(*hmacprf.Parameters)
		if !ok {
			return nil, fmt.Errorf("key is of type %T; needed %T", p, (*hmacprf.Parameters)(nil))
		}
		keyBytes := make([]byte, params.KeySizeInBytes())
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, fmt.Errorf("insufficient pseudorandomness")
		}
		return hmacprf.NewKey(secretdata.NewBytesFromData(keyBytes, token), params)
	}
}

func addSignatureED25519KeyDeriver() {
	parametersType := reflect.TypeFor[*tinked25519.Parameters]()
	keyDerivers[parametersType] = func(p key.Parameters, idRequirement uint32, reader io.Reader, token insecuresecretdataaccess.Token) (key.Key, error) {
		params, ok := p.(*tinked25519.Parameters)
		if !ok {
			return nil, fmt.Errorf("key is of type %T; needed %T", p, (*tinked25519.Parameters)(nil))
		}
		_, priv, err := ed25519.GenerateKey(reader)
		if err != nil {
			return nil, fmt.Errorf("ed25519.GenerateKey() failed: %v", err)
		}
		return tinked25519.NewPrivateKey(secretdata.NewBytesFromData(priv.Seed(), token), idRequirement, *params)
	}
}

func addStreamingAEADAESGCMHKDFKeyDeriver() {
	parametersType := reflect.TypeFor[*aesgcmhkdf.Parameters]()
	keyDerivers[parametersType] = func(p key.Parameters, idRequirement uint32, reader io.Reader, token insecuresecretdataaccess.Token) (key.Key, error) {
		params, ok := p.(*aesgcmhkdf.Parameters)
		if !ok {
			return nil, fmt.Errorf("key is of type %T; needed %T", p, (*aesgcmhkdf.Parameters)(nil))
		}
		keyBytes := make([]byte, params.KeySizeInBytes())
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, fmt.Errorf("insufficient pseudorandomness")
		}
		return aesgcmhkdf.NewKey(params, secretdata.NewBytesFromData(keyBytes, token))
	}
}

func init() {
	addAESGCMKeyDeriver()
	addXChaCha20Poly1305KeyDeriver()
	addAESSIVKeyDeriver()
	addHMACKeyDeriver()
	addHKDFPRFKeyDeriver()
	addHMACPRFKeyDeriver()
	addSignatureED25519KeyDeriver()
	addStreamingAEADAESGCMHKDFKeyDeriver()
}
