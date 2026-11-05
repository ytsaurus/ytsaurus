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

// Package jwk provides functions to convert between JWK and Tink keysets.
package jwk

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math/big"
	"slices"

	spb "google.golang.org/protobuf/types/known/structpb"
	"github.com/tink-crypto/tink-go/v2/jwt/jwtecdsa"
	"github.com/tink-crypto/tink-go/v2/jwt/jwtrsassapkcs1"
	"github.com/tink-crypto/tink-go/v2/jwt/jwtrsassapss"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/signature/ed25519"
)

// Ed25519SupportType is an enum to control Ed25519 key conversion.
type Ed25519SupportType int

const (
	// Ed25519SupportNone means Ed25519 keys are not supported and will return an error.
	Ed25519SupportNone Ed25519SupportType = iota
	// Ed25519SupportTink means Ed25519 keys are supported and will be converted to Tink keys.
	Ed25519SupportTink
)

func hasItem(s *spb.Struct, name string) bool {
	if s.GetFields() == nil {
		return false
	}
	_, ok := s.Fields[name]
	return ok
}

func stringItem(s *spb.Struct, name string) (string, error) {
	fields := s.GetFields()
	if fields == nil {
		return "", fmt.Errorf("no fields")
	}
	val, ok := fields[name]
	if !ok {
		return "", fmt.Errorf("field %q not found", name)
	}
	r, ok := val.Kind.(*spb.Value_StringValue)
	if !ok {
		return "", fmt.Errorf("field %q is not a string", name)
	}
	return r.StringValue, nil
}

func listValue(s *spb.Struct, name string) (*spb.ListValue, error) {
	fields := s.GetFields()
	if fields == nil {
		return nil, fmt.Errorf("empty set")
	}
	vals, ok := fields[name]
	if !ok {
		return nil, fmt.Errorf("%q not found", name)
	}
	list, ok := vals.Kind.(*spb.Value_ListValue)
	if !ok {
		return nil, fmt.Errorf("%q is not a list", name)
	}
	if list.ListValue == nil || len(list.ListValue.GetValues()) == 0 {
		return nil, fmt.Errorf("%q list is empty", name)
	}
	return list.ListValue, nil
}

func expectStringItem(s *spb.Struct, name, value string) error {
	item, err := stringItem(s, name)
	if err != nil {
		return err
	}
	if item != value {
		return fmt.Errorf("unexpected value %q for %q", value, name)
	}
	return nil
}

func decodeItem(s *spb.Struct, name string) ([]byte, error) {
	e, err := stringItem(s, name)
	if err != nil {
		return nil, err
	}
	return base64Decode(e)
}

func validateKeyOPSIsVerify(s *spb.Struct) error {
	if !hasItem(s, "key_ops") {
		return nil
	}
	keyOPSList, err := listValue(s, "key_ops")
	if err != nil {
		return err
	}
	if len(keyOPSList.GetValues()) != 1 {
		return fmt.Errorf("key_ops size is not 1")
	}
	value, ok := keyOPSList.GetValues()[0].Kind.(*spb.Value_StringValue)
	if !ok {
		return fmt.Errorf("key_ops is not a string")
	}
	if value.StringValue != "verify" {
		return fmt.Errorf("key_ops is not equal to [\"verify\"]")
	}
	return nil
}

func validateUseIsSig(s *spb.Struct) error {
	if !hasItem(s, "use") {
		return nil
	}
	return expectStringItem(s, "use", "sig")
}

func algorithmPrefix(s *spb.Struct) (string, error) {
	alg, err := stringItem(s, "alg")
	if err != nil {
		return "", err
	}
	if len(alg) < 2 {
		return "", fmt.Errorf("invalid algorithm")
	}
	return alg[0:2], nil
}

func psPublicKeyDataFromStruct(keyStruct *spb.Struct) (key.Key, error) {
	alg, err := stringItem(keyStruct, "alg")
	if err != nil {
		return nil, err
	}
	var algorithm jwtrsassapss.Algorithm
	switch alg {
	case "PS256":
		algorithm = jwtrsassapss.PS256
	case "PS384":
		algorithm = jwtrsassapss.PS384
	case "PS512":
		algorithm = jwtrsassapss.PS512
	default:
		return nil, fmt.Errorf("invalid alg header: %q", alg)
	}
	rsaPubKey, err := rsaPubKeyFromStruct(keyStruct)
	if err != nil {
		return nil, err
	}

	kidStrategy := jwtrsassapss.IgnoredKID
	if rsaPubKey.hasCustomKID {
		kidStrategy = jwtrsassapss.CustomKID
	}

	publicExponent := new(big.Int).SetBytes(rsaPubKey.exponent)
	if !publicExponent.IsInt64() {
		return nil, fmt.Errorf("public exponent cannot be represented as int64")
	}

	params, err := jwtrsassapss.NewParameters(jwtrsassapss.ParametersOpts{
		ModulusSizeInBits: new(big.Int).SetBytes(rsaPubKey.modulus).BitLen(),
		PublicExponent:    int(publicExponent.Int64()),
		Algorithm:         algorithm,
		KidStrategy:       kidStrategy,
	})
	if err != nil {
		return nil, err
	}

	return jwtrsassapss.NewPublicKey(jwtrsassapss.PublicKeyOpts{
		Parameters:    params,
		Modulus:       rsaPubKey.modulus,
		CustomKID:     rsaPubKey.customKID,
		HasCustomKID:  rsaPubKey.hasCustomKID,
		IDRequirement: 0,
	})
}

func rsPublicKeyDataFromStruct(keyStruct *spb.Struct) (key.Key, error) {
	alg, err := stringItem(keyStruct, "alg")
	if err != nil {
		return nil, err
	}
	var algorithm jwtrsassapkcs1.Algorithm
	switch alg {
	case "RS256":
		algorithm = jwtrsassapkcs1.RS256
	case "RS384":
		algorithm = jwtrsassapkcs1.RS384
	case "RS512":
		algorithm = jwtrsassapkcs1.RS512
	default:
		return nil, fmt.Errorf("invalid alg header: %q", alg)
	}
	rsaPubKey, err := rsaPubKeyFromStruct(keyStruct)
	if err != nil {
		return nil, err
	}

	kidStrategy := jwtrsassapkcs1.IgnoredKID
	if rsaPubKey.hasCustomKID {
		kidStrategy = jwtrsassapkcs1.CustomKID
	}

	params, err := jwtrsassapkcs1.NewParameters(jwtrsassapkcs1.ParametersOpts{
		ModulusSizeInBits: new(big.Int).SetBytes(rsaPubKey.modulus).BitLen(),
		PublicExponent:    int(new(big.Int).SetBytes(rsaPubKey.exponent).Int64()),
		Algorithm:         algorithm,
		KidStrategy:       kidStrategy,
	})
	if err != nil {
		return nil, err
	}

	return jwtrsassapkcs1.NewPublicKey(jwtrsassapkcs1.PublicKeyOpts{
		Parameters:    params,
		Modulus:       rsaPubKey.modulus,
		CustomKID:     rsaPubKey.customKID,
		HasCustomKID:  rsaPubKey.hasCustomKID,
		IDRequirement: 0,
	})
}

type rsaPubKey struct {
	exponent     []byte
	modulus      []byte
	customKID    string
	hasCustomKID bool
}

func rsaPubKeyFromStruct(keyStruct *spb.Struct) (*rsaPubKey, error) {
	if hasItem(keyStruct, "p") ||
		hasItem(keyStruct, "q") ||
		hasItem(keyStruct, "dq") ||
		hasItem(keyStruct, "dp") ||
		hasItem(keyStruct, "d") ||
		hasItem(keyStruct, "qi") {
		return nil, fmt.Errorf("private key can't be converted")
	}
	if err := expectStringItem(keyStruct, "kty", "RSA"); err != nil {
		return nil, err
	}
	if err := validateUseIsSig(keyStruct); err != nil {
		return nil, err
	}
	if err := validateKeyOPSIsVerify(keyStruct); err != nil {
		return nil, err
	}
	e, err := decodeItem(keyStruct, "e")
	if err != nil {
		return nil, err
	}
	n, err := decodeItem(keyStruct, "n")
	if err != nil {
		return nil, err
	}
	customKID := ""
	hasCustomKID := false
	if hasItem(keyStruct, "kid") {
		kid, err := stringItem(keyStruct, "kid")
		if err != nil {
			return nil, err
		}
		customKID = kid
		hasCustomKID = true
	}
	return &rsaPubKey{
		exponent:     e,
		modulus:      n,
		customKID:    customKID,
		hasCustomKID: hasCustomKID,
	}, nil
}

func esPublicKeyDataFromStruct(keyStruct *spb.Struct) (key.Key, error) {
	alg, err := stringItem(keyStruct, "alg")
	if err != nil {
		return nil, err
	}
	curve, err := stringItem(keyStruct, "crv")
	if err != nil {
		return nil, err
	}
	algorithm := jwtecdsa.UnknownAlgorithm
	if alg == "ES256" && curve == "P-256" {
		algorithm = jwtecdsa.ES256
	} else if alg == "ES384" && curve == "P-384" {
		algorithm = jwtecdsa.ES384
	} else if alg == "ES512" && curve == "P-521" {
		algorithm = jwtecdsa.ES512
	} else {
		return nil, fmt.Errorf("invalid algorithm %q and curve %q", alg, curve)
	}

	if hasItem(keyStruct, "d") {
		return nil, fmt.Errorf("private keys cannot be converted")
	}
	if err := expectStringItem(keyStruct, "kty", "EC"); err != nil {
		return nil, err
	}
	if err := validateUseIsSig(keyStruct); err != nil {
		return nil, err
	}
	if err := validateKeyOPSIsVerify(keyStruct); err != nil {
		return nil, err
	}
	x, err := decodeItem(keyStruct, "x")
	if err != nil {
		return nil, fmt.Errorf("failed to decode x: %v", err)
	}
	y, err := decodeItem(keyStruct, "y")
	if err != nil {
		return nil, fmt.Errorf("failed to decode y: %v", err)
	}
	customKID := ""
	hasCustomKID := false
	strategy := jwtecdsa.IgnoredKID
	if hasItem(keyStruct, "kid") {
		kid, err := stringItem(keyStruct, "kid")
		if err != nil {
			return nil, err
		}
		customKID = kid
		hasCustomKID = true
		strategy = jwtecdsa.CustomKID
	}

	params, err := jwtecdsa.NewParameters(strategy, algorithm)
	if err != nil {
		return nil, err
	}
	return jwtecdsa.NewPublicKey(jwtecdsa.PublicKeyOpts{
		Parameters:    params,
		PublicPoint:   slices.Concat([]byte{0x04}, x, y),
		CustomKID:     customKID,
		HasCustomKID:  hasCustomKID,
		IDRequirement: 0,
	})
}

func ed25519PublicKeyDataFromStruct(keyStruct *spb.Struct) (key.Key, error) {
	if err := expectStringItem(keyStruct, "kty", "OKP"); err != nil {
		return nil, err
	}
	if err := expectStringItem(keyStruct, "crv", "Ed25519"); err != nil {
		return nil, err
	}
	if err := validateUseIsSig(keyStruct); err != nil {
		return nil, err
	}
	if err := validateKeyOPSIsVerify(keyStruct); err != nil {
		return nil, err
	}
	x, err := decodeItem(keyStruct, "x")
	if err != nil {
		return nil, fmt.Errorf("failed to decode x: %v", err)
	}
	params, err := ed25519.NewParameters(ed25519.VariantNoPrefix)
	if err != nil {
		return nil, err
	}
	return ed25519.NewPublicKey(x, 0, params)
}

func keysetKeyFromStruct(val *spb.Value, ed25519Support Ed25519SupportType) (key.Key, error) {
	keyStruct := val.GetStructValue()
	if keyStruct == nil {
		return nil, fmt.Errorf("key is not a JSON object")
	}
	algPrefix, err := algorithmPrefix(keyStruct)
	if err != nil {
		return nil, err
	}
	var key key.Key
	switch algPrefix {
	case "ES":
		key, err = esPublicKeyDataFromStruct(keyStruct)
	case "RS":
		key, err = rsPublicKeyDataFromStruct(keyStruct)
	case "PS":
		key, err = psPublicKeyDataFromStruct(keyStruct)
	case "Ed":
		if keyStruct.GetFields()["crv"].GetStringValue() == "Ed25519" && ed25519Support != Ed25519SupportNone {
			key, err = ed25519PublicKeyDataFromStruct(keyStruct)
		} else {
			return nil, fmt.Errorf("Ed25519 is not supported")
		}
	default:
		return nil, fmt.Errorf("unsupported algorithm prefix: %v", algPrefix)
	}
	if err != nil {
		return nil, err
	}
	return key, nil
}

// ToPublicKeysetHandle converts a Json Web Key (JWK) set into a Tink public KeysetHandle.
// It requires that all keys in the set have the "alg" field set. Currently, only
// public keys for algorithms ES256, ES384, ES512, RS256, RS384, RS512 are supported.
// Ed25519 is supported, but it will return a Tink Verification Key instead of JWT verification.
// TODO - b/436348879: separate JWK Signature and JWT conversions.
// JWK is defined in https://www.rfc-editor.org/rfc/rfc7517.txt.
func ToPublicKeysetHandle(jwkSet []byte, ed25519Support Ed25519SupportType) (*keyset.Handle, error) {
	jwk := &spb.Struct{}
	if err := jwk.UnmarshalJSON(jwkSet); err != nil {
		return nil, err
	}
	keyList, err := listValue(jwk, "keys")
	if err != nil {
		return nil, err
	}
	km := keyset.NewManager()
	var lastKeyID uint32
	for _, keyStruct := range keyList.GetValues() {
		key, err := keysetKeyFromStruct(keyStruct, ed25519Support)
		if err != nil {
			return nil, err
		}
		if lastKeyID, err = km.AddKey(key); err != nil {
			return nil, err
		}
	}
	if err := km.SetPrimary(lastKeyID); err != nil {
		return nil, err
	}
	return km.Handle()
}

func addKeyOPSVerify(s *spb.Struct) {
	s.GetFields()["key_ops"] = spb.NewListValue(&spb.ListValue{Values: []*spb.Value{spb.NewStringValue("verify")}})
}

func addStringEntry(s *spb.Struct, key, val string) {
	s.GetFields()[key] = spb.NewStringValue(val)
}

func psPublicKeyToStruct(key *jwtrsassapss.PublicKey) (*spb.Struct, error) {
	params := key.Parameters().(*jwtrsassapss.Parameters)
	alg := params.Algorithm().String()
	outKey := &spb.Struct{
		Fields: map[string]*spb.Value{},
	}
	addStringEntry(outKey, "alg", alg)
	addStringEntry(outKey, "kty", "RSA")
	addStringEntry(outKey, "e", base64Encode(new(big.Int).SetInt64(int64(params.PublicExponent())).Bytes()))
	addStringEntry(outKey, "n", base64Encode(key.Modulus()))
	addStringEntry(outKey, "use", "sig")
	addKeyOPSVerify(outKey)

	idRequirement, hasIDRequirement := key.IDRequirement()
	kid, _ := key.KID()
	var customKID *string = nil
	if params.KIDStrategy() == jwtrsassapss.CustomKID {
		customKID = &kid
	}
	if err := setKeyID(outKey, idRequirement, hasIDRequirement, customKID); err != nil {
		return nil, err
	}
	return outKey, nil
}

func rsPublicKeyToStruct(key *jwtrsassapkcs1.PublicKey) (*spb.Struct, error) {
	params := key.Parameters().(*jwtrsassapkcs1.Parameters)
	alg := params.Algorithm().String()
	outKey := &spb.Struct{
		Fields: map[string]*spb.Value{},
	}
	addStringEntry(outKey, "alg", alg)
	addStringEntry(outKey, "kty", "RSA")
	addStringEntry(outKey, "e", base64Encode(new(big.Int).SetInt64(int64(params.PublicExponent())).Bytes()))
	addStringEntry(outKey, "n", base64Encode(key.Modulus()))
	addStringEntry(outKey, "use", "sig")
	addKeyOPSVerify(outKey)

	idRequirement, hasIDRequirement := key.IDRequirement()
	kid, _ := key.KID()
	var customKID *string = nil
	if params.KIDStrategy() == jwtrsassapkcs1.CustomKID {
		customKID = &kid
	}
	if err := setKeyID(outKey, idRequirement, hasIDRequirement, customKID); err != nil {
		return nil, err
	}
	return outKey, nil
}

// Assumes key is not nil and valid, that is, created via [jwtecdsa.NewPublicKey].
func esPublicKeyToStruct(key *jwtecdsa.PublicKey) (*spb.Struct, error) {
	params := key.Parameters().(*jwtecdsa.Parameters)
	var algorithm, curve string
	var encLen int
	switch params.Algorithm() {
	case jwtecdsa.ES256:
		curve, algorithm, encLen = "P-256", "ES256", 32
	case jwtecdsa.ES384:
		curve, algorithm, encLen = "P-384", "ES384", 48
	case jwtecdsa.ES512:
		curve, algorithm, encLen = "P-521", "ES512", 66
	default:
		return nil, fmt.Errorf("invalid algorithm")
	}

	// Point of the form [0x04, x, y].
	publicPoint := key.PublicPoint()[1:]
	x, y := publicPoint[:encLen], publicPoint[encLen:]
	// RFC 7518 specifies a fixed sized encoding for the x and y coordinates from SEC 1
	// https://datatracker.ietf.org/doc/html/rfc7518#section-6.2.1.2
	if len, wantLen := new(big.Int).SetBytes(x).BitLen(), encLen*8; len > wantLen {
		return nil, fmt.Errorf("invalid x coordinate length; got %d bit, want %d bit", len, wantLen)
	}
	if len, wantLen := new(big.Int).SetBytes(y).BitLen(), encLen*8; len > wantLen {
		return nil, fmt.Errorf("invalid y coordinate length; got %d bit, want %d bit", len, wantLen)
	}

	outKey := &spb.Struct{
		Fields: map[string]*spb.Value{},
	}
	addStringEntry(outKey, "crv", curve)
	addStringEntry(outKey, "alg", algorithm)
	addStringEntry(outKey, "kty", "EC")
	addStringEntry(outKey, "x", base64Encode(x))
	addStringEntry(outKey, "y", base64Encode(y))
	addStringEntry(outKey, "use", "sig")
	addKeyOPSVerify(outKey)

	idRequirement, hasIDRequirement := key.IDRequirement()
	kid, _ := key.KID()
	var customKID *string = nil
	if params.KIDStrategy() == jwtecdsa.CustomKID {
		customKID = &kid
	}
	if err := setKeyID(outKey, idRequirement, hasIDRequirement, customKID); err != nil {
		return nil, err
	}
	return outKey, nil
}

func ed25519PublicKeyToStruct(key *ed25519.PublicKey, encodedKID *string) (*spb.Struct, error) {
	outKey := &spb.Struct{
		Fields: map[string]*spb.Value{},
	}
	addStringEntry(outKey, "kty", "OKP")
	addStringEntry(outKey, "crv", "Ed25519")
	addStringEntry(outKey, "alg", "EdDSA")
	addStringEntry(outKey, "x", base64Encode(key.KeyBytes()))
	addStringEntry(outKey, "use", "sig")
	addKeyOPSVerify(outKey)

	// Signature keys only use the provided key ID, not the idRequirement.
	if err := setKeyID(outKey, 0, false, encodedKID); err != nil {
		return nil, err
	}
	return outKey, nil
}

func setKeyID(outKey *spb.Struct, keyID uint32, hasIDRequirement bool, customKID *string) error {
	if hasIDRequirement { // TINK
		if customKID != nil {
			return fmt.Errorf("TINK keys shouldn't have custom KID")
		}
		kid := keyIDToKID(keyID)
		addStringEntry(outKey, "kid", *kid)
	} else if customKID != nil { // RAW
		addStringEntry(outKey, "kid", *customKID)
	}
	return nil
}

// FromPublicKeysetHandle converts a Tink KeysetHandle into a Json Web Key (JWK) set.
// Currently only public keys for algorithms ES256, ES384, ES512, RS256, RS384, RS512 are supported.
// Ed25519 is supported, but it will return a Tink Verification Key instead of JWT verification.
// TODO - b/436348879: separate JWK Signature and JWT conversions.
// JWK is defined in https://www.rfc-editor.org/rfc/rfc7517.html.
func FromPublicKeysetHandle(kh *keyset.Handle, ed25519Support Ed25519SupportType) ([]byte, error) {
	var keyValList []*spb.Value
	for i := 0; i < kh.Len(); i++ {
		e, err := kh.Entry(i)
		if err != nil {
			return nil, err
		}
		if e.KeyStatus() != keyset.Enabled {
			continue
		}
		keyStruct := &spb.Struct{}
		switch k := e.Key().(type) {
		case *jwtecdsa.PublicKey:
			keyStruct, err = esPublicKeyToStruct(k)
		case *jwtrsassapkcs1.PublicKey:
			keyStruct, err = rsPublicKeyToStruct(k)
		case *jwtrsassapss.PublicKey:
			keyStruct, err = psPublicKeyToStruct(k)
		case *ed25519.PublicKey:
			switch ed25519Support {
			case Ed25519SupportNone:
				return nil, fmt.Errorf("Ed25519 keys are not supported")
			case Ed25519SupportTink:
				// ed25519PublicKeyToStruct will always encode the provided key ID instead of idRequirement.
				keyStruct, err = ed25519PublicKeyToStruct(k, keyIDToKID(e.KeyID()))
			}
		default:
			return nil, fmt.Errorf("unsupported key type %T", k)
		}
		if err != nil {
			return nil, err
		}
		keyValList = append(keyValList, spb.NewStructValue(keyStruct))
	}
	output := &spb.Struct{
		Fields: map[string]*spb.Value{
			"keys": spb.NewListValue(&spb.ListValue{Values: keyValList}),
		},
	}
	return output.MarshalJSON()
}

// From google3/third_party/tink/go/jwt/jwt_encoding.go

// base64Encode encodes a byte array into a base64 URL safe string with no padding.
func base64Encode(content []byte) string {
	return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(content)
}

// base64Decode decodes a URL safe base64 encoded string into a byte array ignoring padding.
func base64Decode(content string) ([]byte, error) {
	for _, c := range content {
		if !isValidURLsafeBase64Char(c) {
			return nil, fmt.Errorf("invalid encoding")
		}
	}
	return base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(content)
}

func isValidURLsafeBase64Char(c rune) bool {
	return (((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z')) ||
		((c >= '0') && (c <= '9')) || ((c == '-') || (c == '_')))
}

// keyIDToKID returns the keyID in big endian format base64 encoded.
func keyIDToKID(keyID uint32) *string {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, keyID)
	s := base64Encode(buf)
	return &s
}
