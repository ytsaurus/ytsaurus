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

// Package subtle provides subtle implementations of the DeterministicAEAD
// primitive.
package subtle

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/subtle"
	"errors"
	"fmt"
	"slices"

	"github.com/tink-crypto/tink-go/v2/internal/mac/aescmac"

	// Placeholder for internal crypto/cipher allowlist, please ignore.
	// Placeholder for internal crypto/subtle allowlist, please ignore.
)

// AESSIV is an implementation of AES-SIV-CMAC as defined in
// https://tools.ietf.org/html/rfc5297.
//
// AESSIV implements a deterministic encryption with associated data (i.e. the
// DeterministicAEAD interface). Hence the implementation below is restricted
// to one AD component.
//
// Security Note:
//
// Chatterjee, Menezes and Sarkar analyze AES-SIV in Section 5.1 of
// https://www.math.uwaterloo.ca/~ajmeneze/publications/tightness.pdf
//
// Their analysis shows that AES-SIV is susceptible to an attack in
// a multi-user setting. Concretely, if an attacker knows the encryption
// of a message m encrypted and authenticated with k different keys,
// then it is possible  to find one of the MAC keys in time 2^b / k
// where b is the size of the MAC key. A consequence of this attack
// is that 128-bit MAC keys give unsufficient security.
// Since 192-bit AES keys are not supported by tink for voodoo reasons
// and RFC 5297 only supports same size encryption and MAC keys this
// implies that keys must be 64 bytes (2*256 bits) long.
type AESSIV struct {
	k1, k2 []byte
	cmac   *aescmac.CMAC
}

const (
	// AESSIVKeySize is the key size in bytes.
	AESSIVKeySize = 64

	intSize = 32 << (^uint(0) >> 63) // 32 or 64
	maxInt  = 1<<(intSize-1) - 1
)

// NewAESSIV returns an AESSIV instance.
func NewAESSIV(key []byte) (*AESSIV, error) {
	if len(key) != AESSIVKeySize {
		return nil, fmt.Errorf("aes_siv: invalid key size %d", len(key))
	}

	k1 := key[:32]
	k2 := key[32:]

	cmac, err := aescmac.New(k1)
	if err != nil {
		return nil, fmt.Errorf("aes_siv: %v", err)
	}

	return &AESSIV{k1: k1, k2: k2, cmac: cmac}, nil
}

// multiplyByX multiplies an element in GF(2^128) by its generator.
//
// This function is incorrectly named "doubling" in section 2.3 of RFC 5297.
func multiplyByX(block []byte) {
	carry := int(block[0] >> 7)
	for i := 0; i < aes.BlockSize-1; i++ {
		block[i] = (block[i] << 1) | (block[i+1] >> 7)
	}

	block[aes.BlockSize-1] = (block[aes.BlockSize-1] << 1) ^ byte(subtle.ConstantTimeSelect(carry, 0x87, 0x00))
}

// EncryptDeterministically deterministically encrypts plaintext with associatedData.
func (asc *AESSIV) EncryptDeterministically(plaintext, associatedData []byte) ([]byte, error) {
	if len(plaintext) > maxInt-aes.BlockSize {
		return nil, fmt.Errorf("aes_siv: plaintext too long")
	}
	siv := asc.s2v(plaintext, associatedData)
	ct := make([]byte, len(plaintext)+aes.BlockSize)
	copy(ct[:aes.BlockSize], siv)
	if err := asc.ctrCrypt(siv, plaintext, ct[aes.BlockSize:]); err != nil {
		return nil, err
	}

	return ct, nil
}

// DecryptDeterministically deterministically decrypts ciphertext with associatedData.
func (asc *AESSIV) DecryptDeterministically(ciphertext, associatedData []byte) ([]byte, error) {
	if len(ciphertext) < aes.BlockSize {
		return nil, errors.New("aes_siv: ciphertext is too short")
	}

	pt := make([]byte, len(ciphertext)-aes.BlockSize)
	siv := ciphertext[:aes.BlockSize]
	asc.ctrCrypt(siv, ciphertext[aes.BlockSize:], pt)
	s2v := asc.s2v(pt, associatedData)

	diff := byte(0)
	for i := 0; i < aes.BlockSize; i++ {
		diff |= siv[i] ^ s2v[i]
	}
	if diff != 0 {
		return nil, errors.New("aes_siv: invalid ciphertext")
	}

	return pt, nil
}

// ctrCrypt encrypts (or decrypts) the bytes in in using an SIV and writes the
// result to out.
func (asc *AESSIV) ctrCrypt(siv, in, out []byte) error {
	// siv might be used outside of ctrCrypt(), so making a copy of it.
	iv := slices.Clone(siv)
	iv[8] &= 0x7f
	iv[12] &= 0x7f

	c, err := aes.NewCipher(asc.k2)
	if err != nil {
		return fmt.Errorf("aes_siv: aes.NewCipher() failed: %v", err)
	}

	steam := cipher.NewCTR(c, iv)
	steam.XORKeyStream(out, in)
	return nil
}

var zeroBlock [aes.BlockSize]byte

// s2v is a Pseudo-Random Function (PRF) construction as defined in
// Section 2.4 of RFC 5297.
func (asc *AESSIV) s2v(msg, ad []byte) []byte {
	block := asc.cmac.Compute(zeroBlock[:])
	multiplyByX(block)

	// block := CMAC(AD) XOR block
	adMac := asc.cmac.Compute(ad)
	subtle.XORBytes(block, block, adMac)
	if len(msg) >= aes.BlockSize {
		// v := CMAC(msg XOREND block)
		res, err := asc.cmac.XOREndAndCompute(msg, block)
		// This should never happen, so we prefer to panic.
		if err != nil {
			panic(err)
		}
		return res
	}
	// block := MultiplyByX(block) XOR pad(msg)
	multiplyByX(block)
	subtle.XORBytes(block, block, msg)
	block[len(msg)] ^= 0x80
	// v := CMAC(block)
	return asc.cmac.Compute(block)
}
