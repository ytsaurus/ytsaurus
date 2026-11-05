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

package mldsa

import (
	"fmt"
	"slices"

	"golang.org/x/crypto/sha3"
)

// The encoding (and decoding) functions come in two variants:
//
// 1. The "unsigned" variant encodes unsigned rZq elements directly.
//    Every element needs to be representable in the range [0, 2^bits).
// 2. The "signed" variant allows for more general elements that can be represented
//    by signed words in the range [-a, 2^bits) for some lower bound a.

// Algorithm 16 (SimpleBitPack)
func (p *poly) simpleBitPack(bits int) []byte {
	encoded := make([]byte, (degree*bits)/8)
	for i := 0; i < len(encoded)*8; i++ {
		cidx := i / bits
		coff := i % bits
		eidx := i >> 3
		eoff := i & 7
		bit := byte((p[cidx] >> coff) & 0x1)
		encoded[eidx] ^= bit << eoff
	}
	return encoded
}

// Algorithm 17 (BitPack)
func (p *poly) bitPack(a rZq, bits int) []byte {
	return p.subFrom(a).simpleBitPack(bits)
}

// Algorithm 16 (SimpleBitPack)
func (p *polyNTT) simpleBitPack(bits int) []byte {
	encoded := make([]byte, (degree*bits)/8)
	for i := 0; i < len(encoded)*8; i++ {
		cidx := i / bits
		coff := i % bits
		eidx := i >> 3
		eoff := i & 7
		bit := byte((p[cidx] >> coff) & 0x1)
		encoded[eidx] ^= bit << eoff
	}
	return encoded
}

// Algorithm 17 (BitPack)
func (p *polyNTT) bitPack(a rZq, bits int) []byte {
	return p.subFrom(a).simpleBitPack(bits)
}

// Algorithm 18 (SimpleBitUnpack)
func simpleBitUnpackPoly(encoded []byte, bits int) *poly {
	res := &poly{}
	for i := 0; i < len(encoded)*8; i++ {
		cidx := i / bits
		coff := i % bits
		eidx := i >> 3
		eoff := i & 7
		bit := rZq((encoded[eidx] >> eoff) & 0x1)
		res[cidx] ^= bit << coff
	}
	return res
}

// Algorithm 19 (BitUnpack)
func bitUnpackPoly(encoded []byte, a rZq, bits int) *poly {
	return simpleBitUnpackPoly(encoded, bits).subFrom(a)
}

// Algorithm 18 (SimpleBitUnpack)
func simpleBitUnpackPolyNTT(encoded []byte, bits int) *polyNTT {
	res := &polyNTT{}
	for i := 0; i < len(encoded)*8; i++ {
		cidx := i / bits
		coff := i % bits
		eidx := i >> 3
		eoff := i & 7
		bit := rZq((encoded[eidx] >> eoff) & 0x1)
		res[cidx] ^= bit << coff
	}
	return res
}

// Algorithm 19 (BitUnpack)
func bitUnpackPolyNTT(encoded []byte, a rZq, bits int) *polyNTT {
	return simpleBitUnpackPolyNTT(encoded, bits).subFrom(a)
}

// Algorithm 20 (HintBitPack)
func (v vector) hintBitPack(par *params) []byte {
	res := make([]byte, par.omega+par.k)
	index := 0
	for i := 0; i < par.k; i++ {
		for j := 0; j < degree; j++ {
			if v[i][j] != 0 {
				res[index] = byte(j)
				index++
			}
		}
		res[par.omega+i] = byte(index)
	}
	return res
}

// Algorithm 21 (HintBitUnpack)
func (par *params) hintBitUnpackVector(encoded []byte) (vector, error) {
	res := makeZeroVector(par.k)
	index := 0
	for i := 0; i < par.k; i++ {
		end := int(encoded[par.omega+i])
		if end < index || end > par.omega {
			return nil, fmt.Errorf("invalid hint bit vector")
		}
		first := index
		for index < end {
			if index > first && encoded[index-1] >= encoded[index] {
				return nil, fmt.Errorf("invalid hint bit vector")
			}
			res[i][encoded[index]] = 1
			index++
		}
	}
	for i := index; i < par.omega; i++ {
		if encoded[i] != 0 {
			return nil, fmt.Errorf("invalid hint bit vector")
		}
	}
	return res, nil
}

// Encode encodes a public key. This is Algorithm 22 (pkEncode) of the ML-DSA specification.
func (pk *PublicKey) Encode() []byte {
	res := make([]byte, 32)
	copy(res[0:32], pk.rho[:])
	for i := range pk.t1 {
		res = slices.Concat(res, pk.t1[i].simpleBitPack(qBits-d))
	}
	return res
}

// PublicKeyLength returns the length of a public key.
func (par *params) PublicKeyLength() int {
	return 32 + 32*par.k*(qBits-d)
}

// DecodePublicKey decodes a public key. This is Algorithm 23 (pkDecode) of the ML-DSA specification.
func (par *params) DecodePublicKey(pkEnc []byte) (*PublicKey, error) {
	if len(pkEnc) != par.PublicKeyLength() {
		return nil, fmt.Errorf("invalid public key length")
	}
	var rho [32]byte
	copy(rho[:], pkEnc[0:32])
	res := makeZeroVector(par.k)
	for i := range res {
		pos := 32 + i*32*(qBits-d)
		res[i] = simpleBitUnpackPoly(pkEnc[pos:pos+32*(qBits-d)], qBits-d)
	}
	// We additionally cache the hash of the public key.
	var tr [64]byte
	sha3.ShakeSum256(tr[:], pkEnc)
	return &PublicKey{rho, res, tr, par}, nil
}

// Encode encodes a secret key. This is Algorithm 24 (skEncode) of the ML-DSA specification.
func (sk *SecretKey) Encode() []byte {
	par := sk.par
	res := make([]byte, 32+32+64)
	copy(res[0:32], sk.rho[:])
	copy(res[32:64], sk.kK[:])
	copy(res[64:128], sk.tr[:])
	for i := range sk.s1 {
		res = slices.Concat(res, sk.s1[i].bitPack(rZq(par.eta), par.etaBits))
	}
	for i := range sk.s2 {
		res = slices.Concat(res, sk.s2[i].bitPack(rZq(par.eta), par.etaBits))
	}
	for i := range sk.t0 {
		res = slices.Concat(res, sk.t0[i].bitPack(rZq(1<<(d-1)), d))
	}
	return res
}

// SecretKeyLength returns the length of a secret key.
func (par *params) SecretKeyLength() int {
	return 32 + 32 + 64 + 32*((par.l+par.k)*par.etaBits+d*par.k)
}

// DecodeSecretKey decodes a secret key. This is Algorithm 25 (skDecode) of the ML-DSA specification.
func (par *params) DecodeSecretKey(skEnc []byte) (*SecretKey, error) {
	if len(skEnc) != par.SecretKeyLength() {
		return nil, fmt.Errorf("invalid secret key length")
	}
	var rho [32]byte
	var K [32]byte
	var tr [64]byte
	copy(rho[:], skEnc[0:32])
	copy(K[:], skEnc[32:64])
	copy(tr[:], skEnc[64:128])
	s1 := makeZeroVector(par.l)
	s2 := makeZeroVector(par.k)
	t0 := makeZeroVector(par.k)
	sStep := 32 * par.etaBits
	for i := range s1 {
		pos := 128 + i*sStep
		s1[i] = bitUnpackPoly(skEnc[pos:pos+sStep], rZq(par.eta), par.etaBits)
	}
	for i := range s2 {
		pos := 128 + par.l*sStep + i*sStep
		s2[i] = bitUnpackPoly(skEnc[pos:pos+sStep], rZq(par.eta), par.etaBits)
	}
	for i := range t0 {
		pos := 128 + par.l*sStep + par.k*sStep + i*32*d
		t0[i] = bitUnpackPoly(skEnc[pos:pos+32*d], rZq(1<<(d-1)), d)
	}
	return &SecretKey{rho, K, tr, s1, s2, t0, nil, par}, nil
}

// Algorithm 26 (SigEncode)
func (par *params) sigEncode(c []byte, z vector, h vector) []byte {
	res := make([]byte, par.lambda/4)
	copy(res, c)
	for i := range z {
		res = slices.Concat(res, z[i].bitPack(rZq(1<<par.log2Gamma1), par.log2Gamma1+1))
	}
	return slices.Concat(res, h.hintBitPack(par))
}

// Algorithm 27 (SigDecode)
func (par *params) sigDecode(sigma []byte) ([]byte, vector, vector, error) {
	if len(sigma) != par.lambda/4+par.l*32*(1+par.log2Gamma1)+par.omega+par.k {
		return nil, nil, nil, fmt.Errorf("invalid signature length")
	}
	c := make([]byte, par.lambda/4)
	copy(c, sigma[0:par.lambda/4])
	z := makeZeroVector(par.l)
	for i := range z {
		pos := par.lambda/4 + i*32*(par.log2Gamma1+1)
		z[i] = bitUnpackPoly(sigma[pos:pos+32*(par.log2Gamma1+1)], rZq(1<<par.log2Gamma1), par.log2Gamma1+1)
	}
	pos := par.lambda/4 + par.l*32*(par.log2Gamma1+1)
	h, err := par.hintBitUnpackVector(sigma[pos:])
	if err != nil {
		return nil, nil, nil, err
	}
	return c, z, h, nil
}

// Algorithm 28 (w1Encode)
func (par *params) w1Encode(w1 vector) []byte {
	res := make([]byte, 0)
	for i := range w1 {
		res = slices.Concat(res, w1[i].simpleBitPack(par.w1Bits))
	}
	return res
}
