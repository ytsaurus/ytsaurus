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

// Package slhdsa implements SLH-DSA as specified in NIST FIPS 205 (https://doi.org/10.6028/NIST.FIPS.205).
// The implementation is constant time assuming that the underlying hashing primitives are constant time.
package slhdsa

import (
	"crypto/rand"
	"fmt"
	"math/bits"
	"slices"
)

type params struct {
	// SLH-DSA parameters (see Table 2 of the SLH-DSA specification).
	n uint32
	// Note that h = d * hp.
	h   uint32
	d   uint32
	hp  uint32
	a   uint32
	k   uint32
	lgw uint32
	m   uint32

	// Derived parameters (defined by Algorithm 1 and Equations 5.1, 5.2, 5.3, and 5.4 of the SLH-DSA specification).
	w    uint32
	len1 uint32
	len2 uint32
	len  uint32

	// Hashing functions.
	pHMsg   func(r []byte, pkSeed []byte, pkRoot []byte, msg []byte, m uint32) []byte
	pPrf    func(pkSeed []byte, skSeed []byte, adrs *address, n uint32) []byte
	pPrfMsg func(skPrf []byte, optRand []byte, M []byte, n uint32) []byte
	pF      func(pkSeed []byte, adrs *address, M1 []byte, n uint32) []byte
	pH      func(pkSeed []byte, adrs *address, M2 []byte, n uint32) []byte
	pTl     func(pkSeed []byte, adrs *address, Ml []byte, n uint32) []byte
}

func (p *params) hHMsg(r []byte, pkSeed []byte, pkRoot []byte, msg []byte) []byte {
	return p.pHMsg(r, pkSeed, pkRoot, msg, p.m)
}

func (p *params) hPrf(pkSeed []byte, skSeed []byte, adrs *address) []byte {
	return p.pPrf(pkSeed, skSeed, adrs, p.n)
}

func (p *params) hPrfMsg(skPrf []byte, optRand []byte, msg []byte) []byte {
	return p.pPrfMsg(skPrf, optRand, msg, p.n)
}

func (p *params) hF(pkSeed []byte, adrs *address, msg1 []byte) []byte {
	return p.pF(pkSeed, adrs, msg1, p.n)
}

func (p *params) hH(pkSeed []byte, adrs *address, msg2 []byte) []byte {
	return p.pH(pkSeed, adrs, msg2, p.n)
}

func (p *params) hTl(pkSeed []byte, adrs *address, msgl []byte) []byte {
	return p.pTl(pkSeed, adrs, msgl, p.n)
}

type paramsOpts struct {
	n   uint32
	h   uint32
	d   uint32
	hp  uint32
	a   uint32
	k   uint32
	lgw uint32
	m   uint32
}

type hashParamsOpts struct {
	pHMsg   func(r []byte, pkSeed []byte, pkRoot []byte, msg []byte, m uint32) []byte
	pPrf    func(pkSeed []byte, skSeed []byte, adrs *address, n uint32) []byte
	pPrfMsg func(skPrf []byte, optRand []byte, M []byte, n uint32) []byte
	pF      func(pkSeed []byte, adrs *address, M1 []byte, n uint32) []byte
	pH      func(pkSeed []byte, adrs *address, M2 []byte, n uint32) []byte
	pTl     func(pkSeed []byte, adrs *address, Ml []byte, n uint32) []byte
}

func newParams(par paramsOpts, hashPar hashParamsOpts) *params {
	// These are defined by Algorithm 1 and Equations 5.1, 5.2, 5.3, and 5.4 of the SLH-DSA specification.
	w := uint32(1) << par.lgw
	len1 := (8*par.n + par.lgw - 1) / par.lgw
	log2 := func(x uint32) uint32 { return uint32(bits.Len(uint(x)) - 1) }
	len2 := log2(len1*(w-1))/par.lgw + 1
	len := len1 + len2
	return &params{
		par.n, par.h, par.d, par.hp, par.a, par.k, par.lgw, par.m,
		w, len1, len2, len,
		hashPar.pHMsg, hashPar.pPrf, hashPar.pPrfMsg, hashPar.pF, hashPar.pH, hashPar.pTl,
	}
}

// Parameters defined in Table 2 of the SLH-DSA specification.
var (
	param128s = paramsOpts{
		n:   16,
		h:   63,
		d:   7,
		hp:  9,
		a:   12,
		k:   14,
		lgw: 4,
		m:   30,
	}

	param128f = paramsOpts{
		n:   16,
		h:   66,
		d:   22,
		hp:  3,
		a:   6,
		k:   33,
		lgw: 4,
		m:   34,
	}

	param192s = paramsOpts{
		n:   24,
		h:   63,
		d:   7,
		hp:  9,
		a:   14,
		k:   17,
		lgw: 4,
		m:   39,
	}

	param192f = paramsOpts{
		n:   24,
		h:   66,
		d:   22,
		hp:  3,
		a:   8,
		k:   33,
		lgw: 4,
		m:   42,
	}

	param256s = paramsOpts{
		n:   32,
		h:   64,
		d:   8,
		hp:  8,
		a:   14,
		k:   22,
		lgw: 4,
		m:   47,
	}

	param256f = paramsOpts{
		n:   32,
		h:   68,
		d:   17,
		hp:  4,
		a:   9,
		k:   35,
		lgw: 4,
		m:   49,
	}

	hashParamShake = hashParamsOpts{
		pHMsg:   shakeHMsg,
		pPrf:    shakePrf,
		pPrfMsg: shakePrfMsg,
		pF:      shakeF,
		pH:      shakeH,
		pTl:     shakeTl,
	}

	hashParamSha2C1 = hashParamsOpts{
		pHMsg:   sha2C1HMsg,
		pPrf:    sha2C1Prf,
		pPrfMsg: sha2C1PrfMsg,
		pF:      sha2C1F,
		pH:      sha2C1H,
		pTl:     sha2C1Tl,
	}

	hashParamSha2C35 = hashParamsOpts{
		pHMsg:   sha2C35HMsg,
		pPrf:    sha2C35Prf,
		pPrfMsg: sha2C35PrfMsg,
		pF:      sha2C35F,
		pH:      sha2C35H,
		pTl:     sha2C35Tl,
	}
)

// Matching parameter set names as in Table 2 of the SLH-DSA specification.
var (
	// SLH_DSA_SHA2_128s defines parameters for SLH-DSA-SHA2-128s.
	SLH_DSA_SHA2_128s = newParams(param128s, hashParamSha2C1)
	// SLH_DSA_SHAKE_128s defines parameters for SLH-DSA-SHAKE-128s.
	SLH_DSA_SHAKE_128s = newParams(param128s, hashParamShake)
	// SLH_DSA_SHA2_128f defines parameters for SLH-DSA-SHA2-128f.
	SLH_DSA_SHA2_128f = newParams(param128f, hashParamSha2C1)
	// SLH_DSA_SHAKE_128f defines parameters for SLH-DSA-SHAKE-128f.
	SLH_DSA_SHAKE_128f = newParams(param128f, hashParamShake)
	// SLH_DSA_SHA2_192s defines parameters for SLH-DSA-SHA2-192s.
	SLH_DSA_SHA2_192s = newParams(param192s, hashParamSha2C35)
	// SLH_DSA_SHAKE_192s defines parameters for SLH-DSA-SHAKE-192s.
	SLH_DSA_SHAKE_192s = newParams(param192s, hashParamShake)
	// SLH_DSA_SHA2_192f defines parameters for SLH-DSA-SHA2-192f.
	SLH_DSA_SHA2_192f = newParams(param192f, hashParamSha2C35)
	// SLH_DSA_SHAKE_192f defines parameters for SLH-DSA-SHAKE-192f.
	SLH_DSA_SHAKE_192f = newParams(param192f, hashParamShake)
	// SLH_DSA_SHA2_256s defines parameters for SLH-DSA-SHA2-256s.
	SLH_DSA_SHA2_256s = newParams(param256s, hashParamSha2C35)
	// SLH_DSA_SHAKE_256s defines parameters for SLH-DSA-SHAKE-256s.
	SLH_DSA_SHAKE_256s = newParams(param256s, hashParamShake)
	// SLH_DSA_SHA2_256f defines parameters for SLH-DSA-SHA2-256f.
	SLH_DSA_SHA2_256f = newParams(param256f, hashParamSha2C35)
	// SLH_DSA_SHAKE_256f defines parameters for SLH-DSA-SHAKE-256f.
	SLH_DSA_SHAKE_256f = newParams(param256f, hashParamShake)
)

// PublicKey represents an SLH-DSA public key.
type PublicKey struct {
	pkSeed []byte
	pkRoot []byte
	// Corresponding parameters.
	p *params
}

// SecretKey represents an SLH-DSA secret key.
type SecretKey struct {
	skSeed []byte
	skPrf  []byte
	pkSeed []byte
	pkRoot []byte
	// Corresponding parameters.
	p *params
}

// Algorithm 18 (slh_keygen_internal).
func (p *params) slhKeygenInternal(skSeed []byte, skPrf []byte, pkSeed []byte) (*SecretKey, *PublicKey) {
	// Generate the public key from the top-level XMSS tree.
	adrs := newAddress()
	adrs.setLayerAddress(p.d - 1)
	pkRoot := p.xmssNode(skSeed, 0, p.hp, pkSeed, adrs)
	return &SecretKey{skSeed, skPrf, pkSeed, pkRoot, p}, &PublicKey{pkSeed, pkRoot, p}
}

// Algorithm 19 (slh_sign_internal).
// This generates a signature of the form R || SIG_FORS || SIG_HT, where:
// - R is a random n-byte string,
// - SIG_FORS is a k * (1 + a) * n-byte FORS signature of the message digest, and
// - SIG_HT is a (h + d * len) * n-byte Hypertree signature of the FORS signature and the message digest.
// Here, addrnd selects between the "hedged" or the "deterministic" variant, depending on whether
// the caller passes a truly random string or otherwise the pkSeed, respectively.
func (sk *SecretKey) signInternal(msg []byte, addrnd []byte) []byte {
	adrs := newAddress()
	// Generate randomizer.
	sig := sk.p.hPrfMsg(sk.skPrf, addrnd, msg)
	// Compute message digest.
	digest := sk.p.hHMsg(sig, sk.pkSeed, sk.pkRoot, msg)
	r := (sk.p.k*sk.p.a + 7) / 8
	s := (sk.p.h - sk.p.hp + 7) / 8
	t := (sk.p.hp + 7) / 8
	md := digest[0:r]
	tmpIdxTree := digest[r : r+s]
	tmpIdxLeaf := digest[r+s : r+s+t]
	idxTree := toInt(tmpIdxTree, s)
	if sk.p.h-sk.p.hp > 64 {
		panic("unreachable")
	}
	// For the 256f parameter sets, h - hp = 64, so mod 2^(h - hp) is equivalent
	// to masking uint64 idxTree with 0xFFFFFFFFFFFFFFFF, which is a no-op.
	if sk.p.h-sk.p.hp != 64 {
		idxTree &= (uint64(1) << (sk.p.h - sk.p.hp)) - uint64(1)
	}
	idxLeaf := uint32(toInt(tmpIdxLeaf, t)) & ((1 << sk.p.hp) - 1)
	adrs.setTreeAddress(idxTree)
	adrs.setTypeAndClear(addressFORSTree)
	adrs.setKeyPairAddress(idxLeaf)
	sigFors := sk.p.forsSign(md, sk.skSeed, sk.pkSeed, adrs)
	sig = append(sig, sigFors...)
	// Get FORS key.
	pkFors := sk.p.forsPkFromSig(sigFors, md, sk.pkSeed, adrs)
	return append(sig, sk.p.htSign(pkFors, sk.skSeed, sk.pkSeed, idxTree, idxLeaf)...)
}

// Algorithm 20 (slh_verify_internal).
func (pk *PublicKey) verifyInternal(msg []byte, sig []byte) error {
	forsIdx := 1 + pk.p.k*(1+pk.p.a)
	if len(sig) != int((forsIdx+pk.p.h+pk.p.d*pk.p.len)*pk.p.n) {
		return fmt.Errorf("invalid signature length")
	}
	adrs := newAddress()
	R := sig[:pk.p.n]
	sigFors := sig[pk.p.n : forsIdx*pk.p.n]
	sigHT := sig[forsIdx*pk.p.n:]
	// Compute message digest.
	digest := pk.p.hHMsg(R, pk.pkSeed, pk.pkRoot, msg)
	r := (pk.p.k*pk.p.a + 7) / 8
	s := (pk.p.h - pk.p.hp + 7) / 8
	t := (pk.p.hp + 7) / 8
	md := digest[0:r]
	tmpIdxTree := digest[r : r+s]
	tmpIdxLeaf := digest[r+s : r+s+t]
	idxTree := toInt(tmpIdxTree, s)
	if pk.p.h-pk.p.hp > 64 {
		panic("unreachable")
	}
	// For the 256f parameter sets, h - hp = 64, so mod 2^(h - hp) is equivalent
	// to masking uint64 idxTree with 0xFFFFFFFFFFFFFFFF, which is a no-op.
	if pk.p.h-pk.p.hp != 64 {
		idxTree &= (uint64(1) << (pk.p.h - pk.p.hp)) - uint64(1)
	}
	idxLeaf := uint32(toInt(tmpIdxLeaf, t)) & ((1 << pk.p.hp) - 1)
	// Compare FORS public key.
	adrs.setTreeAddress(idxTree)
	adrs.setTypeAndClear(addressFORSTree)
	adrs.setKeyPairAddress(idxLeaf)
	pkFors := pk.p.forsPkFromSig(sigFors, md, pk.pkSeed, adrs)
	if !pk.p.htVerify(pkFors, sigHT, pk.pkSeed, idxTree, idxLeaf, pk.pkRoot) {
		return fmt.Errorf("invalid signature")
	}
	return nil
}

// Algorithm 21 (slh_keygen).
func (p *params) KeyGen() (*SecretKey, *PublicKey) {
	// Set skSeed, skPrf, and pkSeed to random n-byte strings. Note that rand.Read never returns an error.
	skSeed := make([]byte, p.n)
	rand.Read(skSeed[:])
	skPrf := make([]byte, p.n)
	rand.Read(skPrf[:])
	pkSeed := make([]byte, p.n)
	rand.Read(pkSeed[:])
	return p.slhKeygenInternal(skSeed, skPrf, pkSeed)
}

// Encode encodes a public key.
func (pk *PublicKey) Encode() []byte {
	return slices.Concat(pk.pkSeed, pk.pkRoot)
}

// PublicKeyLength returns the length of a public key.
func (p *params) PublicKeyLength() int {
	return int(2 * p.n)
}

// Decode decodes a public key.
func (p *params) DecodePublicKey(pkEnc []byte) (*PublicKey, error) {
	if len(pkEnc) != p.PublicKeyLength() {
		return nil, fmt.Errorf("invalid public key length")
	}
	pkSeed := pkEnc[0:p.n]
	pkRoot := pkEnc[p.n : 2*p.n]
	return &PublicKey{pkSeed, pkRoot, p}, nil
}

// Encode encodes a secret key.
func (sk *SecretKey) Encode() []byte {
	return slices.Concat(sk.skSeed, sk.skPrf, sk.pkSeed, sk.pkRoot)
}

// SecretKeyLength returns the length of a secret key.
func (p *params) SecretKeyLength() int {
	return int(4 * p.n)
}

// Decode decodes a secret key.
func (p *params) DecodeSecretKey(skEnc []byte) (*SecretKey, error) {
	if len(skEnc) != p.SecretKeyLength() {
		return nil, fmt.Errorf("invalid secret key length")
	}
	skSeed := skEnc[0:p.n]
	skPrf := skEnc[p.n : 2*p.n]
	pkSeed := skEnc[2*p.n : 3*p.n]
	pkRoot := skEnc[3*p.n : 4*p.n]
	return &SecretKey{skSeed, skPrf, pkSeed, pkRoot, p}, nil
}

// PublicKey returns the public key corresponding to a secret key.
func (sk *SecretKey) PublicKey() *PublicKey {
	return &PublicKey{sk.pkSeed, sk.pkRoot, sk.p}
}

// Sign is the standard signing function. This is Algorithm 22 (slh_sign) of the SLH-DSA specification.
func (sk *SecretKey) Sign(msg []byte, ctx []byte) ([]byte, error) {
	if len(ctx) > 255 {
		return nil, fmt.Errorf("context too long")
	}
	// Generate additional randomness. Note that rand.Read never returns an error.
	addrnd := make([]byte, sk.p.n)
	rand.Read(addrnd[:])
	return sk.signInternal(slices.Concat([]byte{0, byte(len(ctx))}, ctx, msg), addrnd), nil
}

// SignDeterministic signs deterministically. This is Algorithm 22 (slh_sign) of the SLH-DSA specification with PK.seed as randomness.
func (sk *SecretKey) SignDeterministic(msg []byte, ctx []byte) ([]byte, error) {
	if len(ctx) > 255 {
		return nil, fmt.Errorf("context too long")
	}
	return sk.signInternal(slices.Concat([]byte{0, byte(len(ctx))}, ctx, msg), sk.pkSeed), nil
}

// Verify is the standard verification function. This is Algorithm 24 (slh_verify) of the SLH-DSA specification.
func (pk *PublicKey) Verify(msg []byte, sig []byte, ctx []byte) error {
	if len(ctx) > 255 {
		return fmt.Errorf("context too long")
	}
	return pk.verifyInternal(slices.Concat([]byte{0, byte(len(ctx))}, ctx, msg), sig)
}
