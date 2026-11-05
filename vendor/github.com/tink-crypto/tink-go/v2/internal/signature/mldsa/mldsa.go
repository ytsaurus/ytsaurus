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

// Package mldsa implements ML-DSA as specified in NIST FIPS 204 (https://doi.org/10.6028/NIST.FIPS.204).
package mldsa

import (
	"bytes"
	"fmt"
	"math/bits"
	"slices"

	"crypto/rand"

	"golang.org/x/crypto/sha3"
)

const (
	// Base ring modulus.
	q = 8380417
	// Base ring storage bits.
	qBits = 23
	// Root of unity modulo q.
	zeta = 1753
	// Inverse of 256 modulo q.
	inv256 = 8347681
	// Degree of polynomial modulus (= X^256 + 1).
	degree = 256

	// Dropped bits.
	d = 13

	// SecretKeySeedSize is the size of the seed used to generate a secret key.
	SecretKeySeedSize = 32
)

type params struct {
	// ML-DSA parameters (see https://doi.org/10.6028/NIST.FIPS.204).
	tau        int
	lambda     int
	log2Gamma1 int
	gamma2     uint32
	k          int
	l          int
	eta        int
	omega      int
	// Precomputed derived parameters.
	etaBits int
	w1Bits  int
}

type paramsOpts struct {
	tau        int
	lambda     int
	log2Gamma1 int
	invGamma2  uint32
	k          int
	l          int
	eta        int
	omega      int
}

func newParams(par paramsOpts) *params {
	gamma2 := (q - 1) / par.invGamma2
	etaBits := bits.Len(uint(2 * par.eta))
	w1Bits := bits.Len(uint((q-1)/(2*gamma2) - 1))
	return &params{par.tau, par.lambda, par.log2Gamma1, gamma2, par.k, par.l, par.eta, par.omega, etaBits, w1Bits}
}

var (
	// MLDSA44 defines parameters for ML-DSA-44.
	MLDSA44 = newParams(paramsOpts{
		tau:        39,
		lambda:     128,
		log2Gamma1: 17,
		invGamma2:  88,
		k:          4,
		l:          4,
		eta:        2,
		omega:      80,
	})
	// MLDSA65 defines parameters for ML-DSA-65.
	MLDSA65 = newParams(paramsOpts{
		tau:        49,
		lambda:     192,
		log2Gamma1: 19,
		invGamma2:  32,
		k:          6,
		l:          5,
		eta:        4,
		omega:      55,
	})
	// MLDSA87 defines parameters for ML-DSA-87.
	MLDSA87 = newParams(paramsOpts{
		tau:        60,
		lambda:     256,
		log2Gamma1: 19,
		invGamma2:  32,
		k:          8,
		l:          7,
		eta:        2,
		omega:      75,
	})
)

// PublicKey represents a ML-DSA public key.
type PublicKey struct {
	rho [32]byte
	t1  vector
	// Cached public key hash.
	tr [64]byte
	// Corresponding parameters.
	par *params
}

// SecretKey represents a ML-DSA secret key.
type SecretKey struct {
	rho [32]byte
	kK  [32]byte
	tr  [64]byte
	s1  vector
	s2  vector
	t0  vector
	// Cached secret key seed.
	seed *[SecretKeySeedSize]byte
	// Corresponding parameters.
	par *params
}

// Algorithm 32 (ExpandA)
func (par *params) expandA(rho [32]byte) matrixNTT {
	res := makeZeroMatrixNTT(par.k, par.l)
	var rhop [32 + 2]byte
	copy(rhop[:], rho[:])
	for r := range par.k {
		rhop[len(rho)+1] = byte(r)
		for s := range par.l {
			rhop[len(rho)] = byte(s)
			res[r][s] = rejectNTTPoly(rhop)
		}
	}
	return res
}

// Algorithm 33 (ExpandS)
func (par *params) expandS(rho [64]byte) (vector, vector) {
	res1 := makeZeroVector(par.l)
	res2 := makeZeroVector(par.k)
	var rhop [64 + 2]byte
	copy(rhop[:], rho[:])
	for i := range par.l {
		rhop[len(rho)] = byte(i)
		rhop[len(rho)+1] = byte(0)
		res1[i] = par.rejectBoundedPoly(rhop)
	}
	for i := range par.k {
		rhop[len(rho)] = byte(i + par.l)
		rhop[len(rho)+1] = byte(0)
		res2[i] = par.rejectBoundedPoly(rhop)
	}
	return res1, res2
}

// Algorithm 34 (ExpandMask)
func (par *params) expandMask(rho [64]byte, mu int) vector {
	res := makeZeroVector(par.l)
	var rhop [64 + 2]byte
	copy(rhop[:], rho[:])
	for i := range par.l {
		rhop[len(rho)] = byte((mu + i) & 0xFF)
		rhop[len(rho)+1] = byte((mu + i) >> 8)
		v := make([]byte, 32*(par.log2Gamma1+1))
		sha3.ShakeSum256(v, rhop[:])
		res[i] = bitUnpackPoly(v, rZq(1<<par.log2Gamma1), par.log2Gamma1+1)
	}
	return res
}

// Algorithm 6 (KeyGen_internal)
func (par *params) keyGenInternal(seed [SecretKeySeedSize]byte) (*PublicKey, *SecretKey) {
	H := sha3.NewShake256()
	H.Write(append(seed[:], byte(par.k), byte(par.l)))
	var rho [32]byte
	H.Read(rho[:])
	var rhop [64]byte
	H.Read(rhop[:])
	var K [32]byte
	H.Read(K[:])
	Ah := par.expandA(rho)
	s1, s2 := par.expandS(rhop)
	t := Ah.mul(s1.ntt()).intt().add(s2)
	t1, t0 := t.power2Round()
	pk := &PublicKey{rho, t1, [64]byte{}, par}
	sha3.ShakeSum256(pk.tr[:], pk.Encode())
	return pk, &SecretKey{rho, K, pk.tr, s1, s2, t0, &seed, par}
}

func (sk *SecretKey) signInternalWithMu(mu [64]byte, rnd [32]byte) []byte {
	par := sk.par
	beta := uint32(par.tau * par.eta)
	s1h := sk.s1.ntt()
	s2h := sk.s2.ntt()
	t0h := sk.t0.ntt()
	Ah := par.expandA(sk.rho)
	var rhopp [64]byte
	sha3.ShakeSum256(rhopp[:], slices.Concat(sk.kK[:], rnd[:], mu[:]))
	for kappa := 0; ; kappa += par.l {
		y := par.expandMask(rhopp, kappa)
		w := Ah.mul(y.ntt()).intt()
		w1 := w.highBits(par.gamma2)
		ct := make([]byte, par.lambda/4)
		sha3.ShakeSum256(ct, slices.Concat(mu[:], par.w1Encode(w1)))
		c := par.sampleInBall(ct)
		ch := c.ntt()
		cs1 := ch.scalarMul(s1h).intt()
		cs2 := ch.scalarMul(s2h).intt()
		z := y.add(cs1)
		r0 := w.sub(cs2).lowBits(par.gamma2)
		if z.infinityNorm() < (1<<par.log2Gamma1)-beta && r0.infinityNorm() < par.gamma2-beta {
			ct0 := ch.scalarMul(t0h).intt()
			h := ct0.neg().makeHint(par.gamma2, w.sub(cs2).add(ct0))
			if ct0.infinityNorm() < par.gamma2 && h.numOnes() <= par.omega {
				return par.sigEncode(ct, z, h)
			}
		}
	}
}

func (pk *PublicKey) verifyInternalWithMu(mu [64]byte, sigma []byte) error {
	par := pk.par
	ct, z, h, err := par.sigDecode(sigma)
	if err != nil {
		return err
	}
	Ah := par.expandA(pk.rho)
	c := par.sampleInBall(ct)
	Azh := Ah.mul(z.ntt())
	t1sh := pk.t1.scalePower2().ntt()
	wp := Azh.sub(c.ntt().scalarMul(t1sh)).intt()
	w1p := wp.useHint(par.gamma2, h)
	ctp := make([]byte, par.lambda/4)
	sha3.ShakeSum256(ctp, slices.Concat(mu[:], par.w1Encode(w1p)))
	beta := uint32(par.tau * par.eta)
	if !(z.infinityNorm() < (1<<par.log2Gamma1)-beta && bytes.Compare(ct, ctp) == 0) {
		return fmt.Errorf("invalid signature")
	}
	return nil
}

// Algorithm 7 (Sign_internal)
func (sk *SecretKey) signInternal(Mp []byte, rnd [32]byte) []byte {
	var mu [64]byte
	sha3.ShakeSum256(mu[:], slices.Concat(sk.tr[:], Mp))
	return sk.signInternalWithMu(mu, rnd)
}

// Algorithm 8 (Verify_internal)
func (pk *PublicKey) verifyInternal(Mp []byte, sigma []byte) error {
	var mu [64]byte
	sha3.ShakeSum256(mu[:], slices.Concat(pk.tr[:], Mp))
	return pk.verifyInternalWithMu(mu, sigma)
}

// KeyGen generates a new public and secret key. This is Algorithm 1 (KeyGen) of the ML-DSA specification.
func (par *params) KeyGen() (*PublicKey, *SecretKey) {
	var seed [SecretKeySeedSize]byte
	rand.Read(seed[:])
	return par.keyGenInternal(seed)
}

// KeyGenFromSeed generates a public and secret key from a specified seed.
func (par *params) KeyGenFromSeed(seed [SecretKeySeedSize]byte) (*PublicKey, *SecretKey) {
	return par.keyGenInternal(seed)
}

// Seed returns the seed used to generate the secret key.
// The returned value is nil if the secret key was not generated from a seed but decoded from
// an expanded secret key (in which case the initial seed is not available to this library due
// to how the secret key generation works).
func (sk *SecretKey) Seed() *[SecretKeySeedSize]byte {
	return sk.seed
}

// SignWithMu signs with a precomputed mu.
func (sk *SecretKey) SignWithMu(mu [64]byte) []byte {
	var rnd [32]byte
	rand.Read(rnd[:])
	return sk.signInternalWithMu(mu, rnd)
}

// SignDeterministicWithMu signs deterministically with a precomputed mu. It uses a fixed all zeroes randomness.
func (sk *SecretKey) SignDeterministicWithMu(mu [64]byte) []byte {
	var zeroes [32]byte
	return sk.signInternalWithMu(mu, zeroes)
}

// Sign is the standard signing function. This is Algorithm 2 (Sign) of the ML-DSA specification.
func (sk *SecretKey) Sign(M []byte, ctx []byte) ([]byte, error) {
	if len(ctx) > 255 {
		return nil, fmt.Errorf("context too long")
	}
	var rnd [32]byte
	rand.Read(rnd[:])
	return sk.signInternal(slices.Concat([]byte{0, byte(len(ctx))}, ctx, M), rnd), nil
}

// SignDeterministic signs deterministically. This is Algorithm 2 (Sign) of the ML-DSA specification with a fixed all zeroes randomness.
func (sk *SecretKey) SignDeterministic(M []byte, ctx []byte) ([]byte, error) {
	if len(ctx) > 255 {
		return nil, fmt.Errorf("context too long")
	}
	var zeroes [32]byte
	return sk.signInternal(slices.Concat([]byte{0, byte(len(ctx))}, ctx, M), zeroes), nil
}

// VerifyWithMu verifies a signature with a precomputed mu.
// Returns nil if the signature is valid for mu, otherwise returns an error.
func (pk *PublicKey) VerifyWithMu(mu [64]byte, sigma []byte) error {
	return pk.verifyInternalWithMu(mu, sigma)
}

// Verify verifies a signature. This is Algorithm 3 (Verify) of the ML-DSA specification.
// Returns nil if the signature is valid for the message and context, otherwise returns an error.
func (pk *PublicKey) Verify(M []byte, sigma []byte, ctx []byte) error {
	if len(ctx) > 255 {
		return fmt.Errorf("context too long")
	}
	return pk.verifyInternal(slices.Concat([]byte{0, byte(len(ctx))}, ctx, M), sigma)
}
