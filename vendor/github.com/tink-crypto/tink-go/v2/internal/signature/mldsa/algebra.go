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
	"crypto/subtle"
)

// Since integers mod q require 23 bits storage, we keep our base ring elements
// stored as unsigned 32 bit integers. We will use these to represent coefficients
// in the various different rings used in the ML-DSA algorithms.
type rZq uint32

// Array of roots of unity.
// Generated using the following Python snippet:
/*
bitrev8 = lambda n : sum([((n >> i) & 1) << (7 - i) for i in range(8)])
zetas = [0] + [1753**bitrev8(k) % q for k in range(1, 256)]
*/
var zetas = [degree]rZq{
	0, 4808194, 3765607, 3761513, 5178923, 5496691, 5234739, 5178987, 7778734, 3542485, 2682288, 2129892, 3764867, 7375178, 557458, 7159240,
	5010068, 4317364, 2663378, 6705802, 4855975, 7946292, 676590, 7044481, 5152541, 1714295, 2453983, 1460718, 7737789, 4795319, 2815639, 2283733,
	3602218, 3182878, 2740543, 4793971, 5269599, 2101410, 3704823, 1159875, 394148, 928749, 1095468, 4874037, 2071829, 4361428, 3241972, 2156050,
	3415069, 1759347, 7562881, 4805951, 3756790, 6444618, 6663429, 4430364, 5483103, 3192354, 556856, 3870317, 2917338, 1853806, 3345963, 1858416,
	3073009, 1277625, 5744944, 3852015, 4183372, 5157610, 5258977, 8106357, 2508980, 2028118, 1937570, 4564692, 2811291, 5396636, 7270901, 4158088,
	1528066, 482649, 1148858, 5418153, 7814814, 169688, 2462444, 5046034, 4213992, 4892034, 1987814, 5183169, 1736313, 235407, 5130263, 3258457,
	5801164, 1787943, 5989328, 6125690, 3482206, 4197502, 7080401, 6018354, 7062739, 2461387, 3035980, 621164, 3901472, 7153756, 2925816, 3374250,
	1356448, 5604662, 2683270, 5601629, 4912752, 2312838, 7727142, 7921254, 348812, 8052569, 1011223, 6026202, 4561790, 6458164, 6143691, 1744507,
	1753, 6444997, 5720892, 6924527, 2660408, 6600190, 8321269, 2772600, 1182243, 87208, 636927, 4415111, 4423672, 6084020, 5095502, 4663471,
	8352605, 822541, 1009365, 5926272, 6400920, 1596822, 4423473, 4620952, 6695264, 4969849, 2678278, 4611469, 4829411, 635956, 8129971, 5925040,
	4234153, 6607829, 2192938, 6653329, 2387513, 4768667, 8111961, 5199961, 3747250, 2296099, 1239911, 4541938, 3195676, 2642980, 1254190, 8368000,
	2998219, 141835, 8291116, 2513018, 7025525, 613238, 7070156, 6161950, 7921677, 6458423, 4040196, 4908348, 2039144, 6500539, 7561656, 6201452,
	6757063, 2105286, 6006015, 6346610, 586241, 7200804, 527981, 5637006, 6903432, 1994046, 2491325, 6987258, 507927, 7192532, 7655613, 6545891,
	5346675, 8041997, 2647994, 3009748, 5767564, 4148469, 749577, 4357667, 3980599, 2569011, 6764887, 1723229, 1665318, 2028038, 1163598, 5011144,
	3994671, 8368538, 7009900, 3020393, 3363542, 214880, 545376, 7609976, 3105558, 7277073, 508145, 7826699, 860144, 3430436, 140244, 6866265,
	6195333, 3123762, 2358373, 6187330, 5365997, 6663603, 2926054, 7987710, 8077412, 3531229, 4405932, 4606686, 1900052, 7598542, 1054478, 7648983,
}

// Reduces the value of a "mod q" representative from [0, 2q) to the range [0, q).
func (a rZq) reduceOnce() rZq {
	// Constant time version of the following logic:
	// if q <= a {
	// 	return a - q
	// }
	// return a
	c := subtle.ConstantTimeLessOrEq(q, int(a))
	return rZq(subtle.ConstantTimeSelect(c, int(uint32(a)-q), int(a)))
}

func (a rZq) add(b rZq) rZq {
	return (a + b).reduceOnce()
}

func (a rZq) sub(b rZq) rZq {
	return (a + q - b).reduceOnce()
}

func (a rZq) neg() rZq {
	return rZq(0).sub(a)
}

// Constant time multiplication of two "mod q" representatives.
func (a rZq) mul(b rZq) rZq {
	const barretShift = 2 * qBits
	const barretMult = uint64(1 << barretShift / q)
	// We have that a * b * barretMult takes 23 + 23 + 24 = 70 bits.
	// Hence, we only need the lower 6 bits of the top bits of the quotient.
	const hiMask = 1<<(3*qBits+1-64) - 1
	// This is our input product we want to reduce.
	prod := uint64(a) * uint64(b)
	// Compute high and low parts of the product times the Barret multiplier.
	hi := (prod >> 32) * barretMult
	lo := (prod & 0xFFFFFFFF) * barretMult
	// Compute carry.
	hiLo := hi & 0xFFFFFFFF
	carry := (lo>>32 + hiLo) >> 32
	// Form the final quotient which is the product shifted by the Barret shift.
	quoHi := (hi>>32 + carry) & hiMask
	quoLo := hiLo<<32 + lo
	quo := quoHi<<(64-barretShift) | (quoLo >> barretShift)
	return rZq(prod - quo*q).reduceOnce()
}

// Algorithm 35 (Power2Round)
func (a rZq) power2Round() (rZq, rZq) {
	m := uint32(1) << (d - 1)
	r1 := (uint32(a) + m - 1) >> d
	return rZq(r1), a.sub(rZq(r1 << d))
}

// Scale back a rounded value by the number of "dropped" bits.
func (a rZq) scalePower2() rZq {
	return a << d
}

// Constant time division by 2*gamma2 for allowed values of gamma2.
// The constants used here were computed according to the algorithm in Figure 4.1 of the
// classical paper: Granlund, Montgomery, "Division by invariant integers using multiplication"
// (https://doi.org/10.1145/773473.178249). This turns unsigned integer division by a constant
// into a shift-multiply-shift operation.
func divBy2Gamma2(a, gamma2 uint32) uint32 {
	if gamma2 == (q-1)/88 {
		return uint32((uint64(a) * 2955676419) >> 49)
	} else if gamma2 == (q-1)/32 {
		return uint32(((uint64(a) >> 9) * 8396809) >> 33)
	}
	// The ML-DSA spec only allows the above two values for gamma2.
	panic("invalid gamma2")
}

// Algorithm 36 (Decompose)
// Note that we are using an unsigned representation for the decomposition coefficient r0.
// This allows the lowBits algorithm to output an unsigned representation which allows us to
// avoid having to implement two different infinity norm algorithms (unsigned and signed).
func (a rZq) decompose(gamma2 uint32) (rZq, rZq) {
	s := divBy2Gamma2(uint32(a)+gamma2-1, gamma2)
	r0 := a.sub(rZq(s * (gamma2 << 1)))
	t := a.sub(r0)
	r1r := divBy2Gamma2(uint32(t), gamma2)
	// Constant time version of the following logic:
	// if t == rZq(q-1) {
	// 	return rZq(0), r0.sub(rZq(1))
	// }
	// return rZq(r1r), r0
	c := subtle.ConstantTimeEq(int32(t), int32(q-1))
	r1 := rZq(subtle.ConstantTimeSelect(c, 0, int(r1r)))
	r0 = rZq(subtle.ConstantTimeSelect(c, int(r0.sub(rZq(1))), int(r0)))
	return r1, r0
}

// Algorithm 37 (HighBits)
func (a rZq) highBits(gamma2 uint32) rZq {
	r1, _ := a.decompose(gamma2)
	return r1
}

// Algorithm 38 (LowBits)
func (a rZq) lowBits(gamma2 uint32) rZq {
	_, r0 := a.decompose(gamma2)
	return r0
}

// Algorithm 39 (MakeHint)
// The hint polynomials and vectors have coefficients in {0, 1}.
// We will nevertheless treat them as over Zq to avoid having to
// duplicate encoding and decoding functions for this one type.
func (a rZq) makeHint(gamma2 uint32, r rZq) rZq {
	r1 := r.highBits(gamma2)
	v1 := r.add(a).highBits(gamma2)
	if r1 != v1 {
		return rZq(1)
	}
	return rZq(0)
}

// Algorithm 40 (UseHint)
func (a rZq) useHint(gamma2 uint32, h rZq) rZq {
	r1, r0 := a.decompose(gamma2)
	if h == rZq(1) {
		m := (q - 1) / (gamma2 << 1)
		t := rZq(gamma2).neg()
		// We are using an unsigned representation for the decomposition coefficient r0.
		// Hence, when applying the hint, we need to perform an additional comparison "r0 < t"
		// to find out in which half of the range [0, t=gamma2) \cup [gamma2, 2*gamma2) r0 lies.
		// In the ML-DSA spec, which assumes a signed representation, this corresponds to the
		// cases r0 > 0 and r0 <= 0, respectively.
		if r0 > rZq(0) && r0 < t {
			if r1 == rZq(m-1) {
				return rZq(0)
			}
			return r1.add(1)
		}
		if r0 >= t {
			if r1 == rZq(0) {
				return rZq(m - 1)
			}
			return r1.sub(1)
		}
	}
	return r1
}

func (a rZq) centeredAbs() uint32 {
	// Constant time version of the following logic:
	// if (q-1)/2 <= a {
	// 	return uint32(q - a)
	// }
	// return uint32(a)
	c := subtle.ConstantTimeLessOrEq((q-1)/2, int(a))
	return uint32(subtle.ConstantTimeSelect(c, int(q-a), int(a)))
}

func (a rZq) centeredMax(b rZq) rZq {
	aa := a.centeredAbs()
	ba := b.centeredAbs()
	// Constant time version of the following logic:
	// if ba <= aa {
	//  return a
	// }
	// return b
	c := subtle.ConstantTimeLessOrEq(int(ba), int(aa))
	return rZq(subtle.ConstantTimeSelect(c, int(a), int(b)))
}

// poly represents a polynomial in R_m where m <= q.
type poly [degree]rZq

func (p *poly) add(q *poly) *poly {
	res := &poly{}
	for i := range res {
		res[i] = p[i].add(q[i])
	}
	return res
}

func (p *poly) neg() *poly {
	res := &poly{}
	for i := range res {
		res[i] = p[i].neg()
	}
	return res
}

func (p *poly) sub(q *poly) *poly {
	res := &poly{}
	for i := range res {
		res[i] = p[i].sub(q[i])
	}
	return res
}

func (p *poly) subFrom(a rZq) *poly {
	res := &poly{}
	for i := range res {
		res[i] = a.sub(p[i])
	}
	return res
}

func (p *poly) scalePower2() *poly {
	res := &poly{}
	for i := range p {
		res[i] = p[i].scalePower2()
	}
	return res
}

func (p *poly) highBits(gamma2 uint32) *poly {
	res := &poly{}
	for i := range p {
		res[i] = p[i].highBits(gamma2)
	}
	return res
}

func (p *poly) lowBits(gamma2 uint32) *poly {
	res := &poly{}
	for i := range p {
		res[i] = p[i].lowBits(gamma2)
	}
	return res
}

func (p *poly) makeHint(gamma2 uint32, z *poly) *poly {
	res := &poly{}
	for i := range p {
		res[i] = p[i].makeHint(gamma2, z[i])
	}
	return res
}

func (p *poly) useHint(gamma2 uint32, h *poly) *poly {
	res := &poly{}
	for i := range p {
		res[i] = p[i].useHint(gamma2, h[i])
	}
	return res
}

func (p *poly) infinityNorm() uint32 {
	res := rZq(0)
	for i := range p {
		res = res.centeredMax(p[i])
	}
	return res.centeredAbs()
}

// polyNTT represents a polynomial in T_q.
type polyNTT [degree]rZq

// Algorithm 45 (AddNTT)
func (p *polyNTT) add(q *polyNTT) *polyNTT {
	res := &polyNTT{}
	for i := range res {
		res[i] = p[i].add(q[i])
	}
	return res
}

func (p *polyNTT) sub(q *polyNTT) *polyNTT {
	res := &polyNTT{}
	for i := range res {
		res[i] = p[i].sub(q[i])
	}
	return res
}

func (p *polyNTT) subFrom(a rZq) *polyNTT {
	res := &polyNTT{}
	for i := range res {
		res[i] = a.sub(p[i])
	}
	return res
}

// Algorithm 46 (MultiplyNTT)
func (p *polyNTT) mul(q *polyNTT) *polyNTT {
	res := &polyNTT{}
	for i := range res {
		res[i] = p[i].mul(q[i])
	}
	return res
}

// Algorithm 41 (NTT) computes the NTT for a polynomial.
func (p *poly) ntt() *polyNTT {
	// Copy the coefficients to avoid modifying the original polynomial,
	// because the NTT algorithm modifies the coefficients in-place.
	var wh polyNTT
	copy(wh[:], p[:])
	m := 0
	len := 128
	for len >= 1 {
		start := 0
		for start < degree {
			m++
			z := zetas[m]
			for j := start; j < start+len; j++ {
				t := z.mul(wh[j+len])
				wh[j+len] = wh[j].sub(t)
				wh[j] = wh[j].add(t)
			}
			start += 2 * len
		}
		len /= 2
	}
	return &wh
}

// Algorithm 42 (NTT^{-1}) computes the inverse NTT for a polynomial.
func (p *polyNTT) intt() *poly {
	// Copy the coefficients to avoid modifying the original polynomial,
	// because the InverseNTT algorithm modifies the coefficients in-place.
	var w poly
	copy(w[:], p[:])
	m := degree
	len := 1
	for len < degree {
		start := 0
		for start < degree {
			m--
			z := zetas[m].neg()
			for j := start; j < start+len; j++ {
				t := w[j]
				w[j] = t.add(w[j+len])
				w[j+len] = z.mul(t.sub(w[j+len]))
			}
			start += 2 * len
		}
		len *= 2
	}
	for j := range w {
		w[j] = rZq(inv256).mul(w[j])
	}
	return &w
}

// vector represents a vector of polynomials.
type vector []*poly

func makeZeroVector(dim int) vector {
	res := make([]*poly, dim)
	for i := range res {
		res[i] = &poly{}
	}
	return res
}

func (v vector) add(w vector) vector {
	res := makeZeroVector(len(v))
	for i := range res {
		res[i] = v[i].add(w[i])
	}
	return res
}

func (v vector) neg() vector {
	res := makeZeroVector(len(v))
	for i := range res {
		res[i] = v[i].neg()
	}
	return res
}

func (v vector) sub(w vector) vector {
	res := makeZeroVector(len(v))
	for i := range res {
		res[i] = v[i].sub(w[i])
	}
	return res
}

func (v vector) ntt() vectorNTT {
	res := makeZeroVectorNTT(len(v))
	for i := range res {
		res[i] = v[i].ntt()
	}
	return res
}

func (v vector) power2Round() (vector, vector) {
	t1 := makeZeroVector(len(v))
	t0 := makeZeroVector(len(v))
	for i := range v {
		for j := range v[i] {
			r1, r0 := v[i][j].power2Round()
			t1[i][j] = r1
			t0[i][j] = r0
		}
	}
	return t1, t0
}

func (v vector) scalePower2() vector {
	res := makeZeroVector(len(v))
	for i := range v {
		res[i] = v[i].scalePower2()
	}
	return res
}

func (v vector) highBits(gamma2 uint32) vector {
	res := makeZeroVector(len(v))
	for i := range v {
		res[i] = v[i].highBits(gamma2)
	}
	return res
}

func (v vector) lowBits(gamma2 uint32) vector {
	res := makeZeroVector(len(v))
	for i := range v {
		res[i] = v[i].lowBits(gamma2)
	}
	return res
}

func (v vector) makeHint(gamma2 uint32, z vector) vector {
	res := makeZeroVector(len(v))
	for i := range v {
		res[i] = v[i].makeHint(gamma2, z[i])
	}
	return res
}

func (v vector) useHint(gamma2 uint32, z vector) vector {
	res := makeZeroVector(len(v))
	for i := range v {
		res[i] = v[i].useHint(gamma2, z[i])
	}
	return res
}

func (v vector) infinityNorm() uint32 {
	res := uint32(0)
	for i := range v {
		norm := v[i].infinityNorm()
		res = max(res, norm)
	}
	return res
}

// We assume as input a vector with elements in {0, 1}.
func (v vector) numOnes() int {
	res := 0
	for i := range v {
		for j := range v[i] {
			res += int(v[i][j])
		}
	}
	return res
}

// vectorNTT represents a vector of polynomials in T_q.
type vectorNTT []*polyNTT

func makeZeroVectorNTT(dim int) vectorNTT {
	res := make([]*polyNTT, dim)
	for i := range res {
		res[i] = &polyNTT{}
	}
	return res
}

func (v vectorNTT) sub(w vectorNTT) vectorNTT {
	res := makeZeroVectorNTT(len(v))
	for i := range res {
		res[i] = v[i].sub(w[i])
	}
	return res
}

func (p *polyNTT) scalarMul(v vectorNTT) vectorNTT {
	res := makeZeroVectorNTT(len(v))
	for i := range res {
		res[i] = p.mul(v[i])
	}
	return res
}

func (v vectorNTT) intt() vector {
	res := makeZeroVector(len(v))
	for i := range res {
		res[i] = v[i].intt()
	}
	return res
}

// matrixNTT represents a matrix of polynomials in T_q.
type matrixNTT [][]*polyNTT

func makeZeroMatrixNTT(dimK, dimL int) matrixNTT {
	res := make([][]*polyNTT, dimK)
	for i := range res {
		res[i] = make([]*polyNTT, dimL)
	}
	return res
}

func (m matrixNTT) mul(v vectorNTT) vectorNTT {
	res := makeZeroVectorNTT(len(m))
	for i := range m {
		for j := range m[i] {
			t := m[i][j].mul(v[j])
			res[i] = res[i].add(t)
		}
	}
	return res
}
