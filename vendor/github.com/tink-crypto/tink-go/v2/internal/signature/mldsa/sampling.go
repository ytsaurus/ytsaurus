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
	"encoding/binary"

	"golang.org/x/crypto/sha3"
)

// Algorithm 29 (SampleInBall)
func (par *params) sampleInBall(rho []byte) *poly {
	res := &poly{}
	H := sha3.NewShake256()
	H.Write(rho)
	var s [8]byte
	H.Read(s[:])
	signbits := binary.LittleEndian.Uint64(s[:])
	for i := degree - par.tau; i < degree; i++ {
		var jj [1]byte
		H.Read(jj[:])
		for int(jj[0]) > i {
			H.Read(jj[:])
		}
		j := jj[0]
		res[i] = res[j]
		res[j] = rZq(1).sub(rZq(2 * (signbits & 1)))
		signbits >>= 1
	}
	return res
}

// Algorithm 30 (RejectNTTPoly)
func rejectNTTPoly(rho [32 + 2]byte) *polyNTT {
	res := &polyNTT{}
	G := sha3.NewShake128()
	G.Write(rho[:])
	i := 0
	for i < degree {
		// For SHAKE128, the rate (i.e. blocksize) is 168 bytes.
		var s [168]byte
		G.Read(s[:])
		// We have that 168 mod 3 = 0. Then 168/3 = 56 (exact), so we may read 3-blocks
		// from 3-index 0,...,55. Then on 3-index 56 we have j = 168 and the loop exits.
		for j := 0; j < 168 && i < degree; j += 3 {
			// Inlined Algorithm 14 (CoeffFromThreeBytes)
			c := uint32(s[j]) | uint32(s[j+1])<<8 | uint32(s[j+2]&0x7F)<<16
			if c < q {
				res[i] = rZq(c)
				i++
			}
		}
	}
	return res
}

// Algorithm 15 (CoeffFromHalfByte)
func (par *params) coeffFromHalfByte(b byte) (rZq, bool) {
	if par.eta == 2 && b < 15 {
		// Constant time "b mod 5".
		bMod5 := byte(uint16(b) - 5*((uint16(b)*205)>>10))
		return rZq(2).sub(rZq(bMod5)), true
	}
	if par.eta == 4 && b < 9 {
		return rZq(4).sub(rZq(b)), true
	}
	return rZq(0), false
}

// Algorithm 31 (RejectBoundedPoly)
func (par *params) rejectBoundedPoly(rho [64 + 2]byte) *poly {
	res := &poly{}
	H := sha3.NewShake256()
	H.Write(rho[:])
	i := 0
	for i < degree {
		var zz [1]byte
		H.Read(zz[:])
		z := zz[0]
		z0, z0Accept := par.coeffFromHalfByte(z & 0xF)
		z1, z1Accept := par.coeffFromHalfByte(z >> 4)
		if z0Accept {
			res[i] = z0
			i++
		}
		if z1Accept && i < degree {
			res[i] = z1
			i++
		}
	}
	return res
}
