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

package slhdsa

// Algorithm 2 (toInt).
// We do not assume that toInt and toByte are inverses of each other.
// The argument type of toByte does not match the return type of toInt,
// instead these have been chosen to fit the parameter sets accordingly.
func toInt(x []byte, n uint32) uint64 {
	if len(x) < int(n) || n > 8 {
		panic("unreachable")
	}
	total := uint64(0)
	for i := range n {
		total = 256*total + uint64(x[i])
	}
	return total
}

// Algorithm 3 (toByte).
func toByte(x uint32, n uint32) []byte {
	total := x
	s := make([]byte, n)
	for i := range n {
		s[n-1-i] = byte(total)
		total >>= 8
	}
	return s
}

// Algorithm 4 (base_{2^b}).
func base2b(x []byte, b uint32, outLen uint32) []uint32 {
	if len(x) < int((outLen*b+7)/8) {
		panic("unreachable")
	}
	in := 0
	bits := uint32(0)
	total := uint32(0)
	baseb := make([]uint32, outLen)
	for out := range outLen {
		for bits < b {
			total = (total << 8) + uint32(x[in])
			in++
			bits += 8
		}
		bits -= b
		baseb[out] = (total >> bits) & ((1 << b) - 1)
	}
	return baseb
}
