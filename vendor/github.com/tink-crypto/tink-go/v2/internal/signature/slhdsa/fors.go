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

import "slices"

// Algorithm 14 (fors_skGen).
func (p *params) forsSkGen(skSeed []byte, pkSeed []byte, adrs *address, idx uint32) []byte {
	// Copy address to create key generation address.
	skAdrs := adrs.copy()
	skAdrs.setTypeAndClear(addressFORSPrf)
	skAdrs.setKeyPairAddress(adrs.keyPairAddress())
	skAdrs.setTreeIndex(idx)
	return p.hPrf(pkSeed, skSeed, skAdrs)
}

// Algorithm 15 (fors_node).
func (p *params) forsNode(skSeed []byte, i uint32, z uint32, pkSeed []byte, adrs *address) []byte {
	if z == 0 {
		sk := p.forsSkGen(skSeed, pkSeed, adrs, i)
		adrs.setTreeHeight(0)
		adrs.setTreeIndex(i)
		return p.hF(pkSeed, adrs, sk)
	}
	lnode := p.forsNode(skSeed, i<<1, z-1, pkSeed, adrs)
	rnode := p.forsNode(skSeed, (i<<1)+1, z-1, pkSeed, adrs)
	adrs.setTreeHeight(z)
	adrs.setTreeIndex(i)
	return p.hH(pkSeed, adrs, slices.Concat(lnode, rnode))
}

// Algorithm 16 (fors_sign).
func (p *params) forsSign(md []byte, skSeed []byte, pkSeed []byte, adrs *address) []byte {
	// Initialize sigFors as a zero-length byte string.
	var sigFors []byte
	indices := base2b(md, p.a, p.k)
	// Compute signature elements.
	for i := range p.k {
		sigFors = append(sigFors, p.forsSkGen(skSeed, pkSeed, adrs, (i<<p.a)+indices[i])...)
		var auth []byte
		// Compute authentication path.
		for j := range p.a {
			s := (indices[i] >> j) ^ 1
			auth = append(auth, p.forsNode(skSeed, (i<<(p.a-j))+s, j, pkSeed, adrs)...)
		}
		sigFors = append(sigFors, auth...)
	}
	return sigFors
}

// Algorithm 17 (fors_pkFromSig).
// Here sigFors is a flattened k * (a + 1) * n-byte signature of k-many individual (a + 1) * n-byte chunks.
// Each such chunk is a concatenation of an n-byte secret key and an a * n-byte authentication path.
func (p *params) forsPkFromSig(sigFors []byte, md []byte, pkSeed []byte, adrs *address) []byte {
	if len(sigFors) != int((p.k*(p.a+1))*p.n) {
		panic("unreachable")
	}
	indices := base2b(md, p.a, p.k)
	var root []byte
	for i := range p.k {
		sk := sigFors[i*(p.a+1)*p.n : (i*(p.a+1)+1)*p.n]
		// Compute leaf.
		adrs.setTreeHeight(0)
		adrs.setTreeIndex((i << p.a) + indices[i])
		node := p.hF(pkSeed, adrs, sk)
		auth := sigFors[(i*(p.a+1)+1)*p.n : (i+1)*(p.a+1)*p.n]
		// Compute root from leaf and auth.
		for j := range p.a {
			adrs.setTreeHeight(j + 1)
			// This is equivalent to what is done in lines 11 and 14 of algorithm 17 of the SLH-DSA specification.
			// Shifting right by 1 is implicitly doing a floor division (instead of manually subtracting 1 in the odd case).
			adrs.setTreeIndex(adrs.treeIndex() >> 1)
			// Extract the j-th n-byte chunk of auth.
			authJ := auth[j*p.n : (j+1)*p.n]
			if (indices[i]>>j)&1 == 0 {
				node = p.hH(pkSeed, adrs, slices.Concat(node, authJ))
			} else {
				node = p.hH(pkSeed, adrs, slices.Concat(authJ, node))
			}
		}
		root = append(root, node...)
	}
	// Copy address to create a FORS public key address.
	forsPkAdrs := adrs.copy()
	forsPkAdrs.setTypeAndClear(addressFORSRoots)
	forsPkAdrs.setKeyPairAddress(adrs.keyPairAddress())
	// hTl hashes the k * n-byte roots together to compute the n-byte FORS public key.
	return p.hTl(pkSeed, forsPkAdrs, root)
}
