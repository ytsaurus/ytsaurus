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

// Algorithm 9 (xmss_node).
func (p *params) xmssNode(skSeed []byte, i uint32, z uint32, pkSeed []byte, adrs *address) []byte {
	if z == 0 {
		adrs.setTypeAndClear(addressWOTSHash)
		adrs.setKeyPairAddress(i)
		return p.wotsPkGen(skSeed, pkSeed, adrs)
	}
	lnode := p.xmssNode(skSeed, i<<1, z-1, pkSeed, adrs)
	rnode := p.xmssNode(skSeed, (i<<1)+1, z-1, pkSeed, adrs)
	adrs.setTypeAndClear(addressTree)
	adrs.setTreeHeight(z)
	adrs.setTreeIndex(i)
	return p.hH(pkSeed, adrs, slices.Concat(lnode, rnode))
}

// Algorithm 10 (xmss_sign).
func (p *params) xmssSign(msg []byte, skSeed []byte, idx uint32, pkSeed []byte, adrs *address) []byte {
	var auth []byte
	// Build authentication path.
	for j := range p.hp {
		k := (idx >> j) ^ 1
		auth = append(auth, p.xmssNode(skSeed, k, j, pkSeed, adrs)...)
	}
	adrs.setTypeAndClear(addressWOTSHash)
	adrs.setKeyPairAddress(idx)
	return slices.Concat(p.wotsSign(msg, skSeed, pkSeed, adrs), auth)
}

// Algorithm 11 (xmss_pkFromSig).
// Here sigXmss is a concatenation of a WOTS+ signature, and an XMSS authentication path.
// The latter is a flattened hp * n-byte slice of individual n-byte chunks.
func (p *params) xmssPkFromSig(idx uint32, sigXmss []byte, msg []byte, pkSeed []byte, adrs *address) []byte {
	if len(sigXmss) != int((p.len+p.hp)*p.n) {
		panic("unreachable")
	}
	// Compute WOTS+ public key from WOTS+ signature.
	adrs.setTypeAndClear(addressWOTSHash)
	adrs.setKeyPairAddress(idx)
	sig, auth := sigXmss[:p.len*p.n], sigXmss[p.len*p.n:]
	node := p.wotsPkFromSig(sig, msg, pkSeed, adrs)
	// Compute root from WOTS+ public key and auth.
	adrs.setTypeAndClear(addressTree)
	adrs.setTreeIndex(idx)
	for k := range p.hp {
		adrs.setTreeHeight(k + 1)
		// This is equivalent to what is done in lines 11 and 14 of algorithm 11 of the SLH-DSA specification.
		// Shifting right by 1 is implicitly doing a floor division (instead of manually subtracting 1 in the odd case).
		adrs.setTreeIndex(adrs.treeIndex() >> 1)
		// Extract the k-th n-byte chunk of auth.
		authK := auth[k*p.n : (k+1)*p.n]
		if (idx>>k)&1 == 0 {
			node = p.hH(pkSeed, adrs, slices.Concat(node, authK))
		} else {
			node = p.hH(pkSeed, adrs, slices.Concat(authK, node))
		}
	}
	return node
}
