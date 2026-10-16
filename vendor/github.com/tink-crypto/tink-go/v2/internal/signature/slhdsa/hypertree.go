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

import "crypto/subtle"

// Algorithm 12 (ht_sign).
func (p *params) htSign(msg []byte, skSeed []byte, pkSeed []byte, idxTree uint64, idxLeaf uint32) []byte {
	adrs := newAddress()
	adrs.setTreeAddress(idxTree)
	sigHT := p.xmssSign(msg, skSeed, idxLeaf, pkSeed, adrs)
	root := p.xmssPkFromSig(idxLeaf, sigHT, msg, pkSeed, adrs)
	for j := uint32(1); j < p.d; j++ {
		// hp least significant bits of idxTree.
		idxLeaf = uint32(idxTree & ((1 << p.hp) - 1))
		// Remove least significant hp bits from idxTree.
		idxTree >>= p.hp
		adrs.setLayerAddress(j)
		adrs.setTreeAddress(idxTree)
		sigTmp := p.xmssSign(root, skSeed, idxLeaf, pkSeed, adrs)
		sigHT = append(sigHT, sigTmp...)
		if j < p.d-1 {
			root = p.xmssPkFromSig(idxLeaf, sigTmp, root, pkSeed, adrs)
		}
	}
	return sigHT
}

// Algorithm 13 (ht_verify).
// Here sigHT is a flattened (h + d * len) * n-byte signature of d-many individual (hp + len) * n-byte XMSS signatures.
func (p *params) htVerify(msg []byte, sigHT []byte, pkSeed []byte, idxTree uint64, idxLeaf uint32, pkRoot []byte) bool {
	if len(sigHT) != int((p.h+p.d*p.len)*p.n) {
		panic("unreachable")
	}
	adrs := newAddress()
	adrs.setTreeAddress(idxTree)
	sigTmp := sigHT[:(p.hp+p.len)*p.n]
	node := p.xmssPkFromSig(idxLeaf, sigTmp, msg, pkSeed, adrs)
	for j := uint32(1); j < p.d; j++ {
		// hp least significant bits of idxTree.
		idxLeaf = uint32(idxTree & ((1 << p.hp) - 1))
		// Remove least significant hp bits from idxTree.
		idxTree >>= p.hp
		adrs.setLayerAddress(j)
		adrs.setTreeAddress(idxTree)
		sigTmp = sigHT[j*(p.hp+p.len)*p.n : (j+1)*(p.hp+p.len)*p.n]
		node = p.xmssPkFromSig(idxLeaf, sigTmp, node, pkSeed, adrs)
	}
	// Note that since we are in the verification path, the following comparison is not required to be constant time.
	return subtle.ConstantTimeCompare(node, pkRoot) == 1
}
