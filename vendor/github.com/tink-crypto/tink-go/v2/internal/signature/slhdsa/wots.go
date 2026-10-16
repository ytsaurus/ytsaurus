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

// Algorithm 5 (chain).
func (p *params) chain(x []byte, i uint32, s uint32, pkSeed []byte, adrs *address) []byte {
	tmp := x
	for j := i; j < i+s; j++ {
		adrs.setHashAddress(j)
		tmp = p.hF(pkSeed, adrs, tmp)
	}
	return tmp
}

// Algorithm 6 (wots_pkGen).
func (p *params) wotsPkGen(skSeed []byte, pkSeed []byte, adrs *address) []byte {
	// Copy address to create key generation address.
	skAdrs := adrs.copy()
	skAdrs.setTypeAndClear(addressWOTSPrf)
	skAdrs.setKeyPairAddress(adrs.keyPairAddress())
	var tmp []byte
	for i := range p.len {
		skAdrs.setChainAddress(i)
		// Compute secret value for chain i.
		sk := p.hPrf(pkSeed, skSeed, skAdrs)
		adrs.setChainAddress(i)
		// Compute public value for chain i.
		tmp = append(tmp, p.chain(sk, 0, p.w-1, pkSeed, adrs)...)
	}
	// Copy address to create WOTS+ public key address.
	wotsPkAdrs := adrs.copy()
	wotsPkAdrs.setTypeAndClear(addressWOTSPk)
	wotsPkAdrs.setKeyPairAddress(adrs.keyPairAddress())
	// Compress public key.
	return p.hTl(pkSeed, wotsPkAdrs, tmp)
}

func (p *params) wotsChecksum(msg []byte) []uint32 {
	// Convert message to base w.
	msgb := base2b(msg, p.lgw, p.len1)
	// Compute checksum.
	csum := uint32(0)
	for i := range p.len1 {
		csum = csum + p.w - 1 - msgb[i]
	}
	// For lgw = 4, left shift by 4.
	csum <<= ((8 - ((p.len2 * p.lgw) & 7)) & 7)
	// Convert to base w.
	buf := toByte(csum, (p.len2*p.lgw+7)/8)
	return append(msgb, base2b(buf[:], p.lgw, p.len2)...)
}

// Algorithm 7 (wots_sign).
func (p *params) wotsSign(msg []byte, skSeed []byte, pkSeed []byte, adrs *address) []byte {
	msgw := p.wotsChecksum(msg)
	// Copy address to create key generation key address.
	skAdrs := adrs.copy()
	skAdrs.setTypeAndClear(addressWOTSPrf)
	skAdrs.setKeyPairAddress(adrs.keyPairAddress())
	var sig []byte
	for i := range p.len {
		skAdrs.setChainAddress(i)
		// Compute chain i secret value.
		sk := p.hPrf(pkSeed, skSeed, skAdrs)
		adrs.setChainAddress(i)
		// Compute chain i signature value.
		sig = append(sig, p.chain(sk, 0, msgw[i], pkSeed, adrs)...)
	}
	return sig
}

// Algorithm 8 (wots_pkFromSig).
// Here sig is a flattened len * n-byte slice of individual n-byte chunks and M is an n-byte message.
func (p *params) wotsPkFromSig(sig []byte, msg []byte, pkSeed []byte, adrs *address) []byte {
	if len(sig) != int(p.len*p.n) {
		panic("unreachable")
	}
	msgw := p.wotsChecksum(msg)
	var tmp []byte
	for i := range p.len {
		adrs.setChainAddress(i)
		// Extract the i-th n-byte chunk of the signature.
		sigI := sig[i*p.n : (i+1)*p.n]
		tmp = append(tmp, p.chain(sigI, msgw[i], p.w-1-msgw[i], pkSeed, adrs)...)
	}
	// Copy address to create WOTS+ public key address.
	wotspkAdrs := adrs.copy()
	wotspkAdrs.setTypeAndClear(addressWOTSPk)
	wotspkAdrs.setKeyPairAddress(adrs.keyPairAddress())
	return p.hTl(pkSeed, wotspkAdrs, tmp)
}
