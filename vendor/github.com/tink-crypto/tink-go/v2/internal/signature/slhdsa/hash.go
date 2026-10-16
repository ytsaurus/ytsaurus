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

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"slices"

	"golang.org/x/crypto/sha3"
)

// Hashing functions for SLH-DSA using SHAKE, see section 11.1 of the SLH-DSA specification.
// All of the below SHAKE hashing functions take m, n as parameters. Here we pass them as number
// of bytes to ShakeSum256. In the specification they are passed as 8*m, 8*n, i.e. number of bits.

func shakeHMsg(r []byte, pkSeed []byte, pkRoot []byte, msg []byte, m uint32) []byte {
	digest := make([]byte, m)
	sha3.ShakeSum256(digest, slices.Concat(r, pkSeed, pkRoot, msg))
	return digest
}

func shakePrf(pkSeed []byte, skSeed []byte, adrs *address, n uint32) []byte {
	digest := make([]byte, n)
	sha3.ShakeSum256(digest, slices.Concat(pkSeed, adrs[:], skSeed))
	return digest
}

func shakePrfMsg(skPrf []byte, optRand []byte, msg []byte, n uint32) []byte {
	digest := make([]byte, n)
	sha3.ShakeSum256(digest, slices.Concat(skPrf, optRand, msg))
	return digest
}

func shakeF(pkSeed []byte, adrs *address, msg1 []byte, n uint32) []byte {
	digest := make([]byte, n)
	sha3.ShakeSum256(digest, slices.Concat(pkSeed, adrs[:], msg1))
	return digest
}

func shakeH(pkSeed []byte, adrs *address, msg2 []byte, n uint32) []byte {
	digest := make([]byte, n)
	sha3.ShakeSum256(digest, slices.Concat(pkSeed, adrs[:], msg2))
	return digest
}

func shakeTl(pkSeed []byte, adrs *address, msgl []byte, n uint32) []byte {
	digest := make([]byte, n)
	sha3.ShakeSum256(digest, slices.Concat(pkSeed, adrs[:], msgl))
	return digest
}

// Hashing functions for SLH-DSA using SHA2 for Security Category 1, see section 11.2.1 of the SLH-DSA specification.

// Mask generation function based on a hash function, see https://datatracker.ietf.org/doc/html/rfc8017#appendix-B.2.1.
func mgf1(seed []byte, maskLen int, hash func([]byte) []byte) []byte {
	var digest []byte
	ctr := uint32(0)
	for len(digest) < maskLen {
		var ctrBytes [4]byte
		binary.BigEndian.PutUint32(ctrBytes[:], ctr)
		hash := hash(slices.Concat(seed, ctrBytes[:]))
		digest = append(digest, hash...)
		ctr++
	}
	return digest[0:maskLen]
}

func mgf1Sha256(seed []byte, maskLen int) []byte {
	return mgf1(seed, maskLen, func(data []byte) []byte {
		digest := sha256.Sum256(data)
		return digest[:]
	})
}

func sha2C1HMsg(r []byte, pkSeed []byte, pkRoot []byte, msg []byte, m uint32) []byte {
	sha256Digest := sha256.Sum256(slices.Concat(r, pkSeed, pkRoot, msg))
	return mgf1Sha256(slices.Concat(r, pkSeed, sha256Digest[:]), int(m))
}

func sha2C1Prf(pkSeed []byte, skSeed []byte, adrs *address, n uint32) []byte {
	zeroes := make([]byte, 64-n)
	digest := sha256.Sum256(slices.Concat(pkSeed, zeroes, adrs.compress(), skSeed))
	return digest[0:n]
}

func sha2C1PrfMsg(skPrf []byte, optRand []byte, msg []byte, n uint32) []byte {
	sha256Hmac := hmac.New(sha256.New, skPrf)
	sha256Hmac.Write(optRand)
	sha256Hmac.Write(msg)
	digest := sha256Hmac.Sum(nil)
	return digest[0:n]
}

func sha2C1F(pkSeed []byte, adrs *address, msg1 []byte, n uint32) []byte {
	zeroes := make([]byte, 64-n)
	digest := sha256.Sum256(slices.Concat(pkSeed, zeroes, adrs.compress(), msg1))
	return digest[0:n]
}

func sha2C1H(pkSeed []byte, adrs *address, msg2 []byte, n uint32) []byte {
	zeroes := make([]byte, 64-n)
	digest := sha256.Sum256(slices.Concat(pkSeed, zeroes, adrs.compress(), msg2))
	return digest[0:n]
}

func sha2C1Tl(pkSeed []byte, adrs *address, msgl []byte, n uint32) []byte {
	zeroes := make([]byte, 64-n)
	digest := sha256.Sum256(slices.Concat(pkSeed, zeroes, adrs.compress(), msgl))
	return digest[0:n]
}

// Hashing functions for SLH-DSA using SHA2 for Security Categories 3 and 5, see section 11.2.2 of the SLH-DSA specification.

func mgf1Sha512(seed []byte, maskLen int) []byte {
	return mgf1(seed, maskLen, func(data []byte) []byte {
		digest := sha512.Sum512(data)
		return digest[:]
	})
}

func sha2C35HMsg(r []byte, pkSeed []byte, pkRoot []byte, msg []byte, m uint32) []byte {
	sha512Digest := sha512.Sum512(slices.Concat(r, pkSeed, pkRoot, msg))
	return mgf1Sha512(slices.Concat(r, pkSeed, sha512Digest[:]), int(m))
}

func sha2C35Prf(pkSeed []byte, skSeed []byte, adrs *address, n uint32) []byte {
	zeroes := make([]byte, 64-n)
	digest := sha256.Sum256(slices.Concat(pkSeed, zeroes, adrs.compress(), skSeed))
	return digest[0:n]
}

func sha2C35PrfMsg(skPrf []byte, optRand []byte, msg []byte, n uint32) []byte {
	sha512Hmac := hmac.New(sha512.New, skPrf)
	sha512Hmac.Write(optRand)
	sha512Hmac.Write(msg)
	digest := sha512Hmac.Sum(nil)
	return digest[0:n]
}

func sha2C35F(pkSeed []byte, adrs *address, msg1 []byte, n uint32) []byte {
	zeroes := make([]byte, 64-n)
	digest := sha256.Sum256(slices.Concat(pkSeed, zeroes, adrs.compress(), msg1))
	return digest[0:n]
}

func sha2C35H(pkSeed []byte, adrs *address, msg2 []byte, n uint32) []byte {
	zeroes := make([]byte, 128-n)
	digest := sha512.Sum512(slices.Concat(pkSeed, zeroes, adrs.compress(), msg2))
	return digest[0:n]
}

func sha2C35Tl(pkSeed []byte, adrs *address, msgl []byte, n uint32) []byte {
	zeroes := make([]byte, 128-n)
	digest := sha512.Sum512(slices.Concat(pkSeed, zeroes, adrs.compress(), msgl))
	return digest[0:n]
}
