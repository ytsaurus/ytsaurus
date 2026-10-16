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

// Package aescmac implements AES-CMAC.
package aescmac

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/subtle"
	"fmt"
)

const (
	// BlockSize is the block size of AES.
	BlockSize = aes.BlockSize
	mul       = 0x87 // 0x87 is the generator of GF(2^128).
	pad       = byte(0x80)
)

// CMAC is an implementation of AES-CMAC as defined in RFC 4493.
type CMAC struct {
	bc     cipher.Block
	k1, k2 [BlockSize]byte
}

// multiplyByX multiplies an element in GF(2^128) by its generator.
//
// This is used to for key generation in section 2.3 of RFC 4493.
func mulByX(block []byte) {
	// This is computed as:
	// if block[0] >> 7 == 1:
	//   leftShift(block, 1) XOR 0x87
	// else:
	//   leftShift(block, 1)
	v := int(block[0] >> 7)
	for i := 0; i < BlockSize-1; i++ {
		block[i] = block[i]<<1 | block[i+1]>>7
	}
	block[BlockSize-1] = (block[BlockSize-1] << 1) ^ byte(subtle.ConstantTimeSelect(v, mul, 0x00))
}

// New returns a new CMAC instance.
func New(key []byte) (*CMAC, error) {
	if len(key) != 32 && len(key) != 24 && len(key) != 16 {
		return nil, fmt.Errorf("aescmac: invalid key size; got %d, want 16, 24, or 32", len(key))
	}

	bc, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aescmac: could not obtain cipher: %v", err)
	}
	cmac := &CMAC{bc: bc}
	var zeroBlock [BlockSize]byte
	// Generate Subkeys
	cmac.bc.Encrypt(cmac.k1[:], zeroBlock[:])
	mulByX(cmac.k1[:])
	copy(cmac.k2[:], cmac.k1[:])
	mulByX(cmac.k2[:])

	return cmac, nil
}

// Compute computes the AES-CMAC of the given data.
//
// The timing of this function will only depend on len(data), and not leak any
// additional information about the key or the data.
func (c *CMAC) Compute(data []byte) []byte {
	numBlocksButLast := len(data) / BlockSize
	// The following "if" only depends on len(data).
	if len(data) > 0 && len(data)%BlockSize == 0 {
		numBlocksButLast--
	}

	output := make([]byte, BlockSize)
	// Process blocks from M_1, ..., M_(n-1). This is regardless of the
	// length of the last block.
	for i := 0; i < numBlocksButLast; i++ {
		subtle.XORBytes(output, data[:BlockSize], output)
		c.bc.Encrypt(output, output)
		data = data[BlockSize:]
	}

	// Last block M_n. If len(data) == 0, it simply sets lastBlock = 100...0.
	var lastBlock [BlockSize]byte
	// The following "if" only depends on len(data).
	if len(data) == BlockSize {
		// Full last block.
		subtle.XORBytes(lastBlock[:], data[:], c.k1[:])
	} else {
		// Either empty or partial last block.
		copy(lastBlock[:], data[:])
		lastBlock[len(data)] = pad
		subtle.XORBytes(lastBlock[:], lastBlock[:], c.k2[:])
	}
	subtle.XORBytes(output, output, lastBlock[:])
	c.bc.Encrypt(output, output)
	return output
}

// XOREndAndCompute computes the AES-CMAC over "data xorend last".
//
// data must be >= BlockSize.
// last and out must be == BlockSize.
//
// The `xorend` function is XOR(data[len(data)-BlockSize:], last)
//
// The timing of this function will only depend on len(data), and not leak any
// additional information about the key or the data.
func (c *CMAC) XOREndAndCompute(data, last []byte) ([]byte, error) {
	if len(last) != BlockSize {
		return nil, fmt.Errorf("aescmac: invalid size for \"last\"; got %d, want %d", len(last), BlockSize)
	}
	if len(data) < BlockSize {
		return nil, fmt.Errorf("aescmac: invalid size for \"data\"; got %d, want at least %d", len(data), BlockSize)
	}

	numBlocksButLast := len(data) / BlockSize
	// The following "if" only depends on len(data).
	if len(data)%BlockSize == 0 {
		numBlocksButLast--
	}

	// Starting position for the portion of`data` to be XORed with `last`.
	startPos := len(data) - BlockSize
	output := make([]byte, BlockSize)
	// Process blocks from M_0, ..., M_(n/BlockSize-1).
	for i := 0; i < numBlocksButLast; i++ {
		subtle.XORBytes(output, data[:BlockSize], output)
		if (i+1)*BlockSize > startPos {
			// XOR a portion of the current block with `last`.
			portionSize := (i+1)*BlockSize - startPos
			subtle.XORBytes(output[BlockSize-portionSize:], output[BlockSize-portionSize:], last[:portionSize])
			last = last[portionSize:]
		}
		c.bc.Encrypt(output, output)
		data = data[BlockSize:]
	}

	// Last block M_n.
	var lastBlock [BlockSize]byte
	subtle.XORBytes(lastBlock[:], data[:], last[:])
	// The following "if" only depends on len(data).
	if len(data) == BlockSize {
		// Full last block.
		subtle.XORBytes(lastBlock[:], lastBlock[:], c.k1[:])
	} else {
		// Partial last block.
		lastBlock[len(data)] = pad
		subtle.XORBytes(lastBlock[:], lastBlock[:], c.k2[:])
	}
	subtle.XORBytes(output, output, lastBlock[:])
	c.bc.Encrypt(output, output)
	return output, nil
}
