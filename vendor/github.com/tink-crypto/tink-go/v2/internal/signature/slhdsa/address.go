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
	"encoding/binary"
	"slices"
)

// An address is a 32-byte buffer with added structure, see Table 1 of the SLH-DSA specification.
// | layer address (4 bytes) | tree address (12 bytes) | type (4 bytes) | type specific (12 bytes) |
// type specific bytes:
//
//	WOTS_HASH:  | key pair address (4 bytes) | chain address   (4 bytes) | hash address     (4 bytes) |
//	WOTS_PK:    | key pair address (4 bytes) | padding = 0                                  (8 bytes) |
//	TREE:       | padding = 0      (4 bytes) | tree height     (4 bytes) | tree index       (4 bytes) |
//	FORS_TREE:  | key pair address (4 bytes) | tree height     (4 bytes) | tree index       (4 bytes) |
//	FORS_ROOTS: | key pair address (4 bytes) | padding = 0                                  (8 bytes) |
//	WOTS_PRF:   | key pair address (4 bytes) | chain address   (4 bytes) | hash address = 0 (4 bytes) |
//	FORS_PRF:   | key pair address (4 bytes) | tree height = 0 (4 bytes) | tree index       (4 bytes) |
type address [32]byte

type addressType uint32

const (
	addressWOTSHash addressType = iota
	addressWOTSPk
	addressTree
	addressFORSTree
	addressFORSRoots
	addressWOTSPrf
	addressFORSPrf
)

func newAddress() *address {
	return &address{}
}

func (a *address) copy() *address {
	res := address{}
	copy(res[:], a[:])
	return &res
}

func (a *address) setLayerAddress(l uint32) {
	binary.BigEndian.PutUint32(a[:4], l)
}

func (a *address) setTreeAddress(t uint64) {
	// This implementation supports at most 64 bits for the tree address.
	// This covers all the parameter sets given in the SLH-DSA specification.
	binary.BigEndian.PutUint32(a[4:8], 0)
	binary.BigEndian.PutUint64(a[8:16], t)
}

func (a *address) setTypeAndClear(y addressType) {
	binary.BigEndian.PutUint32(a[16:20], uint32(y))
	binary.BigEndian.PutUint32(a[20:24], 0)
	binary.BigEndian.PutUint32(a[24:28], 0)
	binary.BigEndian.PutUint32(a[28:32], 0)
}

func (a *address) setKeyPairAddress(i uint32) {
	binary.BigEndian.PutUint32(a[20:24], i)
}

func (a *address) keyPairAddress() uint32 {
	return binary.BigEndian.Uint32(a[20:24])
}

func (a *address) setChainAddress(i uint32) {
	binary.BigEndian.PutUint32(a[24:28], i)
}

func (a *address) setTreeHeight(i uint32) {
	binary.BigEndian.PutUint32(a[24:28], i)
}

func (a *address) setHashAddress(i uint32) {
	binary.BigEndian.PutUint32(a[28:32], i)
}

func (a *address) setTreeIndex(i uint32) {
	binary.BigEndian.PutUint32(a[28:32], i)
}

func (a *address) treeIndex() uint32 {
	return binary.BigEndian.Uint32(a[28:32])
}

// Address compression function for SLH-DSA using SHA2, see Table 3 of the SLH-DSA specification.
// | layer address (1 byte) | tree address (8 bytes) | type (1 bytes) | type specific (12 bytes) |
func (a *address) compress() []byte {
	return slices.Concat(a[3:4], a[8:16], a[19:32])
}
