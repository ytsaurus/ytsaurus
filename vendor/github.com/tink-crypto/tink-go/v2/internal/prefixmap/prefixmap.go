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

// Package prefixmap provides a map that adds a prefix to each primitive.
package prefixmap

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/core/cryptofmt"
)

const (
	// EmptyPrefix is the empty prefix.
	EmptyPrefix = ""
)

// PrefixMap is a map that adds a prefix to each primitive.
type PrefixMap[P any] struct {
	items map[string][]P
}

// New creates a new PrefixMap.
func New[P any]() *PrefixMap[P] {
	return &PrefixMap[P]{
		items: make(map[string][]P),
	}
}

// Iterator is an iterator over the primitives in [PrefixMap].
//
// The iterator returns the primitives in the following order:
// 1. All primitives with a non-empty prefix.
// 2. All primitives with an empty prefix.
//
// NOTE: We are using a custom iterator instead of [iter.Seq] for performance
// reasons.
type Iterator[P any] struct {
	fiveBytePrefixedPrimitives []P
	rawPrimitives              []P
	index                      int
}

// Next returns the next primitive in the iterator.
func (i *Iterator[P]) Next() (P, bool) {
	if i.index < len(i.fiveBytePrefixedPrimitives) {
		p := i.fiveBytePrefixedPrimitives[i.index]
		i.index++
		return p, true
	}
	if i.index < len(i.fiveBytePrefixedPrimitives)+len(i.rawPrimitives) {
		p := i.rawPrimitives[i.index-len(i.fiveBytePrefixedPrimitives)]
		i.index++
		return p, true
	}
	return *new(P), false
}

// PrimitivesMatchingPrefix returns the primitive with the given prefix.
func (m *PrefixMap[P]) PrimitivesMatchingPrefix(prefix []byte) *Iterator[P] {
	var entriesWithPrefix []P
	if len(prefix) >= cryptofmt.NonRawPrefixSize {
		// Cap the prefix to the size of the non-raw prefix.
		entriesWithPrefix = m.items[string(prefix[:cryptofmt.NonRawPrefixSize])]
	}
	entriesWithoutPrefix := m.items[EmptyPrefix]
	return &Iterator[P]{
		fiveBytePrefixedPrimitives: entriesWithPrefix,
		rawPrimitives:              entriesWithoutPrefix,
		index:                      0,
	}
}

// Insert adds the primitive with the given prefix.
func (m *PrefixMap[P]) Insert(prefix string, primitive P) error {
	if len(prefix) > 0 && len(prefix) != cryptofmt.NonRawPrefixSize {
		return fmt.Errorf("prefixmap: prefix has size %d, want %d", len(prefix), cryptofmt.NonRawPrefixSize)
	}
	m.items[prefix] = append(m.items[prefix], primitive)
	return nil
}
