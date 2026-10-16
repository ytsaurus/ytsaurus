// Copyright 2019 Google LLC
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

// Package primitiveset provides a container for a set of cryptographic
// primitives.
//
// It provides also additional properties for the primitives it holds. In
// particular, one of the primitives in the set can be distinguished as "the
// primary" one.
package primitiveset

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/core/cryptofmt"
	"github.com/tink-crypto/tink-go/v2/key"
)

// Entry represents a single entry in the keyset. In addition to the actual
// primitive, it holds the identifier and status of the primitive.
type Entry[T any] struct {
	KeyID         uint32
	Primitive     T
	FullPrimitive T
	Key           key.Key
	IsPrimary     bool
}

// OutputPrefix returns the output prefix of the key.
func (e *Entry[T]) OutputPrefix() []byte {
	return outputPrefix(e.Key)
}

// PrimitiveSet is used for supporting key rotation: primitives in a set
// correspond to keys in a keyset. Users will usually work with primitive
// instances, which essentially wrap primitive sets. For example an instance of
// an AEAD-primitive for a given keyset holds a set of AEAD-primitives
// corresponding to the keys in the keyset, and uses the set members to do the
// actual crypto operations: to encrypt data the primary AEAD-primitive from
// the set is used, and upon decryption the ciphertext's prefix determines the
// id of the primitive from the set.
type PrimitiveSet[T any] struct {
	// Primary entry.
	Primary *Entry[T]

	// The primitives are stored in a map of (ciphertext prefix, list of
	// primitives sharing the prefix). This allows quickly retrieving the
	// primitives sharing some particular prefix.
	Entries map[string][]*Entry[T]
	// Stores entries in the original keyset key order.
	EntriesInKeysetOrder []*Entry[T]

	Annotations map[string]string
}

// New returns an empty instance of PrimitiveSet.
func New[T any]() *PrimitiveSet[T] {
	return &PrimitiveSet[T]{
		Primary:              nil,
		Entries:              make(map[string][]*Entry[T]),
		EntriesInKeysetOrder: make([]*Entry[T], 0),
		Annotations:          nil,
	}
}

// RawEntries returns all primitives in the set that have RAW prefix.
func (ps *PrimitiveSet[T]) RawEntries() ([]*Entry[T], error) {
	return ps.EntriesForPrefix(cryptofmt.RawPrefix)
}

// EntriesForPrefix returns all primitives in the set that have the given prefix.
func (ps *PrimitiveSet[T]) EntriesForPrefix(prefix string) ([]*Entry[T], error) {
	result, found := ps.Entries[prefix]
	if !found {
		return []*Entry[T]{}, nil
	}
	return result, nil
}

type withOutputPrefix interface {
	OutputPrefix() []byte
}

func outputPrefix(key key.Key) []byte {
	if withOutputPrefix, ok := key.(withOutputPrefix); ok {
		return withOutputPrefix.OutputPrefix()
	}
	return nil
}

// Add creates a new entry in the primitive set and returns the added entry.
func (ps *PrimitiveSet[T]) Add(e *Entry[T]) error {
	// TODO: b/442750026 - Consider forcing T to be a pointer type and check for
	// nil primitives.
	if e.Key == nil {
		return fmt.Errorf("key must be set")
	}
	prefix := string(e.OutputPrefix())
	ps.Entries[prefix] = append(ps.Entries[prefix], e)
	ps.EntriesInKeysetOrder = append(ps.EntriesInKeysetOrder, e)
	if e.IsPrimary {
		ps.Primary = e
	}
	return nil
}
