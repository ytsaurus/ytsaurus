// Copyright 2024 Google LLC
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

// Package keygenregistry is a registry for key creators.
package keygenregistry

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/tink-crypto/tink-go/v2/key"
)

type keyCreator = func(p key.Parameters, idRequirement uint32) (key.Key, error)

var (
	keyCreatorsMutex sync.RWMutex
	keyCreators      = make(map[reflect.Type]keyCreator)
)

// RegisterKeyCreator registers a function that creates a key from
// the given [key.Parameters].
//
// Returns an error if a creator for parametersType is already registered (no
// matter whether it's the same object or different, since constructors are of
// type [Func] and they are never considered equal in Go unless they are nil).
func RegisterKeyCreator[P key.Parameters](creator func(p key.Parameters, idRequirement uint32) (key.Key, error)) error {
	defer keyCreatorsMutex.Unlock()
	keyCreatorsMutex.Lock()
	parametersType := reflect.TypeFor[P]()
	if _, found := keyCreators[parametersType]; found {
		return fmt.Errorf("a different key creator already registered for %v", parametersType)
	}
	keyCreators[parametersType] = creator
	return nil
}

// CreateKey creates a key from the given [key.Parameters] using the registry.
func CreateKey(p key.Parameters, idRequirement uint32) (key.Key, error) {
	creator, found := keyCreators[reflect.TypeOf(p)]
	if !found {
		return nil, fmt.Errorf("no creator found for parameters %T", p)
	}
	return creator(p, idRequirement)
}

// UnregisterKeyCreator unregisters a key creator for the given parametersType.
//
// This is for testing only.
func UnregisterKeyCreator[P key.Parameters]() {
	defer keyCreatorsMutex.Unlock()
	keyCreatorsMutex.Lock()
	parametersType := reflect.TypeFor[P]()
	delete(keyCreators, parametersType)
}
