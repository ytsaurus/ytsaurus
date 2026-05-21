// Copyright 2018 Google LLC
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

// Package keyset provides methods to generate, read, write or validate
// keysets.
package keyset

import (
	"github.com/tink-crypto/tink-go/v2/internal"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

// newKeysetHandleFromProto is used by package insecurecleartextkeyset and package
// testkeyset (via package internal) to create a keyset.Handle from cleartext
// key material.
func newKeysetHandleFromProto(ks *tinkpb.Keyset, opts ...Option) (*Handle, error) {
	entries, err := keysetToEntries(ks)
	if err != nil {
		return nil, err
	}
	return newFromEntries(entries, opts...)
}

// keysetMaterial is used by internal packages to obtain the key material
// contained in a [keyset.Handle] as a [tinkpb.Keyset].
//
// This is used through [internal.KeysetMaterial]
func keysetMaterial(h *Handle) *tinkpb.Keyset {
	ks, err := entriesToProtoKeyset(h.entries, true)
	if err != nil {
		return nil
	}
	return ks
}

func init() {
	internal.KeysetHandle = newKeysetHandleFromProto
	internal.KeysetMaterial = keysetMaterial
}
