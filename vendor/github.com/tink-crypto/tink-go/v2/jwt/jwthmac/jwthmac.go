// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/Lycense-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

// Package jwthmac provides a JWT HMAC signer and verifier.
package jwthmac

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/keygenregistry"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
)

const (
	keyTypeURL = "type.googleapis.com/google.crypto.tink.JwtHmacKey"
)

func init() {
	if err := protoserialization.RegisterParametersSerializer[*Parameters](new(parametersSerializer)); err != nil {
		panic(fmt.Sprintf("jwthmac: failed to register parameters serializer: %v", err))
	}
	if err := protoserialization.RegisterParametersParser(keyTypeURL, new(parametersParser)); err != nil {
		panic(fmt.Sprintf("jwthmac: failed to register parameters parser: %v", err))
	}
	if err := protoserialization.RegisterKeySerializer[*Key](new(keySerializer)); err != nil {
		panic(fmt.Sprintf("jwthmac: failed to register key serializer: %v", err))
	}
	if err := protoserialization.RegisterKeyParser(keyTypeURL, new(keyParser)); err != nil {
		panic(fmt.Sprintf("jwthmac: failed to register key parser: %v", err))
	}
	if err := keygenregistry.RegisterKeyCreator[*Parameters](createKey); err != nil {
		panic(fmt.Sprintf("jwthmac: failed to register key creator: %v", err))
	}
}
