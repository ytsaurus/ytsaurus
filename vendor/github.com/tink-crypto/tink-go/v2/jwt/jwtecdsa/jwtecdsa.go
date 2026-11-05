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

// Package jwtecdsa defines JWT ECDSA keys and parameters.
package jwtecdsa

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/keygenregistry"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
)

func init() {
	if err := protoserialization.RegisterParametersSerializer[*Parameters](new(parametersSerializer)); err != nil {
		panic(fmt.Sprintf("jwtecdsa: failed to register parameters serializer: %v", err))
	}
	if err := protoserialization.RegisterParametersParser(privateKeyTypeURL, new(parametersParser)); err != nil {
		panic(fmt.Sprintf("jwtecdsa: failed to register parameters parser: %v", err))
	}
	if err := protoserialization.RegisterKeySerializer[*PublicKey](new(publicKeySerializer)); err != nil {
		panic(fmt.Sprintf("jwtecdsa: failed to register public key serializer: %v", err))
	}
	if err := protoserialization.RegisterKeyParser(publicKeyTypeURL, new(publicKeyParser)); err != nil {
		panic(fmt.Sprintf("jwtecdsa: failed to register public key parser: %v", err))
	}
	if err := protoserialization.RegisterKeySerializer[*PrivateKey](new(privateKeySerializer)); err != nil {
		panic(fmt.Sprintf("jwtecdsa: failed to register private key serializer: %v", err))
	}
	if err := protoserialization.RegisterKeyParser(privateKeyTypeURL, new(privateKeyParser)); err != nil {
		panic(fmt.Sprintf("jwtecdsa: failed to register private key parser: %v", err))
	}
	if err := keygenregistry.RegisterKeyCreator[*Parameters](createPrivateKey); err != nil {
		panic(fmt.Sprintf("jwtecdsa: failed to register key creator: %v", err))
	}
}
