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

// Package jwtrsassapss defines parameters and keys for the JWT-RSA-SSA-PSS
// algorithm.
package jwtrsassapss

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/keygenregistry"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
)

func init() {
	if err := protoserialization.RegisterParametersSerializer[*Parameters](&parametersSerializer{}); err != nil {
		panic(fmt.Sprintf("protoserialization.RegisterParametersSerializer() failed: %v", err))
	}
	if err := protoserialization.RegisterParametersParser(privateKeyTypeURL, &parametersParser{}); err != nil {
		panic(fmt.Sprintf("protoserialization.RegisterParametersParser() failed: %v", err))
	}
	if err := protoserialization.RegisterKeySerializer[*PublicKey](&publicKeySerializer{}); err != nil {
		panic(fmt.Sprintf("protoserialization.RegisterKeySerializer() failed: %v", err))
	}
	if err := protoserialization.RegisterKeyParser(publicKeyTypeURL, &publicKeyParser{}); err != nil {
		panic(fmt.Sprintf("protoserialization.RegisterKeyParser() failed: %v", err))
	}
	if err := protoserialization.RegisterKeySerializer[*PrivateKey](&privateKeySerializer{}); err != nil {
		panic(fmt.Sprintf("protoserialization.RegisterKeySerializer() failed: %v", err))
	}
	if err := protoserialization.RegisterKeyParser(privateKeyTypeURL, &privateKeyParser{}); err != nil {
		panic(fmt.Sprintf("protoserialization.RegisterKeyParser() failed: %v", err))
	}
	if err := keygenregistry.RegisterKeyCreator[*Parameters](createPrivateKey); err != nil {
		panic(fmt.Sprintf("jwtrsassapss: failed to register key creator: %v", err))
	}
}
