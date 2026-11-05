// Copyright 2022 Google LLC
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

package prfbasedkeyderivation

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/internal/keygenregistry"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	prfderpb "github.com/tink-crypto/tink-go/v2/proto/prf_based_deriver_go_proto"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

type keyManager struct{}

var _ registry.KeyManager = (*keyManager)(nil)

func (km *keyManager) Primitive(serializedKey []byte) (any, error) {
	return nil, errors.New("prf_based_deriver_key_manager: not implemented; users should obtain a keyset.Handle and the primitive with keyderivation.New")
}

func (km *keyManager) NewKey(serializedKeyFormat []byte) (proto.Message, error) {
	keyData, err := km.NewKeyData(serializedKeyFormat)
	if err != nil {
		return nil, err
	}
	key := new(prfderpb.PrfBasedDeriverKey)
	if err := proto.Unmarshal(keyData.GetValue(), key); err != nil {
		return nil, fmt.Errorf("failed to unmarshal key data: %w", err)
	}
	return key, nil
}

func (km *keyManager) NewKeyData(serializedKeyFormat []byte) (*tinkpb.KeyData, error) {
	keyFormat := &prfderpb.PrfBasedDeriverKeyFormat{}
	if err := proto.Unmarshal(serializedKeyFormat, keyFormat); err != nil {
		return nil, fmt.Errorf("failed to unmarshal key format: %w", err)
	}

	parameters, err := protoserialization.ParseParameters(&tinkpb.KeyTemplate{
		TypeUrl:          km.TypeURL(),
		Value:            serializedKeyFormat,
		OutputPrefixType: keyFormat.GetParams().GetDerivedKeyTemplate().GetOutputPrefixType(),
	})
	if err != nil {
		return nil, err
	}

	k, err := keygenregistry.CreateKey(parameters, 0)
	if err != nil {
		return nil, err
	}
	keySerialization, err := protoserialization.SerializeKey(k)
	if err != nil {
		return nil, err
	}
	return keySerialization.KeyData(), nil
}

func (km *keyManager) DoesSupport(typeURL string) bool { return typeURL == km.TypeURL() }

func (km *keyManager) TypeURL() string { return typeURL }
