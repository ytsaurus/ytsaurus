// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hmac

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/mac/subtle"
	"github.com/tink-crypto/tink-go/v2/tink"
)

type fullMAC struct {
	rawMAC  tink.MAC
	prefix  []byte
	variant Variant
}

var _ tink.MAC = (*fullMAC)(nil)

// NewMAC creates a new full HMAC [tink.MAC] primitive.
func NewMAC(key *Key, _ internalapi.Token) (tink.MAC, error) {
	params := key.Parameters().(*Parameters)

	if err := subtle.ValidateHMACParams(params.HashType().String(), uint32(params.KeySizeInBytes()), uint32(params.CryptographicTagSizeInBytes())); err != nil {
		return nil, fmt.Errorf("hmac.NewMAC: invalid parameters: %v", err)
	}

	rawMAC, err := subtle.NewHMAC(params.HashType().String(), key.KeyBytes().Data(insecuresecretdataaccess.Token{}), uint32(params.CryptographicTagSizeInBytes()))
	if err != nil {
		return nil, fmt.Errorf("hmac.NewMAC: failed to create raw MAC: %v", err)
	}
	return &fullMAC{
		rawMAC:  rawMAC,
		prefix:  key.OutputPrefix(),
		variant: params.Variant(),
	}, nil
}

func (m *fullMAC) message(msg []byte) []byte {
	if m.variant == VariantLegacy {
		return slices.Concat(msg, []byte{0x00})
	}
	return msg
}

func (m *fullMAC) ComputeMAC(data []byte) ([]byte, error) {
	rawMAC, err := m.rawMAC.ComputeMAC(m.message(data))
	if err != nil {
		return nil, err
	}
	return slices.Concat(m.prefix, rawMAC), nil
}

func (m *fullMAC) VerifyMAC(mac []byte, data []byte) error {
	if len(mac) < len(m.prefix) {
		return fmt.Errorf("hmac: mac with size %d is too short", len(mac))
	}
	prefix := mac[:len(m.prefix)]
	if !bytes.Equal(prefix, m.prefix) {
		return fmt.Errorf("hmac: mac prefix does not match")
	}
	return m.rawMAC.VerifyMAC(mac[len(m.prefix):], m.message(data))
}
