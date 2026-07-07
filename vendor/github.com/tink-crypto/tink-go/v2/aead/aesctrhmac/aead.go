// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aesctrhmac

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/aead"
	"github.com/tink-crypto/tink-go/v2/internal/mac/hmac"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

type fullAEAD struct {
	aesCTR  *aead.AESCTR
	hmac    *hmac.HMAC
	ivSize  int
	tagSize int
	prefix  []byte
	variant Variant
}

var _ tink.AEAD = (*fullAEAD)(nil)

func newAEAD(key *Key) (tink.AEAD, error) {
	tagSize := key.parameters.TagSizeInBytes()
	ivSize := key.parameters.IVSizeInBytes()
	aesCTR, err := aead.NewAESCTR(key.AESKeyBytes().Data(insecuresecretdataaccess.Token{}), ivSize)
	if err != nil {
		return nil, err
	}
	hmac, err := hmac.New(key.parameters.HashType().String(), key.HMACKeyBytes().Data(insecuresecretdataaccess.Token{}), uint32(tagSize))
	if err != nil {
		return nil, err
	}
	return &fullAEAD{
		aesCTR:  aesCTR,
		hmac:    hmac,
		ivSize:  ivSize,
		tagSize: tagSize,
		prefix:  key.OutputPrefix(),
		variant: key.parameters.Variant(),
	}, nil
}

func aadSizeInBits(associatedData []byte) []byte {
	n := uint64(len(associatedData)) * 8
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

// Encrypt encrypts plaintext with associatedData.
//
// The plaintext is encrypted with an AES-CTR, then HMAC is computed over
// (associatedData || ciphertext || n) where n is associatedData's length
// in bits represented as a 64-bit big endian unsigned integer. The final
// ciphertext format is (IND-CPA ciphertext || mac).
func (a *fullAEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	ctSize := len(a.prefix) + a.ivSize + len(plaintext)
	ciphertext := make([]byte, ctSize, ctSize+a.tagSize)
	if n := copy(ciphertext, a.prefix); n != len(a.prefix) {
		return nil, fmt.Errorf("aesctrhmac: failed to copy prefix")
	}
	ctNoPrefix, err := a.aesCTR.Encrypt(ciphertext[len(a.prefix):], plaintext)
	if err != nil {
		return nil, err
	}
	tag, err := a.hmac.ComputeMAC(associatedData, ctNoPrefix, aadSizeInBits(associatedData))
	if err != nil {
		return nil, err
	}
	if len(tag) != a.tagSize {
		return nil, errors.New("aesctrhmac: invalid tag size")
	}
	return append(ciphertext, tag...), nil
}

// Decrypt decrypts ciphertext with associatedData.
func (a *fullAEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	prefixSize := len(a.prefix)
	if len(ciphertext) < prefixSize+a.ivSize+a.tagSize {
		return nil, fmt.Errorf("aesctrhmac: ciphertext with size %d is too short", len(ciphertext))
	}
	prefix := ciphertext[:prefixSize]
	if !bytes.Equal(prefix, a.prefix) {
		return nil, fmt.Errorf("aesctrhmac: ciphertext prefix does not match: got %x, want %x", prefix, a.prefix)
	}
	payload := ciphertext[prefixSize : len(ciphertext)-a.tagSize]
	tag := ciphertext[len(ciphertext)-a.tagSize:]
	if err := a.hmac.VerifyMAC(tag, associatedData, payload, aadSizeInBits(associatedData)); err != nil {
		return nil, fmt.Errorf("aesctrhmac: %v", err)
	}
	return a.aesCTR.Decrypt(nil, payload)
}

// primitiveConstructor creates a [tink.AEAD] from a [key.Key].
//
// The key must be of type [aesctrhmac.Key].
func primitiveConstructor(k key.Key) (any, error) {
	that, ok := k.(*Key)
	if !ok {
		return nil, fmt.Errorf("invalid key type: got %T, want *aesctrhmac.Key", k)
	}
	return newAEAD(that)
}
