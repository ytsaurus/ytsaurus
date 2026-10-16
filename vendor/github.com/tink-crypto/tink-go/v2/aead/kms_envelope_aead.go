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

package aead

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/tink"
	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

const (
	lenDEK                = 4
	maxLengthEncryptedDEK = 4096
)

// KMSEnvelopeAEAD represents an instance of KMS Envelope AEAD that implements
// the [tink.AEAD] interface.
type KMSEnvelopeAEAD struct {
	dekTemplate *tinkpb.KeyTemplate
	kekAEAD     tink.AEAD
	// if err != nil, then the primitive will always fail with this error.
	// this is needed because NewKMSEnvelopeAEAD2 doesn't return an error.
	err error
}

var tinkAEADKeyTypes map[string]bool = map[string]bool{
	aesCTRHMACAEADTypeURL:    true,
	aesGCMTypeURL:            true,
	chaCha20Poly1305TypeURL:  true,
	xChaCha20Poly1305TypeURL: true,
	aesGCMSIVTypeURL:         true,
}

func isSupporedKMSEnvelopeDEK(dekKeyTypeURL string) bool {
	_, found := tinkAEADKeyTypes[dekKeyTypeURL]
	return found
}

// KMSEnvelopeAEADWithContext represents an instance of KMS Envelope AEAD that implements
// the [tink.AEADWithContext] interface.
type KMSEnvelopeAEADWithContext struct {
	dekTemplate *tinkpb.KeyTemplate
	kekAEAD     tink.AEADWithContext
}

// NewKMSEnvelopeAEADWithContext creates an new instance of [KMSEnvelopeAEADWithContext].
//
// dekTemplate must be a KeyTemplate for any of these Tink AEAD key types (any
// other key template will be rejected):
//   - AesCtrHmacAeadKey
//   - AesGcmKey
//   - ChaCha20Poly1305Key
//   - XChaCha20Poly1305
//   - AesGcmSivKey
//
// keyEncryptionAEAD is used to encrypt the DEK, and is usually a remote AEAD
// provided by a KMS.
func NewKMSEnvelopeAEADWithContext(dekTemplate *tinkpb.KeyTemplate, keyEncryptionAEAD tink.AEADWithContext) (*KMSEnvelopeAEADWithContext, error) {
	if !isSupporedKMSEnvelopeDEK(dekTemplate.GetTypeUrl()) {
		return nil, errors.New("unsupported DEK key type")
	}
	return &KMSEnvelopeAEADWithContext{
		dekTemplate: dekTemplate,
		kekAEAD:     keyEncryptionAEAD,
	}, nil
}

// NewKMSEnvelopeAEAD2 creates an new instance of [KMSEnvelopeAEAD].
//
// dekTemplate specifies the key template of the data encryption key (DEK).
// It must be a KeyTemplate for any of these Tink AEAD key types (any
// other key template will be rejected):
//   - AesCtrHmacAeadKey
//   - AesGcmKey
//   - ChaCha20Poly1305Key
//   - XChaCha20Poly1305
//   - AesGcmSivKey
//
// keyEncryptionAEAD is used to encrypt the DEK, and is usually a remote AEAD
// provided by a KMS. It is preferable to use [NewKMSEnvelopeAEADWithContext] instead.
func NewKMSEnvelopeAEAD2(dekTemplate *tinkpb.KeyTemplate, keyEncryptionAEAD tink.AEAD) *KMSEnvelopeAEAD {
	if !isSupporedKMSEnvelopeDEK(dekTemplate.GetTypeUrl()) {
		return &KMSEnvelopeAEAD{
			kekAEAD:     nil,
			dekTemplate: nil,
			err:         fmt.Errorf("unsupported DEK key type %s", dekTemplate.GetTypeUrl()),
		}
	}
	return &KMSEnvelopeAEAD{
		kekAEAD:     keyEncryptionAEAD,
		dekTemplate: dekTemplate,
		err:         nil,
	}
}

func newDEK(template *tinkpb.KeyTemplate) ([]byte, error) {
	dekKeyData, err := registry.NewKeyData(template)
	if err != nil {
		return nil, err
	}
	return dekKeyData.GetValue(), nil
}

func encryptDataAndSerializeEnvelope(dekTypeURL string, dek, encryptedDEK []byte, plaintext, associatedData []byte) ([]byte, error) {
	if len(encryptedDEK) == 0 {
		return nil, errors.New("encrypted dek is empty")
	}
	p, err := registry.Primitive(dekTypeURL, dek)
	if err != nil {
		return nil, err
	}
	dekAEAD, ok := p.(tink.AEAD)
	if !ok {
		return nil, errors.New("kms_envelope_aead: failed to convert AEAD primitive")
	}

	payload, err := dekAEAD.Encrypt(plaintext, associatedData)
	if err != nil {
		return nil, err
	}
	if len(encryptedDEK) > maxLengthEncryptedDEK {
		return nil, fmt.Errorf(
			"kms_envelope_aead: length of encrypted DEK too large; got %d, want at most %d",
			len(encryptedDEK), maxLengthEncryptedDEK)
	}
	res := make([]byte, 0, lenDEK+len(encryptedDEK)+len(payload))
	res = binary.BigEndian.AppendUint32(res, uint32(len(encryptedDEK)))
	res = append(res, encryptedDEK...)
	res = append(res, payload...)
	return res, nil
}

// Encrypt implements the tink.AEAD interface for encryption.
func (a *KMSEnvelopeAEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	if a.err != nil {
		return nil, a.err
	}
	dek, err := newDEK(a.dekTemplate)
	if err != nil {
		return nil, err
	}
	encryptedDEK, err := a.kekAEAD.Encrypt(dek, []byte{})
	if err != nil {
		return nil, err
	}
	return encryptDataAndSerializeEnvelope(a.dekTemplate.GetTypeUrl(), dek, encryptedDEK, plaintext, associatedData)
}

// parseEnvelope extracts encryptedDEK and payload from the ciphertext.
func parseEnvelope(ciphertext []byte) ([]byte, []byte, error) {
	// Verify we have enough bytes for the length of the encrypted DEK.
	if len(ciphertext) <= lenDEK {
		return nil, nil, errors.New("kms_envelope_aead: invalid ciphertext")
	}

	// Extract length of encrypted DEK and advance past that length.
	encryptedDEKLen := int(binary.BigEndian.Uint32(ciphertext[:lenDEK]))
	if encryptedDEKLen <= 0 || encryptedDEKLen > maxLengthEncryptedDEK || encryptedDEKLen > len(ciphertext)-lenDEK {
		return nil, nil, errors.New("kms_envelope_aead: length of encrypted DEK too large")
	}
	ciphertext = ciphertext[lenDEK:]

	encryptedDEK := ciphertext[:encryptedDEKLen]
	payload := ciphertext[encryptedDEKLen:]
	return encryptedDEK, payload, nil
}

func decryptDataWithDEK(dekTypeURL string, dek []byte, payload, associatedData []byte) ([]byte, error) {
	// Get an AEAD primitive corresponding to the DEK.
	p, err := registry.Primitive(dekTypeURL, dek)
	if err != nil {
		return nil, fmt.Errorf("kms_envelope_aead: %s", err)
	}
	dekAEAD, ok := p.(tink.AEAD)
	if !ok {
		return nil, errors.New("kms_envelope_aead: failed to convert AEAD primitive")
	}

	return dekAEAD.Decrypt(payload, associatedData)
}

// Decrypt implements the [tink.AEAD] interface for decryption.
func (a *KMSEnvelopeAEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	if a.err != nil {
		return nil, a.err
	}

	encryptedDEK, payload, err := parseEnvelope(ciphertext)
	if err != nil {
		return nil, err
	}

	dek, err := a.kekAEAD.Decrypt(encryptedDEK, []byte{})
	if err != nil {
		return nil, err
	}

	return decryptDataWithDEK(a.dekTemplate.GetTypeUrl(), dek, payload, associatedData)
}

// EncryptWithContext implements the [tink.AEADWithContext] interface for encryption.
func (a *KMSEnvelopeAEADWithContext) EncryptWithContext(ctx context.Context, plaintext, associatedData []byte) ([]byte, error) {
	dek, err := newDEK(a.dekTemplate)
	if err != nil {
		return nil, err
	}
	encryptedDEK, err := a.kekAEAD.EncryptWithContext(ctx, dek, []byte{})
	if err != nil {
		return nil, err
	}
	return encryptDataAndSerializeEnvelope(a.dekTemplate.GetTypeUrl(), dek, encryptedDEK, plaintext, associatedData)
}

// DecryptWithContext implements the [tink.AEADWithContext] interface for decryption.
func (a *KMSEnvelopeAEADWithContext) DecryptWithContext(ctx context.Context, ciphertext, associatedData []byte) ([]byte, error) {
	encryptedDEK, payload, err := parseEnvelope(ciphertext)
	if err != nil {
		return nil, err
	}

	dek, err := a.kekAEAD.DecryptWithContext(ctx, encryptedDEK, []byte{})
	if err != nil {
		return nil, err
	}

	return decryptDataWithDEK(a.dekTemplate.GetTypeUrl(), dek, payload, associatedData)
}
