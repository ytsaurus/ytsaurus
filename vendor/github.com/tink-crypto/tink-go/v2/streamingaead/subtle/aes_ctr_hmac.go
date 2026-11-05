// Copyright 2020 Google LLC
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

package subtle

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"errors"
	"fmt"
	"hash"
	"io"

	// Placeholder for internal crypto/cipher allowlist, please ignore.
	subtleaead "github.com/tink-crypto/tink-go/v2/aead/subtle"
	"github.com/tink-crypto/tink-go/v2/streamingaead/subtle/noncebased"
	"github.com/tink-crypto/tink-go/v2/subtle/random"
	"github.com/tink-crypto/tink-go/v2/subtle"
)

const (
	// AESCTRHMACNonceSizeInBytes is the size of the nonces used as IVs for CTR.
	AESCTRHMACNonceSizeInBytes = 16

	// AESCTRHMACNoncePrefixSizeInBytes is the size of the nonce prefix.
	AESCTRHMACNoncePrefixSizeInBytes = 7

	// AESCTRHMACKeySizeInBytes is the size of the HMAC key.
	AESCTRHMACKeySizeInBytes = 32
)

// AESCTRHMAC implements streaming AEAD encryption using AES-CTR and HMAC.
//
// Each ciphertext uses new AES-CTR and HMAC keys. These keys are derived using
// HKDF and are derived from the key derivation key, a randomly chosen salt of
// the same size as the key and a nonce prefix.
type AESCTRHMAC struct {
	mainKey                      []byte
	hkdfAlg                      string
	keySizeInBytes               int
	tagAlg                       string
	tagSizeInBytes               int
	ciphertextSegmentSize        int
	plaintextSegmentSize         int
	firstCiphertextSegmentOffset int
}

// NewAESCTRHMAC initializes an AESCTRHMAC primitive with a key derivation key
// and encryption parameters.
//
// mainKey is input keying material used to derive sub keys.
//
// hkdfAlg is a MAC algorithm name, e.g., HmacSha256, used for the HKDF key
// derivation.
//
// keySizeInBytes is the key size of the sub keys.
//
// tagAlg is the MAC algorithm name, e.g. HmacSha256, used for generating per
// segment tags.
//
// tagSizeInBytes is the size of the per segment tags.
//
// ciphertextSegmentSize is the size of ciphertext segments.
//
// firstSegmentOffset is the offset of the first ciphertext segment.
func NewAESCTRHMAC(mainKey []byte, hkdfAlg string, keySizeInBytes int, tagAlg string, tagSizeInBytes, ciphertextSegmentSize, firstSegmentOffset int) (*AESCTRHMAC, error) {
	if len(mainKey) < 16 || len(mainKey) < keySizeInBytes {
		return nil, errors.New("mainKey too short")
	}
	if err := subtleaead.ValidateAESKeySize(uint32(keySizeInBytes)); err != nil {
		return nil, err
	}
	if tagSizeInBytes < 10 {
		return nil, errors.New("tag size too small")
	}
	digestSize, err := subtle.GetHashDigestSize(tagAlg)
	if err != nil {
		return nil, err
	}
	if uint32(tagSizeInBytes) > digestSize {
		return nil, errors.New("tag size too big")
	}
	if firstSegmentOffset < 0 {
		return nil, errors.New("firstSegmentOffset must not be negative")
	}
	headerLen := 1 + keySizeInBytes + AESCTRHMACNoncePrefixSizeInBytes
	if ciphertextSegmentSize <= firstSegmentOffset+headerLen+tagSizeInBytes {
		return nil, errors.New("ciphertextSegmentSize too small")
	}

	keyClone := make([]byte, len(mainKey))
	copy(keyClone, mainKey)

	return &AESCTRHMAC{
		mainKey:                      keyClone,
		hkdfAlg:                      hkdfAlg,
		keySizeInBytes:               keySizeInBytes,
		tagAlg:                       tagAlg,
		tagSizeInBytes:               tagSizeInBytes,
		ciphertextSegmentSize:        ciphertextSegmentSize,
		firstCiphertextSegmentOffset: firstSegmentOffset + headerLen,
		plaintextSegmentSize:         ciphertextSegmentSize - tagSizeInBytes,
	}, nil
}

// HeaderLength returns the length of the encryption header.
func (a *AESCTRHMAC) HeaderLength() int {
	return 1 + a.keySizeInBytes + AESCTRHMACNoncePrefixSizeInBytes
}

// deriveKeys returns an AES of size a.keySizeInBytes and an HMAC key of size AESCTRHMACKeySizeInBytes.
//
// They are derived from the main key using salt and aad as parameters.
func (a *AESCTRHMAC) deriveKeys(salt, aad []byte) ([]byte, []byte, error) {
	keyMaterialSize := a.keySizeInBytes + AESCTRHMACKeySizeInBytes
	km, err := subtle.ComputeHKDF(a.hkdfAlg, a.mainKey, salt, aad, uint32(keyMaterialSize))
	if err != nil {
		return nil, nil, err
	}
	aesKey := km[:a.keySizeInBytes]
	hmacKey := km[a.keySizeInBytes:]
	return aesKey, hmacKey, nil
}

type aesCTRHMACSegmentEncrypter struct {
	blockCipher    cipher.Block
	mac            hash.Hash
	tagSizeInBytes int
}

func (e aesCTRHMACSegmentEncrypter) EncryptSegment(segment, nonce []byte) ([]byte, error) {
	return e.EncryptSegmentWithDst(nil, segment, nonce)
}

// Implements the noncebased.segmentEncrypterWithDst interface.
func (e aesCTRHMACSegmentEncrypter) EncryptSegmentWithDst(dst, segment, nonce []byte) ([]byte, error) {
	sLen := len(segment)
	ctLen := sLen + e.tagSizeInBytes
	if len(dst) != 0 {
		return nil, errors.New("dst must be empty")
	}
	var ciphertext []byte
	if cap(dst) < ctLen {
		ciphertext = make([]byte, ctLen)
	} else {
		ciphertext = dst[:ctLen]
	}

	stream := cipher.NewCTR(e.blockCipher, nonce)
	stream.XORKeyStream(ciphertext, segment)

	e.mac.Reset()
	e.mac.Write(nonce)
	e.mac.Write(ciphertext[:sLen])
	tag := e.mac.Sum(nil)[:e.tagSizeInBytes]
	copy(ciphertext[sLen:], tag)
	return ciphertext, nil
}

// aesCTRHMACWriter works as a wrapper around underlying io.Writer, which is
// responsible for encrypting written data. The data is encrypted and flushed
// in segments of a given size.  Once all the data is written aesCTRHMACWriter
// must be closed.
type aesCTRHMACWriter struct {
	*noncebased.Writer
}

// NewEncryptingWriter returns a wrapper around underlying io.Writer, such that
// any write-operation via the wrapper results in AEAD-encryption of the
// written data, using aad as associated authenticated data. The associated
// data is not included in the ciphertext and has to be passed in as parameter
// for decryption.
func (a *AESCTRHMAC) NewEncryptingWriter(w io.Writer, aad []byte) (io.WriteCloser, error) {
	salt := random.GetRandomBytes(uint32(a.keySizeInBytes))
	noncePrefix := random.GetRandomBytes(AESCTRHMACNoncePrefixSizeInBytes)

	aesKey, hmacKey, err := a.deriveKeys(salt, aad)
	if err != nil {
		return nil, err
	}

	blockCipher, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, err
	}

	header := make([]byte, a.HeaderLength())
	header[0] = byte(a.HeaderLength())
	copy(header[1:], salt)
	copy(header[1+len(salt):], noncePrefix)
	if _, err := w.Write(header); err != nil {
		return nil, err
	}

	nw, err := noncebased.NewWriter(noncebased.WriterParams{
		W: w,
		SegmentEncrypter: aesCTRHMACSegmentEncrypter{
			blockCipher:    blockCipher,
			mac:            hmac.New(subtle.GetHashFunc(a.tagAlg), hmacKey),
			tagSizeInBytes: a.tagSizeInBytes,
		},
		NonceSize:                    AESCTRHMACNonceSizeInBytes,
		NoncePrefix:                  noncePrefix,
		PlaintextSegmentSize:         a.plaintextSegmentSize,
		FirstCiphertextSegmentOffset: a.firstCiphertextSegmentOffset,
	})
	if err != nil {
		return nil, err
	}
	return &aesCTRHMACWriter{Writer: nw}, nil
}

type aesCTRHMACSegmentDecrypter struct {
	blockCipher    cipher.Block
	mac            hash.Hash
	tagSizeInBytes int
}

func (d aesCTRHMACSegmentDecrypter) DecryptSegment(segment, nonce []byte) ([]byte, error) {
	return d.DecryptSegmentWithDst(nil, segment, nonce)
}

// Implements the noncebased.segmentDecrypterWithDst interface.
func (d aesCTRHMACSegmentDecrypter) DecryptSegmentWithDst(dst, segment, nonce []byte) ([]byte, error) {
	plaintextLen := len(segment) - d.tagSizeInBytes
	if plaintextLen < 0 {
		return nil, errors.New("segment too short")
	}
	if len(dst) != 0 {
		return nil, errors.New("dst must be empty")
	}
	var result []byte
	if cap(dst) < plaintextLen {
		result = make([]byte, plaintextLen)
	} else {
		result = dst[:plaintextLen]
	}
	tag := segment[plaintextLen:]

	d.mac.Reset()
	d.mac.Write(nonce)
	d.mac.Write(segment[:plaintextLen])
	wantTag := d.mac.Sum(nil)[:d.tagSizeInBytes]
	if !hmac.Equal(tag, wantTag) {
		return nil, errors.New("tag mismatch")
	}

	stream := cipher.NewCTR(d.blockCipher, nonce)
	stream.XORKeyStream(result, segment[:plaintextLen])
	return result, nil
}

// aesCTRHMACReader works as a wrapper around underlying io.Reader.
type aesCTRHMACReader struct {
	*noncebased.Reader
}

// NewDecryptingReader returns a wrapper around underlying io.Reader, such that
// any read-operation via the wrapper results in AEAD-decryption of the
// underlying ciphertext, using aad as associated authenticated data.
func (a *AESCTRHMAC) NewDecryptingReader(r io.Reader, aad []byte) (io.Reader, error) {
	hlen := make([]byte, 1)
	if _, err := io.ReadFull(r, hlen); err != nil {
		return nil, err
	}
	if hlen[0] != byte(a.HeaderLength()) {
		return nil, errors.New("invalid header length")
	}

	salt := make([]byte, a.keySizeInBytes)
	if _, err := io.ReadFull(r, salt); err != nil {
		return nil, fmt.Errorf("cannot read salt: %v", err)
	}

	noncePrefix := make([]byte, AESCTRHMACNoncePrefixSizeInBytes)
	if _, err := io.ReadFull(r, noncePrefix); err != nil {
		return nil, fmt.Errorf("cannot read noncePrefix: %v", err)
	}

	aesKey, hmacKey, err := a.deriveKeys(salt, aad)
	if err != nil {
		return nil, err
	}

	blockCipher, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, err
	}

	nr, err := noncebased.NewReader(noncebased.ReaderParams{
		R: r,
		SegmentDecrypter: aesCTRHMACSegmentDecrypter{
			blockCipher:    blockCipher,
			mac:            hmac.New(subtle.GetHashFunc(a.tagAlg), hmacKey),
			tagSizeInBytes: a.tagSizeInBytes,
		},
		NonceSize:                    AESCTRHMACNonceSizeInBytes,
		NoncePrefix:                  noncePrefix,
		CiphertextSegmentSize:        a.ciphertextSegmentSize,
		FirstCiphertextSegmentOffset: a.firstCiphertextSegmentOffset,
	})
	if err != nil {
		return nil, err
	}

	return &aesCTRHMACReader{Reader: nr}, nil
}
