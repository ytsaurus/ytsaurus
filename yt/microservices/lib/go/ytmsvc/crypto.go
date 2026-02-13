package ytmsvc

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type AESCryptor struct {
	gcm cipher.AEAD
}

func NewAESCryptor(secret string) (*AESCryptor, error) {
	key := sha256.Sum256([]byte(secret))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("aes error: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm error: %w", err)
	}
	return &AESCryptor{gcm: gcm}, nil
}

func (ac *AESCryptor) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, ac.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return ac.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func (ac *AESCryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	nonceSize := ac.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("invalid ciphertext length")
	}

	nonce, encryptedData := ciphertext[:nonceSize], ciphertext[nonceSize:]

	return ac.gcm.Open(nil, nonce, encryptedData, nil)
}

type CryptoSerializer[T any] struct {
	ac *AESCryptor
}

func NewCryptoSerializer[T any](secret string) (*CryptoSerializer[T], error) {
	ac, err := NewAESCryptor(secret)
	if err != nil {
		return nil, fmt.Errorf("error creating token manager: %w", err)
	}
	return &CryptoSerializer[T]{ac: ac}, nil
}

func (cs *CryptoSerializer[T]) Encode(item T) (string, error) {
	jsonBytes, err := json.Marshal(item)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	encryptedBytes, err := cs.ac.Encrypt(jsonBytes)
	if err != nil {
		return "", fmt.Errorf("encrypt: %w", err)
	}

	return base64.URLEncoding.EncodeToString(encryptedBytes), nil
}

func (cs *CryptoSerializer[T]) Decode(tokenString string) (T, error) {
	var result T

	encryptedBytes, err := base64.URLEncoding.DecodeString(tokenString)
	if err != nil {
		return result, fmt.Errorf("base64 decode: %w", err)
	}

	jsonBytes, err := cs.ac.Decrypt(encryptedBytes)
	if err != nil {
		return result, fmt.Errorf("decrypt: %w", err)
	}

	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		return result, fmt.Errorf("unmarshal: %w", err)
	}

	return result, nil
}
