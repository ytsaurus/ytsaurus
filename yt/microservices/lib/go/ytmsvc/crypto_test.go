package ytmsvc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAESCryptor(t *testing.T) {
	secret := "secret"
	cryptor, err := NewAESCryptor(secret)
	require.NoError(t, err)
	require.NotNil(t, cryptor)

	t.Run("EncryptDecrypt", func(t *testing.T) {
		plaintext := []byte("foo bar")
		ciphertext, err := cryptor.Encrypt(plaintext)
		require.NoError(t, err)
		require.NotEqual(t, plaintext, ciphertext)

		decrypted, err := cryptor.Decrypt(ciphertext)
		require.NoError(t, err)
		require.Equal(t, plaintext, decrypted)
	})

	t.Run("DecryptWithWrongKey", func(t *testing.T) {
		plaintext := []byte("secret message")
		ciphertext, err := cryptor.Encrypt(plaintext)
		require.NoError(t, err)

		otherCryptor, err := NewAESCryptor("wrong-secret")
		require.NoError(t, err)

		_, err = otherCryptor.Decrypt(ciphertext)
		require.EqualError(t, err, "cipher: message authentication failed")
	})
}

type TestData struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func TestCryptoSerializer(t *testing.T) {
	secret := "secret"
	serializer, err := NewCryptoSerializer[TestData](secret)
	require.NoError(t, err)
	require.NotNil(t, serializer)

	t.Run("EncodeDecode", func(t *testing.T) {
		data := TestData{Name: "test", Value: 123}
		token, err := serializer.Encode(data)
		require.NoError(t, err)
		require.NotEmpty(t, token)

		decoded, err := serializer.Decode(token)
		require.NoError(t, err)
		require.Equal(t, data, decoded)
	})

	t.Run("DecodeInvalidBase64", func(t *testing.T) {
		_, err := serializer.Decode("invalid-base64-$$$")
		require.ErrorContains(t, err, "base64 decode")
	})

	t.Run("DecodeWithWrongKey", func(t *testing.T) {
		data := TestData{Name: "test", Value: 123}
		token, err := serializer.Encode(data)
		require.NoError(t, err)

		otherSerializer, err := NewCryptoSerializer[TestData]("wrong-secret")
		require.NoError(t, err)

		_, err = otherSerializer.Decode(token)
		require.EqualError(t, err, "decrypt: cipher: message authentication failed")
	})
}
