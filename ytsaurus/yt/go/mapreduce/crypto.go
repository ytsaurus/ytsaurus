package mapreduce

import (
	"bytes"
	"errors"
	"os"

	"github.com/google/tink/go/aead"
	"github.com/google/tink/go/keyset"
)

type plaintextAEAD struct{}

func (*plaintextAEAD) Encrypt(plaintext, additionalData []byte) ([]byte, error) {
	return plaintext, nil
}

func (*plaintextAEAD) Decrypt(ciphertext, additionalData []byte) ([]byte, error) {
	return ciphertext, nil
}

func readJobState(ctx *jobContext) ([]byte, error) {
	keyJSON, ok := ctx.LookupVault("job_state_key")
	if !ok {
		return nil, errors.New("job state key is missing in vault")
	}
	keyReader := keyset.NewJSONReader(bytes.NewBufferString(keyJSON))
	handle, err := keyset.Read(keyReader, &plaintextAEAD{})
	if err != nil {
		return nil, err
	}

	cypher, err := aead.New(handle)
	if err != nil {
		return nil, err
	}

	ct, err := os.ReadFile("job-state")
	if err != nil {
		return nil, err
	}

	pt, err := cypher.Decrypt(ct, nil)
	if err != nil {
		return nil, err
	}

	return pt, err
}
