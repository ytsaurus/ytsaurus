package app

import (
	"crypto/rand"
	"encoding/hex"
)

func randHexString(length int) string {
	bytes := make([]byte, (length+1)/2)
	_, _ = rand.Read(bytes)
	hexStr := hex.EncodeToString(bytes)
	return hexStr[:length]
}
