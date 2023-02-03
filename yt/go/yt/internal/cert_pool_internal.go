//go:build internal
// +build internal

package internal

import (
	"crypto/x509"

	"a.yandex-team.ru/library/go/certifi"
)

func NewCertPool() (*x509.CertPool, error) {
	return certifi.NewCertPool()
}
