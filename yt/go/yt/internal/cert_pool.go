package internal

import "crypto/x509"

func NewCertPool() (*x509.CertPool, error) {
	return x509.SystemCertPool()
}
