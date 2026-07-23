package certifi

import (
	"crypto/x509"
	"os"
)

var underYaMake = true

type CertProvider func() []*x509.Certificate

// NewCertPool returns a copy of the system or bundled cert pool.
//
// Default behavior can be modified with env variable, e.g. use system pool:
//
//	CERTIFI_USE_SYSTEM_CA=yes ./my-cool-program
func NewCertPool() (caCertPool *x509.CertPool, err error) {
	if forceSystem() {
		return NewCertPoolSystem()
	}

	return NewCertPoolBundled()
}

// BuildCertPool constructs a pool of certificates
func BuildCertPool(providers ...CertProvider) (caCertPool *x509.CertPool, err error) {
	caCertPool = x509.NewCertPool()

	for _, certProvider := range providers {
		for _, cert := range certProvider() {
			caCertPool.AddCert(cert)
		}
	}

	return caCertPool, nil
}

// BuildCertPoolSystem constructs a pool of certificates with system certs
func BuildCertPoolSystem(providers ...CertProvider) (caCertPool *x509.CertPool, err error) {
	caCertPool, err = x509.SystemCertPool()
	if err != nil || caCertPool == nil {
		caCertPool = x509.NewCertPool()
	}

	for _, certProvider := range providers {
		for _, cert := range certProvider() {
			caCertPool.AddCert(cert)
		}
	}

	return caCertPool, nil
}

// NewCertPoolSystem returns a copy of the system cert pool + common CAs + internal CAs
//
// WARNING: system cert pool is not available on Windows
func NewCertPoolSystem() (caCertPool *x509.CertPool, err error) {
	return BuildCertPoolSystem(CommonCAs, InternalCAs)
}

// NewCertPoolBundled returns a new cert pool with common CAs + internal CAs
func NewCertPoolBundled() (caCertPool *x509.CertPool, err error) {
	return BuildCertPool(CommonCAs, InternalCAs)
}

// NewCertPoolInternal returns a new cert pool with internal CAs
func NewCertPoolInternal() (caCertPool *x509.CertPool, err error) {
	return BuildCertPool(InternalCAs)
}

// NewCertPoolInternalYC returns a new cert pool with common CAs + internal CAs + internal YC CAs(example: YCKZInternalRootCA.crt)
func NewCertPoolInternalYC() (caCertPool *x509.CertPool, err error) {
	return BuildCertPoolSystem(CommonCAs, InternalCAs, InternalYCCAs)
}

func forceSystem() bool {
	if os.Getenv("CERTIFI_USE_SYSTEM_CA") == "yes" {
		return true
	}

	if !underYaMake && len(InternalCAs()) == 0 {
		return true
	}

	return false
}
