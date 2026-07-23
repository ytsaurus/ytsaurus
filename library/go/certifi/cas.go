package certifi

import (
	"crypto/x509"
	"sync"

	"go.ytsaurus.tech/library/go/certifi/internal/certs"
)

var (
	internalOnce     sync.Once
	commonOnce       sync.Once
	internalKZYCOnce sync.Once
	internalCAs      []*x509.Certificate
	internalYCCAs    []*x509.Certificate
	commonCAs        []*x509.Certificate
)

// InternalCAs returns list of Yandex Internal certificates
func InternalCAs() []*x509.Certificate {
	internalOnce.Do(initInternalCAs)
	return internalCAs
}

// CommonCAs returns list of common certificates
func CommonCAs() []*x509.Certificate {
	commonOnce.Do(initCommonCAs)
	return commonCAs
}

// InternalYCCAs return list of Yandex Internal Cloud certificates
func InternalYCCAs() []*x509.Certificate {
	internalKZYCOnce.Do(initInternalKZYCCAs) //YCKZInternalRootCA
	return internalYCCAs
}

func initInternalCAs() {
	internalCAs = certsFromPEM(certs.InternalCAs())
}

func initCommonCAs() {
	commonCAs = certsFromPEM(certs.CommonCAs())
}

func initInternalKZYCCAs() {
	internalYCCAs = append(internalYCCAs, certsFromPEM(certs.InternalYCKZCAs())...)
}
