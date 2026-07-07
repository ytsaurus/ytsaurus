package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"time"
)

//go:generate go run ${GOFILE}

var certTemplate = x509.Certificate{
	SerialNumber: big.NewInt(199999),
	Subject: pkix.Name{
		CommonName: "test",
	},
	NotBefore: time.Now().AddDate(-1, 1, 1),
	NotAfter:  time.Now().AddDate(1, 1, 1),

	KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
	ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning, x509.ExtKeyUsageAny},

	BasicConstraintsValid: true,
}

func generateCertificate(signer crypto.Signer, out io.Writer, isCA bool) error {
	template := certTemplate
	template.IsCA = isCA
	if isCA {
		template.KeyUsage = template.KeyUsage | x509.KeyUsageCertSign
		template.MaxPathLen = 1
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &certTemplate, signer.Public(), signer)
	if err != nil {
		return fmt.Errorf("unable to generate a certificate: %w", err)
	}

	if err = pem.Encode(out, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return fmt.Errorf("unable to write cert to file: %w", err)
	}

	return nil
}

// generates a multiple-certificate CA file with both RSA and ECDSA certs and
// returns the filename so that cleanup can be deferred.
func generateMultiCert() error {
	certOut, err := os.Create("multi.pem")
	if err != nil {
		return fmt.Errorf("unable to create file to write multi-cert to: %w", err)
	}
	defer func() { _ = certOut.Close() }()

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("unable to generate RSA key for multi-cert: %w", err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("unable to generate ECDSA key for multi-cert: %w", err)
	}

	for _, signer := range []crypto.Signer{rsaKey, ecKey} {
		if err := generateCertificate(signer, certOut, true); err != nil {
			return err
		}
	}

	return nil
}

func generateCertAndKey() error {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("unable to generate RSA key: %w", err)

	}
	keyBytes := x509.MarshalPKCS1PrivateKey(rsaKey)

	keyOut, err := os.Create("key.pem")
	if err != nil {
		return fmt.Errorf("unable to create file to write key to: %w", err)
	}
	defer func() { _ = keyOut.Close() }()

	if err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyBytes}); err != nil {
		return fmt.Errorf("unable to write key to file: %w", err)
	}

	certOut, err := os.Create("cert.pem")
	if err != nil {
		return fmt.Errorf("to create file to write cert to: %w", err)
	}
	defer func() { _ = certOut.Close() }()

	return generateCertificate(rsaKey, certOut, false)
}

func main() {
	if err := generateCertAndKey(); err != nil {
		log.Fatal(err)
	}
	if err := generateMultiCert(); err != nil {
		log.Fatal(err)
	}
}
