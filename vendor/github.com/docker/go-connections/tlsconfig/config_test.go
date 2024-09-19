package tlsconfig

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"reflect"
	"runtime"
	"testing"
)

// This is the currently active Amazon Root CA 1 (CN=Amazon Root CA 1,O=Amazon,C=US),
// downloaded from: https://www.amazontrust.com/repository/AmazonRootCA1.pem
// It's valid since May 26 00:00:00 2015 GMT and expires on Jan 17 00:00:00 2038 GMT.
// Download updated versions from https://www.amazontrust.com/repository/
const (
	systemRootTrustedCert = `
-----BEGIN CERTIFICATE-----
MIIDQTCCAimgAwIBAgITBmyfz5m/jAo54vB4ikPmljZbyjANBgkqhkiG9w0BAQsF
ADA5MQswCQYDVQQGEwJVUzEPMA0GA1UEChMGQW1hem9uMRkwFwYDVQQDExBBbWF6
b24gUm9vdCBDQSAxMB4XDTE1MDUyNjAwMDAwMFoXDTM4MDExNzAwMDAwMFowOTEL
MAkGA1UEBhMCVVMxDzANBgNVBAoTBkFtYXpvbjEZMBcGA1UEAxMQQW1hem9uIFJv
b3QgQ0EgMTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALJ4gHHKeNXj
ca9HgFB0fW7Y14h29Jlo91ghYPl0hAEvrAIthtOgQ3pOsqTQNroBvo3bSMgHFzZM
9O6II8c+6zf1tRn4SWiw3te5djgdYZ6k/oI2peVKVuRF4fn9tBb6dNqcmzU5L/qw
IFAGbHrQgLKm+a/sRxmPUDgH3KKHOVj4utWp+UhnMJbulHheb4mjUcAwhmahRWa6
VOujw5H5SNz/0egwLX0tdHA114gk957EWW67c4cX8jJGKLhD+rcdqsq08p8kDi1L
93FcXmn/6pUCyziKrlA4b9v7LWIbxcceVOF34GfID5yHI9Y/QCB/IIDEgEw+OyQm
jgSubJrIqg0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8EBAMC
AYYwHQYDVR0OBBYEFIQYzIU07LwMlJQuCFmcx7IQTgoIMA0GCSqGSIb3DQEBCwUA
A4IBAQCY8jdaQZChGsV2USggNiMOruYou6r4lK5IpDB/G/wkjUu0yKGX9rbxenDI
U5PMCCjjmCXPI6T53iHTfIUJrU6adTrCC2qJeHZERxhlbI1Bjjt/msv0tadQ1wUs
N+gDS63pYaACbvXy8MWy7Vu33PqUXHeeE6V/Uq2V8viTO96LXFvKWlJbYK8U90vv
o/ufQJVtMVT8QtPHRh8jrdkPSHCa2XV4cdFyQzR1bldZwgJcJmApzyMZFo6IQ6XU
5MsI+yMRQ+hDKXJioaldXgjUkK642M4UwtBV8ob2xJNDd2ZhwLnoQdeXeGADbkpy
rqXRfboQnoZsG4q5WTP468SQvvG5
-----END CERTIFICATE-----
`
	rsaPrivateKeyFile             = "fixtures/key.pem"
	certificateFile               = "fixtures/cert.pem"
	multiCertificateFile          = "fixtures/multi.pem"
	rsaEncryptedPrivateKeyFile    = "fixtures/encrypted_key.pem"
	certificateOfEncryptedKeyFile = "fixtures/cert_of_encrypted_key.pem"
)

// returns the name of a pre-generated, multiple-certificate CA file
// with both RSA and ECDSA certs.
func getMultiCert() string {
	return multiCertificateFile
}

// returns the names of pre-generated key and certificate files.
func getCertAndKey() (string, string) {
	return rsaPrivateKeyFile, certificateFile
}

// returns the names of pre-generated, encrypted private key and
// corresponding certificate file
func getCertAndEncryptedKey() (string, string) {
	return rsaEncryptedPrivateKeyFile, certificateOfEncryptedKeyFile
}

// If the cert files and directory are provided but are invalid, an error is
// returned.
func TestConfigServerTLSFailsIfUnableToLoadCerts(t *testing.T) {
	key, cert := getCertAndKey()
	ca := getMultiCert()

	tempFile, err := os.CreateTemp("", "cert-test")
	if err != nil {
		t.Fatal("Unable to create temporary empty file")
	}
	defer os.RemoveAll(tempFile.Name())
	tempFile.Close()

	for _, badFile := range []string{"not-a-file", tempFile.Name()} {
		for i := 0; i < 3; i++ {
			files := []string{cert, key, ca}
			files[i] = badFile

			result, err := Server(Options{
				CertFile:   files[0],
				KeyFile:    files[1],
				CAFile:     files[2],
				ClientAuth: tls.VerifyClientCertIfGiven,
			})
			if err == nil || result != nil {
				t.Fatal("Expected a non-real file to error and return a nil TLS config")
			}
		}
	}
}

// If server cert and key are provided and client auth and client CA are not
// set, a tls config with only the server certs will be returned.
func TestConfigServerTLSServerCertsOnly(t *testing.T) {
	key, cert := getCertAndKey()

	keypair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		t.Fatal("Unable to load the generated cert and key")
	}

	tlsConfig, err := Server(Options{
		CertFile: cert,
		KeyFile:  key,
	})
	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure server TLS", err)
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Fatal("Unexpected server certificates")
	}
	if len(tlsConfig.Certificates[0].Certificate) != len(keypair.Certificate) {
		t.Fatal("Unexpected server certificates")
	}
	for i, cert := range tlsConfig.Certificates[0].Certificate {
		if !bytes.Equal(cert, keypair.Certificate[i]) {
			t.Fatal("Unexpected server certificates")
		}
	}

	if !reflect.DeepEqual(tlsConfig.CipherSuites, DefaultServerAcceptedCiphers) {
		t.Fatal("Unexpected server cipher suites")
	}
	if !tlsConfig.PreferServerCipherSuites { //nolint:staticcheck // Ignore SA1019: tlsConfig.PreferServerCipherSuites has been deprecated since Go 1.18: PreferServerCipherSuites is ignored.
		t.Fatal("Expected server to prefer cipher suites")
	}
	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Fatal("Unexpected server TLS version")
	}
}

// If client CA is provided, it will only be used if the client auth is >=
// VerifyClientCertIfGiven
func TestConfigServerTLSClientCANotSetIfClientAuthTooLow(t *testing.T) {
	key, cert := getCertAndKey()
	ca := getMultiCert()

	tlsConfig, err := Server(Options{
		CertFile:   cert,
		KeyFile:    key,
		ClientAuth: tls.RequestClientCert,
		CAFile:     ca,
	})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure server TLS", err)
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Fatal("Unexpected server certificates")
	}
	if tlsConfig.ClientAuth != tls.RequestClientCert {
		t.Fatal("ClientAuth was not set to what was in the options")
	}
	if tlsConfig.ClientCAs != nil { //nolint:staticcheck // Ignore SA1019: tlsConfig.ClientCAs.Subjects has been deprecated since Go 1.18: if s was returned by SystemCertPool, Subjects will not include the system roots.
		t.Fatalf("Client CAs should never have been set")
	}
}

// If client CA is provided, it will only be used if the client auth is >=
// VerifyClientCertIfGiven
func TestConfigServerTLSClientCASet(t *testing.T) {
	key, cert := getCertAndKey()
	ca := getMultiCert()

	tlsConfig, err := Server(Options{
		CertFile:   cert,
		KeyFile:    key,
		ClientAuth: tls.VerifyClientCertIfGiven,
		CAFile:     ca,
	})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure server TLS", err)
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Fatal("Unexpected server certificates")
	}
	if tlsConfig.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Fatal("ClientAuth was not set to what was in the options")
	}
	basePool, err := SystemCertPool()
	if err != nil {
		basePool = x509.NewCertPool()
	}
	// because we are not enabling `ExclusiveRootPools`, any root pool will also contain the system roots
	if tlsConfig.ClientCAs == nil || len(tlsConfig.ClientCAs.Subjects()) != len(basePool.Subjects())+2 { //nolint:staticcheck // Ignore SA1019: tlsConfig.ClientCAs.Subjects has been deprecated since Go 1.18: if s was returned by SystemCertPool, Subjects will not include the system roots.
		t.Fatalf("Client CAs were never set correctly")
	}
}

// Exclusive root pools determines whether the CA pool will be a union of the system
// certificate pool and custom certs, or an exclusive or of the custom certs and system pool
func TestConfigServerExclusiveRootPools(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		// FIXME: see https://github.com/docker/go-connections/issues/105.
		t.Skip("FIXME: failing on Windows and darwin")
	}
	key, cert := getCertAndKey()
	ca := getMultiCert()

	caBytes, err := os.ReadFile(ca)
	if err != nil {
		t.Fatal("Unable to read CA certs", err)
	}

	var testCerts []*x509.Certificate
	for _, pemBytes := range [][]byte{caBytes, []byte(systemRootTrustedCert)} {
		pemBlock, _ := pem.Decode(pemBytes)
		if pemBlock == nil {
			t.Fatal("Malformed certificate")
		}
		cert, err := x509.ParseCertificate(pemBlock.Bytes)
		if err != nil {
			t.Fatal("Unable to parse certificate")
		}
		testCerts = append(testCerts, cert)
	}

	// ExclusiveRootPools not set, so should be able to verify both system-signed certs
	// and custom CA-signed certs
	tlsConfig, err := Server(Options{
		CertFile:   cert,
		KeyFile:    key,
		ClientAuth: tls.VerifyClientCertIfGiven,
		CAFile:     ca,
	})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure server TLS", err)
	}

	for i, cert := range testCerts {
		if _, err := cert.Verify(x509.VerifyOptions{Roots: tlsConfig.ClientCAs}); err != nil {
			t.Fatalf("Unable to verify certificate %d: %v", i, err)
		}
	}

	// ExclusiveRootPools set and custom CA provided, so system certs should not be verifiable
	// and custom CA-signed certs should be verifiable
	tlsConfig, err = Server(Options{
		CertFile:           cert,
		KeyFile:            key,
		ClientAuth:         tls.VerifyClientCertIfGiven,
		CAFile:             ca,
		ExclusiveRootPools: true,
	})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure server TLS", err)
	}

	for i, cert := range testCerts {
		_, err := cert.Verify(x509.VerifyOptions{Roots: tlsConfig.ClientCAs})
		switch {
		case i == 0 && err != nil:
			t.Fatal("Unable to verify custom certificate, even though the root pool should have only the custom CA", err)
		case i == 1 && err == nil:
			t.Fatal("Successfully verified system root-signed certificate though the root pool should have only the cusotm CA", err)
		}
	}

	// No CA file provided, system cert should be verifiable only
	tlsConfig, err = Server(Options{
		CertFile: cert,
		KeyFile:  key,
	})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure server TLS", err)
	}

	for i, cert := range testCerts {
		_, err := cert.Verify(x509.VerifyOptions{Roots: tlsConfig.ClientCAs})
		switch {
		case i == 1 && err != nil:
			t.Fatal("Unable to verify system root-signed certificate, even though the root pool should be the system pool only", err)
		case i == 0 && err == nil:
			t.Fatal("Successfully verified custom certificate though the root pool should be the system pool only", err)
		}
	}
}

// If we provide a modifier to the server's default TLS configuration generator, it
// should be applied accordingly
func TestConfigServerDefaultWithTLSMinimumModifier(t *testing.T) {
	tlsVersions := []uint16{
		tls.VersionTLS11,
		tls.VersionTLS12,
	}

	for _, tlsVersion := range tlsVersions {
		servDefault := ServerDefault(func(c *tls.Config) {
			c.MinVersion = tlsVersion
		})

		if servDefault.MinVersion != tlsVersion {
			t.Fatalf("Unexpected min TLS version for default server TLS config: %d", servDefault.MinVersion)
		}
	}
}

// If we provide a modifier to the client's default TLS configuration generator, it
// should be applied accordingly
func TestConfigClientDefaultWithTLSMinimumModifier(t *testing.T) {
	tlsVersions := []uint16{
		tls.VersionTLS11,
		tls.VersionTLS12,
	}

	for _, tlsVersion := range tlsVersions {
		clientDefault := ClientDefault(func(c *tls.Config) {
			c.MinVersion = tlsVersion
		})

		if clientDefault.MinVersion != tlsVersion {
			t.Fatalf("Unexpected min TLS version for default client TLS config: %d", clientDefault.MinVersion)
		}
	}
}

// If a valid minimum version is specified in the options, the server's
// minimum version should be set accordingly
func TestConfigServerTLSMinVersionIsSetBasedOnOptions(t *testing.T) {
	versions := []uint16{
		tls.VersionTLS12,
	}
	key, cert := getCertAndKey()

	for _, v := range versions {
		tlsConfig, err := Server(Options{
			MinVersion: v,
			CertFile:   cert,
			KeyFile:    key,
		})

		if err != nil || tlsConfig == nil {
			t.Fatal("Unable to configure server TLS", err)
		}

		if tlsConfig.MinVersion != v {
			t.Fatal("Unexpected minimum TLS version: ", tlsConfig.MinVersion)
		}
	}
}

// An error should be returned if the specified minimum version for the server
// is too low, i.e. less than VersionTLS10
func TestConfigServerTLSMinVersionNotSetIfMinVersionIsTooLow(t *testing.T) {
	key, cert := getCertAndKey()

	_, err := Server(Options{
		MinVersion: tls.VersionTLS10,
		CertFile:   cert,
		KeyFile:    key,
	})

	if err == nil {
		t.Fatal("Should have returned an error for minimum version below TLS10")
	}
}

// An error should be returned if an invalid minimum version for the server is
// in the options struct
func TestConfigServerTLSMinVersionNotSetIfMinVersionIsInvalid(t *testing.T) {
	key, cert := getCertAndKey()

	_, err := Server(Options{
		MinVersion: 1,
		CertFile:   cert,
		KeyFile:    key,
	})

	if err == nil {
		t.Fatal("Should have returned error on invalid minimum version option")
	}
}

// The root CA is never set if InsecureSkipBoolean is set to true, but the
// default client options are set
func TestConfigClientTLSNoVerify(t *testing.T) {
	ca := getMultiCert()

	tlsConfig, err := Client(Options{CAFile: ca, InsecureSkipVerify: true})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure client TLS", err)
	}

	if tlsConfig.RootCAs != nil { //nolint:staticcheck // Ignore SA1019: tlsConfig.RootCAs.Subjects has been deprecated since Go 1.18: if s was returned by SystemCertPool, Subjects will not include the system roots.
		t.Fatal("Should not have set Root CAs", err)
	}

	if !reflect.DeepEqual(tlsConfig.CipherSuites, clientCipherSuites) {
		t.Fatal("Unexpected client cipher suites")
	}
	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Fatal("Unexpected client TLS version")
	}

	if tlsConfig.Certificates != nil {
		t.Fatal("Somehow client certificates were set")
	}
}

// The root CA is never set if InsecureSkipBoolean is set to false and root CA
// is not provided.
func TestConfigClientTLSNoRoot(t *testing.T) {
	tlsConfig, err := Client(Options{})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure client TLS", err)
	}

	if tlsConfig.RootCAs != nil {
		t.Fatal("Should not have set Root CAs", err)
	}

	if !reflect.DeepEqual(tlsConfig.CipherSuites, clientCipherSuites) {
		t.Fatal("Unexpected client cipher suites")
	}
	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Fatal("Unexpected client TLS version")
	}

	if tlsConfig.Certificates != nil {
		t.Fatal("Somehow client certificates were set")
	}
}

// The RootCA is set if the file is provided and InsecureSkipVerify is false
func TestConfigClientTLSRootCAFileWithOneCert(t *testing.T) {
	ca := getMultiCert()

	tlsConfig, err := Client(Options{CAFile: ca})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure client TLS", err)
	}
	basePool, err := SystemCertPool()
	if err != nil {
		basePool = x509.NewCertPool()
	}
	// because we are not enabling `ExclusiveRootPools`, any root pool will also contain the system roots
	if tlsConfig.RootCAs == nil || len(tlsConfig.RootCAs.Subjects()) != len(basePool.Subjects())+2 { //nolint:staticcheck // Ignore SA1019: tlsConfig.ClientCAs.Subjects has been deprecated since Go 1.18: if s was returned by SystemCertPool, Subjects will not include the system roots.
		t.Fatal("Root CAs not set properly", err)
	}
	if tlsConfig.Certificates != nil {
		t.Fatal("Somehow client certificates were set")
	}
}

// An error is returned if a root CA is provided but the file doesn't exist.
func TestConfigClientTLSNonexistentRootCAFile(t *testing.T) {
	tlsConfig, err := Client(Options{CAFile: "nonexistent"})

	if err == nil || tlsConfig != nil {
		t.Fatal("Should not have been able to configure client TLS", err)
	}
}

// An error is returned if either the client cert or the key are provided
// but invalid or blank.
func TestConfigClientTLSClientCertOrKeyInvalid(t *testing.T) {
	key, cert := getCertAndKey()

	tempFile, err := os.CreateTemp("", "cert-test")
	if err != nil {
		t.Fatal("Unable to create temporary empty file")
	}
	defer os.Remove(tempFile.Name())
	_ = tempFile.Close()

	for i := 0; i < 2; i++ {
		for _, invalid := range []string{"not-a-file", "", tempFile.Name()} {
			files := []string{cert, key}
			files[i] = invalid

			tlsConfig, err := Client(Options{CertFile: files[0], KeyFile: files[1]})
			if err == nil || tlsConfig != nil {
				t.Fatal("Should not have been able to configure client TLS", err)
			}
		}
	}
}

// The certificate is set if the client cert and client key are provided and
// valid.
func TestConfigClientTLSValidClientCertAndKey(t *testing.T) {
	key, cert := getCertAndKey()

	keypair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		t.Fatal("Unable to load the generated cert and key")
	}

	tlsConfig, err := Client(Options{CertFile: cert, KeyFile: key})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure client TLS", err)
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Fatal("Unexpected client certificates")
	}
	if len(tlsConfig.Certificates[0].Certificate) != len(keypair.Certificate) {
		t.Fatal("Unexpected client certificates")
	}
	for i, cert := range tlsConfig.Certificates[0].Certificate {
		if !bytes.Equal(cert, keypair.Certificate[i]) {
			t.Fatal("Unexpected client certificates")
		}
	}

	if tlsConfig.RootCAs != nil {
		t.Fatal("Root CAs should not have been set", err)
	}
}

// The certificate is set if the client cert and encrypted client key are
// provided and valid and passphrase can decrypt the key
func TestConfigClientTLSValidClientCertAndEncryptedKey(t *testing.T) {
	key, cert := getCertAndEncryptedKey()

	tlsConfig, err := Client(Options{
		CertFile:   cert,
		KeyFile:    key,
		Passphrase: "FooBar123",
	})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure client TLS", err)
	}

	if len(tlsConfig.Certificates) != 1 {
		t.Fatal("Unexpected client certificates")
	}
}

// The certificate is not set if the provided passphrase cannot decrypt
// the encrypted key.
func TestConfigClientTLSNotSetWithInvalidPassphrase(t *testing.T) {
	key, cert := getCertAndEncryptedKey()

	tlsConfig, err := Client(Options{
		CertFile:   cert,
		KeyFile:    key,
		Passphrase: "InvalidPassphrase",
	})

	if !IsErrEncryptedKey(err) || tlsConfig != nil {
		t.Fatal("Expected failure due to incorrect passphrase.")
	}
}

// Exclusive root pools determines whether the CA pool will be a union of the system
// certificate pool and custom certs, or an exclusive or of the custom certs and system pool
func TestConfigClientExclusiveRootPools(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		// FIXME: see https://github.com/docker/go-connections/issues/105.
		t.Skip("FIXME: failing on Windows and darwin")
	}
	ca := getMultiCert()

	caBytes, err := os.ReadFile(ca)
	if err != nil {
		t.Fatal("Unable to read CA certs", err)
	}

	var testCerts []*x509.Certificate
	for _, pemBytes := range [][]byte{caBytes, []byte(systemRootTrustedCert)} {
		pemBlock, _ := pem.Decode(pemBytes)
		if pemBlock == nil {
			t.Fatal("Malformed certificate")
		}
		cert, err := x509.ParseCertificate(pemBlock.Bytes)
		if err != nil {
			t.Fatal("Unable to parse certificate")
		}
		testCerts = append(testCerts, cert)
	}

	// ExclusiveRootPools not set, so should be able to verify both system-signed certs
	// and custom CA-signed certs
	tlsConfig, err := Client(Options{CAFile: ca})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure client TLS", err)
	}

	for i, cert := range testCerts {
		if _, err := cert.Verify(x509.VerifyOptions{Roots: tlsConfig.RootCAs}); err != nil {
			t.Fatalf("Unable to verify certificate %d: %v", i, err)
		}
	}

	// ExclusiveRootPools set and custom CA provided, so system certs should not be verifiable
	// and custom CA-signed certs should be verifiable
	tlsConfig, err = Client(Options{
		CAFile:             ca,
		ExclusiveRootPools: true,
	})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure client TLS", err)
	}

	for i, cert := range testCerts {
		_, err := cert.Verify(x509.VerifyOptions{Roots: tlsConfig.RootCAs})
		switch {
		case i == 0 && err != nil:
			t.Fatal("Unable to verify custom certificate, even though the root pool should have only the custom CA", err)
		case i == 1 && err == nil:
			t.Fatal("Successfully verified system root-signed certificate though the root pool should have only the cusotm CA", err)
		}
	}

	// No CA file provided, system cert should be verifiable only
	tlsConfig, err = Client(Options{})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure client TLS", err)
	}

	for i, cert := range testCerts {
		_, err := cert.Verify(x509.VerifyOptions{Roots: tlsConfig.RootCAs})
		switch {
		case i == 1 && err != nil:
			t.Fatal("Unable to verify system root-signed certificate, even though the root pool should be the system pool only", err)
		case i == 0 && err == nil:
			t.Fatal("Successfully verified custom certificate though the root pool should be the system pool only", err)
		}
	}
}

// If a valid MinVersion is specified in the options, the client's
// minimum version should be set accordingly
func TestConfigClientTLSMinVersionIsSetBasedOnOptions(t *testing.T) {
	key, cert := getCertAndKey()

	tlsConfig, err := Client(Options{
		MinVersion: tls.VersionTLS12,
		CertFile:   cert,
		KeyFile:    key,
	})

	if err != nil || tlsConfig == nil {
		t.Fatal("Unable to configure client TLS", err)
	}

	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Fatal("Unexpected minimum TLS version: ", tlsConfig.MinVersion)
	}
}

// An error should be returned if the specified minimum version for the client
// is too low, i.e. less than VersionTLS12
func TestConfigClientTLSMinVersionNotSetIfMinVersionIsTooLow(t *testing.T) {
	key, cert := getCertAndKey()

	_, err := Client(Options{
		MinVersion: tls.VersionTLS11,
		CertFile:   cert,
		KeyFile:    key,
	})

	if err == nil {
		t.Fatal("Should have returned an error for minimum version below TLS12")
	}
}

// An error should be returned if an invalid minimum version for the client is
// in the options struct
func TestConfigClientTLSMinVersionNotSetIfMinVersionIsInvalid(t *testing.T) {
	key, cert := getCertAndKey()

	_, err := Client(Options{
		MinVersion: 1,
		CertFile:   cert,
		KeyFile:    key,
	})

	if err == nil {
		t.Fatal("Should have returned error on invalid minimum version option")
	}
}
