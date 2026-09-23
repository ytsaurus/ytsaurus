package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"

	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/ytlog"
)

var (
	flagAddress = flag.String("address", "localhost:9013", "Address to listen on")
	flagUseTLS  = flag.Bool("tls", false, "Use TLS with a self-signed certificate")
)

// generateSelfSignedCertificate creates an ephemeral certificate suitable
// for clients connecting with InsecureSkipVerify.
func generateSelfSignedCertificate() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}, nil
}

func discoverProxies(ctx context.Context, req *bus.Request) (*bus.Response, error) {
	var request rpc_proxy.TReqDiscoverProxies
	if err := proto.Unmarshal(req.Body, &request); err != nil {
		return nil, err
	}

	body, err := proto.Marshal(&rpc_proxy.TRspDiscoverProxies{
		Addresses: []string{*flagAddress},
	})
	if err != nil {
		return nil, err
	}

	return &bus.Response{Body: body}, nil
}

func runServer() error {
	opts := []bus.ServerOption{
		bus.WithServerLogger(ytlog.Must()),
	}
	if *flagUseTLS {
		certificate, err := generateSelfSignedCertificate()
		if err != nil {
			return err
		}
		opts = append(opts,
			bus.WithServerEncryptionMode(bus.EncryptionModeRequired),
			bus.WithServerTLSConfig(&tls.Config{Certificates: []tls.Certificate{certificate}}))
	}

	server := bus.NewServer(opts...)
	server.RegisterMethod("DiscoveryService", "DiscoverProxies", discoverProxies)

	lsn, err := net.Listen("tcp", *flagAddress)
	if err != nil {
		return err
	}

	fmt.Printf("listening on %s\n", lsn.Addr())
	return server.Serve(lsn)
}

func main() {
	flag.Parse()

	if err := runServer(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "err: %+v\n", err)
		os.Exit(1)
	}
}
