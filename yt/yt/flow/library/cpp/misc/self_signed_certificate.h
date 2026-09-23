#pragma once

#include <util/datetime/base.h>

#include <string>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TSelfSignedCertificateOptions
{
    //! Subject and issuer CN; at most 64 bytes.
    std::string CommonName;
    //! Subject alternative names of IP type.
    std::vector<std::string> IPAddresses;
    //! Subject alternative names of DNS type.
    std::vector<std::string> DnsNames;
    //! The certificate expires this long after generation.
    TDuration Validity = TDuration::Days(3650);
    //! The certificate is valid since this long before generation, to tolerate peer clock skew.
    TDuration ClockSkew = TDuration::Hours(1);
};

struct TSelfSignedCertificate
{
    //! PEM-encoded X.509 certificate.
    std::string CertificatePem;
    //! PEM-encoded PKCS#8 private key; must never be logged or persisted.
    std::string PrivateKeyPem;
    //! Uppercase hex SHA-256 of the DER-encoded certificate, as #NCrypto::GetFingerprintSHA256 renders it.
    std::string CertificateSha256;
};

//! Generates an ECDSA P-256 key pair and a self-signed TLS server certificate for it.
//! The certificate is a leaf (CA:FALSE, serverAuth) that peers can pin as their sole trust anchor.
TSelfSignedCertificate GenerateSelfSignedCertificate(const TSelfSignedCertificateOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
