#include "config.h"

#include <yt/yt/core/crypto/config.h>

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

TSslPemKeyCertPairConfig::TSslPemKeyCertPairConfig()
{
    RegisterParameter("private_key", PrivateKey)
        .Optional();
    RegisterParameter("cert_chain", CertChain)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TServerCredentialsConfig::TServerCredentialsConfig()
{
    RegisterParameter("pem_root_certs", PemRootCerts)
        .Optional();
    RegisterParameter("pem_key_cert_pairs", PemKeyCertPairs);
    RegisterParameter("client_certificate_request", ClientCertificateRequest)
        .Default(EClientCertificateRequest::RequestAndRequireClientCertificateAndVerify);
}

////////////////////////////////////////////////////////////////////////////////

TServerAddressConfig::TServerAddressConfig()
{
    RegisterParameter("address", Address);
    RegisterParameter("credentials", Credentials)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TServerConfig::TServerConfig()
{
    RegisterParameter("addresses", Addresses);
    RegisterParameter("grpc_arguments", GrpcArguments)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TChannelCredentialsConfig::TChannelCredentialsConfig()
{
    RegisterParameter("pem_root_certs", PemRootCerts)
        .Optional();
    RegisterParameter("pem_key_cert_pair", PemKeyCertPair)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TChannelConfig::TChannelConfig()
{
    RegisterParameter("address", Address);
    RegisterParameter("credentials", Credentials)
        .Optional();
    RegisterParameter("grpc_arguments", GrpcArguments)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
