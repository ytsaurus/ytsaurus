#include "config.h"

#include <yt/yt/core/crypto/config.h>

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

void TDispatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_count", &TThis::ThreadCount)
        .GreaterThan(0)
        .Default(4);
}

////////////////////////////////////////////////////////////////////////////////

void TSslPemKeyCertPairConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("private_key", &TThis::PrivateKey)
        .Optional();
    registrar.Parameter("cert_chain", &TThis::CertChain)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TServerCredentialsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pem_root_certs", &TThis::PemRootCerts)
        .Optional();
    registrar.Parameter("pem_key_cert_pairs", &TThis::PemKeyCertPairs);
    registrar.Parameter("client_certificate_request", &TThis::ClientCertificateRequest)
        .Default(EClientCertificateRequest::RequestAndRequireClientCertificateAndVerify);
}

////////////////////////////////////////////////////////////////////////////////

void TServerAddressConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("address", &TThis::Address);
    registrar.Parameter("credentials", &TThis::Credentials)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("addresses", &TThis::Addresses);
    registrar.Parameter("grpc_arguments", &TThis::GrpcArguments)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TChannelCredentialsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pem_root_certs", &TThis::PemRootCerts)
        .Optional();
    registrar.Parameter("pem_key_cert_pair", &TThis::PemKeyCertPair)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("address", &TThis::Address);
    registrar.Parameter("credentials", &TThis::Credentials)
        .Optional();
    registrar.Parameter("grpc_arguments", &TThis::GrpcArguments)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
