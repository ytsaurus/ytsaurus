#pragma once

#include "public.h"

#include <yt/yt/core/crypto/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <contrib/libs/grpc/include/grpc/grpc_security_constants.h>

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

class TSslPemKeyCertPairConfig
    : public NYTree::TYsonSerializable
{
public:
    NCrypto::TPemBlobConfigPtr PrivateKey;
    NCrypto::TPemBlobConfigPtr CertChain;

    TSslPemKeyCertPairConfig();
};

DEFINE_REFCOUNTED_TYPE(TSslPemKeyCertPairConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EClientCertificateRequest,
    ((DontRequestClientCertificate)(GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE))
    ((RequestClientCertificateButDontVerify)(GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_BUT_DONT_VERIFY))
    ((RequestClientCertificateAndVerify)(GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY))
    ((RequestAndRequireClientCertificateButDontVerify)(GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_BUT_DONT_VERIFY))
    ((RequestAndRequireClientCertificateAndVerify)(GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY))
);

////////////////////////////////////////////////////////////////////////////////

class TServerCredentialsConfig
    : public NYTree::TYsonSerializable
{
public:
    NCrypto::TPemBlobConfigPtr PemRootCerts;
    std::vector<TSslPemKeyCertPairConfigPtr> PemKeyCertPairs;
    EClientCertificateRequest ClientCertificateRequest;

    TServerCredentialsConfig();
};

DEFINE_REFCOUNTED_TYPE(TServerCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

class TServerAddressConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Address;
    TServerCredentialsConfigPtr Credentials;

    TServerAddressConfig();
};

DEFINE_REFCOUNTED_TYPE(TServerAddressConfig)

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TServerAddressConfigPtr> Addresses;
    THashMap<TString, NYTree::INodePtr> GrpcArguments;

    TServerConfig();
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TChannelCredentialsConfig
    : public NYTree::TYsonSerializable
{
public:
    NCrypto::TPemBlobConfigPtr PemRootCerts;
    TSslPemKeyCertPairConfigPtr PemKeyCertPair;

    TChannelCredentialsConfig();
};

DEFINE_REFCOUNTED_TYPE(TChannelCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

class TChannelConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Address;
    TChannelCredentialsConfigPtr Credentials;
    THashMap<TString, NYTree::INodePtr> GrpcArguments;

    TChannelConfig();
};

DEFINE_REFCOUNTED_TYPE(TChannelConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
