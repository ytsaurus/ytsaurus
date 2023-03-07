#pragma once

#include "public.h"

#include <yt/core/http/config.h>

#include <yt/core/crypto/config.h>

namespace NYT::NHttps {

////////////////////////////////////////////////////////////////////////////////

class TServerCredentialsConfig
    : public NYTree::TYsonSerializable
{
public:
    NCrypto::TPemBlobConfigPtr PrivateKey;
    NCrypto::TPemBlobConfigPtr CertChain;

    TServerCredentialsConfig();
};

DEFINE_REFCOUNTED_TYPE(TServerCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public NHttp::TServerConfig
{
public:
    TServerCredentialsConfigPtr Credentials;

    TServerConfig();
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TClientCredentialsConfig
    : public NYTree::TYsonSerializable
{
public:
    NCrypto::TPemBlobConfigPtr PrivateKey;
    NCrypto::TPemBlobConfigPtr CertChain;

    TClientCredentialsConfig();
};

DEFINE_REFCOUNTED_TYPE(TClientCredentialsConfig)

////////////////////////////////////////////////////////////////////////////////

class TClientConfig
    : public NHttp::TClientConfig
{
public:
    // If missing then builtin certificate store is used.
    TClientCredentialsConfigPtr Credentials;

    TClientConfig();
};

DEFINE_REFCOUNTED_TYPE(TClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
