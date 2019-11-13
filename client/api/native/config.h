#pragma once

#include "public.h"

#include <yt/core/rpc/config.h>
#include <yt/core/rpc/grpc/config.h>

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TAuthenticationConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TString User;
    TString Token;

    TAuthenticationConfig();
};

DEFINE_REFCOUNTED_TYPE(TAuthenticationConfig);

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NYT::NRpc::TRetryingChannelConfig
{
public:
    bool Secure;

    NYT::NRpc::NGrpc::TChannelConfigPtr GrpcChannel;

    TDuration DiscoveryPeriod;
    TDuration DiscoveryTimeout;

    TAuthenticationConfigPtr Authentication;

    TConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig);

////////////////////////////////////////////////////////////////////////////////

class TClientConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TConnectionConfigPtr Connection;

    TDuration Timeout;

    TClientConfig();
};

DEFINE_REFCOUNTED_TYPE(TClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
