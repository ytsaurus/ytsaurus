#pragma once

#include "public.h"

#include <yt/core/ytree/attributes.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/rpc/config.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

class TMemberClientConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TString> ServerAddresses;
    TDuration RpcTimeout;
    TDuration HeartbeatPeriod;
    TDuration AttributeUpdatePeriod;
    TDuration LeaseTimeout;

    int WriteQuorum;
    TDuration ServerBanTimeout;

    TMemberClientConfig();
};

DEFINE_REFCOUNTED_TYPE(TMemberClientConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClientConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    std::vector<TString> ServerAddresses;
    TDuration RpcTimeout;

    int ReadQuorum;
    TDuration ServerBanTimeout;

    TDiscoveryClientConfig();
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

