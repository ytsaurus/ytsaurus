#pragma once

#include "public.h"

#include <yt/core/ytree/attributes.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/rpc/config.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClientBaseConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    std::vector<TString> ServerAddresses;
    TDuration RpcTimeout;
    TDuration ServerBanTimeout;

    TDiscoveryClientBaseConfig();
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryClientBaseConfig)

////////////////////////////////////////////////////////////////////////////////

class TMemberClientConfig
    : public virtual TDiscoveryClientBaseConfig
{
public:
    TDuration HeartbeatPeriod;
    TDuration AttributeUpdatePeriod;
    TDuration LeaseTimeout;
    int MaxFailedHeartbeatsOnStartup;

    int WriteQuorum;

    TMemberClientConfig();
};

DEFINE_REFCOUNTED_TYPE(TMemberClientConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClientConfig
    : public NRpc::TRetryingChannelConfig
    , public virtual TDiscoveryClientBaseConfig
{
public:
    int ReadQuorum;

    TDiscoveryClientConfig();
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

