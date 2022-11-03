#pragma once

#include "public.h"

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryConnectionConfig
    : public virtual NRpc::TBalancingChannelConfig
{
public:
    TDuration RpcTimeout;
    TDuration ServerBanTimeout;

    REGISTER_YSON_STRUCT(TDiscoveryConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TMemberClientConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TDuration HeartbeatPeriod;
    TDuration AttributeUpdatePeriod;
    TDuration LeaseTimeout;
    int MaxFailedHeartbeatsOnStartup;

    std::optional<int> WriteQuorum;

    REGISTER_YSON_STRUCT(TMemberClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemberClientConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClientConfig
    : public virtual NRpc::TRetryingChannelConfig
    , public virtual NYTree::TYsonStruct
{
public:
    std::optional<int> ReadQuorum;

    REGISTER_YSON_STRUCT(TDiscoveryClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

