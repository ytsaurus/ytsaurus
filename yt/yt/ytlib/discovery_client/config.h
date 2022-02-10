#pragma once

#include "public.h"

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClientBaseConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::vector<TString> ServerAddresses;
    TDuration RpcTimeout;
    TDuration ServerBanTimeout;

    REGISTER_YSON_STRUCT(TDiscoveryClientBaseConfig);

    static void Register(TRegistrar registrar);
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

    REGISTER_YSON_STRUCT(TMemberClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemberClientConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClientConfig
    : public NRpc::TRetryingChannelConfig
    , public virtual TDiscoveryClientBaseConfig
{
public:
    int ReadQuorum;

    REGISTER_YSON_STRUCT(TDiscoveryClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

