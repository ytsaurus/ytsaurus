#pragma once

#include "public.h"

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryConnectionConfig
    : public virtual NRpc::TBalancingChannelConfig
{
    TDuration RpcTimeout;
    TDuration ServerBanTimeout;

    REGISTER_YSON_STRUCT(TDiscoveryConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMemberClientConfig
    : public virtual NYTree::TYsonStruct
{
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

struct TDiscoveryClientConfig
    : public virtual NRpc::TRetryingChannelConfig
    , public virtual NYTree::TYsonStruct
{
    std::optional<int> ReadQuorum;

    REGISTER_YSON_STRUCT(TDiscoveryClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryClientConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryBaseConfig
    : public virtual NYTree::TYsonStruct
{
    TGroupId GroupId;
    TDuration UpdatePeriod;
    TDuration BanTimeout;

    REGISTER_YSON_STRUCT(TDiscoveryBaseConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryBaseConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryConfig
    : public virtual TDiscoveryBaseConfig
    , public TDiscoveryClientConfig
    , public TMemberClientConfig
{
    int Version;

    TDuration DiscoveryReadinessTimeout;

    REGISTER_YSON_STRUCT(TDiscoveryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

