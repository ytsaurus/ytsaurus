#pragma once

#include "public.h"

#include <yt/yt/ytlib/discovery_client/config.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryBaseConfig
    : public virtual NYTree::TYsonStruct
{
    NDiscoveryClient::TGroupId GroupId;
    TDuration UpdatePeriod;
    TDuration BanTimeout;

    REGISTER_YSON_STRUCT(TDiscoveryBaseConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryBaseConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryConfig
    : public TDiscoveryBaseConfig
    , public NDiscoveryClient::TDiscoveryClientConfig
    , public NDiscoveryClient::TMemberClientConfig
{
    int Version;

    TDuration DiscoveryReadinessTimeout;

    REGISTER_YSON_STRUCT(TDiscoveryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
