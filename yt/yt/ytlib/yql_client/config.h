#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NYqlClient {

////////////////////////////////////////////////////////////////////////////////

struct TYqlAgentChannelConfig
    : public NRpc::TBalancingChannelConfig
    , public NRpc::TRetryingChannelConfig
{
    //! Default timeout for progress request.
    TDuration DefaultProgressRequestTimeout;

    REGISTER_YSON_STRUCT(TYqlAgentChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentChannelConfig)

////////////////////////////////////////////////////////////////////////////////

struct TYqlAgentStageConfig
    : public NYTree::TYsonStruct
{
    TYqlAgentChannelConfigPtr Channel;

    REGISTER_YSON_STRUCT(TYqlAgentStageConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentStageConfig)

////////////////////////////////////////////////////////////////////////////////

struct TYqlAgentConnectionConfig
    : public NYTree::TYsonStruct
{
    THashMap<std::string, TYqlAgentStageConfigPtr> Stages;

    REGISTER_YSON_STRUCT(TYqlAgentConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlClient
