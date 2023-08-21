#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NYqlClient {

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentChannelConfig
    : public NRpc::TBalancingChannelConfig
    , public NRpc::TRetryingChannelConfig
{
public:
    REGISTER_YSON_STRUCT(TYqlAgentChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentChannelConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentStageConfig
    : public NYTree::TYsonStruct
{
public:
    TYqlAgentChannelConfigPtr Channel;

    REGISTER_YSON_STRUCT(TYqlAgentStageConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentStageConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentConnectionConfig
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TYqlAgentStageConfigPtr> Stages;

    REGISTER_YSON_STRUCT(TYqlAgentConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlClient
