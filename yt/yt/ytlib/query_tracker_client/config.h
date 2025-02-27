#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueryTrackerClient {

////////////////////////////////////////////////////////////////////////////////

struct TQueryTrackerChannelConfig
    : public NRpc::TBalancingChannelConfig
{
    TDuration Timeout;

    REGISTER_YSON_STRUCT(TQueryTrackerChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerChannelConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueryTrackerStageConfig
    : public NYTree::TYsonStruct
{
    TQueryTrackerChannelConfigPtr Channel;
    NYPath::TYPath Root;
    std::string User;

    REGISTER_YSON_STRUCT(TQueryTrackerStageConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerStageConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueryTrackerConnectionConfig
    : public NYTree::TYsonStruct
{
    THashMap<TString, TQueryTrackerStageConfigPtr> Stages;

    REGISTER_YSON_STRUCT(TQueryTrackerConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
