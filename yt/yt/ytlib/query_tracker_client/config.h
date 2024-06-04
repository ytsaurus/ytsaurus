#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueryTrackerClient {

///////////////////////////////////////////////////////////////////////////////

class TQueryTrackerChannelConfig
    : public NRpc::TBalancingChannelConfig
{
public:
    TDuration Timeout;

    REGISTER_YSON_STRUCT(TQueryTrackerChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerChannelConfig)

///////////////////////////////////////////////////////////////////////////////

class TQueryTrackerStageConfig
    : public NYTree::TYsonStruct
{
public:
    TQueryTrackerChannelConfigPtr Channel;
    NYPath::TYPath Root;
    TString User;

    REGISTER_YSON_STRUCT(TQueryTrackerStageConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerStageConfig)

///////////////////////////////////////////////////////////////////////////////

class TQueryTrackerConnectionConfig
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TQueryTrackerStageConfigPtr> Stages;

    REGISTER_YSON_STRUCT(TQueryTrackerConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
