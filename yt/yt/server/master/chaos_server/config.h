#pragma once

#include "public.h"

#include <yt/yt/server/lib/chaos_server/config.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

struct TAlienCellSynchronizerConfig
    : public NYTree::TYsonStruct
{
    bool Enable;
    TDuration SyncPeriod;
    TDuration FullSyncPeriod;

    REGISTER_YSON_STRUCT(TAlienCellSynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAlienCellSynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicChaosManagerConfig
    : public NYTree::TYsonStruct
{
    TAlienCellSynchronizerConfigPtr AlienCellSynchronizer;
    bool EnableMetadataCells;

    REGISTER_YSON_STRUCT(TDynamicChaosManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChaosManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
