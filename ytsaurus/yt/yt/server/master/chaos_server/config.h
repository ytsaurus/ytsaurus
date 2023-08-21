#pragma once

#include "public.h"

#include <yt/yt/server/lib/chaos_server/config.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TAlienCellSynchronizerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    TDuration SyncPeriod;
    TDuration FullSyncPeriod;

    REGISTER_YSON_STRUCT(TAlienCellSynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAlienCellSynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicChaosManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TAlienCellSynchronizerConfigPtr AlienCellSynchronizer;
    bool EnableMetadataCells;

    REGISTER_YSON_STRUCT(TDynamicChaosManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChaosManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
