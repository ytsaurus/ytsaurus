#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TChaosPeerConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<TString> AlienCluster;

    TChaosPeerConfig()
    {
        RegisterParameter("alien_cluster", AlienCluster)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TChaosPeerConfig)

class TChaosHydraConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TChaosPeerConfigPtr> Peers;

    TChaosHydraConfig()
    {
        RegisterParameter("peers", Peers);
    }
};

DEFINE_REFCOUNTED_TYPE(TChaosHydraConfig)

////////////////////////////////////////////////////////////////////////////////

class TAlienCellSynchronizerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;
    TDuration SyncPeriod;

    TAlienCellSynchronizerConfig()
    {
        RegisterParameter("sync_period", SyncPeriod)
            .Default(TDuration::Minutes(1));
        RegisterParameter("enable", Enable)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TAlienCellSynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicChaosManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TAlienCellSynchronizerConfigPtr AlienCellSynchronizer;

    TDynamicChaosManagerConfig()
    {
        RegisterParameter("alien_cell_synchronizer", AlienCellSynchronizer)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicChaosManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
