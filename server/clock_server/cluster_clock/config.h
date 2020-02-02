#pragma once

#include "public.h"

#include <yt/server/lib/hive/config.h>

#include <yt/server/lib/hydra/config.h>

#include <yt/server/lib/misc/config.h>

#include <yt/server/lib/timestamp_server/config.h>

#include <yt/ytlib/election/config.h>

#include <yt/ytlib/hive/config.h>

#include <yt/core/bus/tcp/config.h>

#include <yt/core/rpc/config.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

class TClockHydraManagerConfig
    : public NHydra::TDistributedHydraManagerConfig
    , public NHydra::TLocalHydraJanitorConfig
{
public:
    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    TClockHydraManagerConfig()
    {
        RegisterParameter("response_keeper", ResponseKeeper)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TClockHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterClockConfig
    : public TServerConfig
{
public:
    NElection::TCellConfigPtr ClockCell;
    NElection::TDistributedElectionManagerConfigPtr ElectionManager;

    NHydra::TFileChangelogStoreConfigPtr Changelogs;
    NHydra::TLocalSnapshotStoreConfigPtr Snapshots;
    TClockHydraManagerConfigPtr HydraManager;

    NTimestampServer::TTimestampManagerConfigPtr TimestampManager;

    NBus::TTcpBusConfigPtr BusClient;

    TClusterClockConfig();
};

DEFINE_REFCOUNTED_TYPE(TClusterClockConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
