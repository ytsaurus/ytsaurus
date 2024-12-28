#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/server/lib/hydra/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/timestamp_server/config.h>

#include <yt/yt/server/lib/election/config.h>

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hive/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

class TClockHydraManagerConfig
    : public NHydra::TDistributedHydraManagerConfig
    , public NHydra::TLocalHydraJanitorConfig
{
public:
    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    REGISTER_YSON_STRUCT(TClockHydraManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClockHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterClockBootstrapConfig
    : public NServer::TServerBootstrapConfig
{
public:
    NElection::TCellConfigPtr ClockCell;
    NElection::TDistributedElectionManagerConfigPtr ElectionManager;

    NHydra::TFileChangelogStoreConfigPtr Changelogs;
    NHydra::TLocalSnapshotStoreConfigPtr Snapshots;
    TClockHydraManagerConfigPtr HydraManager;

    NTimestampServer::TTimestampManagerConfigPtr TimestampManager;

    NBus::TBusConfigPtr BusClient;

    //! Primary master cell tag
    NObjectClient::TCellTag ClockClusterTag;

    REGISTER_YSON_STRUCT(TClusterClockBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterClockBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterClockProgramConfig
    : public TClusterClockBootstrapConfig
    , public TServerProgramConfig
{
public:
    REGISTER_YSON_STRUCT(TClusterClockProgramConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
