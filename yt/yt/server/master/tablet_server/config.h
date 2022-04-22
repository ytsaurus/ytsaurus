#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra_common/config.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableInMemoryCellBalancer;
    bool EnableCellBalancer;
    bool EnableTabletSizeBalancer;

    bool EnableTabletCellSmoothing;

    double HardInMemoryCellBalanceThreshold;
    double SoftInMemoryCellBalanceThreshold;

    i64 MinTabletSize;
    i64 MaxTabletSize;
    i64 DesiredTabletSize;

    i64 MinInMemoryTabletSize;
    i64 MaxInMemoryTabletSize;
    i64 DesiredInMemoryTabletSize;

    double TabletToCellRatio;

    TTimeFormula TabletBalancerSchedule;

    bool EnableVerboseLogging;

    TTabletBalancerConfig();
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerMasterConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableTabletBalancer;
    TTimeFormula TabletBalancerSchedule;

    TDuration ConfigCheckPeriod;
    TDuration BalancePeriod;

    TTabletBalancerMasterConfig();
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletCellDecommissionerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableTabletCellDecommission;
    bool EnableTabletCellRemoval;

    TDuration DecommissionCheckPeriod;
    TDuration OrphansCheckPeriod;

    NConcurrency::TThroughputThrottlerConfigPtr DecommissionThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr KickOrphansThrottler;

    TTabletCellDecommissionerConfig();
};

DEFINE_REFCOUNTED_TYPE(TTabletCellDecommissionerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletActionManagerMasterConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration TabletActionsCleanupPeriod;

    TTabletActionManagerMasterConfig();
};

DEFINE_REFCOUNTED_TYPE(TTabletActionManagerMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTablesMulticellGossipConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Multicell tablet cell statistics gossip period.
    TDuration TabletCellStatisticsGossipPeriod;

    //! Multicell tablet cell status full gossip period.
    // COMPAT(gritukan): If not set, #TabletCellStatisticsGossipPeriod is used instead.
    // Set this option at all clusters and drop optional.
    std::optional<TDuration> TabletCellStatusFullGossipPeriod;

    //! Multicell tablet cell status incremental gossip period.
    //! If not set, only full tablet cell status gossip is performed.
    std::optional<TDuration> TabletCellStatusIncrementalGossipPeriod;

    //! Multicell table (e.g. chunk owner) statistics gossip period.
    TDuration TableStatisticsGossipPeriod;

    //! Throttler for table statistics gossip.
    NConcurrency::TThroughputThrottlerConfigPtr TableStatisticsGossipThrottler;

    //! Bundle resource usage gossip period.
    TDuration BundleResourceUsageGossipPeriod;

    bool EnableUpdateStatisticsOnHeartbeat;

    TDynamicTablesMulticellGossipConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicTablesMulticellGossipConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTabletCellBalancerMasterConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableTabletCellSmoothing;
    bool EnableVerboseLogging;
    TDuration RebalanceWaitTime;

    TDynamicTabletCellBalancerMasterConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicTabletCellBalancerMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerExpiringCacheConfig
    : public TAsyncExpiringCacheConfig
{
public:
    REGISTER_YSON_STRUCT(TReplicatedTableTrackerExpiringCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableTrackerExpiringCacheConfig);

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerConfig
    : public NYTree::TYsonSerializable
{
public:
    int CheckerThreadCount;

    TReplicatedTableTrackerConfig();
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicReplicatedTableTrackerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableReplicatedTableTracker;

    TDuration CheckPeriod;
    TDuration UpdatePeriod;

    TDuration GeneralCheckTimeout;

    NTabletNode::TReplicatorHintConfigPtr ReplicatorHint;
    TAsyncExpiringCacheConfigPtr BundleHealthCache;
    TAsyncExpiringCacheConfigPtr ClusterStateCache;
    NHiveServer::TClusterDirectorySynchronizerConfigPtr ClusterDirectorySynchronizer;

    i64 MaxIterationsWithoutAcceptableBundleHealth;

    TDynamicReplicatedTableTrackerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicReplicatedTableTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTabletNodeTrackerConfig
    : public NYTree::TYsonSerializable
{
public:
    int MaxConcurrentHeartbeats;

    TDynamicTabletNodeTrackerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicTabletNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTabletManagerConfig
    : public NHydra::THydraJanitorConfig
{
public:
    static constexpr auto DefaultTamedCellManagerProfilingPeriod = TDuration::Seconds(10);

    //! Time to wait for a node to be back online before revoking it from all
    //! tablet cells.
    TDuration PeerRevocationTimeout;

    //! Time to wait before resetting leader to another peer.
    TDuration LeaderReassignmentTimeout;

    //! Maximum number of snapshots to keep for a tablet cell.
    std::optional<int> MaxSnapshotCountToKeep;

    //! Maximum total size of snapshots to keep for a tablet cell.
    std::optional<i64> MaxSnapshotSizeToKeep;

    //! Maximum number of snapshots to remove per a single check.
    int MaxSnapshotCountToRemovePerCheck;

    //! Maximum number of snapshots to remove per a single check.
    int MaxChangelogCountToRemovePerCheck;

    //! When the number of online nodes drops below this margin,
    //! tablet cell peers are no longer assigned and revoked.
    int SafeOnlineNodeCount;

    //! Internal between tablet cell examinations.
    TDuration CellScanPeriod;

    bool EnableCellTracker;

    //! Additional number of bytes per tablet to charge each cell
    //! for balancing purposes.
    i64 TabletDataSizeFootprint;

    //! Store chunk reader config for all dynamic tables.
    NTabletNode::TTabletStoreReaderConfigPtr StoreChunkReader;

    //! Hunk chunk reader config for all dynamic tables.
    NTabletNode::TTabletHunkReaderConfigPtr HunkChunkReader;

    //! Store chunk writer config for all dynamic tables.
    NTabletNode::TTabletStoreWriterConfigPtr StoreChunkWriter;

    //! Hunk chunk writer config for all dynamic tables.
    NTabletNode::TTabletHunkWriterConfigPtr HunkChunkWriter;

    //! Tablet balancer config.
    TTabletBalancerMasterConfigPtr TabletBalancer;

    //! Tablet cell decommissioner config.
    TTabletCellDecommissionerConfigPtr TabletCellDecommissioner;

    //! Tablet action manager config.
    TTabletActionManagerMasterConfigPtr TabletActionManager;

    //! Dynamic tables multicell gossip config.
    TDynamicTablesMulticellGossipConfigPtr MulticellGossip;

    TDuration TabletCellsCleanupPeriod;

    NTabletNode::EDynamicTableProfilingMode DynamicTableProfilingMode;

    TDynamicTabletCellBalancerMasterConfigPtr TabletCellBalancer;

    TDynamicReplicatedTableTrackerConfigPtr ReplicatedTableTracker;

    bool EnableBulkInsert;

    bool DecommissionThroughExtraPeers;

    // TODO(gritukan): Move it to node dynamic config when it will be ready.
    bool AbandonLeaderLeaseDuringRecovery;

    //! This parameter is used only for testing purposes.
    std::optional<TDuration> DecommissionedLeaderReassignmentTimeout;

    bool EnableDynamicStoreReadByDefault;

    //! Peer revocation reason is reset after this period of time.
    TDuration PeerRevocationReasonExpirationTime;

    //! If set, tablet statistics will be validated upon each attributes request
    //! to the table node.
    bool EnableAggressiveTabletStatisticsValidation;

    //! If set, tablet statistics will be validated upon each @tablet_statistics request
    //! to the table node.
    bool EnableRelaxedTabletStatisticsValidation;

    //! Time to wait before peer count update after new leader assignment
    //! during decommission through extra peers.
    TDuration ExtraPeerDropDelay;

    // COMPAT(ifsmirnov): YT-13541
    bool AccumulatePreloadPendingStoreCountCorrectly;

    // COMPAT(akozhikhov): YT-14187
    bool IncreaseUploadReplicationFactor;

    //! If set, tablet resource limit violation will be validated per-bundle.
    // TODO(ifsmirnov): remove and set default to true.
    bool EnableTabletResourceValidation;

    TDynamicTabletNodeTrackerConfigPtr TabletNodeTracker;

    // COMPAT(babenko)
    bool EnableHunks;

    TDuration ProfilingPeriod;

    TDuration TamedCellManagerProfilingPeriod;

    TDynamicTabletManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicTabletManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
