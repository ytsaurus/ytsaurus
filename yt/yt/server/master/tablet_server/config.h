#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_node/public.h>
#include <yt/yt/server/lib/tablet_node/table_settings.h>

#include <yt/yt/server/lib/tablet_server/config.h>

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra/config.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerMasterConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableTabletBalancer;
    TTimeFormula TabletBalancerSchedule;

    TDuration ConfigCheckPeriod;
    TDuration BalancePeriod;

    REGISTER_YSON_STRUCT(TTabletBalancerMasterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletCellDecommissionerConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableTabletCellDecommission;
    bool EnableTabletCellRemoval;

    TDuration DecommissionCheckPeriod;
    TDuration OrphansCheckPeriod;

    NConcurrency::TThroughputThrottlerConfigPtr DecommissionThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr KickOrphansThrottler;

    REGISTER_YSON_STRUCT(TTabletCellDecommissionerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletCellDecommissionerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletActionManagerMasterConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration TabletActionsCleanupPeriod;

    REGISTER_YSON_STRUCT(TTabletActionManagerMasterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletActionManagerMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTablesMulticellGossipConfig
    : public NYTree::TYsonStruct
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

    REGISTER_YSON_STRUCT(TDynamicTablesMulticellGossipConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTablesMulticellGossipConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTabletCellBalancerMasterConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableTabletCellSmoothing;
    bool EnableVerboseLogging;
    TDuration RebalanceWaitTime;

    REGISTER_YSON_STRUCT(TDynamicTabletCellBalancerMasterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTabletCellBalancerMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    int CheckerThreadCount;

    REGISTER_YSON_STRUCT(TReplicatedTableTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTabletNodeTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    int MaxConcurrentHeartbeats;

    REGISTER_YSON_STRUCT(TDynamicTabletNodeTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTabletNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCellHydraPersistenceSynchronizerConfig
    : public NYTree::TYsonStruct
{
public:
    // COMPAT(danilalexeev)
    //! Remarks the beginning of Hydra persistence migration for tablet and chaos cells.
    //! Further Hydra persistence is created at a new storage at Cypress, whereas the old one
    //! is gradually emptied by janitor.
    bool UseHydraPersistenceDirectory;

    //! Allows safe deletion of the old storage at Cypress without it affecting cell instances.
    //! Reconfigures master in a way that the old storage is no longer being accessed.
    bool MigrateToVirtualCellMaps;

    TDuration SynchronizationPeriod;

    int MaxCellsToRegisterInCypressPerIteration;

    int MaxCellsToUnregisterFromCypressPerIteration;

    int MaxCellAclUpdatesPerIteration;

    TDuration HydraPersistenceFileIdUpdatePeriod;

    int MaxHydraPersistenceFileIdUpdatesPerIteration;

    REGISTER_YSON_STRUCT(TDynamicCellHydraPersistenceSynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellHydraPersistenceSynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTabletManagerConfig
    : public NHydra::THydraJanitorConfig
    , public NTabletNode::TClusterTableConfigPatchSet
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

    //! Hunk store writer config for all hunk storages.
    NTabletNode::THunkStoreWriterConfigPtr HunkStoreWriter;

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

    bool SynchronizeTabletCellLeaderSwitches;

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

    bool EnableBackups;

    bool SendDynamicStoreIdInBackup;

    // COMPAT(ifsmirnov)
    bool IncludeMountConfigAttributesInUserAttributes;

    TDuration ProfilingPeriod;

    TDuration TamedCellManagerProfilingPeriod;

    TDynamicCellHydraPersistenceSynchronizerConfigPtr CellHydraPersistenceSynchronizer;

    bool ForbidArbitraryDataVersionsInRetentionConfig;

    int MaxTableCollocationSize;

    // COMPAT(alexelexa)
    bool AddPerfCountersToTabletsAttribute;

    // COMPAT(ifsmirnov)
    bool UseAvenues;

    // COMPAT(akozhikhov)
    bool ReplicateTableCollocations;

    int MaxChunksPerMountedTablet;

    REGISTER_YSON_STRUCT(TDynamicTabletManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTabletManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
