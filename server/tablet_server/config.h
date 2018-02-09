#pragma once

#include "public.h"

#include <yt/server/tablet_node/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableInMemoryBalancer;
    bool EnableTabletSizeBalancer;

    double CellBalanceFactor;

    i64 MinTabletSize;
    i64 MaxTabletSize;
    i64 DesiredTabletSize;

    i64 MinInMemoryTabletSize;
    i64 MaxInMemoryTabletSize;
    i64 DesiredInMemoryTabletSize;

    double TabletToCellRatio;

    TTabletBalancerConfig()
    {
        RegisterParameter("enable_in_memory_balancer", EnableInMemoryBalancer)
            .Default(true);

        RegisterParameter("enable_tablet_size_balancer", EnableTabletSizeBalancer)
            .Default(true);

        RegisterParameter("cell_balance_factor", CellBalanceFactor)
            .Default(0.05);

        RegisterParameter("min_tablet_size", MinTabletSize)
            .Default(128_MB);

        RegisterParameter("max_tablet_size", MaxTabletSize)
            .Default(20_GB);

        RegisterParameter("desired_tablet_size", DesiredTabletSize)
            .Default(10_GB);

        RegisterParameter("min_in_memory_tablet_size", MinInMemoryTabletSize)
            .Default(512_MB);

        RegisterParameter("max_in_memory_tablet_size", MaxInMemoryTabletSize)
            .Default(2_GB);

        RegisterParameter("desired_in_memory_tablet_size", DesiredInMemoryTabletSize)
            .Default(1_GB);

        RegisterParameter("tablet_to_cell_ratio", TabletToCellRatio)
            .GreaterThan(0)
            .Default(5.0);

        RegisterPostprocessor([&] () {
            if (MinTabletSize > DesiredTabletSize) {
                THROW_ERROR_EXCEPTION("\"min_tablet_size\" must be less than or equal to \"desired_tablet_size\"");
            }
            if (DesiredTabletSize > MaxTabletSize) {
                THROW_ERROR_EXCEPTION("\"desired_tablet_size\" must be less than or equal to \"max_tablet_size\"");
            }
            if (MinInMemoryTabletSize >= DesiredInMemoryTabletSize) {
                THROW_ERROR_EXCEPTION("\"min_in_memory_tablet_size\" must be less than \"desired_in_memory_tablet_size\"");
            }
            if (DesiredInMemoryTabletSize >= MaxInMemoryTabletSize) {
                THROW_ERROR_EXCEPTION("\"desired_in_memory_tablet_size\" must be less than \"max_in_memory_tablet_size\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerMasterConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableTabletBalancer;
    TDuration ConfigCheckPeriod;
    TDuration BalancePeriod;

    TTabletBalancerMasterConfig()
    {
        RegisterParameter("enable_tablet_balancer", EnableTabletBalancer)
            .Default(true);
        RegisterParameter("config_check_period", ConfigCheckPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("balance_period", BalancePeriod)
            .Default(TDuration::Minutes(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Time to wait for a node to be back online before revoking it from all
    //! tablet cells.
    TDuration PeerRevocationTimeout;

    //! Time to wait before resetting leader to another peer.
    TDuration LeaderReassignmentTimeout;

    //! Maximum number of snapshots to keep for a tablet cell.
    TNullable<int> MaxSnapshotCountToKeep;

    //! Maximum total size of snapshots to keep for a tablet cell.
    TNullable<i64> MaxSnapshotSizeToKeep;

    //! Maximum number of snapshots to remove per a single check.
    int MaxSnapshotCountToRemovePerCheck;

    //! Maximum number of snapshots to remove per a single check.
    int MaxChangelogCountToRemovePerCheck;

    //! When the number of online nodes drops below this margin,
    //! tablet cell peers are no longer assigned and revoked.
    int SafeOnlineNodeCount;

    //! Internal between tablet cell examinations.
    TDuration CellScanPeriod;

    //! Additional number of bytes per tablet to charge each cell
    //! for balancing purposes.
    //! NB: Changing this value will invalidate all changelogs!
    i64 TabletDataSizeFootprint;

    //! Chunk reader config for all dynamic tables.
    NTabletNode::TTabletChunkReaderConfigPtr ChunkReader;

    //! Chunk writer config for all dynamic tables.
    NTabletNode::TTabletChunkWriterConfigPtr ChunkWriter;

    //! Tablet balancer (balancer) config.
    TTabletBalancerMasterConfigPtr TabletBalancer;

    TTabletManagerConfig()
    {
        RegisterParameter("peer_revocation_timeout", PeerRevocationTimeout)
            .Default(TDuration::Minutes(1));
        RegisterParameter("leader_reassignment_timeout", LeaderReassignmentTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("max_snapshot_count_to_keep", MaxSnapshotCountToKeep)
            .GreaterThanOrEqual(0)
            .Default(5);
        RegisterParameter("max_snapshot_size_to_keep", MaxSnapshotSizeToKeep)
            .GreaterThanOrEqual(0)
            .Default();
        RegisterParameter("max_snapshot_count_to_remove_per_check", MaxSnapshotCountToRemovePerCheck)
            .GreaterThan(0)
            .Default(100);
        RegisterParameter("max_changelog_count_to_remove_per_check", MaxChangelogCountToRemovePerCheck)
            .GreaterThan(0)
            .Default(100);
        RegisterParameter("safe_online_node_count", SafeOnlineNodeCount)
            .GreaterThanOrEqual(0)
            .Default(0);
        RegisterParameter("cell_scan_period", CellScanPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("tablet_data_size_footprint", TabletDataSizeFootprint)
            .GreaterThanOrEqual(0)
            .Default(64_MB);
        RegisterParameter("chunk_reader", ChunkReader)
            .DefaultNew();
        RegisterParameter("chunk_writer", ChunkWriter)
            .DefaultNew();
        RegisterParameter("tablet_balancer", TabletBalancer)
            .DefaultNew();

        RegisterPreprocessor([&] () {
            // Override default workload descriptors.
            ChunkReader->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserInteractive);
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
