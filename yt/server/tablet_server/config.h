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

    TDuration BalancePeriod;
    TDuration EnabledCheckPeriod;

    double CellBalanceFactor;

    i64 MinTabletSize;
    i64 MaxTabletSize;
    i64 DesiredTabletSize;

    i64 MinInMemoryTabletSize;
    i64 MaxInMemoryTabletSize;
    i64 DesiredInMemoryTabletSize;

    TTabletBalancerConfig()
    {
        RegisterParameter("enable_in_memory_balancer", EnableInMemoryBalancer)
            .Default(false);

        RegisterParameter("enable_tablet_size_balancer", EnableTabletSizeBalancer)
            .Default(false);

        RegisterParameter("balance_period", BalancePeriod)
            .Default(TDuration::Minutes(5));

        RegisterParameter("enabled_check_period", EnabledCheckPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("cell_balance_factor", CellBalanceFactor)
            .Default(0.05);

        RegisterParameter("min_tablet_size", MinTabletSize)
            .Default((i64) 128 * 1024 * 1024);

        RegisterParameter("max_tablet_size", MaxTabletSize)
            .Default((i64) 20 * 1024 * 1024 * 1024);

        RegisterParameter("desired_tablet_size", DesiredTabletSize)
            .Default((i64) 10 * 1024 * 1024 * 1024);

        RegisterParameter("min_in_memory_tablet_size", MinInMemoryTabletSize)
            .Default((i64) 512 * 1024 * 1024);

        RegisterParameter("max_in_memory_tablet_size", MaxInMemoryTabletSize)
            .Default((i64) 2 * 1024 * 1024 * 1024);

        RegisterParameter("desired_in_memory_tablet_size", DesiredInMemoryTabletSize)
            .Default((i64) 1 * 1024 * 1024 * 1024);

        RegisterValidator([&] () {
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
    TTabletBalancerConfigPtr TabletBalancer;

    TTabletManagerConfig()
    {
        RegisterParameter("peer_revocation_timeout", PeerRevocationTimeout)
            .Default(TDuration::Minutes(1));
        RegisterParameter("leader_reassignment_timeout", LeaderReassignmentTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("max_snapshot_count_to_keep", MaxSnapshotCountToKeep)
            .GreaterThanOrEqual(0)
            .Default();
        RegisterParameter("max_snapshot_size_to_keep", MaxSnapshotSizeToKeep)
            .GreaterThanOrEqual(0)
            .Default();
        RegisterParameter("safe_online_node_count", SafeOnlineNodeCount)
            .GreaterThanOrEqual(0)
            .Default(0);
        RegisterParameter("cell_scan_period", CellScanPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("tablet_data_size_footprint", TabletDataSizeFootprint)
            .GreaterThanOrEqual(0)
            .Default((i64) 64 * 1024 * 1024);
        RegisterParameter("chunk_reader", ChunkReader)
            .DefaultNew();
        RegisterParameter("chunk_writer", ChunkWriter)
            .DefaultNew();
        RegisterParameter("tablet_balancer", TabletBalancer)
            .DefaultNew();

        RegisterInitializer([&] () {
            // Override default workload descriptors.
            ChunkReader->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserInteractive);
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
