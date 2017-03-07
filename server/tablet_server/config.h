#pragma once

#include "public.h"

#include <yt/server/tablet_node/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NTabletServer {

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
    int MaxSnapshotsToKeep;

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

    TTabletManagerConfig()
    {
        RegisterParameter("peer_revocation_timeout", PeerRevocationTimeout)
            .Default(TDuration::Minutes(1));
        RegisterParameter("leader_reassignment_timeout", LeaderReassignmentTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("max_snapshots_to_keep", MaxSnapshotsToKeep)
            .GreaterThanOrEqual(0)
            .Default(3);
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
