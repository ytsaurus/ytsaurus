#pragma once

#include "public.h"

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
    TDuration PeerFailoverTimeout;

    //! Maximum number of snapshots to keep for a tablet cell.
    int MaxSnapshotsToKeep;

    //! When the number of online nodes drops below this margin,
    //! tablet cell peers are no longer assigned and revoked.
    int SafeOnlineNodeCount;

    //! Internal between tablet cell examinations.
    TDuration CellScanPeriod;

    //! Additional number of bytes per tablet to charge each cell
    //! for balancing pursposes.
    //! NB: Changing this value will invalidate all changelogs!
    i64 TabletDataSizeFootprint;

    TTabletManagerConfig()
    {
        RegisterParameter("peer_failover_timeout", PeerFailoverTimeout)
            .Default(TDuration::Minutes(1));
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
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
