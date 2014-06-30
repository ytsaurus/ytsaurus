#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration PeerFailoverTimeout;
    int MaxSnapshotsToKeep;

    TTabletManagerConfig()
    {
        RegisterParameter("peer_failover_timeout", PeerFailoverTimeout)
            .Default(TDuration::Minutes(1));
        RegisterParameter("max_snapshots_to_keep", MaxSnapshotsToKeep)
            .GreaterThanOrEqual(0)
            .Default(3);
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
