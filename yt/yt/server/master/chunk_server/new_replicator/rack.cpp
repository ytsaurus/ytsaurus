#include "rack.h"

#include <yt/yt/server/master/node_tracker_server/rack.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

TRack::TRack(NNodeTrackerServer::TRack* rack)
    : Id_(rack->GetId())
    , Index_(rack->GetIndex())
    , Name_(rack->GetName())
{ }

std::unique_ptr<TRack> TRack::FromPrimary(NNodeTrackerServer::TRack* rack)
{
    return std::make_unique<TRack>(rack);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
