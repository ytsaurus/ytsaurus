#include "data_center.h"

#include <yt/yt/server/master/node_tracker_server/data_center.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

TDataCenter::TDataCenter(NNodeTrackerServer::TDataCenter* dataCenter)
    : Id_(dataCenter->GetId())
    , Name_(dataCenter->GetName())
{ }

std::unique_ptr<TDataCenter> TDataCenter::FromPrimary(NNodeTrackerServer::TDataCenter* dataCenter)
{
    return std::make_unique<TDataCenter>(dataCenter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
