#pragma once

#include "public.h"

#include <yt/yt/server/master/node_tracker_server/public.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

struct TDataCenter
{
    DEFINE_BYVAL_RO_PROPERTY(TDataCenterId, Id);

    DEFINE_BYREF_RW_PROPERTY(TString, Name);

    explicit TDataCenter(NNodeTrackerServer::TDataCenter* dataCenter);

    static std::unique_ptr<TDataCenter> FromPrimary(NNodeTrackerServer::TDataCenter* dataCenter);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
