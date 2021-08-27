#pragma once

#include "public.h"

#include <yt/yt/server/master/node_tracker_server/public.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

struct TRack
{
    DEFINE_BYVAL_RO_PROPERTY(TRackId, Id);
    DEFINE_BYVAL_RO_PROPERTY(TRackIndex, Index);

    DEFINE_BYREF_RW_PROPERTY(TString, Name);

    DEFINE_BYVAL_RW_PROPERTY(TDataCenter*, DataCenter, nullptr);

    explicit TRack(NNodeTrackerServer::TRack* rack);

    static std::unique_ptr<TRack> FromPrimary(NNodeTrackerServer::TRack* rack);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
