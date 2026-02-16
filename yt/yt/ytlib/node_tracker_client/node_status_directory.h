#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

bool IsSuspiciousNodeError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

struct INodeStatusDirectory
    : public virtual TRefCounted
{
    //! If #suspicious is true, #nodeId is put to the list of suspicious nodes.
    //! Otherwise #nodeId is removed from suspicious nodes list
    //! if #previousMarkTime matches the actual mark time.
    virtual void UpdateSuspicionMarkTime(
        TNodeId nodeId,
        TStringBuf address,
        bool suspicious,
        std::optional<TInstant> previousMarkTime) = 0;

    //! For each nodeId from #nodeIds that is suspicious returns this node id and mark time.
    virtual THashMap<TNodeId, TInstant> RetrieveSuspicionMarkTimes(
        TRange<TNodeId> nodeIds) const = 0;

    //! Returns whether node should be marked as suspicious or not.
    virtual bool ShouldMarkNodeSuspicious(const TError& error) const = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeStatusDirectory)

////////////////////////////////////////////////////////////////////////////////

INodeStatusDirectoryPtr CreateNodeStatusDirectory(NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
