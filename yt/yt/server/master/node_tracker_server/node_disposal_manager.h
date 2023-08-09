#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

struct INodeDisposalManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual void DisposeNodeWithSemaphore(TNode* node) = 0;

    // This does not dispose sequoia replicas.
    virtual void DisposeNodeCompletely(TNode* node) = 0;

    virtual void OnProfiling(NProfiling::TSensorBuffer* buffer) const = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeDisposalManager)

////////////////////////////////////////////////////////////////////////////////

INodeDisposalManagerPtr CreateNodeDisposalManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
