#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct ICellTracker
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellTracker)

////////////////////////////////////////////////////////////////////////////////

ICellTrackerPtr CreateCellTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
