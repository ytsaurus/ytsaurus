#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/node_tracker_server//public.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/small_set.h>
#include <yt/core/misc/optional.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletTracker
    : public TRefCounted
{
public:
    TTabletTracker(
        TTabletManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTabletTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
