#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTracker
    : public TRefCounted
{
public:
    TReplicatedTableTracker(
        NTabletServer::TReplicatedTableTrackerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
