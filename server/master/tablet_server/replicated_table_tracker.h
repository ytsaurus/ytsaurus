#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTracker
    : public TRefCounted
{
public:
    TReplicatedTableTracker(
        NTabletServer::TReplicatedTableTrackerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    ~TReplicatedTableTracker();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
