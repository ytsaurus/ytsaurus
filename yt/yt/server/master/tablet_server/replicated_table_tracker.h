#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/tablet_server/public.h>

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

// NB: Master implementation of replicated table tracker host interface.
IReplicatedTableTrackerHostPtr CreateReplicatedTableTrackerHost(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
