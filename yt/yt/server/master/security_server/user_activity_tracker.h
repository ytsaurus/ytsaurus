#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////
struct IUserActivityTracker
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual void OnUserSeen(TUser* user) = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserActivityTracker)

////////////////////////////////////////////////////////////////////////////////

IUserActivityTrackerPtr CreateUserActivityTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
