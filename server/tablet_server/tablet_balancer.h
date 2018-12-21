#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancer
    : public TRefCounted
{
public:
    TTabletBalancer(
        TTabletBalancerMasterConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);
    ~TTabletBalancer();

    void Start();
    void Stop();

    void OnTabletHeartbeat(TTablet* tablet);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
