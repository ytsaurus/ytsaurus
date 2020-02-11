#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>
#include <yt/server/master/table_server/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancer
    : public TRefCounted
{
public:
    explicit TTabletBalancer(NCellMaster::TBootstrap* bootstrap);
    ~TTabletBalancer();

    void Start();
    void Stop();

    void Reconfigure(TTabletBalancerMasterConfigPtr config);

    void OnTabletHeartbeat(TTablet* tablet);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
