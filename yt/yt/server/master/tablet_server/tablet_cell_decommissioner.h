#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletCellDecommissioner
    : public TRefCounted
{
public:
    explicit TTabletCellDecommissioner(NCellMaster::TBootstrap* bootstrap);
    ~TTabletCellDecommissioner();

    void Start();
    void Stop();

    void Reconfigure(TTabletCellDecommissionerConfigPtr config);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTabletCellDecommissioner)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
