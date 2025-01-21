#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletCellDecommissioner
    : public TRefCounted
{
public:
    explicit TTabletCellDecommissioner(NCellMaster::TBootstrap* bootstrap);
    ~TTabletCellDecommissioner();

    void Start() const;
    void Stop() const;

    void Reconfigure(TTabletCellDecommissionerConfigPtr config) const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTabletCellDecommissioner)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
