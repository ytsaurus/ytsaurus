#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletCellDecommissioner
    : public TRefCounted
{
public:
    TTabletCellDecommissioner(
        TTabletCellDecommissionerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);
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

} // namespace NTabletServer
} // namespace NYT
