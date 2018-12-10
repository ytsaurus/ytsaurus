#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletService
    : public TRefCounted
{
public:
    explicit TTabletService(NCellMaster::TBootstrap* bootstrap);

    void Initialize();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTabletService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
