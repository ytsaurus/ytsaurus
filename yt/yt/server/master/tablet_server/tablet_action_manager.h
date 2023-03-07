#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>
#include <yt/server/master/table_server/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletActionManager
    : public TRefCounted
{
public:
    explicit TTabletActionManager(NCellMaster::TBootstrap* bootstrap);
    ~TTabletActionManager();

    void Start();
    void Stop();

    void Reconfigure(TTabletActionManagerMasterConfigPtr config);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTabletActionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
