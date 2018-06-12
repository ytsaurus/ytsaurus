#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableManager
    : public TRefCounted
{
public:
    TReplicatedTableManager(const NTabletServer::TReplicatedTableManagerConfigPtr& config, NCellMaster::TBootstrap* bootstrap);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
