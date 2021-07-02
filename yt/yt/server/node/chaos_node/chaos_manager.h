#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct IChaosManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosManager)

IChaosManagerPtr CreateChaosManager(
    TChaosManagerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
