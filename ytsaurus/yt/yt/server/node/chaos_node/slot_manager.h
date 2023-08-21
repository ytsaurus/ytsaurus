#pragma once

#include "public.h"

#include <yt/yt/server/lib/chaos_node/public.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

//! Controls all chaos slots running at this node.
struct ISlotManager
    : public TRefCounted
{
    virtual void Initialize() = 0;
    virtual void Start() = 0;

    DECLARE_INTERFACE_SIGNAL(void(), BeginSlotScan);
    DECLARE_INTERFACE_SIGNAL(void(IChaosSlotPtr), ScanSlot);
    DECLARE_INTERFACE_SIGNAL(void(), EndSlotScan);
};

DEFINE_REFCOUNTED_TYPE(ISlotManager)

////////////////////////////////////////////////////////////////////////////////

ISlotManagerPtr CreateSlotManager(
    TChaosNodeConfigPtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
