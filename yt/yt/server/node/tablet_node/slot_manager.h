#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Controls all tablet slots running at this node.
struct ISlotManager
    : public TRefCounted
{
    // The following method has ControlThread affinity.
    virtual void Initialize() = 0;

    virtual bool IsOutOfMemory(const std::optional<TString>& poolTag) const = 0;

    //! Returns fraction of CPU used by tablet slots (in terms of resource limits).
    virtual double GetUsedCpu(double cpuPerTabletSlot) const = 0;

    //! Finds the slot by cell id, returns null if none.
    virtual TTabletSlotPtr FindSlot(NHydra::TCellId id) = 0;

    DECLARE_INTERFACE_SIGNAL(void(), BeginSlotScan);
    DECLARE_INTERFACE_SIGNAL(void(TTabletSlotPtr), ScanSlot);
    DECLARE_INTERFACE_SIGNAL(void(), EndSlotScan);
};

DEFINE_REFCOUNTED_TYPE(ISlotManager)

ISlotManagerPtr CreateSlotManager(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
