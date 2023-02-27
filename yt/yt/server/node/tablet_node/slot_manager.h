#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Controls all tablet slots running at this node.
struct ISlotManager
    : public TRefCounted
{
    // The following methods have ControlThread affinity.
    virtual void Initialize() = 0;
    virtual void Start() = 0;

    virtual bool IsOutOfMemory(const std::optional<TString>& poolTag) const = 0;

    //! Returns fraction of CPU used by tablet slots (in terms of resource limits).
    virtual double GetUsedCpu(double cpuPerTabletSlot) const = 0;

    //! Finds the slot by cell id, returns null if none.
    virtual ITabletSlotPtr FindSlot(NHydra::TCellId id) = 0;

    virtual const NYTree::IYPathServicePtr& GetOrchidService() const = 0;

    DECLARE_INTERFACE_SIGNAL(void(), BeginSlotScan);
    DECLARE_INTERFACE_SIGNAL(void(ITabletSlotPtr), ScanSlot);
    DECLARE_INTERFACE_SIGNAL(void(), EndSlotScan);
};

DEFINE_REFCOUNTED_TYPE(ISlotManager)

////////////////////////////////////////////////////////////////////////////////

ISlotManagerPtr CreateSlotManager(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
