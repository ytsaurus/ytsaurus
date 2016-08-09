#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/core/actions/public.h>
#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

//! Controls acquisition and release of slots.
class TSlotManager
    : public TRefCounted
{
public:
    TSlotManager(
        TSlotManagerConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    //! Initializes slots etc.
    void Initialize(int slotCount);

    //! Acquires and returns a free slot. Fails if there's none.
    TSlotPtr AcquireSlot();

    //! Releases the slot.
    void ReleaseSlot(TSlotPtr slot);

    int GetSlotCount() const;

    //! Invoker used for heavy job preparation.
    IInvokerPtr GetBackgroundInvoker(int slotIndex) const;

private:
    const TSlotManagerConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    std::vector<TSlotPtr> Slots_;
    std::vector<int> SlotPathCounters_;

    NConcurrency::TActionQueuePtr SlotsQueue_ = New<NConcurrency::TActionQueue>("Slots");
    NConcurrency::TActionQueuePtr BackgroundQueue_ = New<NConcurrency::TActionQueue>("SlotsBackground");

    bool IsEnabled_ = true;
};

DEFINE_REFCOUNTED_TYPE(TSlotManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode
} // namespace NYT
