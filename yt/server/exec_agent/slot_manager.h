#pragma once

#include "public.h"
// TODO(babenko): remove after switching to refcounted macros
#include "slot.h"

#include <server/cell_node/public.h>

#include <core/concurrency/public.h>

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

private:
    const TSlotManagerConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    std::vector<TSlotPtr> Slots_;
    std::vector<int> SlotPathCounters_;

    NConcurrency::TActionQueuePtr ActionQueue_ = New<NConcurrency::TActionQueue>("ExecSlots");

    bool IsEnabled_ = true;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode
} // namespace NYT
