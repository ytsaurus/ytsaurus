#pragma once

#include "public.h"

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

    ~TSlotManager();

    //! Initializes slots etc.
    void Initialize(int slotCount);

    //! Acquires and returns a free slot. Fails if there's none.
    TSlotPtr AcquireSlot();

    //! Releases the slot.
    void ReleaseSlot(TSlotPtr slot);

    int GetSlotCount() const;

private:
    TSlotManagerConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    std::vector<TSlotPtr> Slots_;
    std::vector<int> SlotPathCounters_;

    NConcurrency::TActionQueuePtr ActionQueue_;

    bool IsEnabled_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode
} // namespace NYT
