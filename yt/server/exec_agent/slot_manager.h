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

    int GetSlotCount() const;

private:
    TSlotManagerConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;

    std::vector<TSlotPtr> Slots;

    NConcurrency::TActionQueuePtr ActionQueue;

    bool IsEnabled;

};

DEFINE_REFCOUNTED_TYPE(TSlotManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode
} // namespace NYT
