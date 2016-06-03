#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

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

    //! Acquires a free slot, thows on error.
    ISlotPtr AcquireSlot();

    void ReleaseSlot(int slotIndex);

    int GetSlotCount() const;

    bool IsEnabled() const;

private:
    const TSlotManagerConfigPtr Config_;
    const NCellNode::TBootstrap* Bootstrap_;
    const Stroka NodeTag_;

    NConcurrency::TActionQueuePtr LocationQueue_;
    std::vector<TSlotLocationPtr> Locations_;
    std::vector<TSlotLocationPtr> AliveLocations_;

    IJobEnvironmentPtr JobEnviroment_;

    TSpinLock SpinLock_;
    yhash_set<int> FreeSlots_;
    int SlotCount_ = 0;

    void UpdateAliveLocations();
};

DEFINE_REFCOUNTED_TYPE(TSlotManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode
} // namespace NYT
