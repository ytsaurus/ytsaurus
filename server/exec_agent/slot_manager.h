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
    void Initialize();

    //! Acquires a free slot, thows on error.
    ISlotPtr AcquireSlot();

    void ReleaseSlot(int slotIndex);

    int GetSlotCount() const;

    bool IsEnabled() const;

private:
    const TSlotManagerConfigPtr Config_;
    const NCellNode::TBootstrap* Bootstrap_;
    const int SlotCount_;
    const TString NodeTag_;

    std::vector<TSlotLocationPtr> Locations_;
    std::vector<TSlotLocationPtr> AliveLocations_;

    IJobEnvironmentPtr JobEnvironment_;

    TSpinLock SpinLock_;
    THashSet<int> FreeSlots_;

    bool JobProxySocketNameDirectoryCreated_ = false;

    void UpdateAliveLocations();
};

DEFINE_REFCOUNTED_TYPE(TSlotManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
