#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/job_agent/job.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/fs.h>

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

    TNullable<i64> GetMemoryLimit() const;

    TNullable<i64> GetCpuLimit() const;

    bool ExternalJobMemory() const;

    NNodeTrackerClient::NProto::TDiskResources GetDiskInfo();

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

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void UpdateAliveLocations();
};

DEFINE_REFCOUNTED_TYPE(TSlotManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
