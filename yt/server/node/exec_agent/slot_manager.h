#pragma once

#include "public.h"

#include <yt/server/node/cell_node/public.h>

#include <yt/server/node/data_node/public.h>

#include <yt/server/node/job_agent/job.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/fs.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NExecAgent {

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
    ISlotPtr AcquireSlot(i64 diskSpaceRequest);

    void ReleaseSlot(int slotIndex);

    int GetSlotCount() const;
    int GetUsedSlotCount() const;

    bool IsEnabled() const;

    void UpdateCpuLimit(double cpuLimit);

    NNodeTrackerClient::NProto::TDiskResources GetDiskResources();

    void OnJobFinished(EJobState jobState);

    void Disable(const TError& error );

    void BuildOrchidYson(NYTree::TFluentMap fluent) const;

private:
    const TSlotManagerConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;
    const int SlotCount_;
    const TString NodeTag_;

    NDataNode::IVolumeManagerPtr RootVolumeManager_;

    std::vector<TSlotLocationPtr> Locations_;
    std::vector<TSlotLocationPtr> AliveLocations_;

    IJobEnvironmentPtr JobEnvironment_;

    THashSet<int> FreeSlots_;

    TSpinLock SpinLock_;
    std::optional<TError> PersistentAlert_;
    std::optional<TError> TransientAlert_;

    //! If we observe too much consecutive aborts, we disable user slots on
    //! the node until restart and fire alert.
    std::atomic<int> ConsecutiveAbortedJobCount_ = {0};


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void UpdateAliveLocations();
    void ResetTransientAlert();
    void PopulateAlerts(std::vector<TError>* alerts);
};

DEFINE_REFCOUNTED_TYPE(TSlotManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
