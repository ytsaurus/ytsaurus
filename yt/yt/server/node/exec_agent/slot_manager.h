#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

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
/*!
 *  \note
 *  Thread affinity: Job (unless noted otherwise)
 */
class TSlotManager
    : public TRefCounted
{
public:
    TSlotManager(
        TSlotManagerConfigPtr config,
        NClusterNode::TBootstrap* bootstrap);

    //! Initializes slots etc.
    void Initialize();

    //! Acquires a free slot, thows on error.
    ISlotPtr AcquireSlot(NScheduler::NProto::TDiskRequest diskRequest);

    void ReleaseSlot(int slotIndex);

    int GetSlotCount() const;
    int GetUsedSlotCount() const;

    bool IsEnabled() const;

    NNodeTrackerClient::NProto::TDiskResources GetDiskResources();

    /*!
     *  \note
     *  Thread affinity: any
     */
    void Disable(const TError& error);

    /*!
     *  \note
     *  Thread affinity: any
     */
    void BuildOrchidYson(NYTree::TFluentMap fluent) const;

    /*!
     *  \note
     *  Thread affinity: any
     */
    void InitMedia(const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

private:
    const TSlotManagerConfigPtr Config_;
    NClusterNode::TBootstrap* const Bootstrap_;
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
    //! If we observe too many consecutive aborts, we disable user slots on
    //! the node until restart and fire alert.
    int ConsecutiveAbortedJobCount_ = 0;


    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    void OnJobFinished(const NJobAgent::IJobPtr& job);
    void OnJobsCpuLimitUpdated();
    void UpdateAliveLocations();
    void ResetTransientAlert();
    void PopulateAlerts(std::vector<TError>* alerts);
};

DEFINE_REFCOUNTED_TYPE(TSlotManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
