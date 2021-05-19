#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/node/job_agent/job.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESlotManagerAlertType,
    ((GenericPersistentError)         (0))
    ((GpuCheckFailed)                 (1))
    ((TooManyConsecutiveJobAbortions) (2))
    ((JobProxyUnavailable)            (3))
)

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

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& oldNodeConfig,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig);

    //! Acquires a free slot, thows on error.
    ISlotPtr AcquireSlot(NScheduler::NProto::TDiskRequest diskRequest);

    void ReleaseSlot(int slotIndex);

    int GetSlotCount() const;
    int GetUsedSlotCount() const;

    bool IsEnabled() const;
    bool HasFatalAlert() const;

    NNodeTrackerClient::NProto::TDiskResources GetDiskResources();

    /*!
     *  \note
     *  Thread affinity: any
     */
    std::vector<TSlotLocationPtr> GetLocations() const;

    /*!
     *  \note
     *  Thread affinity: any
     */
    void Disable(const TError& error);

    /*!
     *  \note
     *  Thread affinity: any
     */
    void OnGpuCheckCommandFailed(const TError& error);

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

    //! Returns event which is set when initial value of "JobProxyUnavailable" alert
    //! is consistent (i.e. when OnJobProxyBuildInfoUpdated is called for the first time).
    TFuture<void> GetJobProxyBuildInfoReadyEvent() const;

private:
    const TSlotManagerConfigPtr Config_;
    NClusterNode::TBootstrap* const Bootstrap_;
    const int SlotCount_;
    const TString NodeTag_;

    TAtomicObject<TSlotManagerDynamicConfigPtr> DynamicConfig_;

    NDataNode::IVolumeManagerPtr RootVolumeManager_;

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, LocationsLock_);
    std::vector<TSlotLocationPtr> Locations_;
    std::vector<TSlotLocationPtr> AliveLocations_;

    IJobEnvironmentPtr JobEnvironment_;

    THashSet<int> FreeSlots_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    TEnumIndexedVector<ESlotManagerAlertType, TError> Alerts_;
    //! If we observe too many consecutive aborts, we disable user slots on
    //! the node until restart and fire alert.
    int ConsecutiveAbortedJobCount_ = 0;

    int DefaultMediumIndex_ = NChunkClient::DefaultSlotsMediumIndex;

    //! Sets once at the beginning of the program (namely, during initialization stage of the node)
    //! to make sure we start with a correct understanding of current job proxy status. This eliminates
    //! the possibility of the first job being scheduled to this node before we find out there is no
    //! ytserver-job-proxy binary, which may theoretically happen since job proxy is prepared asynchronously
    //! by ytcfgen.
    TPromise<void> JobProxyBuildInfoReadyEvent_ = NewPromise<void>();

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    bool HasSlotDisablingAlert() const;

    /*!
     *  \note
     *  Thread affinity: any
     */
    void OnJobFinished(const NJobAgent::IJobPtr& job);

    /*!
     *  \note
     *  Thread affinity: any
     */
    void OnJobProxyBuildInfoUpdated(const TError& error);

    void OnJobsCpuLimitUpdated();
    void UpdateAliveLocations();
    void ResetConsecutiveAbortedJobCount();
    void PopulateAlerts(std::vector<TError>* alerts);
};

DEFINE_REFCOUNTED_TYPE(TSlotManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
