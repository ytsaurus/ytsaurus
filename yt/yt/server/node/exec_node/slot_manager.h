#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/node/job_agent/job.h>

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESlotManagerAlertType,
    ((GenericPersistentError)           (0))
    ((GpuCheckFailed)                   (1))
    ((TooManyConsecutiveJobAbortions)   (2))
    ((JobProxyUnavailable)              (3))
    ((TooManyConsecutiveGpuJobFailures) (4))
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
        IBootstrap* bootstrap);

    //! Initializes slots etc.
    void Initialize();

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& oldNodeConfig,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig);

    //! Acquires a free slot, throws on error.
    ISlotPtr AcquireSlot(NScheduler::NProto::TDiskRequest diskRequest, double requestedCpu, bool allowCpuIdlePolicy);

    class TSlotGuard
    {
    public:
        explicit TSlotGuard(TSlotManagerPtr slotManager, ESlotType slotType, double requestedCpu);
        ~TSlotGuard();

        int GetSlotIndex() const;
        ESlotType GetSlotType() const;

    private:
        const TSlotManagerPtr SlotManager_;
        const ESlotType SlotType_;
        const int SlotIndex_;
        const double RequestedCpu_;
    };
    std::unique_ptr<TSlotGuard> AcquireSlot(ESlotType slotType, double requestedCpu);

    int GetSlotCount() const;
    int GetUsedSlotCount() const;

    bool IsInitialized() const;
    bool IsEnabled() const;
    bool HasFatalAlert() const;

    void ResetAlert(ESlotManagerAlertType alertType);

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

    static bool IsResettableAlertType(ESlotManagerAlertType alertType);

private:
    const TSlotManagerConfigPtr Config_;
    IBootstrap* const Bootstrap_;
    const int SlotCount_;
    const TString NodeTag_;

    std::atomic_bool Initialized_ = false;
    std::atomic_bool JobProxyReady_ = false;

    TAtomicObject<TSlotManagerDynamicConfigPtr> DynamicConfig_;

    TAtomicObject<IVolumeManagerPtr> RootVolumeManager_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, LocationsLock_);
    std::vector<TSlotLocationPtr> Locations_;
    std::vector<TSlotLocationPtr> AliveLocations_;

    IJobEnvironmentPtr JobEnvironment_;

    THashSet<int> FreeSlots_;
    int UsedIdleSlotCount_ = 0;

    double IdlePolicyRequestedCpu_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TEnumIndexedVector<ESlotManagerAlertType, TError> Alerts_;

    //! If we observe too many consecutive aborts, we disable user slots on
    //! the node until restart or alert reset.
    int ConsecutiveAbortedJobCount_ = 0;

    //! If we observe too many consecutive GPU job failures, we disable user slots on
    //! the node until restart or alert reset.
    int ConsecutiveFailedGpuJobCount_ = 0;

    int DefaultMediumIndex_ = NChunkClient::DefaultSlotsMediumIndex;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    bool HasSlotDisablingAlert() const;
    double GetIdleCpuFraction() const;

    void AsyncInitialize();

    int DoAcquireSlot(ESlotType slotType);
    void ReleaseSlot(ESlotType slotType, int slotIndex, double requestedCpu);

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
    void ResetConsecutiveFailedGpuJobCount();
    void PopulateAlerts(std::vector<TError>* alerts);
};

DEFINE_REFCOUNTED_TYPE(TSlotManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
