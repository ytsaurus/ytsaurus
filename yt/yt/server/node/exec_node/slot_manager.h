#pragma once

#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESlotManagerAlertType,
    ((GenericPersistentError)           (0))
    ((GpuCheckFailed)                   (1))
    ((TooManyConsecutiveJobAbortions)   (2))
    ((JobProxyUnavailable)              (3))
    ((TooManyConsecutiveGpuJobFailures) (4))
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESlotManagerState,
    ((Disabled)                    (0))
    ((Disabling)                   (1))
    ((Initialized)                 (2))
    ((Initializing)                (3))
);

////////////////////////////////////////////////////////////////////////////////

struct TNumaNodeState
{
    TNumaNodeInfo NumaNodeInfo;
    double FreeCpuCount;
};

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
    DEFINE_SIGNAL(void(), Disabled);

public:
    explicit TSlotManager(IBootstrap* bootstrap);

    //! Initializes slots etc.
    void Initialize();

    void Start();

    TFuture<void> InitializeEnvironment();

    void OnDynamicConfigChanged(
        const TSlotManagerDynamicConfigPtr& oldConfig,
        const TSlotManagerDynamicConfigPtr& newConfig);

    //! Acquires a free slot, throws on error.
    IUserSlotPtr AcquireSlot(NScheduler::NProto::TDiskRequest diskRequest, NScheduler::NProto::TCpuRequest cpuRequest);

    class TSlotGuard
    {
    public:
        TSlotGuard(
            TSlotManagerPtr slotManager,
            ESlotType slotType,
            double requestedCpu,
            std::optional<i64> numaNodeIdAffinity);
        ~TSlotGuard();

    private:
        const TSlotManagerPtr SlotManager_;
        const double RequestedCpu_;
        const std::optional<i64> NumaNodeIdAffinity_;

        DEFINE_BYVAL_RO_PROPERTY(ESlotType, SlotType);
        DEFINE_BYVAL_RO_PROPERTY(int, SlotIndex);
    };
    std::unique_ptr<TSlotGuard> AcquireSlot(
        ESlotType slotType,
        double requestedCpu,
        const std::optional<TNumaNodeInfo>& numaNodeAffinity);

    int GetSlotCount() const;
    int GetUsedSlotCount() const;

    i64 GetMajorPageFaultCount() const;

    bool IsInitialized() const;
    bool IsEnabled() const;
    bool HasFatalAlert() const;

    void ResetAlerts(const std::vector<ESlotManagerAlertType>& alertTypes);

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
    bool Disable(const TError& error);

    /*!
     *  \note
     *  Thread affinity: any
     */
    void OnGpuCheckCommandFailed(const TError& error);

    /*!
     *  \note
     *  Thread affinity: any
     */
    NYTree::IYPathServicePtr GetOrchidService() const;

    /*!
     *  \note
     *  Thread affinity: any
     */
    void InitMedia(const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

    /*!
     *  \note
     *  Thread affinity: any
     */
    NYTree::INodePtr GetJobEnvironmentConfig() const;

    /*!
     *  \note
     *  Thread affinity: any
     */
    bool ShouldSetUserId() const;

    bool IsJobEnvironmentResurrectionEnabled();

    static bool IsResettableAlertType(ESlotManagerAlertType alertType);

private:
    IBootstrap* const Bootstrap_;
    const TSlotManagerConfigPtr StaticConfig_;
    TAtomicIntrusivePtr<TSlotManagerDynamicConfig> DynamicConfig_;

    const int SlotCount_;
    const TString NodeTag_;
    const NContainers::TPortoHealthCheckerPtr PortoHealthChecker_;

    TBackoffStrategy DisableJobsBackoffStrategy_;

    std::atomic<ESlotManagerState> State_ = ESlotManagerState::Disabled;

    std::atomic<bool> JobProxyReady_ = false;

    TAtomicIntrusivePtr<IVolumeManager> RootVolumeManager_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, LocationsLock_);
    std::vector<TSlotLocationPtr> Locations_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, AliveLocationsLock_);
    std::vector<TSlotLocationPtr> AliveLocations_;

    std::vector<TNumaNodeState> NumaNodeStates_;

    IJobEnvironmentPtr JobEnvironment_;

    //! We maintain queue for distributing job logs evenly among slots.
    TRingQueue<int> FreeSlots_;
    int UsedIdleSlotCount_ = 0;

    double IdlePolicyRequestedCpu_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, AlertsLock_);
    TEnumIndexedArray<ESlotManagerAlertType, TError> Alerts_;

    //! If we observe too many consecutive aborts, we disable user slots on
    //! the node until restart or alert reset.
    int ConsecutiveAbortedSchedulerJobCount_ = 0;

    //! If we observe too many consecutive GPU job failures, we disable user slots on
    //! the node until restart or alert reset.
    int ConsecutiveFailedGpuJobCount_ = 0;

    int DefaultMediumIndex_ = NChunkClient::DefaultSlotsMediumIndex;

    struct TSlotManagerInfo
    {
        int SlotCount;
        int FreeSlotCount;
        int UsedIdleSlotCount;

        double IdlePolicyRequestedCpu;

        std::vector<TNumaNodeState> NumaNodeStates;

        TEnumIndexedArray<ESlotManagerAlertType, TError> Alerts;
    };

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    TDuration GetDisableJobsBackoff();

    void VerifyCurrentState(ESlotManagerState expectedState) const;

    TSlotManagerInfo DoGetStateSnapshot() const;
    auto GetStateSnapshot() const;

    bool HasNonFatalAlerts() const;
    bool HasGpuAlerts() const;
    bool HasSlotDisablingAlert() const;

    void SetDisableState();
    bool CanResurrect() const;

    double GetIdleCpuFraction() const;

    bool EnableNumaNodeScheduling() const;

    void OnPortoHealthCheckSuccess();
    void OnPortoHealthCheckFailed(const TError& result);

    void ForceInitialize();
    void AsyncInitialize();

    int DoAcquireSlot(ESlotType slotType);
    void ReleaseSlot(
        ESlotType slotType,
        int slotIndex,
        double requestedCpu,
        const std::optional<i64>& numaNodeIdAffinity);

    /*!
     *  \note
     *  Thread affinity: any
     */
    void OnJobFinished(const TJobPtr& job);

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

    void BuildOrchid(NYson::IYsonConsumer* consumer) const;
};

DEFINE_REFCOUNTED_TYPE(TSlotManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
