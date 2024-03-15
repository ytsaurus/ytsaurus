#include "slot_manager.h"

#include "bootstrap.h"
#include "chunk_cache.h"
#include "job.h"
#include "job_controller.h"
#include "private.h"
#include "slot.h"
#include "job_environment.h"
#include "slot_location.h"
#include "volume_manager.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/library/containers/porto_health_checker.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NExecNode {

using namespace NContainers;
using namespace NClusterNode;
using namespace NChunkClient;
using namespace NDataNode;
using namespace NExecNode;
using namespace NJobAgent;
using namespace NControllerAgent;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TSlotManager::TSlotManager(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , StaticConfig_(Bootstrap_->GetConfig()->ExecNode->SlotManager)
    , DynamicConfig_(New<TSlotManagerDynamicConfig>())
    , SlotCount_(Bootstrap_->GetConfig()->JobResourceManager->ResourceLimits->UserSlots)
    , NodeTag_(Format("yt-node-%v-%v", Bootstrap_->GetConfig()->RpcPort, GetCurrentProcessId()))
    , PortoHealthChecker_(New<TPortoHealthChecker>(
        New<TPortoExecutorDynamicConfig>(),
        Bootstrap_->GetControlInvoker(),
        Logger))
    , DisableJobsBackoffStrategy_(DynamicConfig_.Acquire()->DisableJobsBackoffStrategy)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    YT_VERIFY(StaticConfig_);
}

bool TSlotManager::IsJobEnvironmentResurrectionEnabled()
{
    return DynamicConfig_.Acquire()
        ->EnableJobEnvironmentResurrection;
}

void TSlotManager::OnPortoHealthCheckSuccess()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (IsJobEnvironmentResurrectionEnabled() &&
        CanResurrect())
    {
        YT_LOG_INFO("Porto health check succeeded, try to resurrect slot manager");

        YT_VERIFY(Bootstrap_->IsExecNode());

        auto result = WaitFor(InitializeEnvironment());

        YT_LOG_ERROR_IF(!result.IsOK(), result, "Resurrection failed with error");
    }
}

void TSlotManager::OnPortoHealthCheckFailed(const TError& result)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (IsJobEnvironmentResurrectionEnabled() && IsEnabled()) {
        YT_LOG_INFO("Porto health check failed, disable slot manager");

        YT_VERIFY(Bootstrap_->IsExecNode());

        Disable(result);
    }
}

void TSlotManager::Initialize()
{
    VERIFY_THREAD_AFFINITY_ANY();

    Bootstrap_->SubscribePopulateAlerts(
        BIND_NO_PROPAGATE(&TSlotManager::PopulateAlerts, MakeStrong(this)));
    Bootstrap_->GetJobController()->SubscribeJobFinished(
        BIND_NO_PROPAGATE(&TSlotManager::OnJobFinished, MakeStrong(this))
            .Via(Bootstrap_->GetJobInvoker()));
    Bootstrap_->GetJobController()->SubscribeJobProxyBuildInfoUpdated(
        BIND_NO_PROPAGATE(&TSlotManager::OnJobProxyBuildInfoUpdated, MakeStrong(this)));
    Bootstrap_->GetNodeResourceManager()->SubscribeJobsCpuLimitUpdated(
        BIND_NO_PROPAGATE(&TSlotManager::OnJobsCpuLimitUpdated, MakeWeak(this))
            .Via(Bootstrap_->GetJobInvoker()));

    auto environmentConfig = NYTree::ConvertTo<TJobEnvironmentConfigPtr>(StaticConfig_->JobEnvironment);

    if (environmentConfig->Type == EJobEnvironmentType::Porto) {
        PortoHealthChecker_->SubscribeSuccess(BIND(&TSlotManager::OnPortoHealthCheckSuccess, MakeStrong(this))
            .Via(Bootstrap_->GetJobInvoker()));
        PortoHealthChecker_->SubscribeFailed(BIND(&TSlotManager::OnPortoHealthCheckFailed, MakeStrong(this))
            .Via(Bootstrap_->GetJobInvoker()));
    }
}

void TSlotManager::Start()
{
    auto initializeResult = WaitFor(BIND([=, this, this_ = MakeStrong(this)] () {
        VERIFY_THREAD_AFFINITY(JobThread);

        for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
            FreeSlots_.push(slotIndex);
        }

        YT_UNUSED_FUTURE(InitializeEnvironment());
    })
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run());

    YT_LOG_FATAL_IF(!IsJobEnvironmentResurrectionEnabled() &&
        !initializeResult.IsOK(), initializeResult, "First slot manager initialization failed");
    YT_LOG_ERROR_IF(!initializeResult.IsOK(), initializeResult, "First slot manager initialization failed");

    auto environmentConfig = NYTree::ConvertTo<TJobEnvironmentConfigPtr>(StaticConfig_->JobEnvironment);

    if (environmentConfig->Type == EJobEnvironmentType::Porto) {
        PortoHealthChecker_->Start();
    }
}

TFuture<void> TSlotManager::InitializeEnvironment()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto expected = ESlotManagerState::Disabled;

    if (!State_.compare_exchange_strong(expected, ESlotManagerState::Initializing)) {
        auto error = TError(
            "Slot manager expects other state (Expected: %v, Actual: %v)",
            ESlotManagerState::Disabled,
            expected);
        YT_LOG_WARNING(error);
        return MakeFuture(error);
    }

    YT_LOG_INFO("Slot manager sync initialization started (SlotCount: %v)",
        SlotCount_);

    {
        auto guard = WriterGuard(AliveLocationsLock_);
        AliveLocations_.clear();
    }

    {
        auto guard = WriterGuard(AlertsLock_);
        Alerts_ = {};
    }

    JobEnvironment_ = CreateJobEnvironment(
        StaticConfig_->JobEnvironment,
        Bootstrap_);
    JobEnvironment_->OnDynamicConfigChanged(
        Bootstrap_->GetDynamicConfig()->ExecNode->SlotManager,
        Bootstrap_->GetDynamicConfig()->ExecNode->SlotManager);

    // Job environment must be initialized first, since it cleans up all the processes,
    // which may hold open descriptors to volumes, layers and files in sandboxes.
    // It should also be initialized synchronously, since it may prevent deletion of chunk cache artifacts.
    JobEnvironment_->Init(
        SlotCount_,
        Bootstrap_->GetConfig()->JobResourceManager->ResourceLimits->Cpu,
        GetIdleCpuFraction());

    if (!JobEnvironment_->IsEnabled()) {
        auto error = TError("Job environment is disabled");
        YT_LOG_WARNING(error);

        SetDisableState();

        return MakeFuture(error);
    }

    {
        auto guard = WriterGuard(LocationsLock_);
        Locations_.clear();
    }

    {
        int locationIndex = 0;
        for (const auto& locationConfig : StaticConfig_->Locations) {
            auto newLocation = New<TSlotLocation>(
                std::move(locationConfig),
                Bootstrap_,
                Format("slot%v", locationIndex),
                JobEnvironment_->CreateJobDirectoryManager(locationConfig->Path, locationIndex),
                SlotCount_,
                BIND_NO_PROPAGATE(&IJobEnvironment::GetUserId, JobEnvironment_));

            auto guard = WriterGuard(LocationsLock_);
            Locations_.push_back(std::move(newLocation));
            ++locationIndex;
        }
    }

    YT_LOG_INFO("Slot manager sync initialization finished");

    return BIND(&TSlotManager::AsyncInitialize, MakeStrong(this))
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run()
        .Apply(BIND(&TSlotManager::InitMedia, MakeStrong(this), Bootstrap_->GetClient()->GetNativeConnection()->GetMediumDirectory())
        .AsyncVia(Bootstrap_->GetJobInvoker()));
}

void TSlotManager::OnDynamicConfigChanged(
    const TSlotManagerDynamicConfigPtr& oldConfig,
    const TSlotManagerDynamicConfigPtr& newConfig)
{
    VERIFY_THREAD_AFFINITY_ANY();

    DynamicConfig_.Store(newConfig);

    auto environmentConfig = NYTree::ConvertTo<TJobEnvironmentConfigPtr>(newConfig->JobEnvironment);

    if (environmentConfig->Type == EJobEnvironmentType::Porto) {
        auto portoEnvironmentConfig = NYTree::ConvertTo<TPortoJobEnvironmentConfigPtr>(newConfig->JobEnvironment);
        PortoHealthChecker_->OnDynamicConfigChanged(portoEnvironmentConfig->PortoExecutor);
    }

    Bootstrap_->GetJobInvoker()->Invoke(
        BIND([
            oldConfig,
            newConfig,
            this,
            this_ = MakeStrong(this)
        ] {
            DisableJobsBackoffStrategy_.UpdateOptions(newConfig->DisableJobsBackoffStrategy);
            if (JobEnvironment_) {
                try {
                    JobEnvironment_->OnDynamicConfigChanged(oldConfig, newConfig);
                } catch (const std::exception& ex) {
                    YT_LOG_ERROR(TError(ex));
                }
            }
        }));
}

void TSlotManager::UpdateAliveLocations()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto guard = WriterGuard(AliveLocationsLock_);

    AliveLocations_.clear();
    for (const auto& location : Locations_) {
        if (location->IsEnabled()) {
            AliveLocations_.push_back(location);
        }
    }
}

IUserSlotPtr TSlotManager::AcquireSlot(NScheduler::NProto::TDiskRequest diskRequest, NScheduler::NProto::TCpuRequest cpuRequest)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (!IsEnabled()) {
        THROW_ERROR_EXCEPTION(EErrorCode::SchedulerJobsDisabled, "Slot manager disabled");
    }

    UpdateAliveLocations();

    int feasibleLocationCount = 0;
    int skippedByDiskSpace = 0;
    int skippedByMedium = 0;
    TSlotLocationPtr bestLocation;

    auto guard = ReaderGuard(AliveLocationsLock_);

    for (const auto& location : AliveLocations_) {
        auto diskResources = location->GetDiskResources();
        if (diskResources.usage() + diskRequest.disk_space() > diskResources.limit()) {
            ++skippedByDiskSpace;
            continue;
        }

        if (diskRequest.has_medium_index()) {
            if (diskResources.medium_index() != diskRequest.medium_index()) {
                ++skippedByMedium;
                continue;
            }
        } else {
            if (diskResources.medium_index() != DefaultMediumIndex_) {
                ++skippedByMedium;
                continue;
            }
        }

        ++feasibleLocationCount;

        if (!bestLocation || bestLocation->GetSessionCount() > location->GetSessionCount()) {
            bestLocation = location;
        }
    }

    if (!bestLocation) {
        THROW_ERROR_EXCEPTION(EErrorCode::SlotNotFound, "No feasible slot found")
            << TErrorAttribute("alive_location_count", AliveLocations_.size())
            << TErrorAttribute("feasible_location_count", feasibleLocationCount)
            << TErrorAttribute("skipped_by_disk_space", skippedByDiskSpace)
            << TErrorAttribute("skipped_by_medium", skippedByMedium);
    }

    auto slotType = ESlotType::Common;
    if (cpuRequest.allow_idle_cpu_policy() &&
        IdlePolicyRequestedCpu_ + cpuRequest.cpu() <= JobEnvironment_->GetCpuLimit(ESlotType::Idle))
    {
        slotType = ESlotType::Idle;
        IdlePolicyRequestedCpu_ += cpuRequest.cpu();
        ++UsedIdleSlotCount_;
    }

    std::optional<TNumaNodeInfo> numaNodeAffinity;
    if (EnableNumaNodeScheduling() && !NumaNodeStates_.empty()) {
        auto bestNumaNodeIt = std::max_element(
            NumaNodeStates_.begin(),
            NumaNodeStates_.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.FreeCpuCount < rhs.FreeCpuCount;
            }
        );

        if (bestNumaNodeIt->FreeCpuCount >= cpuRequest.cpu()) {
            numaNodeAffinity = bestNumaNodeIt->NumaNodeInfo;
            bestNumaNodeIt->FreeCpuCount -= cpuRequest.cpu();
        }
    }

    return CreateSlot(
        this,
        std::move(bestLocation),
        JobEnvironment_,
        RootVolumeManager_.Acquire(),
        Bootstrap_,
        NodeTag_,
        slotType,
        cpuRequest.cpu(),
        std::move(diskRequest),
        numaNodeAffinity);
}

std::unique_ptr<TSlotManager::TSlotGuard> TSlotManager::AcquireSlot(
    ESlotType slotType,
    double requestedCpu,
    const std::optional<TNumaNodeInfo>& numaNodeAffinity
) {
    VERIFY_THREAD_AFFINITY(JobThread);

    return std::make_unique<TSlotManager::TSlotGuard>(
        this,
        slotType,
        requestedCpu,
        numaNodeAffinity ? std::optional<i64>(numaNodeAffinity->NumaNodeId) : std::nullopt);
}

int TSlotManager::GetSlotCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SlotCount_;
}

int TSlotManager::GetUsedSlotCount() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return SlotCount_ - std::ssize(FreeSlots_);
}

bool TSlotManager::IsInitialized() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return State_.load() == ESlotManagerState::Initialized;
}

bool TSlotManager::IsEnabled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(AliveLocationsLock_);

    bool enabled =
        JobProxyReady_.load() &&
        IsInitialized() &&
        SlotCount_ > 0 &&
        !AliveLocations_.empty() &&
        JobEnvironment_->IsEnabled();

    return enabled && !HasSlotDisablingAlert();
}

bool TSlotManager::HasGpuAlerts() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    VERIFY_SPINLOCK_AFFINITY(AlertsLock_);

    bool disableJobsOnGpuCheckFailure = DynamicConfig_.Acquire()->DisableJobsOnGpuCheckFailure;

    return !Alerts_[ESlotManagerAlertType::TooManyConsecutiveGpuJobFailures].IsOK() ||
        (disableJobsOnGpuCheckFailure && !Alerts_[ESlotManagerAlertType::GpuCheckFailed].IsOK());
}

bool TSlotManager::HasNonFatalAlerts() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    VERIFY_SPINLOCK_AFFINITY(AlertsLock_);

    return
        !Alerts_[ESlotManagerAlertType::TooManyConsecutiveJobAbortions].IsOK() ||
        !Alerts_[ESlotManagerAlertType::JobProxyUnavailable].IsOK() ||
        HasGpuAlerts();
}

TDuration TSlotManager::GetDisableJobsBackoff()
{
    DisableJobsBackoffStrategy_.Next();
    return DisableJobsBackoffStrategy_.GetBackoff();
}

void TSlotManager::VerifyCurrentState(ESlotManagerState expectedState) const
{
    auto currentState = State_.load();
    YT_LOG_FATAL_IF(
        currentState != expectedState,
        "Slot manager state race detected (Expected: %v, Actual: %v)",
        expectedState,
        currentState);
}

TSlotManager::TSlotManagerInfo TSlotManager::DoGetStateSnapshot() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    TEnumIndexedArray<ESlotManagerAlertType, TError> alerts;

    {
        auto guard = ReaderGuard(AlertsLock_);

        alerts = Alerts_;
    }

    return {
        .SlotCount = SlotCount_,
        .FreeSlotCount = static_cast<int>(FreeSlots_.size()),
        .UsedIdleSlotCount = UsedIdleSlotCount_,
        .IdlePolicyRequestedCpu = IdlePolicyRequestedCpu_,
        .NumaNodeStates = NumaNodeStates_,
        .Alerts = std::move(alerts),
    };
}

auto TSlotManager::GetStateSnapshot() const
{
    auto snapshotOrError = WaitFor(BIND(
        &TSlotManager::DoGetStateSnapshot,
        MakeStrong(this))
        .AsyncVia(Bootstrap_->GetJobInvoker())
        .Run());

    YT_LOG_FATAL_IF(
        !snapshotOrError.IsOK(),
        snapshotOrError,
        "Unexpected failure during slot manager info lookup");

    return std::move(snapshotOrError.Value());
}

bool TSlotManager::HasSlotDisablingAlert() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(AlertsLock_);

    return HasFatalAlert() || HasNonFatalAlerts();
}

bool TSlotManager::CanResurrect() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    bool disabled = !IsEnabled();

    auto guard = ReaderGuard(AlertsLock_);

    return disabled &&
        !HasNonFatalAlerts();
}

double TSlotManager::GetIdleCpuFraction() const
{
    return DynamicConfig_.Acquire()->IdleCpuFraction;
}

i64 TSlotManager::GetMajorPageFaultCount() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    if (JobEnvironment_) {
        return JobEnvironment_->GetMajorPageFaultCount();
    } else {
        return 0;
    }
}

bool TSlotManager::EnableNumaNodeScheduling() const
{
    auto dynamicConfig = DynamicConfig_.Acquire();
    return dynamicConfig
        ? dynamicConfig->EnableNumaNodeScheduling
        : false;
}

bool TSlotManager::HasFatalAlert() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(AlertsLock_);

    return !Alerts_[ESlotManagerAlertType::GenericPersistentError].IsOK();
}

void TSlotManager::ForceInitialize()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto expected = ESlotManagerState::Disabled;

    if (!State_.compare_exchange_strong(expected, ESlotManagerState::Initializing)) {
        YT_LOG_WARNING("Slot manager expects other state (Expected: %v, Actual: %v)",
            ESlotManagerState::Disabled,
            expected);
    } else {
        State_.store(ESlotManagerState::Initialized);
    }
}

void TSlotManager::ResetAlerts(const std::vector<ESlotManagerAlertType>& alertTypes)
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        auto guard = WriterGuard(AlertsLock_);

        for (auto alertType : alertTypes) {
            Alerts_[alertType] = {};
        }
    }

    auto needInitialize = !HasSlotDisablingAlert();

    if (!IsInitialized() && needInitialize) {
        SubscribeDisabled(BIND(&TSlotManager::ForceInitialize, MakeWeak(this)));
    }
}

void TSlotManager::OnJobsCpuLimitUpdated()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    try {
        const auto& resourceManager = Bootstrap_->GetNodeResourceManager();
        auto cpuLimit = resourceManager->GetJobsCpuLimit();
        JobEnvironment_->UpdateCpuLimit(cpuLimit);
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Error updating job environment CPU limit");
    }
}

std::vector<TSlotLocationPtr> TSlotManager::GetLocations() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(LocationsLock_);
    return Locations_;
}

void TSlotManager::SetDisableState()
{
    State_.store(ESlotManagerState::Disabled);
    Disabled_.FireAndClear();
}

bool TSlotManager::Disable(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(!error.IsOK());

    auto expected = ESlotManagerState::Initialized;

    if (!State_.compare_exchange_strong(expected, ESlotManagerState::Disabling)) {
        YT_LOG_WARNING(
            "Slot manager expects other state (Expected: %v, Actual: %v)",
            ESlotManagerState::Initialized,
            expected);
        return false;
    }

    {
        auto guard = WriterGuard(AlertsLock_);

        auto wrappedError = TError(EErrorCode::SchedulerJobsDisabled, "Scheduler jobs disabled")
            << error;
        YT_LOG_WARNING(wrappedError, "Disabling slot manager");

        Alerts_[ESlotManagerAlertType::GenericPersistentError] = std::move(wrappedError);
    }

    auto dynamicConfig = DynamicConfig_.Acquire();
    auto timeout = dynamicConfig->SlotReleaseTimeout;

    auto jobsAbortionError = TError("Job aborted due to fatal alert")
        << TErrorAttribute("abort_reason", EAbortReason::NodeWithDisabledJobs);

    const auto& jobController = Bootstrap_->GetJobController();

    if (auto syncResult = WaitFor(jobController->AbortAllJobs(jobsAbortionError).WithTimeout(timeout));
        !syncResult.IsOK())
    {
        YT_LOG_EVENT(
            Logger,
            dynamicConfig->AbortOnFreeSlotSynchronizationFailed ? NLogging::ELogLevel::Fatal : NLogging::ELogLevel::Error,
            syncResult,
            "Free slot synchronization failed");
    }

    if (auto volumeManager = RootVolumeManager_.Acquire()) {
        auto result = WaitFor(volumeManager->GetVolumeReleaseEvent()
            .Apply(BIND(&IVolumeManager::DisableLayerCache, volumeManager, error)
            .AsyncVia(Bootstrap_->GetControlInvoker()))
            .WithTimeout(timeout));
        if (!result.IsOK()) {
            YT_LOG_EVENT(
                Logger,
                dynamicConfig->AbortOnFreeVolumeSynchronizationFailed ? NLogging::ELogLevel::Fatal : NLogging::ELogLevel::Error,
                result,
                "Free volume synchronization failed");
        }
    }

    YT_LOG_WARNING("Disable slot manager finished");

    VerifyCurrentState(ESlotManagerState::Disabling);

    SetDisableState();

    return true;
}

void TSlotManager::OnGpuCheckCommandFailed(const TError& error)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_LOG_WARNING(
        error,
        "GPU check failed alert set, jobs may be disabled if \"disable_jobs_on_gpu_check_failure\" specified");

    {
        auto guard = WriterGuard(AlertsLock_);
        Alerts_[ESlotManagerAlertType::GpuCheckFailed] = error;
    }
}

void TSlotManager::OnJobFinished(const TJobPtr& job)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto guard = WriterGuard(AlertsLock_);
    if (job->GetState() == EJobState::Aborted) {
        ++ConsecutiveAbortedSchedulerJobCount_;
    } else {
        ConsecutiveAbortedSchedulerJobCount_ = 0;
    }

    auto dynamicConfig = DynamicConfig_.Acquire();

    auto setAlert = [&] (ESlotManagerAlertType alertType, TError error, TClosure resetter) {
        if (Alerts_[alertType].IsOK()) {
            auto delay = GetDisableJobsBackoff();

            YT_LOG_WARNING(
                error,
                "Scheduler jobs disabled (Until: %v)",
                TInstant::Now() + delay);
            Alerts_[alertType] = std::move(error);

            TDelayedExecutor::Submit(std::move(resetter), delay, Bootstrap_->GetJobInvoker());
        }
    };

    if (ConsecutiveAbortedSchedulerJobCount_ > dynamicConfig->MaxConsecutiveJobAborts) {
        setAlert(
            ESlotManagerAlertType::TooManyConsecutiveJobAbortions,
            TError("Too many consecutive scheduler job abortions")
                << TErrorAttribute("max_consecutive_job_aborts", dynamicConfig->MaxConsecutiveJobAborts),
            BIND(&TSlotManager::ResetConsecutiveAbortedJobCount, MakeStrong(this)));
    }

    if (job->IsGpuRequested()) {
        if (job->GetState() == EJobState::Failed) {
            ++ConsecutiveFailedGpuJobCount_;
        } else {
            ConsecutiveFailedGpuJobCount_ = 0;
        }


        if (ConsecutiveFailedGpuJobCount_ > dynamicConfig->MaxConsecutiveGpuJobFailures) {
            setAlert(
                ESlotManagerAlertType::TooManyConsecutiveGpuJobFailures,
                TError("Too many consecutive GPU job failures")
                    << TErrorAttribute("max_consecutive_job_aborts", dynamicConfig->MaxConsecutiveGpuJobFailures),
                BIND(&TSlotManager::ResetConsecutiveFailedGpuJobCount, MakeStrong(this)));
        }
    }
}

void TSlotManager::OnJobProxyBuildInfoUpdated(const TError& error)
{
    auto guard = WriterGuard(AlertsLock_);

    // TODO(gritukan): Most likely #IsExecNode condition will not be required after bootstraps split.
    if (!StaticConfig_->Testing->SkipJobProxyUnavailableAlert && Bootstrap_->IsExecNode()) {
        auto& alert = Alerts_[ESlotManagerAlertType::JobProxyUnavailable];

        if (alert.IsOK() && !error.IsOK()) {
            YT_LOG_INFO(error, "Disabling scheduler jobs due to job proxy unavailability");
        } else if (!alert.IsOK() && error.IsOK()) {
            YT_LOG_INFO("Enable scheduler jobs as job proxy became available");
        }

        alert = error;
    }
    JobProxyReady_.store(true);
}

void TSlotManager::ResetConsecutiveAbortedJobCount()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto guard = WriterGuard(AlertsLock_);

    Alerts_[ESlotManagerAlertType::TooManyConsecutiveJobAbortions] = {};
    ConsecutiveAbortedSchedulerJobCount_ = 0;
    DisableJobsBackoffStrategy_.Restart();
}

void TSlotManager::ResetConsecutiveFailedGpuJobCount()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    auto guard = WriterGuard(AlertsLock_);

    Alerts_[ESlotManagerAlertType::TooManyConsecutiveGpuJobFailures] = {};
    ConsecutiveFailedGpuJobCount_ = 0;
    DisableJobsBackoffStrategy_.Restart();
}

void TSlotManager::PopulateAlerts(std::vector<TError>* alerts)
{
    auto guard = WriterGuard(AlertsLock_);

    for (const auto& alert : Alerts_) {
        if (!alert.IsOK()) {
            alerts->push_back(alert);
        }
    }
}

IYPathServicePtr TSlotManager::GetOrchidService() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return IYPathService::FromProducer(BIND_NO_PROPAGATE(
        &TSlotManager::BuildOrchid,
        MakeStrong(this)));
}

void TSlotManager::BuildOrchid(NYson::IYsonConsumer* consumer) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto slotManagerInfo = GetStateSnapshot();

    auto rootVolumeManager = RootVolumeManager_.Acquire();

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("slot_count").Value(slotManagerInfo.SlotCount)
            .Item("free_slot_count").Value(slotManagerInfo.FreeSlotCount)
            .Item("used_idle_slot_count").Value(slotManagerInfo.UsedIdleSlotCount)
            .Item("idle_policy_requested_cpu").Value(slotManagerInfo.IdlePolicyRequestedCpu)
            .Item("numa_node_states").DoMapFor(
                slotManagerInfo.NumaNodeStates,
                [&] (TFluentMap fluent, const TNumaNodeState& numaNodeState) {
                    fluent
                        .Item(Format("node_%v", numaNodeState.NumaNodeInfo.NumaNodeId)).BeginMap()
                            .Item("free_cpu_count").Value(numaNodeState.FreeCpuCount)
                            .Item("cpu_set").Value(numaNodeState.NumaNodeInfo.CpuSet)
                        .EndMap();
                })
            .Item("alerts").DoMapFor(
                TEnumTraits<ESlotManagerAlertType>::GetDomainValues(),
                [&] (TFluentMap fluent, ESlotManagerAlertType alertType) {
                    const auto& error = slotManagerInfo.Alerts[alertType];
                    if (!error.IsOK()) {
                        fluent
                            .Item(FormatEnum(alertType)).Value(error);
                    }
                })
            .DoIf(
                static_cast<bool>(rootVolumeManager),
                [rootVolumeManager = std::move(rootVolumeManager)](TFluentMap fluent){
                    fluent
                        .Item("root_volume_manager").Do(std::bind(
                            &IVolumeManager::BuildOrchid,
                            rootVolumeManager,
                            std::placeholders::_1));
                })
        .EndMap();
}

void TSlotManager::InitMedia(const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(LocationsLock_);

    for (const auto& location : Locations_) {
        auto oldDescriptor = location->GetMediumDescriptor();
        auto newDescriptor = mediumDirectory->FindByName(location->GetMediumName());
        if (!newDescriptor) {
            THROW_ERROR_EXCEPTION("Location %Qv refers to unknown medium %Qv",
                location->GetId(),
                location->GetMediumName());
        }
        if (oldDescriptor.Index != NChunkClient::GenericMediumIndex &&
            oldDescriptor.Index != newDescriptor->Index)
        {
            THROW_ERROR_EXCEPTION("Medium %Qv has changed its index from %v to %v",
                location->GetMediumName(),
                oldDescriptor.Index,
                newDescriptor->Index);
        }
        location->SetMediumDescriptor(*newDescriptor);
        location->InvokeUpdateDiskResources();
    }

    {
        auto defaultMediumName = StaticConfig_->DefaultMediumName;
        auto descriptor = mediumDirectory->FindByName(defaultMediumName);
        if (!descriptor) {
            THROW_ERROR_EXCEPTION("Default medium is unknown (MediumName: %v)",
                defaultMediumName);
        }
        DefaultMediumIndex_ = descriptor->Index;
    }
}

NYTree::INodePtr TSlotManager::GetJobEnvironmentConfig() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return StaticConfig_->JobEnvironment;
}

bool TSlotManager::ShouldSetUserId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return !StaticConfig_->DoNotSetUserId;
}

bool TSlotManager::IsResettableAlertType(ESlotManagerAlertType alertType)
{
    return
        alertType == ESlotManagerAlertType::GpuCheckFailed ||
        alertType == ESlotManagerAlertType::TooManyConsecutiveJobAbortions ||
        alertType == ESlotManagerAlertType::TooManyConsecutiveGpuJobFailures;
}

void TSlotManager::AsyncInitialize()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    try {
        YT_LOG_INFO("Slot manager async initialization started");

        std::vector<TFuture<void>> initLocationFutures;
        for (const auto& location : Locations_) {
            initLocationFutures.push_back(location->Initialize());
        }

        YT_LOG_INFO("Waiting for all locations to initialize");
        auto initResult = WaitFor(AllSet(initLocationFutures));
        YT_LOG_INFO("Locations initialization finished");

        if (!initResult.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to initialize slot locations")
                << initResult;
        }

        // To this moment all old processed must have been killed, so we can safely clean up old volumes
        // during root volume manager initialization.
        auto environmentConfig = NYTree::ConvertTo<TJobEnvironmentConfigPtr>(StaticConfig_->JobEnvironment);
        if (environmentConfig->Type == EJobEnvironmentType::Porto) {
            auto volumeManagerOrError = WaitFor(CreatePortoVolumeManager(
                Bootstrap_->GetConfig()->DataNode,
                Bootstrap_->GetDynamicConfigManager(),
                CreateVolumeChunkCacheAdapter(Bootstrap_->GetChunkCache()),
                Bootstrap_->GetControlInvoker(),
                Bootstrap_->GetMemoryUsageTracker()->WithCategory(EMemoryCategory::TmpfsLayers),
                Bootstrap_));
            if (volumeManagerOrError.IsOK()) {
                RootVolumeManager_.Store(volumeManagerOrError.Value());
            } else {
                THROW_ERROR_EXCEPTION("Failed to initialize volume manager")
                    << volumeManagerOrError;
            }
        }

        auto dynamicConfig = DynamicConfig_.Acquire();
        auto timeout = dynamicConfig->SlotReleaseTimeout;
        auto slotSync = WaitFor(Bootstrap_->GetJobController()->GetAllJobsCleanupFinishedFuture()
            .WithTimeout(timeout));

        YT_LOG_FATAL_IF(!slotSync.IsOK(), slotSync, "Slot synchronization failed");
        YT_LOG_FATAL_IF(std::ssize(FreeSlots_) != SlotCount_,
            "Some slots are still acquired (FreeSlots: %v, SlotCount: %v)",
            std::ssize(FreeSlots_),
            SlotCount_);

        NumaNodeStates_.clear();

        for (const auto& numaNode : StaticConfig_->NumaNodes) {
            NumaNodeStates_.push_back(TNumaNodeState{
                .NumaNodeInfo = TNumaNodeInfo{
                    .NumaNodeId = numaNode->NumaNodeId,
                    .CpuSet = numaNode->CpuSet
                },
                .FreeCpuCount = static_cast<double>(numaNode->CpuCount),
            });
        }

        UpdateAliveLocations();

        VerifyCurrentState(ESlotManagerState::Initializing);

        YT_LOG_INFO("Slot manager async initialization finished");
        State_.store(ESlotManagerState::Initialized);
    } catch (const std::exception& ex) {
        auto wrappedError = TError(EErrorCode::SchedulerJobsDisabled, "Initialization failed")
            << ex;

        YT_LOG_WARNING(wrappedError, "Initialization failed");

        {
            auto guard = WriterGuard(AlertsLock_);
            Alerts_[ESlotManagerAlertType::GenericPersistentError] = std::move(wrappedError);
        }

        SetDisableState();
    }
}

int TSlotManager::DoAcquireSlot(ESlotType slotType)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(!FreeSlots_.empty());
    int slotIndex = FreeSlots_.front();
    FreeSlots_.pop();

    YT_LOG_DEBUG("Exec slot acquired (SlotType: %v, SlotIndex: %v)",
        slotType,
        slotIndex);

    return slotIndex;
}

void TSlotManager::ReleaseSlot(ESlotType slotType, int slotIndex, double requestedCpu, const std::optional<i64>& numaNodeIdAffinity)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    FreeSlots_.push(slotIndex);
    YT_VERIFY(std::ssize(FreeSlots_) <= SlotCount_);

    if (slotType == ESlotType::Idle) {
        --UsedIdleSlotCount_;
        IdlePolicyRequestedCpu_ -= requestedCpu;
    }

    if (numaNodeIdAffinity) {
        for (auto& numaNodeState : NumaNodeStates_) {
            if (numaNodeState.NumaNodeInfo.NumaNodeId == *numaNodeIdAffinity) {
                numaNodeState.FreeCpuCount += requestedCpu;
                break;
            }
        }
    }

    YT_LOG_DEBUG("Exec slot released (SlotType: %v, SlotIndex: %v, RequestedCpu: %v)",
        slotType,
        slotIndex,
        requestedCpu);
}

NNodeTrackerClient::NProto::TDiskResources TSlotManager::GetDiskResources()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    NNodeTrackerClient::NProto::TDiskResources result;
    result.set_default_medium_index(DefaultMediumIndex_);

    UpdateAliveLocations();

    // Make a copy, since GetDiskResources is async and iterator over AliveLocations_
    // may have been invalidated between iterations.
    auto guard = ReaderGuard(AliveLocationsLock_);

    auto locations = AliveLocations_;
    for (const auto& location : locations) {
        try {
            auto info = location->GetDiskResources();
            auto* locationResources = result.add_disk_location_resources();
            locationResources->set_usage(info.usage());
            locationResources->set_limit(info.limit());
            locationResources->set_medium_index(info.medium_index());
        } catch (const std::exception& ex) {
            auto alert = TError("Failed to get location disk info")
                << ex;
            location->Disable(alert);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TSlotManager::TSlotGuard::TSlotGuard(
    TSlotManagerPtr slotManager,
    ESlotType slotType,
    double requestedCpu,
    std::optional<i64> numaNodeIdAffinity)
    : SlotManager_(std::move(slotManager))
    , RequestedCpu_(requestedCpu)
    , NumaNodeIdAffinity_(numaNodeIdAffinity)
    , SlotType_(slotType)
    , SlotIndex_(SlotManager_->DoAcquireSlot(slotType))
{ }

TSlotManager::TSlotGuard::~TSlotGuard()
{
    SlotManager_->ReleaseSlot(SlotType_, SlotIndex_, RequestedCpu_, NumaNodeIdAffinity_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode::NYT
