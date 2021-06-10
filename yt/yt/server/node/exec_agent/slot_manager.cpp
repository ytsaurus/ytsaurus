#include "slot_manager.h"
#include "private.h"
#include "slot.h"
#include "job_environment.h"
#include "slot_location.h"

#include <yt/yt/server/lib/exec_agent/config.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/data_node/chunk_cache.h>
#include <yt/yt/server/node/data_node/volume_manager.h>

#include <yt/yt/server/node/job_agent/job_controller.h>

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NExecAgent {

using namespace NClusterNode;
using namespace NDataNode;
using namespace NJobAgent;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TSlotManager::TSlotManager(
    TSlotManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , SlotCount_(Bootstrap_->GetConfig()->ExecAgent->JobController->ResourceLimits->UserSlots)
    , NodeTag_(Format("yt-node-%v-%v", Bootstrap_->GetConfig()->RpcPort, GetCurrentProcessId()))
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);
}

void TSlotManager::Initialize()
{
    Bootstrap_->GetClusterNodeMasterConnector()->SubscribePopulateAlerts(
        BIND(&TSlotManager::PopulateAlerts, MakeStrong(this)));
    Bootstrap_->GetJobController()->SubscribeJobFinished(
        BIND(&TSlotManager::OnJobFinished, MakeStrong(this)));
    Bootstrap_->GetJobController()->SubscribeJobProxyBuildInfoUpdated(
        BIND(&TSlotManager::OnJobProxyBuildInfoUpdated, MakeStrong(this)));

    const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
    dynamicConfigManager->SubscribeConfigChanged(BIND(&TSlotManager::OnDynamicConfigChanged, MakeWeak(this)));

    YT_LOG_INFO("Initializing exec slots (Count: %v)", SlotCount_);

    for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
        FreeSlots_.insert(slotIndex);
    }

    JobEnvironment_ = CreateJobEnvironment(
        Config_->JobEnvironment,
        Bootstrap_);

    // Job environment must be initialized first, since it cleans up all the processes, 
    // which may hold open decsriptors to volumes, layers and files in sandboxes.
    // It should also be initialized synchronously, since it may prevent delection of chunk cache artifacts.
    JobEnvironment_->Init(
        SlotCount_,
        Bootstrap_->GetConfig()->ExecAgent->JobController->ResourceLimits->Cpu);

    if (!JobEnvironment_->IsEnabled()) {
        YT_LOG_INFO("Job environment is disabled");
        return;
    }

    int locationIndex = 0;
    for (const auto& locationConfig : Config_->Locations) {
        auto guard = WriterGuard(LocationsLock_);
        Locations_.push_back(New<TSlotLocation>(
            std::move(locationConfig),
            Bootstrap_,
            Format("slot%v", locationIndex),
            JobEnvironment_->CreateJobDirectoryManager(locationConfig->Path, locationIndex),
            Config_->EnableTmpfs,
            SlotCount_,
            BIND(&IJobEnvironment::GetUserId, JobEnvironment_)));
        ++locationIndex;
    }

    Bootstrap_->GetJobInvoker()->Invoke(BIND(&TSlotManager::AsyncInitialize, MakeStrong(this)));
}

void TSlotManager::OnDynamicConfigChanged(
    const TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
    const TClusterNodeDynamicConfigPtr& newNodeConfig)
{
    DynamicConfig_.Store(newNodeConfig->ExecAgent->SlotManager);
}

void TSlotManager::UpdateAliveLocations()
{
    AliveLocations_.clear();
    for (const auto& location : Locations_) {
        if (location->IsEnabled()) {
            AliveLocations_.push_back(location);
        }
    }
}

ISlotPtr TSlotManager::AcquireSlot(NScheduler::NProto::TDiskRequest diskRequest)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    UpdateAliveLocations();

    int feasibleLocationCount = 0;
    int skippedByDiskSpace = 0;
    int skippedByMedium = 0;
    TSlotLocationPtr bestLocation;
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

    YT_VERIFY(!FreeSlots_.empty());
    int slotIndex = *FreeSlots_.begin();
    FreeSlots_.erase(slotIndex);

    if (diskRequest.disk_space() > 0) {
        bestLocation->AcquireDiskSpace(diskRequest.disk_space());
    }

    return CreateSlot(slotIndex, std::move(bestLocation), JobEnvironment_, RootVolumeManager_, NodeTag_);
}

void TSlotManager::ReleaseSlot(int slotIndex)
{
    VERIFY_THREAD_AFFINITY(JobThread);

    YT_VERIFY(FreeSlots_.insert(slotIndex).second);
}

int TSlotManager::GetSlotCount() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return IsEnabled() ? SlotCount_ : 0;
}

int TSlotManager::GetUsedSlotCount() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    return IsEnabled() ? SlotCount_ - FreeSlots_.size() : 0;
}

bool TSlotManager::IsEnabled() const
{
    VERIFY_THREAD_AFFINITY(JobThread);

    bool enabled =
        JobProxyReady_.load() &&
        Initialized_.load() &&
        SlotCount_ > 0 &&
        !AliveLocations_.empty() &&
        JobEnvironment_->IsEnabled();

    auto guard = Guard(SpinLock_);
    return enabled && !HasSlotDisablingAlert();
}

bool TSlotManager::HasSlotDisablingAlert() const
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    auto dynamicConfig = DynamicConfig_.Load();
    bool disableJobsOnGpuCheckFailure = dynamicConfig
        ? dynamicConfig->DisableJobsOnGpuCheckFailure.value_or(Config_->DisableJobsOnGpuCheckFailure)
        : Config_->DisableJobsOnGpuCheckFailure;
    return
        !Alerts_[ESlotManagerAlertType::GenericPersistentError].IsOK() ||
        !Alerts_[ESlotManagerAlertType::TooManyConsecutiveJobAbortions].IsOK() ||
        !Alerts_[ESlotManagerAlertType::JobProxyUnavailable].IsOK() ||
        (disableJobsOnGpuCheckFailure && !Alerts_[ESlotManagerAlertType::GpuCheckFailed].IsOK());
}

bool TSlotManager::HasFatalAlert() const
{
    auto guard = Guard(SpinLock_);
    return !Alerts_[ESlotManagerAlertType::GenericPersistentError].IsOK();
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

void TSlotManager::Disable(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_VERIFY(!error.IsOK());

    auto guard = Guard(SpinLock_);

    if (!Alerts_[ESlotManagerAlertType::GenericPersistentError].IsOK()) {
        return;
    }

    auto errorWrapper = TError("Scheduler jobs disabled")
        << error;

    YT_LOG_WARNING(errorWrapper, "Disabling slot manager");
    Alerts_[ESlotManagerAlertType::GenericPersistentError] = errorWrapper;
}

void TSlotManager::OnGpuCheckCommandFailed(const TError& error)
{
    YT_LOG_WARNING(
        error,
        "Gpu check failed alert set, jobs may be disabled if \"disable_jobs_on_gpu_check_failure\" specified");

    auto guard = Guard(SpinLock_);
    Alerts_[ESlotManagerAlertType::GpuCheckFailed] = error;
}

void TSlotManager::OnJobFinished(const IJobPtr& job)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);

    if (job->GetState() == EJobState::Aborted) {
        ++ConsecutiveAbortedJobCount_;
    } else {
        ConsecutiveAbortedJobCount_ = 0;
    }

    if (ConsecutiveAbortedJobCount_ > Config_->MaxConsecutiveAborts) {
        if (Alerts_[ESlotManagerAlertType::TooManyConsecutiveJobAbortions].IsOK()) {
            auto delay = Config_->DisableJobsTimeout + RandomDuration(Config_->DisableJobsTimeout);

            auto error = TError("Too many consecutive job abortions")
                << TErrorAttribute("max_consecutive_aborts", Config_->MaxConsecutiveAborts);
            YT_LOG_WARNING(error, "Scheduler jobs disabled until %v", TInstant::Now() + delay);
            Alerts_[ESlotManagerAlertType::TooManyConsecutiveJobAbortions] = error;

            TDelayedExecutor::Submit(BIND(&TSlotManager::ResetConsecutiveAbortedJobCount, MakeStrong(this)), delay);
        }
    }
}

void TSlotManager::OnJobProxyBuildInfoUpdated(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!Config_->Testing->SkipJobProxyUnavailableAlert) {
        auto guard = Guard(SpinLock_);

        auto& alert = Alerts_[ESlotManagerAlertType::JobProxyUnavailable];

        if (alert.IsOK() && !error.IsOK()) {
            YT_LOG_INFO(error, "Disabling scheduler jobs due to job proxy unavailability");
        } else if (!alert.IsOK() && error.IsOK()) {
            YT_LOG_INFO(error, "Enable scheduler jobs as job proxy became available");
        }

        alert = error;
    }
    JobProxyReady_.store(true);
}

void TSlotManager::ResetConsecutiveAbortedJobCount()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    Alerts_[ESlotManagerAlertType::TooManyConsecutiveJobAbortions] = TError();
    ConsecutiveAbortedJobCount_ = 0;
}

void TSlotManager::PopulateAlerts(std::vector<TError>* alerts)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    for (const auto& alert : Alerts_) {
        if (!alert.IsOK()) {
            alerts->push_back(alert);
        }
    }
}

void TSlotManager::BuildOrchidYson(TFluentMap fluent) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        auto guard = Guard(SpinLock_);
        fluent
            .Item("slot_count").Value(SlotCount_)
            .Item("free_slot_count").Value(FreeSlots_.size())
            .Item("alerts").DoMapFor(
                TEnumTraits<ESlotManagerAlertType>::GetDomainValues(),
                [&] (TFluentMap fluent, ESlotManagerAlertType alertType)
                {
                    const auto& error = Alerts_[alertType];
                    if (!error.IsOK()) {
                        fluent
                            .Item(FormatEnum(alertType)).Value(error);
                    }
                });
    }

    fluent
       .DoIf(static_cast<bool>(RootVolumeManager_), [&] (auto fluent) {
           fluent.Item("root_volume_manager").DoMap(BIND(
               &IVolumeManager::BuildOrchidYson,
               RootVolumeManager_));
       });
}

void TSlotManager::InitMedia(const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
{
    VERIFY_THREAD_AFFINITY_ANY();

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
        auto defaultMediumName = Config_->DefaultMediumName;
        auto descriptor = mediumDirectory->FindByName(defaultMediumName);
        if (!descriptor) {
            THROW_ERROR_EXCEPTION("Default medium is unknown (MediumName: %v)",
                defaultMediumName);
        }
        DefaultMediumIndex_ = descriptor->Index;
    }
}

void TSlotManager::AsyncInitialize()
{
    auto finally = Finally([&] () {
        Initialized_.store(true);
    }); 

    YT_LOG_INFO("Start async slot manager initialization");

    std::vector<TFuture<void>> initLocationFutures;
    for (auto& location : Locations_) {
        initLocationFutures.push_back(location->Initialize());
    }

    auto initResults = WaitFor(AllSet(initLocationFutures));
    if (!initResults.IsOK()) {
        auto error =  TError("Failed to initialize slot locations") << initResults;
        Disable(error);
    } else {
        // We ignore results from initLocationsFutures,
        // they are provided via TSlotLocation::IsEnabled method.
        for (const auto& location: Locations_) {
            if (!location->IsEnabled()) {
                AliveLocations_.push_back(location);
            }
        }
    }

    // To this moment all old processed must have been killed, so we can safely clean up old volumes
    // during root volume manager initialization.
    auto environmentConfig = NYTree::ConvertTo<TJobEnvironmentConfigPtr>(Config_->JobEnvironment);
    if (environmentConfig->Type == EJobEnvironmentType::Porto) {
        RootVolumeManager_ = CreatePortoVolumeManager(
            Bootstrap_->GetConfig()->DataNode->VolumeManager,
            Bootstrap_);
    }

    UpdateAliveLocations();

    Bootstrap_->GetNodeResourceManager()->SubscribeJobsCpuLimitUpdated(
        BIND(&TSlotManager::OnJobsCpuLimitUpdated, MakeWeak(this))
            .Via(Bootstrap_->GetJobInvoker()));

    YT_LOG_INFO("Exec slot manager initialized");
}

NNodeTrackerClient::NProto::TDiskResources TSlotManager::GetDiskResources()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    UpdateAliveLocations();
    NNodeTrackerClient::NProto::TDiskResources result;

    result.set_default_medium_index(DefaultMediumIndex_);

    // Make a copy, since GetDiskResources is async and iterator over AliveLocations_
    // may have been invalidated between iterations.
    auto locations = AliveLocations_;
    for (const auto& location : locations) {
        try {
            auto info = location->GetDiskResources();
            auto* locationResources = result.add_disk_location_resources();
            locationResources->set_usage(info.usage());
            locationResources->set_limit(info.limit());
            locationResources->set_medium_index(info.medium_index());
        } catch (const std::exception& ex) {
            auto alert = TError("Failed to get disk info of location")
                << ex;
            location->Disable(alert);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent::NYT
