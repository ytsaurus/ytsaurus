#include "slot_manager.h"
#include "private.h"
#include "slot.h"
#include "job_environment.h"
#include "slot_location.h"

#include <yt/server/lib/exec_agent/config.h>

#include <yt/server/node/cell_node/bootstrap.h>
#include <yt/server/node/cell_node/node_resource_manager.h>
#include <yt/server/node/cell_node/config.h>

#include <yt/server/node/data_node/chunk_cache.h>
#include <yt/server/node/data_node/master_connector.h>
#include <yt/server/node/data_node/volume_manager.h>

#include <yt/server/node/job_agent/job_controller.h>

#include <yt/ytlib/chunk_client/medium_directory.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/utilex/random.h>

namespace NYT::NExecAgent {

using namespace NCellNode;
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
    , NodeTag_(Format("yt-node-%v", Bootstrap_->GetConfig()->RpcPort))
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);
}

void TSlotManager::Initialize()
{
    Bootstrap_->GetMasterConnector()->SubscribePopulateAlerts(
        BIND(&TSlotManager::PopulateAlerts, MakeStrong(this)));
    Bootstrap_->GetJobController()->SubscribeJobFinished(
        BIND(&TSlotManager::OnJobFinished, MakeStrong(this)));

    YT_LOG_INFO("Initializing exec slots (Count: %v)", SlotCount_);

    for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
        FreeSlots_.insert(slotIndex);
    }

    JobEnvironment_ = CreateJobEnvironment(
        Config_->JobEnvironment,
        Bootstrap_);

    JobEnvironment_->Init(
        SlotCount_,
        Bootstrap_->GetConfig()->ExecAgent->JobController->ResourceLimits->Cpu);

    if (!JobEnvironment_->IsEnabled()) {
        YT_LOG_INFO("Job environment is disabled");
        return;
    }

    int locationIndex = 0;
    for (const auto& locationConfig : Config_->Locations) {
        try {
            Locations_.push_back(New<TSlotLocation>(
                std::move(locationConfig),
                Bootstrap_,
                Format("slot:%v", locationIndex),
                JobEnvironment_->CreateJobDirectoryManager(locationConfig->Path, locationIndex),
                Config_->EnableTmpfs,
                SlotCount_));

            if (Locations_.back()->IsEnabled()) {
                AliveLocations_.push_back(Locations_.back());
            }
        } catch (const std::exception& ex) {
            auto alert = TError("Failed to initialize slot location %v", locationConfig->Path)
                << ex;
            Disable(alert);
        }

        ++locationIndex;
    }

    // Then clean all the sandboxes.
    auto environmentConfig = NYTree::ConvertTo<TJobEnvironmentConfigPtr>(Config_->JobEnvironment);
    for (const auto& location : AliveLocations_) {
        try {
            for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
                WaitFor(location->CleanSandboxes(slotIndex))
                    .ThrowOnError();
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to clean up sandboxes during initialization");
        }
    }

    // To this moment all old processed must have been killed, so we can safely clean up old volumes
    // during root volume manager initialization.
    if (environmentConfig->Type == EJobEnvironmentType::Porto) {
        RootVolumeManager_ = CreatePortoVolumeManager(
            Bootstrap_->GetConfig()->DataNode->VolumeManager,
            Bootstrap_);
    }

    UpdateAliveLocations();

    Bootstrap_->GetNodeResourceManager()->SubscribeJobsCpuLimitUpdated(
        BIND(&TSlotManager::OnJobsCpuLimitUpdated, MakeWeak(this))
            .Via(Bootstrap_->GetJobInvoker()));

    YT_LOG_INFO("Exec slots initialized");
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

    int feasibleSlotCount = 0;
    int skippedByDiskSpace = 0;
    int skippedByMedium = 0;
    TSlotLocationPtr bestLocation;
    for (const auto& location : AliveLocations_) {
        auto diskResources = location->GetDiskResources();
        if (diskResources.usage() + diskRequest.disk_space() > diskResources.limit()) {
            ++skippedByDiskSpace;
            continue;
        }
        if (diskResources.medium_index() != diskRequest.medium_index()) {
            ++skippedByMedium;
            continue;
        }
        ++feasibleSlotCount;
        if (!bestLocation || bestLocation->GetSessionCount() > location->GetSessionCount()) {
            bestLocation = location;
        }
    }

    if (!bestLocation) {
        THROW_ERROR_EXCEPTION(EErrorCode::SlotNotFound, "No feasible slot found")
            << TErrorAttribute("alive_slot_count", AliveLocations_.size())
            << TErrorAttribute("feasible_slot_count", feasibleSlotCount)
            << TErrorAttribute("skipped_by_disk_space", skippedByDiskSpace)
            << TErrorAttribute("skipped_by_medium", skippedByMedium);
    }

    YT_VERIFY(!FreeSlots_.empty());
    int slotIndex = *FreeSlots_.begin();
    FreeSlots_.erase(slotIndex);

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
        SlotCount_ > 0 &&
        !AliveLocations_.empty() &&
        JobEnvironment_->IsEnabled();

    TGuard<TSpinLock> guard(SpinLock_);
    return enabled && !PersistentAlert_ && !TransientAlert_;
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

void TSlotManager::Disable(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();
    
    TGuard<TSpinLock> guard(SpinLock_);

    if (PersistentAlert_) {
        return;
    }

    auto errorWrapper = TError("Scheduler jobs disabled")
        << error;

    YT_LOG_WARNING(errorWrapper);
    PersistentAlert_ = errorWrapper;
}

void TSlotManager::OnJobFinished(const IJobPtr& job)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard guard(SpinLock_);

    if (job->GetState() == EJobState::Aborted) {
        ++ConsecutiveAbortedJobCount_;
    } else {
        ConsecutiveAbortedJobCount_ = 0;
    }

    if (ConsecutiveAbortedJobCount_ > Config_->MaxConsecutiveAborts) {
        if (!TransientAlert_) {
            auto delay = Config_->DisableJobsTimeout + RandomDuration(Config_->DisableJobsTimeout);
            TransientAlert_ = TError("Too many consecutive job abortions; scheduler jobs disabled until %v",
                TInstant::Now() + delay)
                << TErrorAttribute("max_consecutive_aborts", Config_->MaxConsecutiveAborts);
            TDelayedExecutor::Submit(BIND(&TSlotManager::ResetTransientAlert, MakeStrong(this)), delay);
        }
    }
}

void TSlotManager::ResetTransientAlert()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock_);
    TransientAlert_ = std::nullopt;
    ConsecutiveAbortedJobCount_ = 0;
}

void TSlotManager::PopulateAlerts(std::vector<TError>* alerts)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard guard(SpinLock_);
    if (TransientAlert_) {
        alerts->push_back(*TransientAlert_);
    }
    if (PersistentAlert_) {
        alerts->push_back(*PersistentAlert_);
    }
}

void TSlotManager::BuildOrchidYson(TFluentMap fluent) const
{
    VERIFY_THREAD_AFFINITY_ANY();
    
    {
        TGuard guard(SpinLock_);
        fluent
            .Item("slot_count").Value(SlotCount_)
            .Item("free_slot_count").Value(FreeSlots_.size())
            .DoIf(static_cast<bool>(TransientAlert_), [&] (auto fluent) {
                fluent.Item("transient_alert").Value(*TransientAlert_);
            })
            .DoIf(static_cast<bool>(PersistentAlert_), [&] (auto fluent) {
                fluent.Item("persistent_alert").Value(*PersistentAlert_);
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
        if (oldDescriptor.Index != NChunkClient::InvalidMediumIndex &&
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
}

NNodeTrackerClient::NProto::TDiskResources TSlotManager::GetDiskResources()
{
    VERIFY_THREAD_AFFINITY(JobThread);

    UpdateAliveLocations();
    NNodeTrackerClient::NProto::TDiskResources result;
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
