#include "slot_manager.h"
#include "private.h"
#include "slot.h"
#include "job_environment.h"
#include "slot_location.h"

#include <yt/server/lib/exec_agent/config.h>

#include <yt/server/node/cell_node/bootstrap.h>
#include <yt/server/node/cell_node/config.h>

#include <yt/server/node/data_node/chunk_cache.h>
#include <yt/server/node/data_node/master_connector.h>
#include <yt/server/node/data_node/volume_manager.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/utilex/random.h>

namespace NYT::NExecAgent {

using namespace NCellNode;
using namespace NDataNode;
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
{ }

void TSlotManager::Initialize()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Bootstrap_->GetMasterConnector()->SubscribePopulateAlerts(BIND(
        &TSlotManager::PopulateAlerts,
        MakeStrong(this)));

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
    for (auto locationConfig : Config_->Locations) {
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

ISlotPtr TSlotManager::AcquireSlot(i64 diskSpaceRequest)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    UpdateAliveLocations();

    int feasibleSlotCount = 0;
    TSlotLocationPtr bestLocation;
    for (const auto& location : AliveLocations_) {
        auto diskResources = location->GetDiskResources();
        if (diskResources.usage() + diskSpaceRequest > diskResources.limit()) {
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
            << TErrorAttribute("feasible_slot_count", feasibleSlotCount);
    }

    YT_VERIFY(!FreeSlots_.empty());
    int slotIndex = *FreeSlots_.begin();
    FreeSlots_.erase(slotIndex);

    return CreateSlot(slotIndex, std::move(bestLocation), JobEnvironment_, RootVolumeManager_, NodeTag_);
}

void TSlotManager::ReleaseSlot(int slotIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(FreeSlots_.insert(slotIndex).second);
}

int TSlotManager::GetSlotCount() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    return IsEnabled() ? SlotCount_ : 0;
}

int TSlotManager::GetUsedSlotCount() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    return IsEnabled() ? SlotCount_ - FreeSlots_.size() : 0;
}

bool TSlotManager::IsEnabled() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    bool isEnabled = SlotCount_ > 0 &&
        !AliveLocations_.empty() &&
        JobEnvironment_->IsEnabled();

    TGuard<TSpinLock> guard(SpinLock_);
    return isEnabled && !PersistentAlert_ && !TransientAlert_;
}

void TSlotManager::UpdateCpuLimit(double cpuLimit)
{
    JobEnvironment_->UpdateCpuLimit(cpuLimit);
}

void TSlotManager::Disable(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    TGuard<TSpinLock> guard(SpinLock_);

    if (PersistentAlert_) {
        return;
    }

    auto errorWrapper = TError("Scheduler jobs disabled")
        << error;

    YT_LOG_WARNING(errorWrapper);
    PersistentAlert_ = errorWrapper;
}

void TSlotManager::OnJobFinished(EJobState jobState)
{
    if (jobState == EJobState::Aborted) {
        ++ConsecutiveAbortedJobCount_;
    } else {
        ConsecutiveAbortedJobCount_ = 0;
    }

    if (ConsecutiveAbortedJobCount_ > Config_->MaxConsecutiveAborts) {
        TGuard guard(SpinLock_);
        if (!TransientAlert_) {
            auto delay = Config_->DisableJobsTimeout + RandomDuration(Config_->DisableJobsTimeout);
            TransientAlert_ = TError("Too many consecutive job abortions; scheduler jobs disabled until %v", TInstant::Now() + delay)
                << TErrorAttribute("max_consecutive_aborts", Config_->MaxConsecutiveAborts);
            TDelayedExecutor::Submit(BIND(&TSlotManager::ResetTransientAlert, MakeStrong(this)), delay);
        }
    }
}

void TSlotManager::ResetTransientAlert()
{
    TGuard<TSpinLock> guard(SpinLock_);
    TransientAlert_ = std::nullopt;
    ConsecutiveAbortedJobCount_ = 0;
}

void TSlotManager::PopulateAlerts(std::vector<TError>* alerts)
{
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
    fluent
       .Item("slot_count").Value(SlotCount_)
       .Item("free_slot_count").Value(FreeSlots_.size())
       .DoIf(static_cast<bool>(TransientAlert_), [&] (auto fluentMap) {
           fluentMap.Item("transient_alert").Value(*TransientAlert_);
       })
       .DoIf(static_cast<bool>(PersistentAlert_), [&] (auto fluentMap) {
           fluentMap.Item("persistent_alert").Value(*PersistentAlert_);
       })
       .DoIf(static_cast<bool>(RootVolumeManager_), [&] (auto fluentMap) {
           fluentMap.Item("root_volume_manager").DoMap(BIND(
               &IVolumeManager::BuildOrchidYson,
               RootVolumeManager_));
       });
}

NNodeTrackerClient::NProto::TDiskResources TSlotManager::GetDiskResources()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    UpdateAliveLocations();
    NNodeTrackerClient::NProto::TDiskResources result;
    // Make a copy, since GetDiskResources is async and iterator over AliveLocations_
    // may have been invalidated between iterations.
    auto locations = AliveLocations_;
    for (auto& location : locations) {
        try {
            auto info = location->GetDiskResources();
            auto* locationResources = result.add_disk_location_resources();
            locationResources->set_usage(info.usage());
            locationResources->set_limit(info.limit());
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
