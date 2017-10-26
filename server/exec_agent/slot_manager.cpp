#include "slot_manager.h"
#include "private.h"
#include "config.h"
#include "slot.h"
#include "job_environment.h"

#include "slot_location.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/data_node/chunk_cache.h>
#include <yt/server/data_node/master_connector.h>

#include <yt/core/concurrency/action_queue.h>

namespace NYT {
namespace NExecAgent {

using namespace NCellNode;
using namespace NConcurrency;

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
    LOG_INFO("Initializing %v exec slots", SlotCount_);

    for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
        FreeSlots_.insert(slotIndex);
    }

    JobEnvironment_ = CreateJobEnvironment(
        Config_->JobEnvironment,
        Bootstrap_);

    int locationIndex = 0;
    for (auto locationConfig : Config_->Locations) {
        Locations_.push_back(New<TSlotLocation>(
            std::move(locationConfig),
            Bootstrap_,
            Format("slots%v", locationIndex),
            Config_->DetachedTmpfsUmount));

        if (Locations_.back()->IsEnabled()) {
            AliveLocations_.push_back(Locations_.back());
        }

        ++locationIndex;
    }

    // First shutdown all possible processes.
    try {
        for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
            JobEnvironment_->CleanProcesses(slotIndex);
        }
    } catch (const std::exception& ex) {
        LOG_WARNING(ex, "Failed to clean up processes during initialization");
    }

    if (!JobEnvironment_->IsEnabled()) {
        LOG_INFO("Job environment is disabled");
        return;
    }

    // Then clean all the sandboxes.
    auto environmentConfig = NYTree::ConvertTo<TJobEnvironmentConfigPtr>(Config_->JobEnvironment);
    for (auto& location : AliveLocations_) {
        try {
            for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
                WaitFor(location->CleanSandboxes(
                    slotIndex,
                    JobEnvironment_->CreateMounter(slotIndex)))
                    .ThrowOnError();
            }
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Failed to clean up sandboxes during initialization");
        }
    }

    if (Config_->JobProxySocketNameDirectory) {
        try {
            // Create for each slot a file containing the name of Unix Domain Socket
            // that the corresponding job proxy listens to.
            for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
                auto filePath = Format("%v/%v", *Config_->JobProxySocketNameDirectory, JobEnvironment_->GetUserId(slotIndex));
                TFile file(filePath, CreateAlways | WrOnly | Seq | CloseOnExec);
                TUnbufferedFileOutput fileOutput(file);
                fileOutput << GetJobProxyUnixDomainName(NodeTag_, slotIndex) << Endl;
            }
            JobProxySocketNameDirectoryCreated_ = true;
        } catch (const std::exception& ex) {
            auto alert = TError("Failed to create a job proxy socket name directory")
                << ex;
            LOG_WARNING(alert);
            Bootstrap_->GetMasterConnector()->RegisterAlert(alert);
        }
    }

    UpdateAliveLocations();

    LOG_INFO("Exec slots initialized");
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

ISlotPtr TSlotManager::AcquireSlot()
{
    UpdateAliveLocations();

    if (AliveLocations_.empty()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::AllLocationsDisabled,
            "Cannot acquire slot: all slot locations are disabled");
    }

    auto locationIt = std::min_element(
        AliveLocations_.begin(),
        AliveLocations_.end(),
        [] (const TSlotLocationPtr& lhs, const TSlotLocationPtr& rhs) {
            return lhs->GetSessionCount() < rhs->GetSessionCount();
        });

    YCHECK(!FreeSlots_.empty());
    int slotIndex = *FreeSlots_.begin();
    FreeSlots_.erase(slotIndex);

    return CreateSlot(slotIndex, std::move(*locationIt), JobEnvironment_, NodeTag_);
}

void TSlotManager::ReleaseSlot(int slotIndex)
{
    YCHECK(FreeSlots_.insert(slotIndex).second);
}

int TSlotManager::GetSlotCount() const
{
    return IsEnabled() ? SlotCount_ : 0;
}

bool TSlotManager::IsEnabled() const
{
    bool isEnabled = SlotCount_ > 0 &&
        !AliveLocations_.empty() &&
        JobEnvironment_->IsEnabled();

    if (Config_->JobProxySocketNameDirectory) {
        isEnabled = isEnabled && JobProxySocketNameDirectoryCreated_;
    }

    return isEnabled;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NExecAgent
