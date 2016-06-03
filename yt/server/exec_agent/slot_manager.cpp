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
    , NodeTag_(Format("yt-node-%v", bootstrap->GetConfig()->RpcPort))
    , LocationQueue_(New<TActionQueue>("SlotLocations"))
{ }

void TSlotManager::Initialize(int slotCount)
{
    SlotCount_ = slotCount;

    for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
        FreeSlots_.insert(slotIndex);
    }

    JobEnviroment_ = CreateJobEnvironment(
        Config_->JobEnvironment,
        Bootstrap_);

    int locationIndex = 0;
    for (auto locationConfig : Config_->Locations) {
        Locations_.push_back(New<TSlotLocation>(
            std::move(locationConfig),
            Bootstrap_,
            Format("slots%v", locationIndex),
            LocationQueue_->GetInvoker()));

        if (Locations_.back()->IsEnabled()) {
            AliveLocations_.push_back(Locations_.back());
        }

        ++locationIndex;
    }

    // Fisrt shutdown all possible processes.
    try {
        for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
            JobEnviroment_->CleanProcesses(slotIndex);
        }
    } catch (const std::exception& ex) {
        LOG_WARNING("Failed do clean up processes on slot manager initialization");
    }

    if (!JobEnviroment_->IsEnabled()) {
        return;
    }

    // Then clean all the sandboxes.
    for (auto& location : AliveLocations_) {
        try {
            for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
                location->CleanSandboxes(slotIndex);
            }
        } catch (const std::exception& ex) {
            LOG_WARNING("Failed do clean up processes on slot manager initialization");
        }
    }

    UpdateAliveLocations();
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

    return CreateSlot(slotIndex, std::move(*locationIt), JobEnviroment_, NodeTag_);
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
    return SlotCount_ > 0 && 
        !AliveLocations_.empty() && 
        JobEnviroment_->IsEnabled();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NExecAgent
