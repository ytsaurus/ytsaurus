#include "slot_manager.h"
#include "config.h"
#include "slot.h"
#include "private.h"

#include <ytlib/misc/fs.h>

#include <server/cell_node/bootstrap.h>
#include <server/chunk_holder/chunk_cache.h>

#ifdef _unix_
    #include <sys/types.h>
    #include <sys/stat.h>
#endif

namespace NYT {
namespace NExecAgent {

using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& SILENT_UNUSED Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TSlotManager::TSlotManager(
    TSlotManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , IsEnabled(true)
{
    YCHECK(config);
    YCHECK(bootstrap);
}

TSlotManager::~TSlotManager()
{ }

void TSlotManager::Initialize(int slotCount)
{
    bool jobControlEnabled = false;

#if defined(_unix_) && !defined(_darwin_)
    if (Config->EnforceJobControl) {
        uid_t ruid, euid, suid;
        YCHECK(getresuid(&ruid, &euid, &suid) == 0);
        if (suid != 0) {
            THROW_ERROR_EXCEPTION("Cannot enable job control, please run as root");
        }
        umask(0000);
        jobControlEnabled = true;
    }
#endif

    try {
        for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
            auto slotName = ToString(slotIndex);
            auto slotPath = NFS::CombinePaths(Config->SlotLocation, slotName);
            int uid = jobControlEnabled ? Config->StartUid + slotIndex : EmptyUserId;
            auto slot = New<TSlot>(slotPath, slotIndex, uid);
            slot->Initialize();
            Slots.push_back(slot);
        }
    } catch (const std::exception& ex) {
        LOG_WARNING(ex, "Failed to initilize slots");
        IsEnabled = false;
    }

    IsEnabled &= Bootstrap->GetChunkCache()->IsEnabled();
}

TSlotPtr TSlotManager::FindFreeSlot()
{
    FOREACH (auto slot, Slots) {
        if (slot->IsFree()) {
            return slot;
        }
    }
    return nullptr;
}

int TSlotManager::GetSlotCount() const
{
    return IsEnabled ? static_cast<int>(Slots.size()) : 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NExecAgent
