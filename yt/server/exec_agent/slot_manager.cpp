#include "slot_manager.h"
#include "config.h"
#include "slot.h"
#include "private.h"

#include <ytlib/misc/fs.h>

#include <server/cell_node/bootstrap.h>

#ifdef _unix_
    #include <sys/types.h>
    #include <sys/stat.h>
#endif

namespace NYT {
namespace NExecAgent {

using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

TSlotManager::TSlotManager(
    TSlotManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{
    YCHECK(config);
    YCHECK(bootstrap);
}

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

    for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
        auto slotName = ToString(slotIndex);
        auto slotPath = NFS::CombinePaths(Config->SlotLocation, slotName);
        int uid = jobControlEnabled ? Config->StartUid + slotIndex : EmptyUserId;
        auto slot = New<TSlot>(slotPath, slotIndex, uid);
        slot->Initialize();
        Slots.push_back(slot);
    }
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NExecAgent
