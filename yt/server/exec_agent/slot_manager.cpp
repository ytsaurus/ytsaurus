#include "slot_manager.h"
#include "config.h"
#include "slot.h"
#include "private.h"

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>
#include <server/data_node/chunk_cache.h>
#include <server/data_node/master_connector.h>

#include <core/concurrency/action_queue.h>

#ifdef _unix_
    #include <sys/stat.h>
#endif

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
    : Config(config)
    , Bootstrap(bootstrap)
    , ActionQueue(New<TActionQueue>("ExecSlots"))
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
            THROW_ERROR_EXCEPTION("Failed to initialize job control, make sure you run as root");
        }
        umask(0000);
        jobControlEnabled = true;
    }
#endif

    try {
        auto nodeRpcPort = Bootstrap->GetConfig()->RpcPort;

        const auto& execAgentConfig = Bootstrap->GetConfig()->ExecAgent;
        Config->EnableCGroups = execAgentConfig->EnableCGroups;
        Config->SupportedCGroups = execAgentConfig->SupportedCGroups;

        for (int slotId = 0; slotId < slotCount; ++slotId) {
            auto slotName = ToString(slotId);
            auto slotPath = NFS::CombinePaths(Config->Path, slotName);
            TNullable<int> userId = Null;
            if (jobControlEnabled) {
                userId = Config->StartUid + slotId;
            }
            auto slot = New<TSlot>(
                Config,
                slotPath,
                Format("yt-node-%v", nodeRpcPort),
                ActionQueue->GetInvoker(),
                slotId,
                userId);
            slot->Initialize();
            Slots.push_back(slot);
        }
    } catch (const std::exception& ex) {
        auto error = TError("Failed to initialize slots") << ex;
        LOG_WARNING(error);
        Bootstrap->GetMasterConnector()->RegisterAlert(error);
        IsEnabled = false;
    }

    auto chunkCache = Bootstrap->GetChunkCache();
    IsEnabled &= chunkCache->IsEnabled();
}

TSlotPtr TSlotManager::AcquireSlot()
{
    for (auto slot : Slots) {
        if (slot->IsFree()) {
            slot->Acquire();
            return slot;
        }
    }
    YUNREACHABLE();
}

int TSlotManager::GetSlotCount() const
{
    return IsEnabled ? static_cast<int>(Slots.size()) : 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NExecAgent
