#include "stdafx.h"
#include "cell_registry.h"
#include "private.h"

#include <core/rpc/channel.h>

#include <ytlib/hydra/peer_channel.h>
#include <ytlib/hydra/config.h>

namespace NYT {
namespace NHive {

using namespace NRpc;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HiveLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellRegistry::TImpl
{
public:
    IChannelPtr GetChannel(const TCellGuid& cellGuid)
    {
        TGuard<TSpinLock> guard(Spinlock);
        auto it = CellMap.find(cellGuid);
        if (it == CellMap.end()) {
            return nullptr;
        }
        return it->second.Channel;
    }

    bool RegisterCell(const TCellGuid& cellGuid, const TCellConfig& config)
    {
        bool result = false;
        TGuard<TSpinLock> guard(Spinlock);
        auto it = CellMap.find(cellGuid);
        auto* entry = it == CellMap.end() ? nullptr : &it->second;
        if (!entry ) {
            auto it = CellMap.insert(std::make_pair(cellGuid, TEntry())).first;
            entry = &it->second;
            result = true;
        }
        if (entry->Config.version() < config.version()) {
            entry->Config = config;
            InitChannel(cellGuid, entry);
            result = true;
        }
        return result;
    }

    bool UnregisterCell(const TCellGuid& cellGuid)
    {
        TGuard<TSpinLock> guard(Spinlock);
        auto it = CellMap.find(cellGuid);
        if (it == CellMap.end()) {
            return false;
        } else {
            CellMap.erase(it);
            return true;
        }
    }

    void Clear()
    {
        TGuard<TSpinLock> guard(Spinlock);
        CellMap.clear();
    }

    std::vector<std::pair<TCellGuid, TCellConfig>> GetRegisteredCells()
    {
        TGuard<TSpinLock> guard(Spinlock);
        std::vector<std::pair<TCellGuid, TCellConfig>> result;
        result.reserve(CellMap.size());
        for (const auto& pair : CellMap) {
            result.push_back(std::make_pair(pair.first, pair.second.Config));
        }
        return result;
    }

private:
    struct TEntry
    {
        TCellConfig Config;
        IChannelPtr Channel;
    };

    TSpinLock Spinlock;
    yhash_map<TCellGuid, TEntry> CellMap;


    void InitChannel(const TCellGuid& cellGuid, TEntry* entry)
    {
        if (entry->Config.version() > 0) {
            auto config = New<TPeerDiscoveryConfig>();
            config->CellGuid = cellGuid;
            for (const auto& peer : entry->Config.peers()) {
                config->Addresses.push_back(peer.address());
            }
            // TODO(babenko): configure timeout
            entry->Channel = CreatePeerChannel(config, EPeerRole::Leader);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////


TCellRegistry::TCellRegistry()
    : Impl(new TImpl())
{ }

IChannelPtr TCellRegistry::GetChannel(const TCellGuid& cellGuid)
{
    return Impl->GetChannel(cellGuid);
}

bool TCellRegistry::RegisterCell(const TCellGuid& cellGuid, const TCellConfig& config)
{
    return Impl->RegisterCell(cellGuid, config);
}

bool TCellRegistry::UnregisterCell(const TCellGuid& cellGuid)
{
    return Impl->UnregisterCell(cellGuid);
}

void TCellRegistry::Clear()
{
    Impl->Clear();
}

std::vector<std::pair<TCellGuid, TCellRegistry::TCellConfig>> TCellRegistry::GetRegisteredCells()
{
    return Impl->GetRegisteredCells();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
