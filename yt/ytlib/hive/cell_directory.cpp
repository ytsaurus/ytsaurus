#include "stdafx.h"
#include "cell_directory.h"
#include "config.h"

#include <core/rpc/channel.h>

#include <ytlib/hydra/peer_channel.h>
#include <ytlib/hydra/config.h>

#include <ytlib/election/config.h>

namespace NYT {
namespace NHive {

using namespace NRpc;
using namespace NHydra;
using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectory::TImpl
{
public:
    TImpl(
        TCellDirectoryConfigPtr config,
        IChannelFactoryPtr channelFactory)
        : Config(config)
        , ChannelFactory(channelFactory)
    { }

    IChannelPtr FindChannel(const TCellGuid& cellGuid)
    {
        TGuard<TSpinLock> guard(Spinlock);
        auto it = CellMap.find(cellGuid);
        if (it == CellMap.end()) {
            return nullptr;
        }
        return it->second.Channel;
    }

    IChannelPtr GetChannelOrThrow(const TCellGuid& cellGuid)
    {
        auto channel = FindChannel(cellGuid);
        if (!channel) {
            THROW_ERROR_EXCEPTION("Unknown cell %s",
                ~ToString(cellGuid));
        }
        return channel;
    }

    TNullable<TCellConfig> FindCellConfig(const TCellGuid& cellGuid)
    {
        TGuard<TSpinLock> guard(Spinlock);
        auto it = CellMap.find(cellGuid);
        return it == CellMap.end() ? TNullable<TCellConfig>(Null) : it->second.Config;
    }

    TCellConfig GetCellConfigOrThrow(const TCellGuid& cellGuid)
    {
        auto config = FindCellConfig(cellGuid);
        if (!config) {
            THROW_ERROR_EXCEPTION("Unknown cell %s",
                ~ToString(cellGuid));
        }
        return *config;
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

    bool RegisterCell(TPeerConnectionConfigPtr config)
    {
        return RegisterCell(config->CellGuid, BuildProtoConfig(config->Addresses));
    }

    bool RegisterCell(TCellConfigPtr config)
    {
        return RegisterCell(config->CellGuid, BuildProtoConfig(config->Addresses));
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

private:
    TCellDirectoryConfigPtr Config;
    IChannelFactoryPtr ChannelFactory;

    struct TEntry
    {
        TCellConfig Config;
        IChannelPtr Channel;
    };

    TSpinLock Spinlock;
    yhash_map<TCellGuid, TEntry> CellMap;


    void InitChannel(const TCellGuid& cellGuid, TEntry* entry)
    {
        if (entry->Config.version() == 0)
            return;

        auto config = New<TPeerConnectionConfig>();
        config->CellGuid = cellGuid;
        for (const auto& peer : entry->Config.peers()) {
            config->Addresses.push_back(peer.address());
        }

        auto leaderChannel = CreateLeaderChannel(config, ChannelFactory);
        leaderChannel->SetDefaultTimeout(Config->RpcTimeout);
        entry->Channel = leaderChannel;
    }
    
    NHydra::NProto::TCellConfig BuildProtoConfig(const std::vector<Stroka>& addresses)
    {
        NHydra::NProto::TCellConfig protoConfig;
        protoConfig.set_size(addresses.size());
        protoConfig.set_version(1); // expect this to be a master cell whose config version never changes
        for (const auto& address : addresses) {
            auto* peer = protoConfig.add_peers();
            peer->set_peer_id(protoConfig.peers_size() - 1);
            peer->set_address(address);
        }
        return protoConfig;
    }

};

////////////////////////////////////////////////////////////////////////////////

TCellDirectory::TCellDirectory(
    TCellDirectoryConfigPtr config,
    IChannelFactoryPtr channelFactory)
    : Impl_(new TImpl(
        config,
        channelFactory))
{ }

TCellDirectory::~TCellDirectory()
{ }

IChannelPtr TCellDirectory::FindChannel(const TCellGuid& cellGuid)
{
    return Impl_->FindChannel(cellGuid);
}

IChannelPtr TCellDirectory::GetChannelOrThrow(const TCellGuid& cellGuid)
{
    return Impl_->GetChannelOrThrow(cellGuid);
}

TNullable<TCellDirectory::TCellConfig> TCellDirectory::FindCellConfig(const TCellGuid& cellGuid)
{
    return Impl_->FindCellConfig(cellGuid);
}

TCellDirectory::TCellConfig TCellDirectory::GetCellConfigOrThrow(const TCellGuid& cellGuid)
{
    return Impl_->GetCellConfigOrThrow(cellGuid);
}

std::vector<std::pair<TCellGuid, TCellDirectory::TCellConfig>> TCellDirectory::GetRegisteredCells()
{
    return Impl_->GetRegisteredCells();
}

bool TCellDirectory::RegisterCell(const TCellGuid& cellGuid, const TCellConfig& config)
{
    return Impl_->RegisterCell(cellGuid, config);
}

bool TCellDirectory::RegisterCell(TPeerConnectionConfigPtr config)
{
    return Impl_->RegisterCell(config);
}

bool TCellDirectory::RegisterCell(TCellConfigPtr config)
{
    return Impl_->RegisterCell(config);
}

bool TCellDirectory::UnregisterCell(const TCellGuid& cellGuid)
{
    return Impl_->UnregisterCell(cellGuid);
}

void TCellDirectory::Clear()
{
    Impl_->Clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
