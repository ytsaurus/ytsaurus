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
        : Config_(config)
        , ChannelFactory_(channelFactory)
    { }

    IChannelPtr FindChannel(const TCellGuid& cellGuid)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto it = CellMap_.find(cellGuid);
        if (it == CellMap_.end()) {
            return nullptr;
        }
        return it->second.Channel;
    }

    IChannelPtr GetChannelOrThrow(const TCellGuid& cellGuid)
    {
        auto channel = FindChannel(cellGuid);
        if (!channel) {
            THROW_ERROR_EXCEPTION("Unknown cell %v",
                cellGuid);
        }
        return channel;
    }

    TCellConfigPtr FindCellConfig(const TCellGuid& cellGuid)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto it = CellMap_.find(cellGuid);
        return it == CellMap_.end() ? nullptr : it->second.Descriptor.Config;
    }

    TCellConfigPtr GetCellConfigOrThrow(const TCellGuid& cellGuid)
    {
        auto config = FindCellConfig(cellGuid);
        if (!config) {
            THROW_ERROR_EXCEPTION("Unknown cell %v",
                cellGuid);
        }
        return config;
    }

    std::vector<TCellDescriptor> GetRegisteredCells()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        std::vector<TCellDescriptor> result;
        result.reserve(CellMap_.size());
        for (const auto& pair : CellMap_) {
            result.push_back(pair.second.Descriptor);
        }
        return result;
    }

    bool RegisterCell(TCellConfigPtr config, int version)
    {
        bool result = false;
        TGuard<TSpinLock> guard(SpinLock_);
        auto it = CellMap_.find(config->CellGuid);
        auto* entry = it == CellMap_.end() ? nullptr : &it->second;
        if (!entry ) {
            auto it = CellMap_.insert(std::make_pair(config->CellGuid, TEntry())).first;
            entry = &it->second;
            result = true;
        }
        if (entry->Descriptor.Version < version) {
            entry->Descriptor.Config = config;
            entry->Descriptor.Version = version;
            InitChannel(entry);
            result = true;
        }
        return result;
    }

    bool RegisterCell(TPeerConnectionConfigPtr config, int version)
    {
        auto cellConfig = New<TCellConfig>();
        cellConfig->CellGuid = config->CellGuid;
        cellConfig->Addresses = config->Addresses;
        return RegisterCell(cellConfig, version);
    }

    bool UnregisterCell(const TCellGuid& cellGuid)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto it = CellMap_.find(cellGuid);
        if (it == CellMap_.end()) {
            return false;
        } else {
            CellMap_.erase(it);
            return true;
        }
    }

    void Clear()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        CellMap_.clear();
    }

private:
    TCellDirectoryConfigPtr Config_;
    IChannelFactoryPtr ChannelFactory_;

    struct TEntry
    {
        TCellDescriptor Descriptor;
        IChannelPtr Channel;
    };

    TSpinLock SpinLock_;
    yhash_map<TCellGuid, TEntry> CellMap_;


    void InitChannel(TEntry* entry)
    {
        auto peerConfig = New<TPeerConnectionConfig>();
        peerConfig->CellGuid = entry->Descriptor.Config->CellGuid;
        peerConfig->Addresses = entry->Descriptor.Config->Addresses;
        peerConfig->DiscoverTimeout = Config_->DiscoverTimeout;
        peerConfig->SoftBackoffTime = Config_->SoftBackoffTime;
        peerConfig->HardBackoffTime = Config_->HardBackoffTime;

        auto leaderChannel = CreateLeaderChannel(peerConfig, ChannelFactory_);
        leaderChannel->SetDefaultTimeout(Config_->RpcTimeout);
        entry->Channel = leaderChannel;
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

TCellConfigPtr TCellDirectory::FindCellConfig(const TCellGuid& cellGuid)
{
    return Impl_->FindCellConfig(cellGuid);
}

TCellConfigPtr TCellDirectory::GetCellConfigOrThrow(const TCellGuid& cellGuid)
{
    return Impl_->GetCellConfigOrThrow(cellGuid);
}

std::vector<TCellDirectory::TCellDescriptor> TCellDirectory::GetRegisteredCells()
{
    return Impl_->GetRegisteredCells();
}

bool TCellDirectory::RegisterCell(TCellConfigPtr config, int version)
{
    return Impl_->RegisterCell(config, version);
}

bool TCellDirectory::RegisterCell(TPeerConnectionConfigPtr config, int version)
{
    return Impl_->RegisterCell(config, version);
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
