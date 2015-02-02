#include "stdafx.h"
#include "cell_directory.h"
#include "config.h"

#include <core/concurrency/rw_spinlock.h>

#include <ytlib/hydra/peer_channel.h>
#include <ytlib/hydra/config.h>

#include <ytlib/election/config.h>

namespace NYT {
namespace NHive {

using namespace NConcurrency;
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

    IChannelPtr FindChannel(const TCellId& cellId, EPeerKind peerKind)
    {
        TReaderGuard guard(SpinLock_);
        auto it = CellMap_.find(cellId);
        if (it == CellMap_.end()) {
            return nullptr;
        }
        return it->second.Channels[peerKind];
    }

    IChannelPtr GetChannelOrThrow(const TCellId& cellId, EPeerKind peerKind)
    {
        auto channel = FindChannel(cellId, peerKind);
        if (!channel) {
            THROW_ERROR_EXCEPTION("Unknown cell %v",
                cellId);
        }
        return channel;
    }

    TCellConfigPtr FindCellConfig(const TCellId& cellId)
    {
        TReaderGuard guard(SpinLock_);
        auto it = CellMap_.find(cellId);
        return it == CellMap_.end() ? nullptr : it->second.Descriptor.Config;
    }

    TCellConfigPtr GetCellConfigOrThrow(const TCellId& cellId)
    {
        auto config = FindCellConfig(cellId);
        if (!config) {
            THROW_ERROR_EXCEPTION("Unknown cell %v",
                cellId);
        }
        return config;
    }

    std::vector<TCellDescriptor> GetRegisteredCells()
    {
        TReaderGuard guard(SpinLock_);
        std::vector<TCellDescriptor> result;
        result.reserve(CellMap_.size());
        for (const auto& pair : CellMap_) {
            result.push_back(pair.second.Descriptor);
        }
        return result;
    }

    bool RegisterCell(TCellConfigPtr config, int version)
    {
        TWriterGuard guard(SpinLock_);
        bool result = false;
        auto it = CellMap_.find(config->CellId);
        auto* entry = it == CellMap_.end() ? nullptr : &it->second;
        if (!entry ) {
            auto it = CellMap_.insert(std::make_pair(config->CellId, TEntry())).first;
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
        cellConfig->CellId = config->CellId;
        for (const auto& address : config->Addresses) {
            cellConfig->Addresses.push_back(address);
        }
        return RegisterCell(cellConfig, version);
    }

    bool UnregisterCell(const TCellId& cellId)
    {
        TWriterGuard guard(SpinLock_);
        auto it = CellMap_.find(cellId);
        if (it == CellMap_.end()) {
            return false;
        } else {
            CellMap_.erase(it);
            return true;
        }
    }

    void Clear()
    {
        TWriterGuard guard(SpinLock_);
        CellMap_.clear();
    }

private:
    const TCellDirectoryConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;

    struct TEntry
    {
        TCellDescriptor Descriptor;
        TEnumIndexedVector<IChannelPtr, EPeerKind> Channels;
    };

    TReaderWriterSpinLock SpinLock_;
    yhash_map<TCellId, TEntry> CellMap_;


    void InitChannel(TEntry* entry)
    {
        auto peerConfig = New<TPeerConnectionConfig>();
        peerConfig->CellId = entry->Descriptor.Config->CellId;
        for (const auto& maybeAddress : entry->Descriptor.Config->Addresses) {
            if (maybeAddress) {
                peerConfig->Addresses.push_back(*maybeAddress);
            }
        }
        peerConfig->DiscoverTimeout = Config_->DiscoverTimeout;
        peerConfig->SoftBackoffTime = Config_->SoftBackoffTime;
        peerConfig->HardBackoffTime = Config_->HardBackoffTime;

        for (auto kind : TEnumTraits<EPeerKind>::GetDomainValues()) {
            auto channel = CreatePeerChannel(peerConfig, ChannelFactory_, kind);
            channel->SetDefaultTimeout(Config_->RpcTimeout);
            entry->Channels[kind] = channel;
        }
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

IChannelPtr TCellDirectory::FindChannel(const TCellId& cellId, EPeerKind peerKind)
{
    return Impl_->FindChannel(cellId, peerKind);
}

IChannelPtr TCellDirectory::GetChannelOrThrow(const TCellId& cellId, EPeerKind peerKind)
{
    return Impl_->GetChannelOrThrow(cellId, peerKind);
}

TCellConfigPtr TCellDirectory::FindCellConfig(const TCellId& cellId)
{
    return Impl_->FindCellConfig(cellId);
}

TCellConfigPtr TCellDirectory::GetCellConfigOrThrow(const TCellId& cellId)
{
    return Impl_->GetCellConfigOrThrow(cellId);
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

bool TCellDirectory::UnregisterCell(const TCellId& cellId)
{
    return Impl_->UnregisterCell(cellId);
}

void TCellDirectory::Clear()
{
    Impl_->Clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
