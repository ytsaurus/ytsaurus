#include "stdafx.h"
#include "cell_directory.h"
#include "config.h"

#include <core/concurrency/rw_spinlock.h>

#include <ytlib/election/config.h>

#include <ytlib/hive/cell_directory.pb.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/hydra/peer_channel.h>
#include <ytlib/hydra/config.h>

#include <ytlib/election/config.h>

namespace NYT {
namespace NHive {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;
using namespace NHydra;
using namespace NElection;
using namespace NNodeTrackerClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TCellConfigPtr TCellDescriptor::ToConfig(const Stroka& networkName) const
{
    auto config = New<TCellConfig>();
    config->CellId = CellId;
    for (const auto& peer : Peers) {
        config->Addresses.push_back(
            peer
            ? MakeNullable(peer->GetAddressOrThrow(networkName))
            : Null);
    }
    return config;
}

TCellInfo TCellDescriptor::ToInfo() const
{
    TCellInfo info;
    info.CellId = CellId;
    info.ConfigVersion = ConfigVersion;
    return info;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TCellInfo* protoInfo, const TCellInfo& info)
{
    ToProto(protoInfo->mutable_cell_id(), info.CellId);
    protoInfo->set_config_version(info.ConfigVersion);
}

void FromProto(TCellInfo* info, const NProto::TCellInfo& protoInfo)
{
    info->CellId = FromProto<TCellId>(protoInfo.cell_id());
    info->ConfigVersion = protoInfo.config_version();
}

void ToProto(NProto::TCellDescriptor* protoDescriptor, const TCellDescriptor& descriptor)
{
    ToProto(protoDescriptor->mutable_cell_id(), descriptor.CellId);
    protoDescriptor->set_config_version(descriptor.ConfigVersion);
    for (const auto& maybePeer : descriptor.Peers) {
        auto* protoPeer = protoDescriptor->add_peers();
        ToProto(protoPeer, maybePeer.Get(TNodeDescriptor()));
    }
}

void FromProto(TCellDescriptor* descriptor, const NProto::TCellDescriptor& protoDescriptor)
{
    descriptor->CellId = FromProto<TCellId>(protoDescriptor.cell_id());
    descriptor->ConfigVersion = protoDescriptor.config_version();
    for (const auto& protoPeer : protoDescriptor.peers()) {
        descriptor->Peers.push_back(
            protoPeer.addresses().entries_size() == 0
            ? Null
            : MakeNullable(FromProto<TNodeDescriptor>(protoPeer)));
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCellDirectory::TImpl
{
public:
    TImpl(
        TCellDirectoryConfigPtr config,
        IChannelFactoryPtr channelFactory,
        const Stroka& networkName)
        : Config_(config)
        , ChannelFactory_(channelFactory)
        , NetworkName_(networkName)
    { }

    IChannelPtr FindChannel(const TCellId& cellId, EPeerKind peerKind)
    {
        TReaderGuard guard(SpinLock_);
        auto it = RegisteredCellMap_.find(cellId);
        return it == RegisteredCellMap_.end() ? nullptr : it->second.Channels[peerKind];
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

    std::vector<TCellInfo> GetRegisteredCells()
    {
        TReaderGuard guard(SpinLock_);
        std::vector<TCellInfo> result;
        result.reserve(RegisteredCellMap_.size());
        for (const auto& pair : RegisteredCellMap_) {
            TCellInfo info;
            info.CellId = pair.first;
            info.ConfigVersion = pair.second.ConfigVersion;
            result.push_back(info);
        }
        return result;
    }

    bool IsCellUnregistered(const TCellId& cellId)
    {
        TReaderGuard guard(SpinLock_);
        return UnregisteredCellIds_.find(cellId) != UnregisteredCellIds_.end();
    }

    TNullable<std::vector<Stroka>> FindAddresses(const TCellId& cellId)
    {
        TReaderGuard guard(SpinLock_);
        auto it = RegisteredCellMap_.find(cellId);
        if (it == RegisteredCellMap_.end()) {
            return Null;
        }
        std::vector<Stroka> addresses;
        for (const auto& maybeAddress : it->second.Config->Addresses) {
            if (maybeAddress) {
                addresses.push_back(*maybeAddress);
            }
        }
        return addresses;
    }

    std::vector<Stroka> GetAddressesOrThrow(const TCellId& cellId)
    {
        auto result = FindAddresses(cellId);
        if (!result) {
            THROW_ERROR_EXCEPTION("Unknown cell %v",
                cellId);
        }
        return *result;
    }

    bool ReconfigureCell(TCellConfigPtr config, int configVersion)
    {
        TWriterGuard guard(SpinLock_);
        bool result = false;
        if (UnregisteredCellIds_.find(config->CellId) == UnregisteredCellIds_.end()) {
            auto it = RegisteredCellMap_.find(config->CellId);
            auto* entry = (it == RegisteredCellMap_.end()) ? nullptr : &it->second;
            if (!entry) {
                auto it = RegisteredCellMap_.insert(std::make_pair(config->CellId, TEntry())).first;
                entry = &it->second;
                result = true;
            }
            if (entry->ConfigVersion < configVersion) {
                entry->Config = CloneYsonSerializable(config);
                entry->ConfigVersion = configVersion;
                InitChannel(entry);
                result = true;
            }
        }
        return result;
    }

    bool ReconfigureCell(TPeerConnectionConfigPtr config, int configVersion)
    {
        auto cellConfig = New<TCellConfig>();
        cellConfig->CellId = config->CellId;
        for (const auto& address : config->Addresses) {
            cellConfig->Addresses.push_back(address);
        }
        return ReconfigureCell(cellConfig, configVersion);
    }

    bool ReconfigureCell(const TCellDescriptor& descriptor)
    {
        auto cellConfig = New<TCellConfig>();
        cellConfig->CellId = descriptor.CellId;
        for (const auto& peer : descriptor.Peers) {
            cellConfig->Addresses.push_back(
                peer
                ? MakeNullable(peer->GetAddressOrThrow(NetworkName_))
                : Null);
        }
        return ReconfigureCell(cellConfig, descriptor.ConfigVersion);
    }

    bool UnregisterCell(const TCellId& cellId)
    {
        TWriterGuard guard(SpinLock_);
        UnregisteredCellIds_.insert(cellId);
        return RegisteredCellMap_.erase(cellId) == 1;
    }

    void Clear()
    {
        TWriterGuard guard(SpinLock_);
        RegisteredCellMap_.clear();
    }

private:
    const TCellDirectoryConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const Stroka NetworkName_;

    struct TEntry
    {
        TCellConfigPtr Config;
        int ConfigVersion = -1;
        TEnumIndexedVector<IChannelPtr, EPeerKind> Channels;
    };

    TReaderWriterSpinLock SpinLock_;
    yhash_map<TCellId, TEntry> RegisteredCellMap_;
    yhash_set<TCellId> UnregisteredCellIds_;


    void InitChannel(TEntry* entry)
    {
        auto peerConfig = New<TPeerConnectionConfig>();
        peerConfig->CellId = entry->Config->CellId;
        for (const auto& maybeAddress : entry->Config->Addresses) {
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
    IChannelFactoryPtr channelFactory,
    const Stroka& networkName)
    : Impl_(new TImpl(
        config,
        channelFactory,
        networkName))
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

TNullable<std::vector<Stroka>> TCellDirectory::FindAddresses(const TCellId& cellId)
{
    return Impl_->FindAddresses(cellId);
}

std::vector<Stroka> TCellDirectory::GetAddressesOrThrow(const TCellId& cellId)
{
    return Impl_->GetAddressesOrThrow(cellId);
}

std::vector<TCellInfo> TCellDirectory::GetRegisteredCells()
{
    return Impl_->GetRegisteredCells();
}

bool TCellDirectory::IsCellUnregistered(const TCellId& cellId)
{
    return Impl_->IsCellUnregistered(cellId);
}

bool TCellDirectory::ReconfigureCell(TCellConfigPtr config, int configVersion)
{
    return Impl_->ReconfigureCell(config, configVersion);
}

bool TCellDirectory::ReconfigureCell(TPeerConnectionConfigPtr config, int configVersion)
{
    return Impl_->ReconfigureCell(config, configVersion);
}

bool TCellDirectory::ReconfigureCell(const TCellDescriptor& descriptor)
{
    return Impl_->ReconfigureCell(descriptor);
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
