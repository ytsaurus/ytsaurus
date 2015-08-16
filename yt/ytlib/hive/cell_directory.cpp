#include "stdafx.h"
#include "cell_directory.h"
#include "config.h"
#include "hive_service_proxy.h"
#include "private.h"

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
using namespace NRpc;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = HiveLogger;

////////////////////////////////////////////////////////////////////////////////

TCellConfigPtr TCellDescriptor::ToConfig(const Stroka& networkName) const
{
    auto config = New<TCellConfig>();
    config->CellId = CellId;
    for (const auto& peer : Peers) {
        config->Addresses.push_back(
            peer.IsNull()
            ? Null
            : MakeNullable(peer.GetAddressOrThrow(networkName)));
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
    ToProto(protoDescriptor->mutable_peers(), descriptor.Peers);
}

void FromProto(TCellDescriptor* descriptor, const NProto::TCellDescriptor& protoDescriptor)
{
    descriptor->CellId = FromProto<TCellId>(protoDescriptor.cell_id());
    descriptor->ConfigVersion = protoDescriptor.config_version();
    descriptor->Peers = FromProto<TNodeDescriptor>(protoDescriptor.peers());
}

////////////////////////////////////////////////////////////////////////////////

class TCellDirectory::TImpl
    : public TRefCounted
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

    IChannelPtr GetChannel(const TCellId& cellId, EPeerKind peerKind)
    {
        auto channel = FindChannel(cellId, peerKind);
        YCHECK(channel);
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
            info.ConfigVersion = pair.second.Descriptor.ConfigVersion;
            result.push_back(info);
        }
        return result;
    }

    TFuture<void> Synchronize(IChannelPtr channel)
    {
        LOG_INFO("Synchronizing cell directory");

        THiveServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config_->SyncRpcTimeout);

        auto req = proxy.SyncCells();
        ToProto(req->mutable_known_cells(), GetRegisteredCells());

        return req->Invoke().Apply(BIND(&TImpl::OnSynchronized, MakeStrong(this)));
    }

    bool IsCellUnregistered(const TCellId& cellId)
    {
        TReaderGuard guard(SpinLock_);
        return UnregisteredCellIds_.find(cellId) != UnregisteredCellIds_.end();
    }

    TNullable<TCellDescriptor> FindDescriptor(const TCellId& cellId)
    {
        TReaderGuard guard(SpinLock_);
        auto it = RegisteredCellMap_.find(cellId);
        return it == RegisteredCellMap_.end() ? Null : MakeNullable(it->second.Descriptor);
    }

    TCellDescriptor GetDescriptorOrThrow(const TCellId& cellId)
    {
        auto result = FindDescriptor(cellId);
        if (!result) {
            THROW_ERROR_EXCEPTION("Unknown cell %v",
                cellId);
        }
        return *result;
    }

    bool ReconfigureCell(TCellConfigPtr config, int configVersion)
    {
        TCellDescriptor descriptor;
        descriptor.CellId = config->CellId;
        descriptor.ConfigVersion = configVersion;
        for (const auto& maybeAddress : config->Addresses) {
            descriptor.Peers.push_back(maybeAddress ? TNodeDescriptor(*maybeAddress) : TNodeDescriptor());
        }
        return ReconfigureCell(descriptor);
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
        TWriterGuard guard(SpinLock_);
        bool result = false;
        if (UnregisteredCellIds_.find(descriptor.CellId) == UnregisteredCellIds_.end()) {
            auto it = RegisteredCellMap_.find(descriptor.CellId);
            auto* entry = (it == RegisteredCellMap_.end()) ? nullptr : &it->second;
            if (!entry) {
                auto it = RegisteredCellMap_.insert(std::make_pair(descriptor.CellId, TEntry())).first;
                entry = &it->second;
                result = true;
            }
            if (entry->Descriptor.ConfigVersion < descriptor.ConfigVersion) {
                entry->Descriptor = descriptor;
                InitChannel(entry);
                result = true;
            }
        }
        if (result) {
            LOG_INFO("Cell reconfigured (CellId: %v, ConfigVersion: %v)",
                descriptor.CellId,
                descriptor.ConfigVersion);
        }
        return result;
    }

    bool UnregisterCell(const TCellId& cellId)
    {
        bool result;
        {
            TWriterGuard guard(SpinLock_);
            UnregisteredCellIds_.insert(cellId);
            result = RegisteredCellMap_.erase(cellId) == 1;
        }
        if (result) {
            LOG_INFO("Cell unregistered (CellId: %v)",
                cellId);
        }
        return result;
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
        TCellDescriptor Descriptor;
        TEnumIndexedVector<IChannelPtr, EPeerKind> Channels;
    };

    TReaderWriterSpinLock SpinLock_;
    yhash_map<TCellId, TEntry> RegisteredCellMap_;
    yhash_set<TCellId> UnregisteredCellIds_;


    void InitChannel(TEntry* entry)
    {
        auto peerConfig = New<TPeerConnectionConfig>();
        peerConfig->CellId = entry->Descriptor.CellId;
        for (const auto& peer : entry->Descriptor.Peers) {
            if (!peer.IsNull()) {
                peerConfig->Addresses.push_back(peer.GetAddressOrThrow(NetworkName_));
            }
        }
        peerConfig->DiscoverTimeout = Config_->DiscoverTimeout;
        peerConfig->SoftBackoffTime = Config_->SoftBackoffTime;
        peerConfig->HardBackoffTime = Config_->HardBackoffTime;

        for (auto kind : TEnumTraits<EPeerKind>::GetDomainValues()) {
            entry->Channels[kind] = CreatePeerChannel(peerConfig, ChannelFactory_, kind);
        }
    }

    void OnSynchronized(const THiveServiceProxy::TErrorOrRspSyncCellsPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Error synchronizing cell directory")
                << rspOrError;
        }

        const auto& rsp = rspOrError.Value();

        for (const auto& info : rsp->cells_to_unregister()) {
            auto cellId = FromProto<TCellId>(info.cell_id());
            UnregisterCell(cellId);
        }

        for (const auto& info : rsp->cells_to_reconfigure()) {
            auto descriptor = FromProto<NHive::TCellDescriptor>(info.cell_descriptor());
            ReconfigureCell(descriptor);
        }

        LOG_INFO("Cell directory synchronized");
    }

};

////////////////////////////////////////////////////////////////////////////////

TCellDirectory::TCellDirectory(
    TCellDirectoryConfigPtr config,
    IChannelFactoryPtr channelFactory,
    const Stroka& networkName)
    : Impl_(New<TImpl>(
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

IChannelPtr TCellDirectory::GetChannel(const TCellId& cellId, EPeerKind peerKind)
{
    return Impl_->GetChannel(cellId, peerKind);
}

TNullable<TCellDescriptor> TCellDirectory::FindDescriptor(const TCellId& cellId)
{
    return Impl_->FindDescriptor(cellId);
}

TCellDescriptor TCellDirectory::GetDescriptorOrThrow(const TCellId& cellId)
{
    return Impl_->GetDescriptorOrThrow(cellId);
}

std::vector<TCellInfo> TCellDirectory::GetRegisteredCells()
{
    return Impl_->GetRegisteredCells();
}

TFuture<void> TCellDirectory::Synchronize(NRpc::IChannelPtr channel)
{
    return Impl_->Synchronize(channel);
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
