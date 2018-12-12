#include "cell_directory.h"
#include "config.h"
#include "private.h"

#include <yt/ytlib/election/config.h>

#include <yt/ytlib/hive/proto/cell_directory.pb.h>

#include <yt/ytlib/hydra/config.h>
#include <yt/ytlib/hydra/peer_channel.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT::NHiveClient {

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

TCellPeerDescriptor::TCellPeerDescriptor()
    : Voting_(true)
{ }

TCellPeerDescriptor::TCellPeerDescriptor(const TNodeDescriptor& other, bool voting)
    : TNodeDescriptor(other)
    , Voting_(voting)
{ }

namespace {

TAddressMap ToAddressMap(const TCellPeerConfig& config, const TNetworkPreferenceList& networks)
{
    TAddressMap result;
    if (config.Address) {
        result.reserve(networks.size() + 1);
        for (const auto& network : networks) {
            YCHECK(result.emplace(network, *config.Address).second);
        }
        // Default network must always be present in address map.
        result.emplace(DefaultNetworkName, *config.Address);
    }
    return result;
}

} // namespace

TCellPeerDescriptor::TCellPeerDescriptor(
    const TCellPeerConfig& config,
    const TNetworkPreferenceList& networks)
    : TNodeDescriptor(ToAddressMap(config, networks))
    , Voting_(config.Voting)
{ }

TCellPeerConfig TCellPeerDescriptor::ToConfig(const TNetworkPreferenceList& networks) const
{
    TCellPeerConfig config;
    config.Voting = Voting_;
    config.Address = IsNull() ? std::nullopt : std::make_optional(GetAddressOrThrow(networks));
    return config;
}

////////////////////////////////////////////////////////////////////////////////

TCellDescriptor::TCellDescriptor(const TCellId& cellId)
    : CellId(cellId)
{ }

TCellConfigPtr TCellDescriptor::ToConfig(const TNetworkPreferenceList& networks) const
{
    auto config = New<TCellConfig>();
    config->CellId = CellId;
    config->Peers.reserve(Peers.size());
    for (const auto& peer : Peers) {
        config->Peers.emplace_back(peer.ToConfig(networks));
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

void ToProto(NProto::TCellPeerDescriptor* protoDescriptor, const TCellPeerDescriptor& descriptor)
{
    ToProto(protoDescriptor->mutable_node_descriptor(), descriptor);
    protoDescriptor->set_voting(descriptor.GetVoting());
}

void FromProto(TCellPeerDescriptor* descriptor, const NProto::TCellPeerDescriptor& protoDescriptor)
{
    FromProto(descriptor, protoDescriptor.node_descriptor());
    descriptor->SetVoting(protoDescriptor.voting());
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
    descriptor->Peers = FromProto<std::vector<TCellPeerDescriptor>>(protoDescriptor.peers());
}

////////////////////////////////////////////////////////////////////////////////

class TCellDirectory::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TCellDirectoryConfigPtr config,
        IChannelFactoryPtr channelFactory,
        const TNetworkPreferenceList& networks,
        const NLogging::TLogger& logger)
        : Config_(config)
        , ChannelFactory_(channelFactory)
        , Networks_(networks)
        , Logger(logger)
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

    bool IsCellUnregistered(const TCellId& cellId)
    {
        TReaderGuard guard(SpinLock_);
        return UnregisteredCellIds_.find(cellId) != UnregisteredCellIds_.end();
    }

    std::optional<TCellDescriptor> FindDescriptor(const TCellId& cellId)
    {
        TReaderGuard guard(SpinLock_);
        auto it = RegisteredCellMap_.find(cellId);
        return it == RegisteredCellMap_.end() ? std::nullopt : std::make_optional(it->second.Descriptor);
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
        descriptor.Peers.reserve(config->Peers.size());
        for (const auto& peer : config->Peers) {
            descriptor.Peers.emplace_back(peer, Networks_);
        }
        return ReconfigureCell(descriptor);
    }

    bool ReconfigureCell(TPeerConnectionConfigPtr config, int configVersion)
    {
        auto cellConfig = New<TCellConfig>();
        cellConfig->CellId = config->CellId;
        for (const auto& address : config->Addresses) {
            cellConfig->Peers.emplace_back(address);
        }
        return ReconfigureCell(cellConfig, configVersion);
    }

    bool ReconfigureCell(const TCellDescriptor& descriptor)
    {
        TWriterGuard guard(SpinLock_);
        if (UnregisteredCellIds_.find(descriptor.CellId) != UnregisteredCellIds_.end()) {
            return false;
        }
        auto it = RegisteredCellMap_.find(descriptor.CellId);
        if (it == RegisteredCellMap_.end()) {
            it = RegisteredCellMap_.insert(std::make_pair(descriptor.CellId, TEntry(descriptor))).first;
            if (descriptor.ConfigVersion >= 0) {
                InitChannel(&it->second);
            }
            YT_LOG_DEBUG("Cell registered (CellId: %v, ConfigVersion: %v)",
                descriptor.CellId,
                descriptor.ConfigVersion);
            return true;
        } else if (it->second.Descriptor.ConfigVersion < descriptor.ConfigVersion) {
            it->second.Descriptor = descriptor;
            InitChannel(&it->second);
            YT_LOG_DEBUG("Cell reconfigured (CellId: %v, ConfigVersion: %v)",
                descriptor.CellId,
                descriptor.ConfigVersion);
            return true;
        }
        return false;
    }

    void RegisterCell(const TCellId& cellId)
    {
        TCellDescriptor descriptor;
        descriptor.CellId = cellId;
        ReconfigureCell(descriptor);
    }

    bool UnregisterCell(const TCellId& cellId)
    {
        TWriterGuard guard(SpinLock_);
        UnregisteredCellIds_.insert(cellId);
        if (RegisteredCellMap_.erase(cellId) == 0) {
            return false;
        }
        YT_LOG_INFO("Cell unregistered (CellId: %v)",
            cellId);
        return true;
    }

    void Clear()
    {
        TWriterGuard guard(SpinLock_);
        RegisteredCellMap_.clear();
    }

private:
    const TCellDirectoryConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const TNetworkPreferenceList Networks_;
    const NLogging::TLogger Logger;

    struct TEntry
    {
        explicit TEntry(const TCellDescriptor& descriptor)
            : Descriptor(descriptor)
        { }

        TCellDescriptor Descriptor;
        TEnumIndexedVector<IChannelPtr, EPeerKind> Channels;
    };

    TReaderWriterSpinLock SpinLock_;
    THashMap<TCellId, TEntry> RegisteredCellMap_;
    THashSet<TCellId> UnregisteredCellIds_;


    void InitChannel(TEntry* entry)
    {
        auto peerConfig = New<TPeerConnectionConfig>();
        peerConfig->CellId = entry->Descriptor.CellId;
        for (const auto& peer : entry->Descriptor.Peers) {
            if (!peer.IsNull()) {
                peerConfig->Addresses.emplace_back(peer.GetAddressOrThrow(Networks_));
            }
        }
        peerConfig->DiscoverTimeout = Config_->DiscoverTimeout;
        peerConfig->SoftBackoffTime = Config_->SoftBackoffTime;
        peerConfig->HardBackoffTime = Config_->HardBackoffTime;

        for (auto kind : TEnumTraits<EPeerKind>::GetDomainValues()) {
            entry->Channels[kind] = CreatePeerChannel(peerConfig, ChannelFactory_, kind);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TCellDirectory::TCellDirectory(
    TCellDirectoryConfigPtr config,
    IChannelFactoryPtr channelFactory,
    const TNetworkPreferenceList& networks,
    const NLogging::TLogger& logger)
    : Impl_(New<TImpl>(
        config,
        channelFactory,
        networks,
        logger))
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

std::optional<TCellDescriptor> TCellDirectory::FindDescriptor(const TCellId& cellId)
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

void TCellDirectory::RegisterCell(const TCellId& cellId)
{
    Impl_->RegisterCell(cellId);
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

} // namespace NYT::NHiveClient
