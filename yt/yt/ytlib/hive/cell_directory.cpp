#include "cell_directory.h"

#include "cluster_directory.h"
#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hive/proto/cell_directory.pb.h>

#include <yt/yt/ytlib/hydra/config.h>
#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/rpc/roaming_channel.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NHiveClient {

using namespace NConcurrency;
using namespace NElection;
using namespace NHydra;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TAlienClusterChannelProvider
    : public IRoamingChannelProvider
{
public:
    TAlienClusterChannelProvider(
        TString address,
        TString cluster,
        TClusterDirectoryPtr clusterDirectory,
        TLogger logger)
        : Address_(std::move(address))
        , Cluster_(std::move(cluster))
        , ClusterDirectory_(std::move(clusterDirectory))
        , Logger(std::move(logger))
    { }

    const TString& GetEndpointDescription() const override
    {
        return Address_;
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        static auto attributes = CreateEphemeralAttributes();
        return *attributes;
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(std::string /*serviceName*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        auto connection = ClusterDirectory_->FindConnection(Cluster_);
        if (connection) {
            return MakeFuture(connection->GetChannelFactory()->CreateChannel(Address_));
        }

        YT_LOG_DEBUG("Cannot find cluster in the cluster directory (Cluster: %v)",
            Cluster_);
        return MakeFuture<IChannelPtr>(
            TError(NRpc::EErrorCode::Unavailable, "Cannot find cluster in the cluster directory")
                << TErrorAttribute("cluster", Cluster_));
    }

    void Terminate(const TError& /*error*/) override
    {
        // This channel provider does not own any channel, so it is not responsible
        // for any termination.
    }

private:
    const TString Address_;
    const TString Cluster_;
    const TClusterDirectoryPtr ClusterDirectory_;
    const TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TAlienClusterChannelProvider)

////////////////////////////////////////////////////////////////////////////////

class TAlienClusterChannelFactory
    : public IChannelFactory
{
public:
    TAlienClusterChannelFactory(
        THashSet<TString> nativeClusterAddresses,
        IChannelFactoryPtr nativeChannelFactory,
        THashMap<TString, TString> addressToAlienCluster,
        TClusterDirectoryPtr clusterDirectory,
        TLogger logger)
        : NativeClusterAddresses_(std::move(nativeClusterAddresses))
        , NativeChannelFactory_(std::move(nativeChannelFactory))
        , AddressToAlienCluster_(std::move(addressToAlienCluster))
        , ClusterDirectory_(std::move(clusterDirectory))
        , Logger(std::move(logger))
    { }

    IChannelPtr CreateChannel(const TString& address) override
    {
        if (NativeClusterAddresses_.contains(address)) {
            return NativeChannelFactory_->CreateChannel(address);
        }

        auto it = AddressToAlienCluster_.find(address);
        if (it == AddressToAlienCluster_.end()) {
            THROW_ERROR_EXCEPTION("Unknown address %Qv", address);
        }

        auto provider = New<TAlienClusterChannelProvider>(
            address,
            it->second,
            ClusterDirectory_,
            Logger);
        return CreateRoamingChannel(std::move(provider));
    }

private:
    const THashSet<TString> NativeClusterAddresses_;
    const IChannelFactoryPtr NativeChannelFactory_;
    const THashMap<TString, TString> AddressToAlienCluster_;
    const TClusterDirectoryPtr ClusterDirectory_;
    const TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TAlienClusterChannelFactory)

////////////////////////////////////////////////////////////////////////////////

TCellPeerDescriptor::TCellPeerDescriptor()
    : Voting_(true)
{ }

TCellPeerDescriptor::TCellPeerDescriptor(const TNodeDescriptor& other, bool voting)
    : TNodeDescriptor(other)
    , Voting_(voting)
{ }

namespace {

TAddressMap ToAddressMap(const TCellPeerConfigPtr& config, const TNetworkPreferenceList& networks)
{
    TAddressMap result;
    if (config->Address) {
        result.reserve(networks.size() + 1);
        for (const auto& network : networks) {
            EmplaceOrCrash(result, network, *config->Address);
        }
        // Default network must always be present in address map.
        result.emplace(DefaultNetworkName, *config->Address);
    }
    return result;
}

} // namespace

TCellPeerDescriptor::TCellPeerDescriptor(
    const TCellPeerConfigPtr& config,
    const TNetworkPreferenceList& networks)
    : TNodeDescriptor(ToAddressMap(config, networks))
    , Voting_(config->Voting)
{ }

TCellPeerConfigPtr TCellPeerDescriptor::ToConfig(const TNetworkPreferenceList& networks) const
{
    auto config = New<TCellPeerConfig>();
    config->Voting = Voting_;
    config->Address = IsNull() ? std::nullopt : std::make_optional(GetAddressOrThrow(networks));
    config->AlienCluster = AlienCluster_;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

TCellDescriptor::TCellDescriptor(TCellId cellId)
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
    return TCellInfo{
        .CellId = CellId,
        .ConfigVersion = ConfigVersion
    };
}

void ToProto(NProto::TCellPeerDescriptor* protoDescriptor, const TCellPeerDescriptor& descriptor)
{
    ToProto(protoDescriptor->mutable_node_descriptor(), descriptor);
    protoDescriptor->set_voting(descriptor.GetVoting());
    if (descriptor.GetAlienCluster()) {
        protoDescriptor->set_alien_cluster(*descriptor.GetAlienCluster());
    } else {
        protoDescriptor->clear_alien_cluster();
    }
}

void FromProto(TCellPeerDescriptor* descriptor, const NProto::TCellPeerDescriptor& protoDescriptor)
{
    FromProto(descriptor, protoDescriptor.node_descriptor());
    descriptor->SetVoting(protoDescriptor.voting());
    descriptor->SetAlienCluster(protoDescriptor.has_alien_cluster()
        ? std::make_optional(protoDescriptor.alien_cluster())
        : std::nullopt);
}

void Serialize(const TCellPeerDescriptor& descriptor, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Do([&] (auto fluent) {
                SerializeFragment(static_cast<const TNodeDescriptor&>(descriptor), fluent.GetConsumer());
            })
            .Item("voting").Value(descriptor.GetVoting())
            .OptionalItem("alien_cluster", descriptor.GetAlienCluster())
        .EndMap();
}

void Deserialize(TCellPeerDescriptor& descriptor, INodePtr node)
{
    descriptor = {};

    DeserializeFragment(static_cast<TNodeDescriptor&>(descriptor), node);

    auto mapNode = node->AsMap();

    descriptor.SetVoting(mapNode->GetChildValueOrThrow<bool>("voting"));
    if (auto child = mapNode->FindChildValue<TString>("alien_cluster")) {
        descriptor.SetAlienCluster(*child);
    }
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

void Serialize(const TCellDescriptor& descriptor, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("cell_id").Value(descriptor.CellId)
            .Item("config_version").Value(descriptor.ConfigVersion)
            .Item("peers").Value(descriptor.Peers)
        .EndMap();
}

void Deserialize(TCellDescriptor& descriptor, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();

    Deserialize(descriptor.CellId, mapNode->GetChildOrThrow("cell_id"));
    Deserialize(descriptor.ConfigVersion, mapNode->GetChildOrThrow("config_version"));
    Deserialize(descriptor.Peers, mapNode->GetChildOrThrow("peers"));
}

////////////////////////////////////////////////////////////////////////////////

class TCellDirectory
    : public ICellDirectory
{
public:
    TCellDirectory(
        TCellDirectoryConfigPtr config,
        IChannelFactoryPtr channelFactory,
        TClusterDirectoryPtr clusterDirectory,
        const TNetworkPreferenceList& networks,
        TLogger logger)
        : Config_(std::move(config))
        , ChannelFactory_(std::move(channelFactory))
        , ClusterDirectory_(std::move(clusterDirectory))
        , Networks_(networks)
        , Logger(std::move(logger))
    { }

    IChannelPtr FindChannelByCellId(TCellId cellId, EPeerKind peerKind) override
    {
        auto guard = ReaderGuard(SpinLock_);
        auto it = CellIdToEntry_.find(cellId);
        return it == CellIdToEntry_.end() ? nullptr : it->second.Channels[peerKind];
    }

    IChannelPtr GetChannelByCellIdOrThrow(TCellId cellId, EPeerKind peerKind) override
    {
        auto channel = FindChannelByCellId(cellId, peerKind);
        if (!channel) {
            THROW_ERROR_EXCEPTION("No cell with id %v is known",
                cellId);
        }
        return channel;
    }

    IChannelPtr GetChannelByCellId(TCellId cellId, EPeerKind peerKind) override
    {
        auto channel = FindChannelByCellId(cellId, peerKind);
        YT_VERIFY(channel);
        return channel;
    }

    IChannelPtr FindChannelByCellTag(TCellTag cellTag, EPeerKind peerKind) override
    {
        auto guard = ReaderGuard(SpinLock_);
        auto it = CellTagToEntry_.find(cellTag);
        return it == CellTagToEntry_.end() ? nullptr : it->second->Channels[peerKind];
    }

    IChannelPtr GetChannelByCellTagOrThrow(TCellTag cellTag, EPeerKind peerKind) override
    {
        auto channel = FindChannelByCellTag(cellTag, peerKind);
        if (!channel) {
            THROW_ERROR_EXCEPTION("No cell with tag %v is known",
                cellTag);
        }
        return channel;
    }

    IChannelPtr GetChannelByCellTag(TCellTag cellTag, EPeerKind peerKind) override
    {
        auto channel = FindChannelByCellTag(cellTag, peerKind);
        YT_VERIFY(channel);
        return channel;
    }

    std::vector<TCellInfo> GetRegisteredCells() override
    {
        auto guard = ReaderGuard(SpinLock_);
        std::vector<TCellInfo> result;
        result.reserve(CellIdToEntry_.size());
        for (const auto& [cellId, entry] : CellIdToEntry_) {
            result.push_back({cellId, entry.Descriptor->ConfigVersion});
        }
        return result;
    }

    bool IsCellUnregistered(TCellId cellId) override
    {
        auto guard = ReaderGuard(SpinLock_);
        return UnregisteredCellIds_.find(cellId) != UnregisteredCellIds_.end();
    }

    bool IsCellRegistered(TCellId cellId) override
    {
        auto guard = ReaderGuard(SpinLock_);
        return CellIdToEntry_.find(cellId) != CellIdToEntry_.end();
    }

    TCellDescriptorPtr FindDescriptorByCellId(TCellId cellId) override
    {
        auto guard = ReaderGuard(SpinLock_);
        auto it = CellIdToEntry_.find(cellId);
        return it == CellIdToEntry_.end() ? nullptr : it->second.Descriptor;
    }

    TCellDescriptorPtr FindDescriptorByCellTag(TCellTag cellTag) override
    {
        auto guard = ReaderGuard(SpinLock_);
        auto it = CellTagToEntry_.find(cellTag);
        return it == CellTagToEntry_.end() ? nullptr : it->second->Descriptor;
    }

    TCellDescriptorPtr GetDescriptorByCellIdOrThrow(TCellId cellId) override
    {
        auto result = FindDescriptorByCellId(cellId);
        if (!result) {
            THROW_ERROR_EXCEPTION("Unknown cell %v",
                cellId);
        }
        return result;
    }

    std::optional<TString> FindPeerAddress(TCellId cellId, int peerId) override
    {
        auto guard = ReaderGuard(SpinLock_);
        auto it = CellIdToEntry_.find(cellId);
        if (!it) {
            return {};
        }

        const auto& peers = it->second.Descriptor->Peers;
        if (peerId >= std::ssize(peers)) {
            return {};
        }
        return peers[peerId].FindAddress(Networks_);
    }

    TSynchronizationResult Synchronize(const std::vector<TCellInfo>& knownCells) override
    {
        auto guard = ReaderGuard(SpinLock_);

        TSynchronizationResult result;

        for (const auto& knownCell : knownCells) {
            auto cellId = knownCell.CellId;
            if (auto it = CellIdToEntry_.find(cellId)) {
                const auto& entry = it->second;
                if (knownCell.ConfigVersion < entry.Descriptor->ConfigVersion) {
                    result.ReconfigureRequests.push_back({entry.Descriptor, knownCell.ConfigVersion});
                }
            } else {
                // NB: Currently we never request to unregister chaos cells; cf. YT-16393.
                if (TypeFromId(cellId) != EObjectType::ChaosCell) {
                    result.UnregisterRequests.push_back({
                        .CellId = cellId
                    });
                }
            }
        }

        THashMap<TCellId, const TEntry*> missingMap;
        missingMap.reserve(CellIdToEntry_.size());

        for (const auto& [cellId, entry] : CellIdToEntry_) {
            EmplaceOrCrash(missingMap, cellId, &entry);
        }

        for (const auto& knownCell : knownCells) {
            missingMap.erase(knownCell.CellId);
        }

        for (auto [cellId, entry] : missingMap) {
            result.ReconfigureRequests.push_back({
                .NewDescriptor = entry->Descriptor,
                .OldConfigVersion = -1
            });
        }

        return result;
    }

    bool ReconfigureCell(TCellConfigPtr config, int configVersion) override
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

    bool ReconfigureCell(TPeerConnectionConfigPtr config, int configVersion) override
    {
        auto cellConfig = New<TCellConfig>();
        cellConfig->CellId = config->CellId;
        if (config->Addresses) {
            for (const auto& address : *config->Addresses) {
                auto peerConfig = New<TCellPeerConfig>();
                peerConfig->Address = address;
                cellConfig->Peers.push_back(peerConfig);
            }
        }
        return ReconfigureCell(cellConfig, configVersion);
    }

    bool ReconfigureCell(const TCellDescriptor& descriptor) override
    {
        auto guard = WriterGuard(SpinLock_);
        if (UnregisteredCellIds_.contains(descriptor.CellId)) {
            return false;
        }
        auto it = CellIdToEntry_.find(descriptor.CellId);
        if (it == CellIdToEntry_.end()) {
            it = CellIdToEntry_.emplace(descriptor.CellId, TEntry(descriptor)).first;
            auto* entry = &it->second;
            if (descriptor.ConfigVersion >= 0) {
                InitChannel(entry);
            }
            if (IsGlobalCellId(descriptor.CellId)) {
                auto cellTag = CellTagFromId(descriptor.CellId);
                if (auto [jt, inserted] = CellTagToEntry_.emplace(cellTag, entry); !inserted) {
                    YT_LOG_ALERT("Duplicate global cell id (CellTag: %v, ExistingCellId: %v, NewCellId: %v)",
                        cellTag,
                        jt->second->Descriptor->CellId,
                        descriptor.CellId);
                }
            }
            YT_LOG_DEBUG("Cell registered (CellId: %v, ConfigVersion: %v)",
                descriptor.CellId,
                descriptor.ConfigVersion);
            return true;
        } else if (it->second.Descriptor->ConfigVersion < descriptor.ConfigVersion) {
            it->second.Descriptor = New<TCellDescriptor>(descriptor);
            InitChannel(&it->second);
            YT_LOG_DEBUG("Cell reconfigured (CellId: %v, ConfigVersion: %v)",
                descriptor.CellId,
                descriptor.ConfigVersion);
            return true;
        }
        return false;
    }

    void RegisterCell(TCellId cellId) override
    {
        ReconfigureCell(TCellDescriptor(cellId));
    }

    bool UnregisterCell(TCellId cellId) override
    {
        auto guard = WriterGuard(SpinLock_);
        UnregisteredCellIds_.insert(cellId);
        if (CellIdToEntry_.erase(cellId) == 0) {
            return false;
        }
        if (IsGlobalCellId(cellId)) {
            EraseOrCrash(CellTagToEntry_, CellTagFromId(cellId));
        }
        YT_LOG_INFO("Cell unregistered (CellId: %v)",
            cellId);
        return true;
    }

    void Clear() override
    {
        auto guard = WriterGuard(SpinLock_);
        CellTagToEntry_.clear();
        CellIdToEntry_.clear();
    }

private:
    const TCellDirectoryConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const TClusterDirectoryPtr ClusterDirectory_;
    const TNetworkPreferenceList Networks_;
    const TLogger Logger;

    struct TEntry
    {
        explicit TEntry(const TCellDescriptor& descriptor)
            : Descriptor(New<TCellDescriptor>(descriptor))
        { }

        TCellDescriptorPtr Descriptor;
        TEnumIndexedArray<EPeerKind, IChannelPtr> Channels;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TCellId, TEntry> CellIdToEntry_;
    THashMap<TCellTag, TEntry*> CellTagToEntry_;
    THashSet<TCellId> UnregisteredCellIds_;


    void InitChannel(TEntry* entry)
    {
        THashMap<TString, TString> addressToAlienCluster;
        THashSet<TString> nativeClusterAddresses;

        auto peerConfig = New<TPeerConnectionConfig>();
        peerConfig->CellId = entry->Descriptor->CellId;
        peerConfig->Addresses.emplace();

        for (const auto& peer : entry->Descriptor->Peers) {
            if (peer.IsNull()) {
                continue;
            }

            auto address = peer.GetAddressOrThrow(Networks_);
            peerConfig->Addresses->push_back(address);

            if (auto alienCluster = peer.GetAlienCluster()) {
                addressToAlienCluster[address] = *alienCluster;
            } else {
                nativeClusterAddresses.insert(address);
            }
        }
        peerConfig->DiscoverTimeout = Config_->DiscoverTimeout;
        peerConfig->AcknowledgementTimeout = Config_->AcknowledgementTimeout;
        peerConfig->RediscoverPeriod = Config_->RediscoverPeriod;
        peerConfig->RediscoverSplay = Config_->RediscoverSplay;
        peerConfig->SoftBackoffTime = Config_->SoftBackoffTime;
        peerConfig->HardBackoffTime = Config_->HardBackoffTime;

        auto alienClusterChannelFactory = New<TAlienClusterChannelFactory>(
            std::move(nativeClusterAddresses),
            ChannelFactory_,
            std::move(addressToAlienCluster),
            ClusterDirectory_,
            Logger);

        for (auto kind : TEnumTraits<EPeerKind>::GetDomainValues()) {
            entry->Channels[kind] = CreatePeerChannel(peerConfig, alienClusterChannelFactory, kind);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ICellDirectoryPtr CreateCellDirectory(
    TCellDirectoryConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TClusterDirectoryPtr clusterDirectory,
    const NNodeTrackerClient::TNetworkPreferenceList& networks,
    TLogger logger)
{
    return New<TCellDirectory>(
        std::move(config),
        std::move(channelFactory),
        std::move(clusterDirectory),
        networks,
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
