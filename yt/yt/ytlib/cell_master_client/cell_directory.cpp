#include "cell_directory.h"

#include "private.h"
#include "config.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/ytlib/node_tracker_client/node_addresses_provider.h>

#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/caching_object_service.h>
#include <yt/yt/ytlib/object_client/object_service_cache.h>

#include <yt/yt_proto/yt/client/cell_master/proto/cell_directory.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NCellMasterClient {

using namespace NApi;
using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

class TCellDirectory
    : public ICellDirectory
{
public:
    DEFINE_SIGNAL_OVERRIDE(TCellReconfigurationSignature, CellDirectoryChanged);

public:
    TCellDirectory(
        TCellDirectoryConfigPtr config,
        NNative::TConnectionOptions options,
        IChannelFactoryPtr channelFactory,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , PrimaryMasterCellId_(Config_->PrimaryMaster->CellId)
        , PrimaryMasterCellTag_(CellTagFromId(PrimaryMasterCellId_))
        , ChannelFactory_(std::move(std::move(channelFactory)))
        , Logger(std::move(logger))
        , Cache_(New<TObjectServiceCache>(
            Config_->CachingObjectService,
            GetNullMemoryUsageTracker(),
            Logger,
            NProfiling::TProfiler()))
        , RpcServer_(CreateLocalServer())
        , Options_(std::move(options))
        , RandomGenerator_(TInstant::Now().GetValue())
    {
        for (const auto& masterConfig : Config_->SecondaryMasters) {
            auto cellId = masterConfig->CellId;
            SecondaryMasterConnectionConfigs_[CellTagFromId(cellId)] = masterConfig;
            SecondaryMasterCellTags_.push_back(CellTagFromId(cellId));
            SecondaryMasterCellIds_.push_back(cellId);
        }
        // Sort tag list to simplify subsequent equality checks.
        Sort(SecondaryMasterCellTags_);

        // NB: unlike channels, roles will be filled on first sync.

        {
            auto guard = WriterGuard(SpinLock_);
            InitMasterChannels(Config_->PrimaryMaster);
            for (const auto& [_, masterConfig] : SecondaryMasterConnectionConfigs_) {
                InitMasterChannels(masterConfig);
            }
        }
        RpcServer_->Start();
    }

    TCellId GetPrimaryMasterCellId() override
    {
        return PrimaryMasterCellId_;
    }

    TCellTag GetPrimaryMasterCellTag() override
    {
        return PrimaryMasterCellTag_;
    }

    TCellTagList GetSecondaryMasterCellTags() override
    {
        auto guard = ReaderGuard(SpinLock_);
        return SecondaryMasterCellTags_;
    }

    TCellIdList GetSecondaryMasterCellIds() override
    {
        auto guard = ReaderGuard(SpinLock_);
        return SecondaryMasterCellIds_;
    }

    TSecondaryMasterConnectionConfigs GetSecondaryMasterConnectionConfigs()
    {
        auto guard = ReaderGuard(SpinLock_);
        return SecondaryMasterConnectionConfigs_;
    }

    IChannelPtr GetMasterChannelOrThrow(EMasterChannelKind kind, TCellTag cellTag) override
    {
        cellTag = cellTag == PrimaryMasterCellTagSentinel ? GetPrimaryMasterCellTag() : cellTag;
        return GetCellChannelOrThrow(cellTag, kind);
    }

    IChannelPtr GetMasterChannelOrThrow(EMasterChannelKind kind, TCellId cellId) override
    {
        if (ReplaceCellTagInId(cellId, TCellTag(0)) != ReplaceCellTagInId(GetPrimaryMasterCellId(), TCellTag(0))) {
            THROW_ERROR_EXCEPTION("Unknown master cell id %v",
                cellId);
        }
        return GetMasterChannelOrThrow(kind, CellTagFromId(cellId));
    }

    TCellTagList GetMasterCellTagsWithRole(EMasterCellRole role) override
    {
        auto guard = ReaderGuard(SpinLock_);
        return RoleToCellTags_[role];
    }

    TCellId GetRandomMasterCellWithRoleOrThrow(EMasterCellRole role) override
    {
        auto candidateCellTags = GetMasterCellTagsWithRole(role);
        if (candidateCellTags.empty()) {
            THROW_ERROR_EXCEPTION("No master cell with %Qlv role is known",
                role);
        }

        size_t randomIndex = 0;
        {
            auto guard = ReaderGuard(SpinLock_);
            randomIndex = RandomGenerator_.Generate<size_t>();
        }

        auto cellTag = candidateCellTags[randomIndex % candidateCellTags.size()];
        return ReplaceCellTagInId(GetPrimaryMasterCellId(), cellTag);
    }

    void Update(const NCellMasterClient::NProto::TCellDirectory& protoDirectory) override
    {
        THashMap<TCellTag, EMasterCellRoles> cellTagToRoles;
        cellTagToRoles.reserve(protoDirectory.items_size());

        TEnumIndexedArray<EMasterCellRole, TCellTagList> roleToCellTags;
        THashMap<TCellTag, std::vector<TString>> cellAddresses;
        cellAddresses.reserve(protoDirectory.items_size());

        TSecondaryMasterConnectionConfigs cellTagToSecondaryMaster;
        TCellTagList secondaryCellTags;
        cellTagToSecondaryMaster.reserve(protoDirectory.items_size());
        secondaryCellTags.reserve(protoDirectory.items_size());

        auto primaryCellFound = false;

        for (const auto& item : protoDirectory.items()) {
            // TODO(cherepashka): parsing of proto version of TMasterConnectionConfig.
            auto masterConnectionConfig = New<TMasterConnectionConfig>();
            masterConnectionConfig->CellId = FromProto<TCellId>(item.cell_id());
            masterConnectionConfig->Addresses = FromProto<std::vector<TString>>(item.addresses());
            Sort(*masterConnectionConfig->Addresses);

            auto cellId = masterConnectionConfig->CellId;
            auto cellTag = CellTagFromId(cellId);

            auto roles = EMasterCellRoles::None;
            for (auto protoRole : item.roles()) {
                auto role = CheckedEnumCast<EMasterCellRole>(protoRole);
                roles = roles | EMasterCellRoles(role);
                roleToCellTags[role].push_back(cellTag);
            }
            EmplaceOrCrash(cellTagToRoles, cellTag, roles);

            YT_VERIFY(masterConnectionConfig->Addresses);
            auto addresses = *masterConnectionConfig->Addresses;
            Sort(addresses);
            EmplaceOrCrash(cellAddresses, cellTag, std::move(addresses));

            if (cellTag == PrimaryMasterCellTag_) {
                YT_VERIFY(cellId == PrimaryMasterCellId_);
                primaryCellFound = true;
            } else {
                EmplaceOrCrash(cellTagToSecondaryMaster, cellTag, std::move(masterConnectionConfig));
                secondaryCellTags.push_back(cellTag);
            }
        }

        YT_VERIFY(primaryCellFound);
        YT_VERIFY(cellTagToRoles.contains(PrimaryMasterCellTag_) && cellAddresses.contains(PrimaryMasterCellTag_));

        // To get the actual values under lock.
        const auto& oldSecondaryMasterCellTags = GetSecondaryMasterCellTags();
        const auto& oldSecondaryMasterConnectionConfigs = GetSecondaryMasterConnectionConfigs();

        if (ClusterMasterCompositionChanged(cellTagToSecondaryMaster)) {
            YT_LOG_INFO("Cluster membership configuration has changed, starting reconfiguration "
                "(SecondaryMasterCellTags: %v, ReceivedSecondaryMasterCellTags: %v)",
                oldSecondaryMasterCellTags,
                secondaryCellTags);
            ReconfigureMasterCellDirectory(oldSecondaryMasterConnectionConfigs, cellTagToSecondaryMaster, secondaryCellTags);
        }

        if (oldSecondaryMasterCellTags.empty() &&
            !secondaryCellTags.empty()) {
            const auto primaryMasterCellRoles = cellTagToRoles[PrimaryMasterCellTag_];
            cellTagToRoles.clear();
            cellTagToRoles.emplace(PrimaryMasterCellTag_, primaryMasterCellRoles);
            for (auto role : TEnumTraits<EMasterCellRole>::GetDomainValues()) {
                roleToCellTags[role].clear();
                if (Any(primaryMasterCellRoles & EMasterCellRoles(role))) {
                    roleToCellTags[role].push_back(PrimaryMasterCellTag_);
                }
            }
        } else {
            if (Config_->PrimaryMaster->Addresses) {
                auto expectedPrimaryCellAddresses = *Config_->PrimaryMaster->Addresses;
                Sort(expectedPrimaryCellAddresses);
                const auto& actualPrimaryCellAddresses = cellAddresses[PrimaryMasterCellTag_];
                YT_LOG_WARNING_UNLESS(
                    expectedPrimaryCellAddresses == actualPrimaryCellAddresses,
                    "Synchronized primary master cell addresses do not match, connection config is probably incorrect (ConfigPrimaryMasterAddresses: %v, SynchronizedPrimaryMasterAddresses: %v)",
                    expectedPrimaryCellAddresses,
                    actualPrimaryCellAddresses);

                for (auto [_, cellConfig] : oldSecondaryMasterConnectionConfigs) {
                    if (cellConfig->Addresses) {
                        auto expectedCellAddresses = *cellConfig->Addresses;
                        Sort(expectedCellAddresses);
                        const auto& actualCellAddresses = cellAddresses[CellTagFromId(cellConfig->CellId)];

                        YT_LOG_WARNING_UNLESS(
                            expectedCellAddresses == actualCellAddresses,
                            "Synchronized secondary master cell addresses do not match, connection config is probably incorrect (ConfigSecondaryMasterAddresses: %v, SynchronizedSecondaryMasterAddresses: %v)",
                            expectedCellAddresses,
                            actualCellAddresses);
                    }
                }
            }
        }

        YT_LOG_DEBUG("Successfully synchronized master cell roles (CellTagToRoles: %v)",
            cellTagToRoles);

        {
            auto guard = WriterGuard(SpinLock_);
            CellTagToRoles_ = std::move(cellTagToRoles);
            RoleToCellTags_ = std::move(roleToCellTags);
        }
    }

    void UpdateDefault() override
    {
        {
            auto guard = WriterGuard(SpinLock_);

            CellTagToRoles_.clear();
            RoleToCellTags_ = {};
            auto addRole = [&] (TCellTag cellTag, EMasterCellRole role) {
                CellTagToRoles_[cellTag] |= EMasterCellRoles(role);
                RoleToCellTags_[role].push_back(cellTag);
            };

            addRole(PrimaryMasterCellTag_, EMasterCellRole::TransactionCoordinator);
            addRole(PrimaryMasterCellTag_, EMasterCellRole::CypressNodeHost);

            for (auto cellTag : SecondaryMasterCellTags_) {
                addRole(cellTag, EMasterCellRole::ChunkHost);
            }

            if (SecondaryMasterCellTags_.empty()) {
                addRole(PrimaryMasterCellTag_, EMasterCellRole::ChunkHost);
            }
        }

        YT_LOG_DEBUG("Default master cell roles set");
    }

private:
    const TCellDirectoryConfigPtr Config_;
    const TCellId PrimaryMasterCellId_;
    const TCellTag PrimaryMasterCellTag_;
    const IChannelFactoryPtr ChannelFactory_;
    const NLogging::TLogger Logger;
    const TObjectServiceCachePtr Cache_;
    const IServerPtr RpcServer_;
    const NNative::TConnectionOptions Options_;


    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TCellTag, TEnumIndexedArray<EMasterChannelKind, IChannelPtr>> CellChannelMap_;
    THashMap<TCellTag, EMasterCellRoles> CellTagToRoles_;
    TEnumIndexedArray<EMasterCellRole, TCellTagList> RoleToCellTags_;
    TRandomGenerator RandomGenerator_;
    TSecondaryMasterConnectionConfigs SecondaryMasterConnectionConfigs_;
    TCellTagList SecondaryMasterCellTags_;
    TCellIdList SecondaryMasterCellIds_;
    THashMap<TCellTag, IServicePtr> CachingObjectServices_;

    bool ClusterMasterCompositionChanged(const TSecondaryMasterConnectionConfigs& secondaryMasterConnectionConfigs)
    {
        const auto& oldSecondaryMasterConnectionConfigs = GetSecondaryMasterConnectionConfigs();

        if (secondaryMasterConnectionConfigs.size() != oldSecondaryMasterConnectionConfigs.size()) {
            return true;
        }

        for (const auto& [cellTag, secondaryMasterConnectionConfig] : secondaryMasterConnectionConfigs) {
            if (!oldSecondaryMasterConnectionConfigs.contains(cellTag)) {
                return true;
            }
            const auto& secondaryMaster = GetOrCrash(oldSecondaryMasterConnectionConfigs, cellTag);
            // TODO(cherepashka): replace with connection config comparison after changing NProto::TCellDirectory.
            if (secondaryMaster->Addresses != secondaryMasterConnectionConfig->Addresses) {
                return true;
            }
        }
        return false;
    }

    void ReconfigureMasterCellDirectory(
        const TSecondaryMasterConnectionConfigs& oldSecondaryMasterConnectionConfigs,
        const TSecondaryMasterConnectionConfigs& secondaryMasterConnectionConfigs,
        const TCellTagList& secondaryMasterCellTags)
    {
        TCellIdList secondaryMasterCellIds;
        THashSet<TCellTag> addedSecondaryCellTags;
        TSecondaryMasterConnectionConfigs reconfiguredSecondaryMasterConfigs;
        secondaryMasterCellIds.reserve(secondaryMasterConnectionConfigs.size());
        addedSecondaryCellTags.reserve(secondaryMasterConnectionConfigs.size());
        reconfiguredSecondaryMasterConfigs.reserve(secondaryMasterConnectionConfigs.size());

        // TODO(cherepashka): add logic for removal and addition of master cells.
        for (const auto& [cellTag, secondaryMaster] : secondaryMasterConnectionConfigs) {
            if (!oldSecondaryMasterConnectionConfigs.contains(cellTag)) {
                YT_LOG_DEBUG("Unexpected master cell cluster reconfiguration (AppearedCellTag: %v)",
                    cellTag);
                InsertOrCrash(addedSecondaryCellTags, cellTag);
            } else if (secondaryMaster->Addresses != GetOrCrash(oldSecondaryMasterConnectionConfigs, cellTag)->Addresses) {
                YT_LOG_INFO("Master cell will be reconfigured (CellTag: %v, NewCellAddresses: %v, OldCellAddresses: %v)",
                    cellTag,
                    secondaryMaster->Addresses,
                    GetOrCrash(oldSecondaryMasterConnectionConfigs, cellTag)->Addresses);
                EmplaceOrCrash(reconfiguredSecondaryMasterConfigs, cellTag, secondaryMaster);
            }
            secondaryMasterCellIds.push_back(secondaryMaster->CellId);
        }

        Sort(secondaryMasterCellIds);

        THashSet<TCellTag> removedSecondaryTags;
        for (const auto& [cellTag, _] : oldSecondaryMasterConnectionConfigs) {
            if (!secondaryMasterConnectionConfigs.contains(cellTag)) {
                removedSecondaryTags.insert(cellTag);
            }
        }
        YT_LOG_WARNING_UNLESS(
            removedSecondaryTags.empty(),
            "Some master cells were removed in new configuration of secondary masters (RemovedCellTags: %v)",
            removedSecondaryTags);

        {
            auto guard = WriterGuard(SpinLock_);
            for (const auto& [cellTag, secondaryMaster] : reconfiguredSecondaryMasterConfigs) {
                RemoveMasterChannels(cellTag);
                InitMasterChannels(secondaryMaster);
            }
            SecondaryMasterConnectionConfigs_ = secondaryMasterConnectionConfigs;
            SecondaryMasterCellIds_ = std::move(secondaryMasterCellIds);
            SecondaryMasterCellTags_ = secondaryMasterCellTags;
        }

        CellDirectoryChanged_.Fire(
            addedSecondaryCellTags,
            reconfiguredSecondaryMasterConfigs,
            removedSecondaryTags);
    }

    IChannelPtr GetCellChannelOrThrow(TCellTag cellTag, EMasterChannelKind kind) const
    {
        auto guard = ReaderGuard(SpinLock_);
        auto it = CellChannelMap_.find(cellTag);
        if (it == CellChannelMap_.end()) {
            ThrowUnknownMasterCellTag(cellTag);
        }
        return it->second[kind];
    }

    void ThrowUnknownMasterCellTag(TCellTag cellTag) const
    {
        THROW_ERROR_EXCEPTION("Unknown master cell tag %v", cellTag);
    }

    TMasterConnectionConfigPtr BuildMasterCacheConfig(const TMasterConnectionConfigPtr& config)
    {
        if (!Config_->MasterCache) {
            return config;
        }

        auto masterCacheConfig = CloneYsonStruct(Config_->MasterCache);
        masterCacheConfig->CellId = config->CellId;
        if (masterCacheConfig->EnableMasterCacheDiscovery) {
            masterCacheConfig->Addresses = config->Addresses;
        }
        return masterCacheConfig;
    }

    void InitMasterChannels(
        const TMasterConnectionConfigPtr& config)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        auto cellTag = CellTagFromId(config->CellId);

        InitMasterChannel(EMasterChannelKind::Leader, config, EPeerKind::Leader);
        InitMasterChannel(EMasterChannelKind::Follower, config, EPeerKind::Follower);
        InitMasterChannel(EMasterChannelKind::MasterCache, config, EPeerKind::Follower);

        auto masterCacheConfig = BuildMasterCacheConfig(config);
        if (Config_->MasterCache && Config_->MasterCache->EnableMasterCacheDiscovery) {
            auto channel = CreateNodeAddressesChannel(
                Config_->MasterCache->MasterCacheDiscoveryPeriod,
                Config_->MasterCache->MasterCacheDiscoveryPeriodSplay,
                MakeWeak(this),
                ENodeRole::MasterCache,
                BIND(&TCellDirectory::CreatePeerChannelFromAddresses, ChannelFactory_, masterCacheConfig, EPeerKind::Follower, Options_));
            CellChannelMap_[cellTag][EMasterChannelKind::Cache] = channel;
        } else {
            InitMasterChannel(EMasterChannelKind::Cache, masterCacheConfig, EPeerKind::Follower);
        }

        auto cachingObjectService = CreateCachingObjectService(
            Config_->CachingObjectService,
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            CellChannelMap_[cellTag][EMasterChannelKind::Cache],
            Cache_,
            config->CellId,
            ObjectClientLogger,
            /*profiler*/ {},
            /*authenticator*/ nullptr);
        EmplaceOrCrash(CachingObjectServices_, cellTag, cachingObjectService);
        RpcServer_->RegisterService(cachingObjectService);
        CellChannelMap_[cellTag][EMasterChannelKind::LocalCache] = CreateRealmChannel(CreateLocalChannel(RpcServer_), config->CellId);
    }

    void InitMasterChannel(
        EMasterChannelKind channelKind,
        const TMasterConnectionConfigPtr& config,
        EPeerKind peerKind)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        auto cellTag = CellTagFromId(config->CellId);
        auto peerChannel = CreatePeerChannel(ChannelFactory_, config, peerKind, Options_);

        CellChannelMap_[cellTag][channelKind] = peerChannel;
    }

    void RemoveMasterChannels(TCellTag cellTag)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        auto cachingObjectServiceIt = CachingObjectServices_.find(cellTag);
        YT_VERIFY(cachingObjectServiceIt != CachingObjectServices_.end());

        const auto& cachingObjectService = cachingObjectServiceIt->second;
        RpcServer_->UnregisterService(cachingObjectService);
        CachingObjectServices_.erase(cachingObjectServiceIt);
        EraseOrCrash(CellChannelMap_, cellTag);
    }

    static IChannelPtr CreatePeerChannelFromAddresses(
        IChannelFactoryPtr channelFactory,
        const TMasterConnectionConfigPtr& config,
        EPeerKind peerKind,
        const NNative::TConnectionOptions& options,
        const std::vector<TString>& discoveredAddresses)
    {
        auto peerChannelConfig = CloneYsonStruct(config);
        if (!discoveredAddresses.empty()) {
            peerChannelConfig->Addresses = discoveredAddresses;
        }

        return CreatePeerChannel(channelFactory, peerChannelConfig, peerKind, options);
    }

    static IChannelPtr CreatePeerChannel(
        IChannelFactoryPtr channelFactory,
        const TMasterConnectionConfigPtr& config,
        EPeerKind kind,
        const NNative::TConnectionOptions& options)
    {
        auto isRetriableError = BIND_NO_PROPAGATE([options] (const TError& error) {
            const auto* effectiveError = &error;
            if (error.GetCode() == NObjectClient::EErrorCode::ForwardedRequestFailed &&
                !error.InnerErrors().empty())
            {
                effectiveError = &error.InnerErrors().front();
            }

            if (options.RetryRequestQueueSizeLimitExceeded &&
                effectiveError->GetCode() == NSecurityClient::EErrorCode::RequestQueueSizeLimitExceeded)
            {
                return true;
            }

            return IsRetriableError(*effectiveError);
        });

        auto channel = NHydra::CreatePeerChannel(config, channelFactory, kind);
        channel = CreateRetryingChannel(config, channel, isRetriableError);
        channel = CreateDefaultTimeoutChannel(channel, config->RpcTimeout);
        return channel;
    }
};

////////////////////////////////////////////////////////////////////////////////

ICellDirectoryPtr CreateCellDirectory(
    TCellDirectoryConfigPtr config,
    NApi::NNative::TConnectionOptions options,
    IChannelFactoryPtr channelFactory,
    NLogging::TLogger logger)
{
    return New<TCellDirectory>(
        std::move(config),
        std::move(options),
        std::move(channelFactory),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
