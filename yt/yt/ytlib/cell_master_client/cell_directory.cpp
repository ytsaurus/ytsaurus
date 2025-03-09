#include "cell_directory.h"

#include "private.h"
#include "config.h"
#include "protobuf_helpers.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/ytlib/node_tracker_client/node_addresses_provider.h>

#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/caching_object_service.h>
#include <yt/yt/ytlib/object_client/object_service_cache.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/sequoia_client/public.h>

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

////////////////////////////////////////////////////////////////////////////////

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
            auto cellTag = CellTagFromId(cellId);
            EmplaceOrCrash(SecondaryMasterConnectionConfigs_, cellTag, masterConfig);
            SecondaryMasterCellTags_.push_back(cellTag);
            InsertOrCrash(SecondaryMasterCellIds_, cellId);
        }
        // Sort tag list to simplify subsequent equality checks.
        Sort(SecondaryMasterCellTags_);

        // NB: Unlike channels, roles will be filled on first sync.
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

    THashSet<NObjectClient::TCellId> GetSecondaryMasterCellIds() override
    {
        auto guard = ReaderGuard(SpinLock_);
        return SecondaryMasterCellIds_;
    }

    TSecondaryMasterConnectionConfigs GetSecondaryMasterConnectionConfigs()
    {
        auto guard = ReaderGuard(SpinLock_);
        return SecondaryMasterConnectionConfigs_;
    }

    IChannelPtr FindMasterChannel(EMasterChannelKind kind, TCellTag cellTag) override
    {
        cellTag = cellTag == PrimaryMasterCellTagSentinel ? GetPrimaryMasterCellTag() : cellTag;
        auto guard = ReaderGuard(SpinLock_);
        auto it = CellChannelMap_.find(cellTag);
        if (it == CellChannelMap_.end()) {
            return nullptr;
        }
        return it->second[kind];
    }

    IChannelPtr GetMasterChannelOrThrow(EMasterChannelKind kind, TCellTag cellTag) override
    {
        cellTag = cellTag == PrimaryMasterCellTagSentinel ? GetPrimaryMasterCellTag() : cellTag;
        auto channel = FindMasterChannel(kind, cellTag);
        if (!channel) {
            ThrowUnknownMasterCellTag(cellTag);
        }
        return channel;
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
        THashMap<TCellTag, std::vector<std::string>> cellTagToAddresses;
        cellTagToAddresses.reserve(protoDirectory.items_size());

        TSecondaryMasterConnectionConfigs newSecondaryMasterConnectionConfigs;
        TCellTagList newSecondaryMasterCellTags;
        newSecondaryMasterConnectionConfigs.reserve(protoDirectory.items_size());
        newSecondaryMasterCellTags.reserve(protoDirectory.items_size());

        auto primaryCellFound = false;

        for (const auto& item : protoDirectory.items()) {
            TMasterConnectionConfigPtr masterConnectionConfig;
            FromProto(&masterConnectionConfig, item);
            YT_VERIFY(masterConnectionConfig->Addresses);

            auto cellId = masterConnectionConfig->CellId;
            auto cellTag = CellTagFromId(cellId);

            auto roles = EMasterCellRoles::None;
            for (auto protoRole : item.roles()) {
                auto role = TryCheckedEnumCast<EMasterCellRole>(protoRole);
                if (!role) {
                    YT_LOG_ALERT("Skipped an unknown cell role while synchronizing master cell directory (MasterCellRole: %v, CellTag: %v)",
                        protoRole,
                        cellTag);
                    continue;
                }
                roles |= EMasterCellRoles(*role);
                roleToCellTags[*role].push_back(cellTag);
            }
            EmplaceOrCrash(cellTagToRoles, cellTag, roles);
            EmplaceOrCrash(cellTagToAddresses, cellTag, *masterConnectionConfig->Addresses);

            if (cellTag == PrimaryMasterCellTag_) {
                YT_VERIFY(cellId == PrimaryMasterCellId_);
                primaryCellFound = true;
            } else {
                EmplaceOrCrash(newSecondaryMasterConnectionConfigs, cellTag, std::move(masterConnectionConfig));
                newSecondaryMasterCellTags.push_back(cellTag);
            }
        }

        YT_VERIFY(primaryCellFound);
        YT_VERIFY(cellTagToRoles.contains(PrimaryMasterCellTag_) && cellTagToAddresses.contains(PrimaryMasterCellTag_));

        // To get the actual values under lock.
        auto oldSecondaryMasterCellTags = GetSecondaryMasterCellTags();
        auto oldSecondaryMasterConnectionConfigs = GetSecondaryMasterConnectionConfigs();

        if (ClusterMasterCompositionChanged(newSecondaryMasterConnectionConfigs)) {
            YT_LOG_INFO("Cluster membership configuration has changed, starting reconfiguration "
                "(SecondaryMasterCellTags: %v, ReceivedSecondaryMasterCellTags: %v)",
                oldSecondaryMasterCellTags,
                newSecondaryMasterCellTags);
            ReconfigureMasterCellDirectory(oldSecondaryMasterConnectionConfigs, newSecondaryMasterConnectionConfigs, newSecondaryMasterCellTags);
        }

        if (oldSecondaryMasterCellTags.empty() &&
            !newSecondaryMasterCellTags.empty()) {
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
                const auto& actualPrimaryCellAddresses = cellTagToAddresses[PrimaryMasterCellTag_];
                YT_LOG_WARNING_UNLESS(
                    expectedPrimaryCellAddresses == actualPrimaryCellAddresses,
                    "Synchronized primary master cell addresses do not match, connection config is probably incorrect (ConfigPrimaryMasterAddresses: %v, SynchronizedPrimaryMasterAddresses: %v)",
                    expectedPrimaryCellAddresses,
                    actualPrimaryCellAddresses);

                for (const auto& [cellTag, cellConfig] : oldSecondaryMasterConnectionConfigs) {
                    if (!newSecondaryMasterConnectionConfigs.contains(cellTag)) {
                        continue;
                    }
                    if (cellConfig->Addresses) {
                        const auto& expectedCellAddresses = *cellConfig->Addresses;
                        const auto& actualCellAddresses = cellTagToAddresses[CellTagFromId(cellConfig->CellId)];

                        YT_LOG_WARNING_UNLESS(
                            expectedCellAddresses == actualCellAddresses,
                            "Synchronized secondary master cell addresses do not match, connection config is probably incorrect "
                            "(CellTag: %v, ConfigSecondaryMasterAddresses: %v, SynchronizedSecondaryMasterAddresses: %v)",
                            cellTag,
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
    THashSet<NObjectClient::TCellId> SecondaryMasterCellIds_;
    THashMap<TCellTag, IServicePtr> CachingObjectServices_;

    bool ClusterMasterCompositionChanged(const TSecondaryMasterConnectionConfigs& newSecondaryMasterConnectionConfigs)
    {
        const auto& oldSecondaryMasterConnectionConfigs = GetSecondaryMasterConnectionConfigs();

        if (newSecondaryMasterConnectionConfigs.size() != oldSecondaryMasterConnectionConfigs.size()) {
            return true;
        }

        for (const auto& [cellTag, secondaryMasterConnectionConfig] : newSecondaryMasterConnectionConfigs) {
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
        const TSecondaryMasterConnectionConfigs& newSecondaryMasterConnectionConfigs,
        const TCellTagList& secondaryMasterCellTags)
    {
        THashSet<TCellId> newSecondaryMasterCellIds;
        THashSet<TCellTag> newSecondaryMasterCellTags;
        THashSet<TCellTag> changedSecondaryMasterCellTags;
        TSecondaryMasterConnectionConfigs newSecondaryMasterConfigs;
        TSecondaryMasterConnectionConfigs changedSecondaryMasterConfigs;
        newSecondaryMasterCellIds.reserve(newSecondaryMasterConnectionConfigs.size());
        newSecondaryMasterCellTags.reserve(newSecondaryMasterConnectionConfigs.size());
        changedSecondaryMasterCellTags.reserve(newSecondaryMasterConnectionConfigs.size());
        newSecondaryMasterConfigs.reserve(newSecondaryMasterConnectionConfigs.size());
        changedSecondaryMasterConfigs.reserve(newSecondaryMasterConnectionConfigs.size());

        for (const auto& [cellTag, secondaryMaster] : newSecondaryMasterConnectionConfigs) {
            if (!oldSecondaryMasterConnectionConfigs.contains(cellTag)) {
                EmplaceOrCrash(newSecondaryMasterConfigs, cellTag, secondaryMaster);
                InsertOrCrash(newSecondaryMasterCellTags, cellTag);
            } else if (secondaryMaster->Addresses != GetOrCrash(oldSecondaryMasterConnectionConfigs, cellTag)->Addresses) {
                YT_LOG_INFO("Master cell will be reconfigured (CellTag: %v, NewCellAddresses: %v, OldCellAddresses: %v)",
                    cellTag,
                    secondaryMaster->Addresses,
                    GetOrCrash(oldSecondaryMasterConnectionConfigs, cellTag)->Addresses);
                EmplaceOrCrash(changedSecondaryMasterConfigs, cellTag, secondaryMaster);
                InsertOrCrash(changedSecondaryMasterCellTags, cellTag);
            }
            InsertOrCrash(newSecondaryMasterCellIds, secondaryMaster->CellId);
        }

        THashSet<TCellTag> removedSecondaryMasterCellTags;
        removedSecondaryMasterCellTags.reserve(oldSecondaryMasterConnectionConfigs.size());
        for (const auto& [cellTag, _] : oldSecondaryMasterConnectionConfigs) {
            if (!newSecondaryMasterConnectionConfigs.contains(cellTag)) {
                InsertOrCrash(removedSecondaryMasterCellTags, cellTag);
            }
        }
        YT_LOG_ALERT_UNLESS(
            removedSecondaryMasterCellTags.empty(),
            "Some master cells were removed in new configuration of secondary masters (RemovedCellTags: %v)",
            removedSecondaryMasterCellTags);

        {
            auto guard = WriterGuard(SpinLock_);
            for (const auto& [_, secondaryMaster] : newSecondaryMasterConfigs) {
                InitMasterChannels(secondaryMaster);
            }
            for (const auto& [cellTag, secondaryMaster] : changedSecondaryMasterConfigs) {
                RemoveMasterChannels(cellTag);
                InitMasterChannels(secondaryMaster);
            }

            SecondaryMasterConnectionConfigs_ = newSecondaryMasterConnectionConfigs;
            SecondaryMasterCellIds_ = std::move(newSecondaryMasterCellIds);
            SecondaryMasterCellTags_ = secondaryMasterCellTags;
        }

        YT_LOG_DEBUG("Finished reconfiguration of cell cluster membership "
            "(NewCellTags: %v, ChangedCellTags: %v, RemovedCellTags: %v)",
            newSecondaryMasterCellTags,
            changedSecondaryMasterCellTags,
            removedSecondaryMasterCellTags);

        CellDirectoryChanged_.Fire(
            newSecondaryMasterConfigs,
            changedSecondaryMasterConfigs,
            removedSecondaryMasterCellTags);
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
        YT_ASSERT_WRITER_SPINLOCK_AFFINITY(SpinLock_);

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
            ObjectClientLogger(),
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
        YT_ASSERT_WRITER_SPINLOCK_AFFINITY(SpinLock_);

        auto cellTag = CellTagFromId(config->CellId);
        auto peerChannel = CreatePeerChannel(ChannelFactory_, config, peerKind, Options_);

        CellChannelMap_[cellTag][channelKind] = peerChannel;
    }

    void RemoveMasterChannels(TCellTag cellTag)
    {
        YT_ASSERT_WRITER_SPINLOCK_AFFINITY(SpinLock_);

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
        const std::vector<std::string>& discoveredAddresses)
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
            if (error.GetCode() == NSequoiaClient::EErrorCode::SequoiaRetriableError) {
                return true;
            }

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
