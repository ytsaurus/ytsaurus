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
    TCellDirectory(
        TCellDirectoryConfigPtr config,
        const NNative::TConnectionOptions& options,
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
        , RandomGenerator_(TInstant::Now().GetValue())
    {
        for (const auto& masterConfig : Config_->SecondaryMasters) {
            auto cellId = masterConfig->CellId;
            SecondaryMasterCellTags_.push_back(CellTagFromId(cellId));
            SecondaryMasterCellIds_.push_back(cellId);
        }
        // Sort tag list to simplify subsequent equality checks.
        std::sort(SecondaryMasterCellTags_.begin(), SecondaryMasterCellTags_.end());

        // NB: unlike channels, roles will be filled on first sync.

        InitMasterChannels(Config_->PrimaryMaster, options);
        for (const auto& masterConfig : Config_->SecondaryMasters) {
            InitMasterChannels(masterConfig, options);
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

    const TCellTagList& GetSecondaryMasterCellTags() override
    {
        return SecondaryMasterCellTags_;
    }

    const TCellIdList& GetSecondaryMasterCellIds() override
    {
        return SecondaryMasterCellIds_;
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
        return RoleCells_[role];
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
        THashMap<TCellTag, EMasterCellRoles> cellRoles;
        cellRoles.reserve(protoDirectory.items_size());
        TEnumIndexedArray<EMasterCellRole, TCellTagList> roleCells;
        THashMap<TCellTag, std::vector<TString>> cellAddresses;
        cellAddresses.reserve(protoDirectory.items_size());
        TCellTagList secondaryCellTags;
        secondaryCellTags.reserve(protoDirectory.items_size());

        auto primaryCellFound = false;

        for (auto i = 0; i < protoDirectory.items_size(); ++i) {
            const auto& item = protoDirectory.items(i);

            auto cellId = FromProto<TGuid>(item.cell_id());
            auto cellTag = CellTagFromId(cellId);

            auto roles = EMasterCellRoles::None;
            for (auto j = 0; j < item.roles_size(); ++j) {
                auto role = EMasterCellRole(item.roles(j));
                roles = roles | EMasterCellRoles(role);
                roleCells[role].push_back(cellTag);
            }

            YT_VERIFY(cellRoles.emplace(cellTag, roles).second);

            auto addresses = FromProto<std::vector<TString>>(item.addresses());
            std::sort(addresses.begin(), addresses.end());
            YT_VERIFY(cellAddresses.emplace(cellTag, std::move(addresses)).second);

            if (cellTag == PrimaryMasterCellTag_) {
                YT_VERIFY(cellId == PrimaryMasterCellId_);
                primaryCellFound = true;
            } else {
                secondaryCellTags.push_back(cellTag);
            }
        }

        YT_VERIFY(primaryCellFound);
        YT_VERIFY(cellRoles.contains(PrimaryMasterCellTag_) && cellAddresses.contains(PrimaryMasterCellTag_));

        std::sort(secondaryCellTags.begin(), secondaryCellTags.end());

        if (SecondaryMasterCellTags_.empty() &&
            !secondaryCellTags.empty()) {
            YT_LOG_WARNING("Synchronized master cell tag list does not match, connection config is probably meant for a direct connection to a secondary cell tag (ConfigPrimaryCellTag: %v, SynchronizedSecondaryMasters: %v)",
                PrimaryMasterCellTag_,
                secondaryCellTags);

            const auto primaryMasterCellRoles = cellRoles[PrimaryMasterCellTag_];
            cellRoles.clear();
            cellRoles.emplace(PrimaryMasterCellTag_, primaryMasterCellRoles);
            for (auto role : TEnumTraits<EMasterCellRole>::GetDomainValues()) {
                roleCells[role].clear();
                if (Any(primaryMasterCellRoles & EMasterCellRoles(role))) {
                    roleCells[role].push_back(PrimaryMasterCellTag_);
                }
            }
        } else {
            YT_LOG_WARNING_UNLESS(
                SecondaryMasterCellTags_ == secondaryCellTags,
                "Synchronized secondary master cell tag list does not match, connection config is probably incorrect (ConfigSecondaryMasters: %v, SynchronizedSecondaryMasters: %v)",
                SecondaryMasterCellTags_,
                secondaryCellTags);

            if (Config_->PrimaryMaster->Addresses) {
                auto expectedPrimaryCellAddresses = *Config_->PrimaryMaster->Addresses;
                std::sort(expectedPrimaryCellAddresses.begin(), expectedPrimaryCellAddresses.end());
                const auto& actualPrimaryCellAddresses = cellAddresses[PrimaryMasterCellTag_];
                YT_LOG_WARNING_UNLESS(
                    expectedPrimaryCellAddresses == actualPrimaryCellAddresses,
                    "Synchronized primary master cell addresses do not match, connection config is probably incorrect (ConfigPrimaryMasterAddresses: %v, SynchronizedPrimaryMasterAddresses: %v)",
                    expectedPrimaryCellAddresses,
                    actualPrimaryCellAddresses);

                for (auto cellConfig : Config_->SecondaryMasters) {
                    if (cellConfig->Addresses) {
                        auto expectedCellAddresses = *cellConfig->Addresses;
                        std::sort(expectedCellAddresses.begin(), expectedCellAddresses.end());
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

        YT_LOG_DEBUG("Successfully synchronized master cell roles (CellRoles: %v)",
            cellRoles);

        {
            auto guard = WriterGuard(SpinLock_);
            CellRoleMap_ = std::move(cellRoles);
            RoleCells_ = std::move(roleCells);
        }
    }

    void UpdateDefault() override
    {
        {
            auto guard = WriterGuard(SpinLock_);

            CellRoleMap_.clear();
            RoleCells_ = {};
            auto addRole = [&] (TCellTag cellTag, EMasterCellRole role) {
                CellRoleMap_[cellTag] |= EMasterCellRoles(role);
                RoleCells_[role].push_back(cellTag);
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

    /*const*/ TCellTagList SecondaryMasterCellTags_;
    /*const*/ TCellIdList SecondaryMasterCellIds_;

    /*const*/ THashMap<TCellTag, TEnumIndexedArray<EMasterChannelKind, IChannelPtr>> CellChannelMap_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TCellTag, EMasterCellRoles> CellRoleMap_;
    TEnumIndexedArray<EMasterCellRole, TCellTagList> RoleCells_;
    TRandomGenerator RandomGenerator_;

    std::vector<IServicePtr> CachingObjectServices_;

    IChannelPtr GetCellChannelOrThrow(TCellTag cellTag, EMasterChannelKind kind) const
    {
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
        const TMasterConnectionConfigPtr& config,
        const NNative::TConnectionOptions& options)
    {
        auto cellTag = CellTagFromId(config->CellId);

        InitMasterChannel(EMasterChannelKind::Leader, config, EPeerKind::Leader, options);
        InitMasterChannel(EMasterChannelKind::Follower, config, EPeerKind::Follower, options);
        InitMasterChannel(EMasterChannelKind::MasterCache, config, EPeerKind::Follower, options);

        auto masterCacheConfig = BuildMasterCacheConfig(config);
        if (Config_->MasterCache && Config_->MasterCache->EnableMasterCacheDiscovery) {
            auto channel = CreateNodeAddressesChannel(
                Config_->MasterCache->MasterCacheDiscoveryPeriod,
                Config_->MasterCache->MasterCacheDiscoveryPeriodSplay,
                MakeWeak(this),
                ENodeRole::MasterCache,
                BIND(&TCellDirectory::CreatePeerChannelFromAddresses, ChannelFactory_, masterCacheConfig, EPeerKind::Follower, options));
            CellChannelMap_[cellTag][EMasterChannelKind::Cache] = channel;
        } else {
            InitMasterChannel(EMasterChannelKind::Cache, masterCacheConfig, EPeerKind::Follower, options);
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
        CachingObjectServices_.push_back(cachingObjectService);
        RpcServer_->RegisterService(cachingObjectService);
        CellChannelMap_[cellTag][EMasterChannelKind::LocalCache] = CreateRealmChannel(CreateLocalChannel(RpcServer_), config->CellId);
    }

    void InitMasterChannel(
        EMasterChannelKind channelKind,
        const TMasterConnectionConfigPtr& config,
        EPeerKind peerKind,
        const NNative::TConnectionOptions& options)
    {
        auto cellTag = CellTagFromId(config->CellId);
        auto peerChannel = CreatePeerChannel(ChannelFactory_, config, peerKind, options);

        CellChannelMap_[cellTag][channelKind] = peerChannel;
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
