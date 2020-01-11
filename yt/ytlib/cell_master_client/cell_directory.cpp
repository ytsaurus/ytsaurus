#include "cell_directory.h"
#include "private.h"

#include "config.h"

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/api/native/config.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/hydra/peer_channel.h>

#include <yt/ytlib/node_tracker_client/master_cache_synchronizer.h>

#include <yt/client/cell_master_client/proto/cell_directory.pb.h>

#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/random.h>

#include <yt/core/rpc/caching_channel_factory.h>
#include <yt/core/rpc/retrying_channel.h>
#include <yt/core/rpc/reconfigurable_roaming_channel_provider.h>

namespace NYT::NCellMasterClient {

using namespace NApi;
using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;

///////////////////////////////////////////////////////////////////////////////

class TCellDirectory::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TCellDirectoryConfigPtr config,
        const TConnectionOptions& options,
        IChannelFactoryPtr channelFactory,
        TMasterCacheSynchronizerPtr masterCacheSynchronizer,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , PrimaryMasterCellId_(Config_->PrimaryMaster->CellId)
        , PrimaryMasterCellTag_(CellTagFromId(PrimaryMasterCellId_))
        , ChannelFactory_(CreateCachingChannelFactory(std::move(channelFactory)))
        , MasterCacheSynchronizer_(std::move(masterCacheSynchronizer))
        , Logger(std::move(logger))
        , RandomGenerator_(TInstant::Now().GetValue())
    {
        for (const auto& masterConfig : Config_->SecondaryMasters) {
            SecondaryMasterCellTags_.push_back(CellTagFromId(masterConfig->CellId));
        }
        // Sort tag list to simplify subsequent equality checks.
        std::sort(SecondaryMasterCellTags_.begin(), SecondaryMasterCellTags_.end());

        // NB: unlike channels, roles will be filled on first sync.

        InitMasterChannels(Config_->PrimaryMaster, options);
        for (const auto& masterConfig : Config_->SecondaryMasters) {
            InitMasterChannels(masterConfig, options);
        }
    }

    TCellId GetPrimaryMasterCellId() const
    {
        return PrimaryMasterCellId_;
    }

    TCellTag GetPrimaryMasterCellTag() const
    {
        return PrimaryMasterCellTag_;
    }

    const TCellTagList& GetSecondaryMasterCellTags() const
    {
        return SecondaryMasterCellTags_;
    }

    IChannelPtr GetMasterChannelOrThrow(EMasterChannelKind kind, TCellTag cellTag)
    {
        cellTag = cellTag == PrimaryMasterCellTag ? GetPrimaryMasterCellTag() : cellTag;
        return GetCellChannelOrThrow(cellTag, kind);
    }

    IChannelPtr GetMasterChannelOrThrow(EMasterChannelKind kind, TCellId cellId)
    {
        if (ReplaceCellTagInId(cellId, 0) != ReplaceCellTagInId(GetPrimaryMasterCellId(), 0)) {
            THROW_ERROR_EXCEPTION("Unknown master cell id %v",
                cellId);
        }
        return GetMasterChannelOrThrow(kind, CellTagFromId(cellId));
    }

    TCellId PickRandomMasterCellWithRole(EMasterCellRoles role)
    {
        auto candidateCellTags = GetMasterCellTagsWithRole(role);
        if (candidateCellTags.empty()) {
            return {};
        }

        size_t randomIndex = 0;
        {
            TReaderGuard guard(SpinLock_);
            randomIndex = RandomGenerator_.Generate<size_t>();
        }

        auto cellTag = candidateCellTags[randomIndex % candidateCellTags.size()];
        return ReplaceCellTagInId(GetPrimaryMasterCellId(), cellTag);
    }

    void Update(const NCellMasterClient::NProto::TCellDirectory& protoDirectory)
    {
        THashMap<TCellTag, EMasterCellRoles> cellRoles;
        cellRoles.reserve(protoDirectory.items_size());
        THashMultiMap<EMasterCellRoles, TCellTag> roleCells;
        roleCells.reserve(protoDirectory.items_size());
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
                auto role = EMasterCellRoles(item.roles(j));
                Y_ASSERT(role != EMasterCellRoles::None);
                roles = roles | role;
                roleCells.emplace(role, cellTag);
            }

            YT_VERIFY(cellRoles.emplace(cellTag, roles).second);

            auto addresses = FromProto<std::vector<TString>>(item.addresses());
            std::sort(addresses.begin(), addresses.end());
            YT_VERIFY(cellAddresses.emplace(cellTag, std::move(addresses)).second);

            if (cellTag == PrimaryMasterCellTag_) {
                YT_VERIFY(cellId = PrimaryMasterCellId_);
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

            const auto primaryMasterCellRole = cellRoles[PrimaryMasterCellTag_];
            cellRoles.clear();
            cellRoles.emplace(PrimaryMasterCellTag_, primaryMasterCellRole);
            roleCells.clear();
        } else {
            YT_LOG_WARNING_UNLESS(
                SecondaryMasterCellTags_ == secondaryCellTags,
                "Synchronized secondary master cell tag list does not match, connection config is probably incorrect (ConfigSecondaryMasters: %v, SynchronizedSecondaryMasters: %v)",
                SecondaryMasterCellTags_,
                secondaryCellTags);

            auto expectedPrimaryCellAddresses = Config_->PrimaryMaster->Addresses;
            std::sort(expectedPrimaryCellAddresses.begin(), expectedPrimaryCellAddresses.end());
            const auto& actualPrimaryCellAddresses = cellAddresses[PrimaryMasterCellTag_];
            YT_LOG_WARNING_UNLESS(
                expectedPrimaryCellAddresses == actualPrimaryCellAddresses,
                "Synchronized primary master cell addresses do not match, connection config is probably incorrect (ConfigPrimaryMasterAddresses: %v, SynchronizedPrimaryMasterAddresses: %v)",
                expectedPrimaryCellAddresses,
                actualPrimaryCellAddresses);

            for (auto cellConfig : Config_->SecondaryMasters) {
                auto expectedCellAddresses = cellConfig->Addresses;
                std::sort(expectedCellAddresses.begin(), expectedCellAddresses.end());
                const auto& actualCellAddresses = cellAddresses[CellTagFromId(cellConfig->CellId)];

                YT_LOG_WARNING_UNLESS(
                    expectedCellAddresses == actualCellAddresses,
                    "Synchronized secondary master cell addresses do not match, connection config is probably incorrect (ConfigSecondaryMasterAddresses: %v, SynchronizedSecondaryMasterAddresses: %v)",
                    expectedCellAddresses,
                    actualCellAddresses);
            }
        }

        YT_LOG_DEBUG("Successfully synchronized master cell roles (CellRoles: %v)",
            cellRoles);

        {
            TWriterGuard guard(SpinLock_);
            CellRoleMap_ = std::move(cellRoles);
            RoleCellsMap_ = std::move(roleCells);
        }
    }

    void UpdateDefault()
    {
        {
            TWriterGuard guard(SpinLock_);

            CellRoleMap_.clear();
            RoleCellsMap_.clear();
            auto addRole = [&] (auto cellTag, auto role) {
                 CellRoleMap_[cellTag] |= role;
                 RoleCellsMap_.emplace(role, cellTag);
            };

            addRole(PrimaryMasterCellTag_, EMasterCellRoles::TransactionCoordinator);
            addRole(PrimaryMasterCellTag_, EMasterCellRoles::CypressNodeHost);

            for (auto cellTag : SecondaryMasterCellTags_) {
                addRole(cellTag, EMasterCellRoles::ChunkHost);
            }

            if (SecondaryMasterCellTags_.empty()) {
                addRole(PrimaryMasterCellTag_, EMasterCellRoles::ChunkHost);
            }
        }

        YT_LOG_DEBUG("Default master cell roles set");
    }

private:
    const TCellDirectoryConfigPtr Config_;
    const TCellId PrimaryMasterCellId_;
    const TCellTag PrimaryMasterCellTag_;
    const ICachingChannelFactoryPtr  ChannelFactory_;
    const TMasterCacheSynchronizerPtr MasterCacheSynchronizer_;
    const NLogging::TLogger Logger;

    /*const*/ TCellTagList SecondaryMasterCellTags_;

    /*const*/ THashMap<TCellTag, TEnumIndexedVector<EMasterChannelKind, IChannelPtr>> CellChannelMap_;

    TReaderWriterSpinLock SpinLock_;
    THashMap<TCellTag, EMasterCellRoles> CellRoleMap_;
    // The keys are always single roles (i.e. each key is a role set consisting of exactly on member).
    THashMultiMap<EMasterCellRoles, TCellTag> RoleCellsMap_;
    TRandomGenerator RandomGenerator_;

    TCellTagList GetMasterCellTagsWithRole(EMasterCellRoles role) const
    {
        TCellTagList result;

        {
            TReaderGuard guard(SpinLock_);
            auto range = RoleCellsMap_.equal_range(role);
            for (auto it = range.first; it != range.second; ++it) {
                result.emplace_back(it->second);
            }
        }

        return result;
    }

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

        auto masterCacheConfig = CloneYsonSerializable(Config_->MasterCache);
        masterCacheConfig->CellId = config->CellId;
        if (masterCacheConfig->EnableMasterCacheDiscovery) {
            masterCacheConfig->Addresses = config->Addresses;
        }
        return masterCacheConfig;
    }

    void InitMasterChannels(
        const TMasterConnectionConfigPtr& config,
        const TConnectionOptions& options)
    {
        InitMasterChannel(EMasterChannelKind::Leader, config, EPeerKind::Leader, options);
        InitMasterChannel(EMasterChannelKind::Follower, config, EPeerKind::Follower, options);
        InitMasterChannel(EMasterChannelKind::Cache, BuildMasterCacheConfig(config), EPeerKind::Follower, options);
        InitMasterChannel(EMasterChannelKind::SecondLevelCache, config, EPeerKind::Follower, options);
    }

    void InitMasterChannel(
        EMasterChannelKind channelKind,
        const TMasterConnectionConfigPtr& config,
        EPeerKind peerKind,
        const TConnectionOptions& options)
    {
        auto cellTag = CellTagFromId(config->CellId);
        auto peerChannel = CreatePeerChannel(config, peerKind, options);

        if (channelKind == EMasterChannelKind::Cache && MasterCacheSynchronizer_) {
            auto provider = CreateReconfigurableRoamingChannelProvider(
                peerChannel,
                peerChannel->GetEndpointDescription(),
                peerChannel->GetEndpointAttributes(),
                peerChannel->GetNetworkId());
            peerChannel = CreateRoamingChannel(provider);
            MasterCacheSynchronizer_->SubscribeMasterCacheNodeAddressesUpdated(BIND(
                &TImpl::OnMasterCacheNodeAddressesUpdated,
                MakeWeak(this),
                provider,
                config,
                peerKind,
                options));
        }

        CellChannelMap_[cellTag][channelKind] = peerChannel;
    }

    void OnMasterCacheNodeAddressesUpdated(
        const IReconfigurableRoamingChannelProviderPtr& provider,
        const TMasterConnectionConfigPtr& config,
        EPeerKind peerKind,
        const TConnectionOptions& options,
        const std::vector<TString>& discoveredAddresses)
    {
        YT_LOG_WARNING_IF(discoveredAddresses.empty(), "Received master cache node list is empty; falling back to masters");

        auto peerChannelConfig = CloneYsonSerializable(Config_->MasterCache);
        peerChannelConfig->Addresses = discoveredAddresses.empty() ? config->Addresses : discoveredAddresses;
        peerChannelConfig->CellId = config->CellId;

        provider->SetUnderlyingChannel(CreatePeerChannel(peerChannelConfig, peerKind, options));
    }

    IChannelPtr CreatePeerChannel(
        const TMasterConnectionConfigPtr& config,
        EPeerKind kind,
        const TConnectionOptions& options)
    {
        auto isRetryableError = BIND([options] (const TError& error) {
            if (error.FindMatching(NChunkClient::EErrorCode::OptimisticLockFailure)) {
                return true;
            }

            if (options.RetryRequestQueueSizeLimitExceeded &&
                error.GetCode() == NSecurityClient::EErrorCode::RequestQueueSizeLimitExceeded)
            {
                return true;
            }

            return IsRetriableError(error);
        });

        auto channel = NHydra::CreatePeerChannel(config, ChannelFactory_, kind);
        channel = CreateRetryingChannel(config, channel, isRetryableError);
        channel = CreateDefaultTimeoutChannel(channel, config->RpcTimeout);
        return channel;
    }
};

////////////////////////////////////////////////////////////////////////////////

TCellDirectory::TCellDirectory(
    TCellDirectoryConfigPtr config,
    const NApi::NNative::TConnectionOptions& options,
    IChannelFactoryPtr channelFactory,
    NNodeTrackerClient::TMasterCacheSynchronizerPtr masterCacheSynchronizer,
    NLogging::TLogger logger)
    : Impl_(New<TCellDirectory::TImpl>(
        std::move(config),
        options,
        std::move(channelFactory),
        std::move(masterCacheSynchronizer),
        std::move(logger)))
{ }

void TCellDirectory::Update(const NCellMasterClient::NProto::TCellDirectory& protoDirectory)
{
    return Impl_->Update(protoDirectory);
}

void TCellDirectory::UpdateDefault()
{
    return Impl_->UpdateDefault();
}

TCellId TCellDirectory::GetPrimaryMasterCellId() const
{
    return Impl_->GetPrimaryMasterCellId();
}

TCellTag TCellDirectory::GetPrimaryMasterCellTag() const
{
    return Impl_->GetPrimaryMasterCellTag();
}

const TCellTagList& TCellDirectory::GetSecondaryMasterCellTags() const
{
    return Impl_->GetSecondaryMasterCellTags();
}

IChannelPtr TCellDirectory::GetMasterChannelOrThrow(EMasterChannelKind kind, TCellTag cellTag)
{
    return Impl_->GetMasterChannelOrThrow(kind, cellTag);
}

IChannelPtr TCellDirectory::GetMasterChannelOrThrow(EMasterChannelKind kind, TCellId cellId)
{
    return Impl_->GetMasterChannelOrThrow(kind, cellId);
}

TCellId TCellDirectory::PickRandomMasterCellWithRole(EMasterCellRoles role) const
{
    return Impl_->PickRandomMasterCellWithRole(role);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
