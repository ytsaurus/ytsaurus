#include "multicell_manager.h"

#include "alert_manager.h"
#include "automaton.h"
#include "bootstrap.h"
#include "cell_statistics.h"
#include "config.h"
#include "config_manager.h"
#include "hydra_facade.h"
#include "private.h"
#include "serialize.h"
#include "world_initializer.h"
#include "multicell_node_statistics.h"
#include "multicell_statistics_collector.h"
#include "alert_manager.h"

#include <yt/yt/server/master/cell_master/proto/multicell_manager.pb.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_replicator.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/portal_manager.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/helpers.h>
#include <yt/yt/server/lib/hive/proto/hive_manager.pb.h>

#include <yt/yt/server/lib/hydra/mutation.h>
#include <yt/yt/server/lib/hydra/snapshot_load_context.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/async_batcher.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <util/generic/algorithm.h>

namespace NYT::NCellMaster {

using namespace NApi::NNative;
using namespace NElection;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NObjectServer;
using namespace NHiveServer;
using namespace NHiveClient;
using namespace NHiveClient::NProto;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CellMasterLogger;
static const auto RegisterRetryPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPrimaryRegisterState,
    (None)
    (Registering)
    (Registered)
);

class TMulticellManager
    : public IMulticellManager
    , public TMasterAutomatonPart
{
public:
    TMulticellManager(
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::MulticellManager)
        , Config_(Bootstrap_->GetConfig()->MulticellManager)
        , UpstreamSyncBatcher_(New<TAsyncBatcher<void>>(
            BIND_NO_PROPAGATE(&TMulticellManager::DoSyncWithUpstream, MakeWeak(this)),
            Config_->UpstreamSyncDelay))
    {
        TMasterAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TMulticellManager::HydraRegisterSecondaryMasterAtPrimary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TMulticellManager::HydraReplicateDynamicallyPropagatedMasterCellTags, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TMulticellManager::HydraOnSecondaryMasterRegisteredAtPrimary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TMulticellManager::HydraRegisterSecondaryMasterAtSecondary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TMulticellManager::HydraStartSecondaryMasterRegistration, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TMulticellManager::HydraSyncHiveClocksAtMasters, Unretained(this)));

        RegisterLoader(
            "MulticellManager.Values",
            BIND_NO_PROPAGATE(&TMulticellManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "MulticellManager.Values",
            BIND_NO_PROPAGATE(&TMulticellManager::SaveValues, Unretained(this)));
    }

    void Initialize() override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TMulticellManager::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& alertManager = Bootstrap_->GetAlertManager();
        alertManager->RegisterAlertSource(BIND_NO_PROPAGATE(&TMulticellManager::GetAlerts, MakeWeak(this)));
    }

    bool IsPrimaryMaster() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->IsPrimaryMaster();
    }

    bool IsSecondaryMaster() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->IsSecondaryMaster();
    }

    bool IsMulticell() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->IsMulticell();
    }

    bool IsDynamicallyPropagatedMaster() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return DynamicallyPropagated_.load();
    }

    TCellId GetCellId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetCellId();
    }

    TCellId GetCellId(TCellTag cellTag) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return cellTag == PrimaryMasterCellTagSentinel
            ? GetPrimaryCellId()
            : ReplaceCellTagInId(GetPrimaryCellId(), cellTag);
    }

    TCellTag GetCellTag() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetCellTag();
    }

    TCellId GetPrimaryCellId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetPrimaryCellId();
    }

    TCellTag GetPrimaryCellTag() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetPrimaryCellTag();
    }

    const std::set<TCellTag>& GetSecondaryCellTags() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetSecondaryCellTags();
    }

    THashSet<TCellTag> GetDynamicallyPropagatedMastersCellTags() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(DynamicallyPropagatedMastersCellTagsLock_);
        return DynamicallyPropagatedMastersCellTags_;
    }

    const TConnectionStaticConfigPtr& GetMasterCellConnectionConfigs() const override
    {
        return Bootstrap_->GetConfig()->ClusterConnection->Static;
    }

    int GetCellCount() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return GetSecondaryCellCount() + 1;
    }

    int GetSecondaryCellCount() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return std::ssize(GetSecondaryCellTags());
    }

    void PostToMaster(
        const TCrossCellMessage& message,
        TCellTag cellTag,
        bool reliable) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto encapsulatedMessage = BuildHiveMessage(message);
        DoPostMessage(std::move(encapsulatedMessage), TCellTagList{cellTag}, reliable);
    }

    void PostToMasters(
        const TCrossCellMessage& message,
        TRange<TCellTag> cellTags,
        bool reliable) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (cellTags.empty()) {
            return;
        }

        auto encapsulatedMessage = BuildHiveMessage(message);
        DoPostMessage(std::move(encapsulatedMessage), cellTags, reliable);
    }

    void PostToPrimaryMaster(
        const TCrossCellMessage& message,
        bool reliable) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        PostToMaster(message, PrimaryMasterCellTagSentinel, reliable);
    }

    void PostToSecondaryMasters(
        const TCrossCellMessage& message,
        bool reliable) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsPrimaryMaster());

        if (IsMulticell()) {
            PostToMasters(message, GetRegisteredMasterCellTags(), reliable);
        }
    }

    bool IsLocalMasterCellRegistered() const override
    {
        VerifyPersistentStateRead();

        if (IsPrimaryMaster()) {
            return true;
        }

        if (RegisterState_ == EPrimaryRegisterState::Registered) {
            return true;
        }

        return false;
    }

    bool IsRegisteredMasterCell(TCellTag cellTag) const override
    {
        VerifyPersistentStateRead();

        return MasterEntryExists(cellTag);
    }

    void ValidateRegisteredMasterCell() const override
    {
        // The new cell, that will be dynamically propagated, will receive information about it's
        // "dynamically propagated" state only after registration on primary, so here validation via
        // IsDynamicallyPropagatedMaster will not work.
        THROW_ERROR_EXCEPTION_UNLESS(IsLocalMasterCellRegistered(),
            NCellServer::EErrorCode::MasterCellNotReady,
            "Master cell is not ready, it is not registered at primary yet")
    }

    EMasterCellRoles GetMasterCellRoles(TCellTag cellTag) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(MasterCellRolesLock_);

        auto it = MasterCellRolesMap_.find(cellTag);
        return it == MasterCellRolesMap_.end() ? EMasterCellRoles::None : it->second;
    }

    TCellTagList GetRoleMasterCells(EMasterCellRole cellRole) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(MasterCellRolesLock_);

        return RoleMasterCells_[cellRole];
    }

    int GetRoleMasterCellCount(EMasterCellRole cellRole) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // NB: No locking here - just accessing atomics.
        return RoleMasterCellCounts_[cellRole].load();
    }

    std::string GetMasterCellName(TCellTag cellTag) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(MasterCellNamesLock_);

        return GetOrCrash(MasterCellNameMap_, cellTag);
    }

    std::optional<TCellTag> FindMasterCellTagByName(const std::string& cellName) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(MasterCellNamesLock_);

        auto it = NameMasterCellMap_.find(cellName);
        return it == NameMasterCellMap_.end() ? std::nullopt : std::make_optional(it->second);
    }

    const TCellTagList& GetRegisteredMasterCellTags() const override
    {
        VerifyPersistentStateRead();

        return RegisteredMasterCellTags_;
    }

    TCellTag PickSecondaryChunkHostCell(double bias) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (!IsMulticell()) {
            return InvalidCellTag;
        }

        // List candidates.
        TCompactVector<std::pair<TCellTag, i64>, MaxSecondaryMasterCells> candidates;
        auto maybeAddCandidate = [&] (TCellTag cellTag, i64 chunkCount) {
            if (cellTag == GetPrimaryCellTag()) {
                return;
            }
            if (None(GetMasterCellRoles(cellTag) & EMasterCellRoles::ChunkHost)) {
                return;
            }
            candidates.emplace_back(cellTag, chunkCount);
        };

        const auto& multicellStatisticsCollector = Bootstrap_->GetMulticellStatisticsCollector();
        const auto& statistics = multicellStatisticsCollector->GetMulticellNodeStatistics();;

        for (const auto& [cellTag, entry] : RegisteredMasterMap_) {
            auto cellStatistics = statistics.GetCellStatistics(cellTag);
            maybeAddCandidate(cellTag, cellStatistics.chunk_count());
        }

        // Sanity check.
        if (candidates.empty()) {
            return InvalidCellTag;
        }

        // Compute the average number of chunks.
        i64 totalChunkCount = 0;
        for (auto [cellTag, chunkCount] : candidates) {
            totalChunkCount += chunkCount;
        }
        i64 avgChunkCount = totalChunkCount / candidates.size();

        // Split the candidates into two subsets: less-that-avg and more-than-avg.
        TCompactVector<TCellTag, MaxSecondaryMasterCells> loCandidates;
        TCompactVector<TCellTag, MaxSecondaryMasterCells> hiCandidates;
        for (auto [cellTag, chunkCount] : candidates) {
            if (chunkCount < avgChunkCount) {
                loCandidates.push_back(cellTag);
            } else {
                hiCandidates.push_back(cellTag);
            }
        }

        // Sample candidates.
        // loCandidates have weight 2^8 + bias * 2^8.
        // hiCandidates have weight 2^8.
        ui64 scaledBias = static_cast<ui64>(bias * (1ULL << 8));
        ui64 weightPerLo = (1ULL << 8) + scaledBias;
        ui64 totalLoWeight = weightPerLo * loCandidates.size();
        ui64 weightPerHi = 1ULL << 8;
        ui64 totalHiWeight = weightPerHi * hiCandidates.size();
        ui64 totalTokens = totalLoWeight + totalHiWeight;
        auto* mutationContext = GetCurrentMutationContext();
        ui64 random = mutationContext->RandomGenerator()->Generate<ui64>() % totalTokens;
        return random < totalLoWeight
            ? loCandidates[random / weightPerLo]
            : hiCandidates[(random - totalLoWeight) / weightPerHi];
    }

    const NProto::TCellStatistics& GetClusterStatistics() const override
    {
        VerifyPersistentStateRead();

        const auto& multicellStatisticsCollector = Bootstrap_->GetMulticellStatisticsCollector();
        const auto& statistics = multicellStatisticsCollector->GetMulticellNodeStatistics();

        return statistics.GetClusterStatistics();
    }

    IChannelPtr GetMasterChannelOrThrow(TCellTag cellTag, EPeerKind peerKind) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto channel = FindMasterChannel(cellTag, peerKind);
        if (!channel) {
            THROW_ERROR_EXCEPTION("Unknown cell tag %v",
                cellTag);
        }
        return channel;
    }

    IChannelPtr FindMasterChannel(TCellTag cellTag, EPeerKind peerKind) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto key = std::tuple(cellTag, peerKind);

        {
            auto guard = ReaderGuard(MasterChannelCacheLock_);
            auto it = MasterChannelCache_.find(key);
            if (it != MasterChannelCache_.end()) {
                return it->second;
            }
        }

        const auto& cellDirectory = Bootstrap_->GetCellDirectory();
        auto cellId = GetCellId(cellTag);
        auto channel = cellDirectory->FindChannelByCellId(cellId, peerKind);
        if (!channel) {
            return nullptr;
        }

        channel = CreateRetryingChannel(Config_->MasterConnection, channel);
        channel = CreateDefaultTimeoutChannel(channel, Config_->MasterConnection->RpcTimeout);

        {
            // NB: Insertions are racy.
            auto guard = WriterGuard(MasterChannelCacheLock_);
            MasterChannelCache_.emplace(key, channel);
        }

        return channel;
    }

    TMailboxHandle FindPrimaryMasterMailbox() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        return PrimaryMasterMailbox_;
    }

    TFuture<void> SyncWithUpstream() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // This shortcut prevents waiting for the (no-op) batcher at primary cell leader.
        // XXX(babenko): tx cells
        if (IsPrimaryMaster()) {
            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            return hydraManager->SyncWithLeader();
        }

        return UpstreamSyncBatcher_->Run();
    }

    DEFINE_SIGNAL_OVERRIDE(void(TCellTag), ReplicateKeysToSecondaryMaster);
    DEFINE_SIGNAL_OVERRIDE(void(TCellTag), ReplicateValuesToSecondaryMaster);
    DEFINE_SIGNAL_OVERRIDE(void(TCellTag), SecondaryMasterRegisteredAtPrimary);

private:
    const TMulticellManagerConfigPtr Config_;

    THashMap<TCellTag, TError> ConflictingCellRolesAlerts_;

    struct TMasterEntry
    {
        int Index = -1;
        NProto::TCellStatistics Statistics;

        void Save(NCellMaster::TSaveContext& context) const
        {
            using NYT::Save;
            Save(context, Index);
        }

        void Load(NCellMaster::TLoadContext& context)
        {
            using NYT::Load;
            Load(context, Index);
            // COMPAT(koloshmet)
            if (context.GetVersion() < EMasterReign::MulticellStatisticsCollector) {
                Load(context, Statistics);
            }
        }
    };

    // NB: Must ensure stable order.
    std::map<TCellTag, TMasterEntry> RegisteredMasterMap_;
    TCellTagList RegisteredMasterCellTags_;
    EPrimaryRegisterState RegisterState_ = EPrimaryRegisterState::None;
    // NB: After registration on primary, the current cell doesn't add itself into RegisteredMasterMap_,
    // so after loading from snapshot it will consider itself as "in process of dynamic propogation" even if it was already propagated or statically known,
    // to differ that cases this flag is used.
    bool EverRegistered_ = false;

    NProto::TCellStatistics LocalCellStatistics_;
    NProto::TCellStatistics ClusterCellStatisics_;

    TMailboxHandle PrimaryMasterMailbox_;
    THashMap<TCellTag, TMailboxHandle> CellTagToMasterMailbox_;

    TPeriodicExecutorPtr RegisterAtPrimaryMasterExecutor_;
    TPeriodicExecutorPtr CellStatisticsGossipExecutor_;
    TPeriodicExecutorPtr SyncHiveClocksExecutor_;

    //! Caches master channels returned by FindMasterChannel and GetMasterChannelOrThrow.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MasterChannelCacheLock_);
    THashMap<std::tuple<TCellTag, EPeerKind>, IChannelPtr> MasterChannelCache_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MasterCellRolesLock_);
    THashMap<TCellTag, EMasterCellRoles> MasterCellRolesMap_;
    TEnumIndexedArray<EMasterCellRole, TCellTagList> RoleMasterCells_;
    TEnumIndexedArray<EMasterCellRole, std::atomic<int>> RoleMasterCellCounts_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MasterCellNamesLock_);
    THashMap<TCellTag, std::string> MasterCellNameMap_;
    THashMap<std::string, TCellTag> NameMasterCellMap_;

    static const TCellTagList EmptyCellTagList;

    const TIntrusivePtr<TAsyncBatcher<void>> UpstreamSyncBatcher_;
    NProfiling::TEventTimer UpstreamSyncTimer_ = CellMasterProfiler().Timer("/upstream_sync_time");

    std::atomic<bool> DynamicallyPropagated_ = false;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, DynamicallyPropagatedMastersCellTagsLock_);
    THashSet<TCellTag> DynamicallyPropagatedMastersCellTags_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void OnAfterSnapshotLoaded() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& portalManager = Bootstrap_->GetPortalManager();

        std::vector<std::map<TCellTag, TMasterEntry>::iterator> removedMasterCellTagsIterators;
        THashSet<TCellTag> removedMasterCellTags;
        for (auto it = RegisteredMasterMap_.begin(); it != RegisteredMasterMap_.end(); ++it) {
            auto cellTag = it->first;
            const auto& entry = it->second;
            if (!IsKnownCellTag(cellTag)) {
                YT_LOG_FATAL_UNLESS(
                    GetDynamicConfig()->Testing->AllowMasterCellRemoval,
                    "Unknown master cell tag in saved master entry (CellTag: %v)",
                    cellTag);

                YT_LOG_FATAL_UNLESS(
                    GetCurrentSnapshotLoadContext()->ReadOnly,
                    "Master cell was removed without readonly mode (CellTag: %v)",
                    cellTag);

                YT_LOG_FATAL_IF(
                    entry.Statistics.chunk_count() > 0,
                    "Master cell with chunks was removed (CellTag: %v, ChunkCount: %v)",
                    cellTag,
                    entry.Statistics.chunk_count());

                auto cellRoles = ComputeMasterCellRolesFromConfig(cellTag);
                YT_LOG_FATAL_UNLESS(
                    cellRoles == EMasterCellRoles::None,
                    "Master cell with roles was removed (CellTag: %v, Roles: %v)",
                    cellTag,
                    cellRoles);

                removedMasterCellTagsIterators.emplace_back(it);
                InsertOrCrash(removedMasterCellTags, cellTag);
            }
        }
        if (!removedMasterCellTags.empty()) {
            cypressManager->ValidateNoExternalizedNodesOnRemovedMasters(removedMasterCellTags);
            portalManager->ValidateNoNodesBehindRemovedMastersPortal(removedMasterCellTags);
        }
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        for (auto iterator : removedMasterCellTagsIterators) {
            auto cellTag = iterator->first;
            RegisteredMasterMap_.erase(iterator);

            YT_LOG_INFO("Master cell removed (CellTag: %v)",
                cellTag);
        }

        RegisteredMasterCellTags_.resize(RegisteredMasterMap_.size());
        for (auto& [cellTag, entry] : RegisteredMasterMap_) {
            YT_LOG_FATAL_IF(
                entry.Index >= std::ssize(RegisteredMasterCellTags_),
                "Master cell from the middle of master entry map was removed");

            RegisteredMasterCellTags_[entry.Index] = cellTag;

            auto cellId = GetCellId(cellTag);
            auto mailbox = hiveManager->GetMailbox(cellId);
            EmplaceOrCrash(CellTagToMasterMailbox_, cellTag, mailbox);
            if (cellTag == GetPrimaryCellTag()) {
                PrimaryMasterMailbox_ = mailbox;
            }

            YT_LOG_INFO("Master cell registered (CellTag: %v, CellIndex: %v)",
                cellTag,
                entry.Index);
        }

        const auto& multicellStatisticsCollector = Bootstrap_->GetMulticellStatisticsCollector();
        auto& statistics = multicellStatisticsCollector->GetMutableMulticellNodeStatistics();

        if (LocalCellStatistics_.IsInitialized()) {
            statistics.PopulateLocalStatisticsAfterSnapshotLoaded(LocalCellStatistics_);
        }
        if (ClusterCellStatisics_.IsInitialized()) {
            statistics.PopulateClusterStatisticsAfterSnapshotLoaded(ClusterCellStatisics_);
        }

        statistics.PopulateMasterCellStatisticsAfterSnapshotLoaded(
                RegisteredMasterMap_
                | std::views::filter([] (const std::pair<const TCellTag, TMasterEntry>& mapping) {
                    return mapping.second.Statistics.IsInitialized();
                })
                | std::views::transform([] (const std::pair<const TCellTag, TMasterEntry>& mapping) {
                    return std::pair(mapping.first, mapping.second.Statistics);
                }));

        if (!RegisteredMasterMap_.empty() && RegisteredMasterMap_.begin()->second.Statistics.IsInitialized()) {
            statistics.PopulateMasterCellStatisticsAfterSnapshotLoaded(
                RegisteredMasterMap_
                | std::views::transform([] (const std::pair<const TCellTag, TMasterEntry>& mapping) {
                    return std::pair(mapping.first, mapping.second.Statistics);
                }));
        }

        {
            auto guard = WriterGuard(DynamicallyPropagatedMastersCellTagsLock_);
            for (auto cellTag : GetSecondaryCellTags()) {
                if (!RegisteredMasterMap_.contains(cellTag)) {
                    InsertOrCrash(DynamicallyPropagatedMastersCellTags_, cellTag);
                }
            }
            if (EverRegistered_) {
                EraseOrCrash(DynamicallyPropagatedMastersCellTags_, GetCellTag());
            }
            DynamicallyPropagated_.store(DynamicallyPropagatedMastersCellTags_.contains(GetCellTag()));
        }
        RecomputeMasterCellRoles();
        RecomputeMasterCellNames();
    }

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        RegisteredMasterMap_.clear();
        RegisteredMasterCellTags_.clear();
        RegisterState_ = EPrimaryRegisterState::None;
        CellTagToMasterMailbox_.clear();
        {
            auto guard = WriterGuard(DynamicallyPropagatedMastersCellTagsLock_);
            DynamicallyPropagatedMastersCellTags_.clear();
        }
        PrimaryMasterMailbox_ = {};
        LocalCellStatistics_ = {};
        ClusterCellStatisics_ = {};
        DynamicallyPropagated_.store(false);
        EverRegistered_ = false;
    }

    void LoadValues(TLoadContext& context)
    {
        using NYT::Load;

        Load(context, RegisteredMasterMap_);
        Load(context, RegisterState_);
        // COMPAT(koloshmet)
        if (context.GetVersion() < EMasterReign::MulticellStatisticsCollector) {
            Load(context, LocalCellStatistics_);
            Load(context, ClusterCellStatisics_);
        }
        if (context.GetVersion() >= EMasterReign::DynamicMasterCellReconfigurationOnNodes) {
            Load(context, EverRegistered_);
        } else {
            EverRegistered_ = IsSecondaryMaster();
        }
    }

    void SaveValues(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, RegisteredMasterMap_);
        Save(context, RegisterState_);
        Save(context, EverRegistered_);
    }

    void OnRecoveryComplete() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnRecoveryComplete();
    }

    void OnLeaderActive() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        if (IsPrimaryMaster()) {
            SyncHiveClocksExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
                BIND(&TMulticellManager::OnSyncHiveClocks, MakeWeak(this)),
                RegisterRetryPeriod);
            SyncHiveClocksExecutor_->Start();
        } else {
            RegisterAtPrimaryMasterExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
                BIND(&TMulticellManager::OnStartSecondaryMasterRegistration, MakeWeak(this)),
                RegisterRetryPeriod);
            RegisterAtPrimaryMasterExecutor_->Start();
        }
    }

    void OnStartLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStartLeading();

        OnStartEpoch();
    }

    void OnStopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        if (RegisterAtPrimaryMasterExecutor_) {
            YT_UNUSED_FUTURE(RegisterAtPrimaryMasterExecutor_->Stop());
            RegisterAtPrimaryMasterExecutor_.Reset();
        }

        if (CellStatisticsGossipExecutor_) {
            YT_UNUSED_FUTURE(CellStatisticsGossipExecutor_->Stop());
            CellStatisticsGossipExecutor_.Reset();
        }

        if (SyncHiveClocksExecutor_) {
            YT_UNUSED_FUTURE(SyncHiveClocksExecutor_->Stop());
            SyncHiveClocksExecutor_.Reset();
        }

        OnStopEpoch();
    }

    void OnStartFollowing() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStartFollowing();

        OnStartEpoch();
    }

    void OnStopFollowing() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopFollowing();

        OnStopEpoch();
    }

    void OnStartEpoch()
    { }

    void OnStopEpoch()
    {
        auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
        UpstreamSyncBatcher_->Cancel(error);

        ClearCaches();
    }

    void ClearCaches()
    {
        auto guard = WriterGuard(MasterChannelCacheLock_);
        MasterChannelCache_.clear();
    }

    void HydraRegisterSecondaryMasterAtPrimary(NProto::TReqRegisterSecondaryMasterAtPrimary* request) noexcept
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsPrimaryMaster());

        auto cellTag = FromProto<TCellTag>(request->cell_tag());
        if (!IsKnownSecondaryCellTag(cellTag)) {
            YT_LOG_ALERT("Received registration request from an unknown secondary cell, ignored (CellTag: %v)",
                cellTag);
            return;
        }

        RegisterMasterMailbox(cellTag);

        try {
            if (MasterEntryExists(cellTag))  {
                THROW_ERROR_EXCEPTION("Attempted to re-register secondary master %v", cellTag);
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error registering secondary master (CellTag: %v)",
                cellTag);
            NProto::TRspRegisterSecondaryMasterAtPrimary response;
            ToProto(response.mutable_error(), TError(ex));
            PostToMaster(response, cellTag, true);
            return;
        }

        RegisterMasterEntry(cellTag);

        {
            // Before replicating keys & values it is needed to replicate dynamically propagated master cell tags,
            // because some further replication results depend on these cell tags.
            NProto::TReqReplicateDynamicallyPropagatedMasterCellTags request;
            for (auto cellTag : GetDynamicallyPropagatedMastersCellTags()) {
                request.add_dynamically_propagated_masters_cell_tags(ToProto(cellTag));
            }
            PostToMaster(request, cellTag, true);
        }

        ReplicateKeysToSecondaryMaster_.Fire(cellTag);
        ReplicateValuesToSecondaryMaster_.Fire(cellTag);

        for (const auto& [registeredCellTag, entry] : RegisteredMasterMap_) {
            if (registeredCellTag == cellTag) {
                continue;
            }

            {
                // Inform others about the new secondary.
                NProto::TReqRegisterSecondaryMasterAtSecondary request;
                request.set_cell_tag(ToProto(cellTag));
                PostToMaster(request, registeredCellTag, true);
            }

            {
                // Inform the new secondary about others.
                NProto::TReqRegisterSecondaryMasterAtSecondary request;
                request.set_cell_tag(ToProto(registeredCellTag));
                PostToMaster(request, cellTag, true);
            }
        }

        {
            NProto::TRspRegisterSecondaryMasterAtPrimary response;
            PostToMaster(response, cellTag, true);
        }

        SecondaryMasterRegisteredAtPrimary_.Fire(cellTag);
    }

    void HydraReplicateDynamicallyPropagatedMasterCellTags(NProto::TReqReplicateDynamicallyPropagatedMasterCellTags* request) noexcept
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        if (!EverRegistered_) {
            auto guard = WriterGuard(DynamicallyPropagatedMastersCellTagsLock_);
            for (auto cellTag : request->dynamically_propagated_masters_cell_tags()) {
                InsertOrCrash(DynamicallyPropagatedMastersCellTags_, FromProto<TCellTag>(cellTag));
            }
            DynamicallyPropagated_.store(DynamicallyPropagatedMastersCellTags_.contains(GetCellTag()));
        }
    }

    void HydraOnSecondaryMasterRegisteredAtPrimary(NProto::TRspRegisterSecondaryMasterAtPrimary* response) noexcept
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        auto error = FromProto<TError>(response->error());
        if (!error.IsOK()) {
            YT_LOG_ERROR(error, "Error registering at primary master, will retry");
            RegisterState_ = EPrimaryRegisterState::None;
            return;
        }

        RegisterState_ = EPrimaryRegisterState::Registered;
        EverRegistered_ = true;

        YT_LOG_INFO("Successfully registered at primary master");
    }

    void HydraRegisterSecondaryMasterAtSecondary(NProto::TReqRegisterSecondaryMasterAtSecondary* request) noexcept
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        auto cellTag = FromProto<TCellTag>(request->cell_tag());
        if (!IsKnownSecondaryCellTag(cellTag)) {
            YT_LOG_ALERT("Received registration request for an unknown secondary cell, ignored (CellTag: %v)",
                cellTag);
            return;
        }

        if (MasterEntryExists(cellTag))  {
            YT_LOG_ALERT("Attempted to re-register secondary master, ignored (CellTag: %v)",
                cellTag);
            return;
        }

        RegisterMasterMailbox(cellTag);
        RegisterMasterEntry(cellTag);
    }

    void HydraStartSecondaryMasterRegistration(NProto::TReqStartSecondaryMasterRegistration* /*request*/) noexcept
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        if (RegisterState_ != EPrimaryRegisterState::None) {
            return;
        }

        YT_LOG_INFO("Registering at primary master");

        RegisterState_ = EPrimaryRegisterState::Registering;
        RegisterMasterMailbox(GetPrimaryCellTag());
        RegisterMasterEntry(GetPrimaryCellTag());

        NProto::TReqRegisterSecondaryMasterAtPrimary request;
        request.set_cell_tag(ToProto(GetCellTag()));
        PostToPrimaryMaster(request, true);
    }

    void HydraSyncHiveClocksAtMasters(NProto::TReqSyncHiveClocksAtMasters* request)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (request->is_reply_from_secondary()) {
            return;
        }

        if (IsPrimaryMaster()) {
            PostToSecondaryMasters(*request, /*reliable*/ true);
        } else {
            request->set_is_reply_from_secondary(true);
            PostToPrimaryMaster(*request, /*reliable*/ true);
        }
    }

    bool IsKnownSecondaryCellTag(TCellTag cellTag)
    {
        const auto& config = Bootstrap_->GetConfig();
        for (const auto& cellConfig : config->SecondaryMasters) {
            if (CellTagFromId(cellConfig->CellId) == cellTag) {
                return true;
            }
        }
        return false;
    }

    bool IsKnownCellTag(TCellTag cellTag)
    {
        const auto& config = Bootstrap_->GetConfig();
        if (CellTagFromId(config->PrimaryMaster->CellId) == cellTag) {
            return true;
        }
        return IsKnownSecondaryCellTag(cellTag);
    }

    bool IsDiscoveredMasterCell(TCellTag cellTag) const
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        auto guard = ReaderGuard(DynamicallyPropagatedMastersCellTagsLock_);
        return !DynamicallyPropagatedMastersCellTags_.contains(cellTag);
    }

    void RegisterMasterMailbox(TCellTag cellTag)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (CellTagToMasterMailbox_.contains(cellTag)) {
            return;
        }

        auto cellId = GetCellId(cellTag);
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto mailbox = hiveManager->GetOrCreateCellMailbox(cellId);

        EmplaceOrCrash(CellTagToMasterMailbox_, cellTag, mailbox);
        if (cellTag == GetPrimaryCellTag()) {
            PrimaryMasterMailbox_ = mailbox;
        }
    }

    void RegisterMasterEntry(TCellTag cellTag)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());
        YT_VERIFY(FindMasterMailbox(cellTag));

        if (RegisteredMasterMap_.find(cellTag) != RegisteredMasterMap_.end()) {
            return;
        }

        YT_VERIFY(RegisteredMasterMap_.size() == RegisteredMasterCellTags_.size());
        int index = std::ssize(RegisteredMasterMap_);
        RegisteredMasterCellTags_.push_back(cellTag);

        auto [it, inserted] = RegisteredMasterMap_.emplace(cellTag, TMasterEntry());
        YT_VERIFY(inserted);

        auto& entry = it->second;
        entry.Index = index;

        RecomputeMasterCellRoles();
        RecomputeMasterCellNames();

        YT_LOG_INFO("Master cell registered (CellTag: %v, CellIndex: %v)",
            cellTag,
            index);
    }

    TMasterEntry* FindMasterEntry(TCellTag cellTag)
    {
        auto it = RegisteredMasterMap_.find(cellTag);
        return it == RegisteredMasterMap_.end() ? nullptr : &it->second;
    }

    const TMasterEntry* GetMasterEntry(TCellTag cellTag) const
    {
        return &GetOrCrash(RegisteredMasterMap_, cellTag);
    }

    bool MasterEntryExists(TCellTag cellTag) const
    {
        VerifyPersistentStateRead();

        return RegisteredMasterMap_.contains(cellTag);
    }

    TMailboxHandle FindMasterMailbox(TCellTag cellTag)
    {
        // Fast path.
        if (cellTag == PrimaryMasterCellTagSentinel) {
            return PrimaryMasterMailbox_;
        }

        // Slow path.
        return GetOrDefault(CellTagToMasterMailbox_, cellTag);
    }

    void OnStartSecondaryMasterRegistration()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        const auto& worldInitializer = Bootstrap_->GetWorldInitializer();
        if (!worldInitializer->IsInitialized()) {
            return;
        }

        if (RegisterState_ != EPrimaryRegisterState::None) {
            return;
        }

        NProto::TReqStartSecondaryMasterRegistration request;
        YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
            ->CommitAndLog(Logger()));
    }

    void OnSyncHiveClocks()
    {
        NProto::TReqSyncHiveClocksAtMasters request;
        YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
            ->CommitAndLog(Logger()));
    }

    NProto::TReqSetCellStatistics GetTransientLocalCellStatistics()
    {
        NProto::TReqSetCellStatistics result;
        result.set_cell_tag(ToProto(GetCellTag()));
        auto* cellStatistics = result.mutable_statistics();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        cellStatistics->set_chunk_count(chunkManager->Chunks().GetSize());

        auto lostVitalChunkCount = WaitFor(chunkManager->GetCellLostVitalChunkCount())
            .ValueOrThrow();
        cellStatistics->set_lost_vital_chunk_count(lostVitalChunkCount);

        if (IsPrimaryMaster()) {
            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            cellStatistics->set_online_node_count(nodeTracker->GetOnlineNodeCount());
        }

        return result;
    }

    static TFuture<void> DoSyncWithUpstream(const TWeakPtr<TMulticellManager>& weakThis)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto this_ = weakThis.Lock();
        if (!this_) {
            return MakeFuture(TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped"));
        }

        return this_->DoSyncWithUpstreamCore();
    }

    TFuture<void> DoSyncWithUpstreamCore()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_VERIFY(IsSecondaryMaster());

        YT_LOG_DEBUG("Synchronizing with upstream");

        NProfiling::TWallTimer timer;

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        const auto& hiveManager = Bootstrap_->GetHiveManager();

        std::vector<TFuture<void>> asyncResults{
            hydraManager->SyncWithLeader(),
            hiveManager->SyncWith(GetPrimaryCellId(), false)
        };

        // NB: Many subscribers are typically waiting for the upstream sync to complete.
        // Make sure the promise is set in a large thread pool.
        return AllSucceeded(std::move(asyncResults)).Apply(
            BIND(&TMulticellManager::OnUpstreamSyncReached, MakeStrong(this), timer)
                .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    void OnUpstreamSyncReached(const NProfiling::TWallTimer& timer, const TError& error)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        THROW_ERROR_EXCEPTION_IF_FAILED(
            error,
            NRpc::EErrorCode::Unavailable,
            "Error synchronizing with upstream");

        YT_LOG_DEBUG("Upstream synchronization complete");
        UpstreamSyncTimer_.Record(timer.GetElapsedTime());
    }

    TSerializedMessagePtr BuildHiveMessage(const TCrossCellMessage& crossCellMessage)
    {
        if (const auto* protoPtr = std::get_if<TCrossCellMessage::TProtoMessage>(&crossCellMessage.Payload)) {
            return NHiveServer::SerializeOutcomingMessage(*protoPtr->Message);
        }

        NObjectServer::NProto::TReqExecute hydraRequest;
        TSharedRefArray parts;
        if (const auto* clientPtr = std::get_if<TCrossCellMessage::TClientMessage>(&crossCellMessage.Payload)) {
            parts = clientPtr->Request->Serialize();
        } else if (const auto* servicePtr = std::get_if<TCrossCellMessage::TServiceMessage>(&crossCellMessage.Payload)) {
            auto requestMessage = servicePtr->Context->GetRequestMessage();
            auto requestHeader = servicePtr->Context->RequestHeader();
            auto updatedYPath = FromObjectId(servicePtr->ObjectId) + GetRequestTargetYPath(requestHeader);
            SetRequestTargetYPath(&requestHeader, updatedYPath);
            SetTransactionId(&requestHeader, servicePtr->TransactionId);
            parts = SetRequestHeader(requestMessage, requestHeader);
        } else {
            YT_ABORT();
        }

        for (const auto& part : parts) {
            hydraRequest.add_request_parts(part.Begin(), part.Size());
        }

        WriteAuthenticationIdentityToProto(&hydraRequest, GetCurrentAuthenticationIdentity());

        return NHiveServer::SerializeOutcomingMessage(hydraRequest);
    }

    void DoPostMessage(
        const TSerializedMessagePtr& message,
        TRange<TCellTag> cellTags,
        bool reliable)
    {
        TCompactVector<TMailboxHandle, 16> mailboxes;
        for (auto cellTag : cellTags) {
            if (cellTag == PrimaryMasterCellTagSentinel) {
                cellTag = GetPrimaryCellTag();
            }
            if (auto mailbox = FindMasterMailbox(cellTag)) {
                mailboxes.push_back(mailbox);
            }
        }

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        hiveManager->PostMessage(mailboxes, message, reliable);
    }

    const TDynamicMulticellManagerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->MulticellManager;
    }

    void HandleTestingFacilities()
    {
        if (!Bootstrap_->GetConfig()->ExposeTestingFacilities) {
            return;
        }
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        const auto& testingConfig = GetDynamicConfig()->Testing;
        const auto& edges = testingConfig->FrozenHiveEdges;

        std::vector<THiveEdge> edgesToFreeze;
        for (const auto& edge : edges) {
            edgesToFreeze.emplace_back(THiveEdge{
                .SourceCellId = GetCellId(edge[0]),
                .DestinationCellId = GetCellId(edge[1]),
            });
        }
        hiveManager->FreezeEdges(std::move(edgesToFreeze));
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& dynamicConfig = GetDynamicConfig();
        if (CellStatisticsGossipExecutor_) {
            CellStatisticsGossipExecutor_->SetPeriod(dynamicConfig->CellStatisticsGossipPeriod);
        }
        if (SyncHiveClocksExecutor_) {
            SyncHiveClocksExecutor_->SetPeriod(dynamicConfig->SyncHiveClocksPeriod);
        }

        const auto& testingConfig = GetDynamicConfig()->Testing;
        // TODO(cherepashka): remove this temporary logic after make sure dynamic master cell configs propagation works as expected.
        if (testingConfig->MasterCellDirectoryOverride) {
            GetMasterCellConnectionConfigs()->SecondaryMasters = testingConfig->MasterCellDirectoryOverride->SecondaryMasters;
        }

        {
            auto guard = WriterGuard(DynamicallyPropagatedMastersCellTagsLock_);
            for (auto cellTag : testingConfig->DiscoveredMastersCellTags) {
                DynamicallyPropagatedMastersCellTags_.erase(cellTag);
            }
            DynamicallyPropagated_.store(DynamicallyPropagatedMastersCellTags_.contains(GetCellTag()));
        }

        RecomputeMasterCellRoles();
        RecomputeMasterCellNames();

        HandleTestingFacilities();
    }

    static std::vector<TError> GetAlerts(const TWeakPtr<TMulticellManager>& weakThis)
    {
        if (auto this_ = weakThis.Lock()) {
            return this_->DoGetAlerts();
        } else {
            return {};
        }
    }

    std::vector<TError> DoGetAlerts()
    {
        std::vector<TError> alerts;
        alerts.reserve(ConflictingCellRolesAlerts_.size());
        std::transform(
            ConflictingCellRolesAlerts_.begin(),
            ConflictingCellRolesAlerts_.end(),
            std::back_inserter(alerts),
            [] (const std::pair<TCellTag, TError>& elem) {
                return elem.second;
            });

        for (auto [cellTag, roles] : MasterCellRolesMap_) {
            if (roles == EMasterCellRoles::None && !GetDynamicConfig()->Testing->AllowMasterCellWithEmptyRole) {
                alerts.push_back(TError("No roles configured for cell")
                    << TErrorAttribute("cell_tag", cellTag));
            }
        }
        return alerts;
    }

    void RecomputeMasterCellRoles()
    {
        auto guard = WriterGuard(MasterCellRolesLock_);

        MasterCellRolesMap_.clear();
        RoleMasterCells_ = {};
        // NB: RoleMasterCellCounts_ is not accessed under a lock and is probably
        // best updated gradually, without intermediate resetting to zero.
        auto populateCellRoles = [&] (TCellTag cellTag) {
            auto roles = ComputeMasterCellRolesFromConfig(cellTag);
            THROW_ERROR_EXCEPTION_IF(!IsDiscoveredMasterCell(cellTag) && roles != EMasterCellRoles::None,
                "Attempted to set master cell roles %v to master with cell tag %v that is not discovered by all nodes",
                roles,
                cellTag)

            MasterCellRolesMap_[cellTag] = roles;

            for (auto role : TEnumTraits<EMasterCellRole>::GetDomainValues()) {
                if (Any(roles & EMasterCellRoles(role))) {
                    RoleMasterCells_[role].push_back(cellTag);
                }
            }
        };

        populateCellRoles(GetCellTag());
        for (auto& [cellTag, entry] : RegisteredMasterMap_) {
            populateCellRoles(cellTag);
        }

        auto ensureCellRoleConfigured = [&] (EMasterCellRole role) {
            auto& roleCellTags = RoleMasterCells_[role];

            if (!roleCellTags.empty()) {
                return;
            }

            roleCellTags.push_back(GetPrimaryCellTag());

            auto& primaryCellRoles = MasterCellRolesMap_[GetPrimaryCellTag()];
            YT_VERIFY(None(primaryCellRoles & EMasterCellRoles(role)));
            primaryCellRoles = primaryCellRoles | EMasterCellRoles(role);
        };

        ensureCellRoleConfigured(EMasterCellRole::CypressNodeHost);
        ensureCellRoleConfigured(EMasterCellRole::TransactionCoordinator);

        for (auto& roleCellTags : RoleMasterCells_) {
            SortUnique(roleCellTags);
        }

        for (auto role : TEnumTraits<EMasterCellRole>::GetDomainValues()) {
            RoleMasterCellCounts_[role] = std::ssize(RoleMasterCells_[role]);
        }
    }

    void RecomputeMasterCellNames()
    {
        auto guard = WriterGuard(MasterCellNamesLock_);

        MasterCellNameMap_.clear();
        NameMasterCellMap_.clear();

        auto populateCellName = [&] (TCellTag cellTag) {
            auto name = ComputeMasterCellNameFromConfig(cellTag);
            EmplaceOrCrash(MasterCellNameMap_, cellTag, name);
            EmplaceOrCrash(NameMasterCellMap_, name, cellTag);
        };

        populateCellName(GetCellTag());
        for (auto& [cellTag, entry] : RegisteredMasterMap_) {
            populateCellName(cellTag);
        }
    }

    EMasterCellRoles GetDefaultMasterCellRoles(TCellTag cellTag)
    {
        const auto& config = GetDynamicConfig();

        if (cellTag == GetPrimaryCellTag()) {
            return EMasterCellRoles::CypressNodeHost |
                EMasterCellRoles::TransactionCoordinator |
                (IsMulticell() ? EMasterCellRoles::None : EMasterCellRoles::ChunkHost);
        }

        if (config->RemoveSecondaryCellDefaultRoles || !IsDiscoveredMasterCell(cellTag)) {
            return EMasterCellRoles::None;
        }

        return EMasterCellRoles::CypressNodeHost | EMasterCellRoles::ChunkHost;
    }

    EMasterCellRoles ComputeMasterCellRolesFromConfig(TCellTag cellTag)
    {
        const auto& config = GetDynamicConfig();
        ConflictingCellRolesAlerts_.erase(cellTag);
        auto it = config->CellDescriptors.find(cellTag);
        if (it != config->CellDescriptors.end() && it->second->Roles) {
            auto roles = *it->second->Roles;
            if (Any(roles & EMasterCellRoles::ChunkHost) && Any(roles & EMasterCellRoles::DedicatedChunkHost)) {
                auto alert = TError("Cell received conflicting \"chunk_host\" and \"dedicated_chunk_host\" roles")
                    << TErrorAttribute("cell_tag", cellTag);
                ConflictingCellRolesAlerts_.emplace(cellTag, std::move(alert));
            }
            if (Any(roles & EMasterCellRoles::ChunkHost) && Any(roles & EMasterCellRoles::SequoiaNodeHost)) {
                auto alert = TError("Cell received conflicting \"chunk_host\" and \"sequoia_node_host\" roles")
                    << TErrorAttribute("cell_tag", cellTag);
                ConflictingCellRolesAlerts_.emplace(cellTag, std::move(alert));
            }
            return *it->second->Roles;
        }

        return GetDefaultMasterCellRoles(cellTag);
    }

    std::string ComputeMasterCellNameFromConfig(TCellTag cellTag)
    {
        const auto& config = GetDynamicConfig();
        auto it = config->CellDescriptors.find(cellTag);
        if (it != config->CellDescriptors.end() && it->second->Name) {
            return *it->second->Name;
        }

        return ToString(cellTag);
    }
};

/*static*/ const TCellTagList TMulticellManager::EmptyCellTagList = {};

////////////////////////////////////////////////////////////////////////////////

IMulticellManagerPtr CreateMulticellManager(TBootstrap* bootstrap)
{
    return New<TMulticellManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
