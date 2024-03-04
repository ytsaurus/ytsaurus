#include "multicell_manager.h"

#include "config.h"
#include "config_manager.h"
#include "bootstrap.h"
#include "private.h"
#include "automaton.h"
#include "serialize.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "cell_statistics.h"
#include "alert_manager.h"

#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/async_batcher.h>

#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>
#include <yt/yt/server/lib/hive/helpers.h>
#include <yt/yt/server/lib/hive/proto/hive_manager.pb.h>

#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_replicator.h>

#include <yt/yt/server/master/cell_master/proto/multicell_manager.pb.h>

#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/ytlib/election/config.h>

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

static const auto& Logger = CellMasterLogger;
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
        TMasterAutomatonPart::RegisterMethod(BIND(&TMulticellManager::HydraRegisterSecondaryMasterAtPrimary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TMulticellManager::HydraOnSecondaryMasterRegisteredAtPrimary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TMulticellManager::HydraRegisterSecondaryMasterAtSecondary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TMulticellManager::HydraStartSecondaryMasterRegistration, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TMulticellManager::HydraSetCellStatistics, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TMulticellManager::HydraSetMulticellStatistics, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TMulticellManager::HydraSyncHiveClocksAtMasters, Unretained(this)));

        RegisterLoader(
            "MulticellManager.Values",
            BIND(&TMulticellManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "MulticellManager.Values",
            BIND(&TMulticellManager::SaveValues, Unretained(this)));
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TMulticellManager::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& alertManager = Bootstrap_->GetAlertManager();
        alertManager->RegisterAlertSource(BIND(&TMulticellManager::GetAlerts, MakeWeak(this)));
    }


    bool IsPrimaryMaster() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->IsPrimaryMaster();
    }

    bool IsSecondaryMaster() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->IsSecondaryMaster();
    }

    bool IsMulticell() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->IsMulticell();
    }

    TCellId GetCellId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetCellId();
    }

    TCellId GetCellId(TCellTag cellTag) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return cellTag == PrimaryMasterCellTagSentinel
            ? GetPrimaryCellId()
            : ReplaceCellTagInId(GetPrimaryCellId(), cellTag);
    }

    TCellTag GetCellTag() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetCellTag();
    }

    TCellId GetPrimaryCellId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetPrimaryCellId();
    }

    TCellTag GetPrimaryCellTag() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetPrimaryCellTag();
    }

    const TCellTagList& GetSecondaryCellTags() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetSecondaryCellTags();
    }

    const TConnectionStaticConfigPtr& GetMasterCellConnectionConfigs() const override
    {
        return Bootstrap_->GetConfig()->ClusterConnection->Static;
    }

    int GetCellCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetSecondaryCellCount() + 1;
    }

    int GetSecondaryCellCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return std::ssize(GetSecondaryCellTags());
    }


    void PostToMaster(
        const TCrossCellMessage& message,
        TCellTag cellTag,
        bool reliable) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto encapsulatedMessage = BuildHiveMessage(message);
        DoPostMessage(std::move(encapsulatedMessage), TCellTagList{cellTag}, reliable);
    }

    void PostToMasters(
        const TCrossCellMessage& message,
        const TCellTagList& cellTags,
        bool reliable) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

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
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        PostToMaster(message, PrimaryMasterCellTagSentinel, reliable);
    }

    void PostToSecondaryMasters(
        const TCrossCellMessage& message,
        bool reliable) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsPrimaryMaster());

        if (IsMulticell()) {
            PostToMasters(message, GetRegisteredMasterCellTags(), reliable);
        }
    }


    bool IsLocalMasterCellRegistered() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

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
        Bootstrap_->VerifyPersistentStateRead();

        return MasterEntryExists(cellTag);
    }

    EMasterCellRoles GetMasterCellRoles(TCellTag cellTag) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(MasterCellRolesLock_);

        auto it = MasterCellRolesMap_.find(cellTag);
        return it == MasterCellRolesMap_.end() ? EMasterCellRoles::None : it->second;
    }

    TCellTagList GetRoleMasterCells(EMasterCellRole cellRole) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(MasterCellRolesLock_);

        return RoleMasterCells_[cellRole];
    }

    int GetRoleMasterCellCount(EMasterCellRole cellRole) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // NB: no locking here - just accessing atomics.

        return RoleMasterCellCounts_[cellRole].load();
    }

    TString GetMasterCellName(NObjectClient::TCellTag cellTag) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(MasterCellNamesLock_);

        return GetOrCrash(MasterCellNameMap_, cellTag);
    }

    std::optional<NObjectClient::TCellTag> FindMasterCellTagByName(const TString& cellName) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(MasterCellNamesLock_);

        auto it = NameMasterCellMap_.find(cellName);
        return it == NameMasterCellMap_.end() ? std::nullopt : std::make_optional(it->second);
    }

    const TCellTagList& GetRegisteredMasterCellTags() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        return RegisteredMasterCellTags_;
    }

    int GetRegisteredMasterCellIndex(TCellTag cellTag) const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        return GetMasterEntry(cellTag)->Index;
    }


    TCellTag PickSecondaryChunkHostCell(double bias) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

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

        for (const auto& [cellTag, entry] : RegisteredMasterMap_) {
            maybeAddCandidate(cellTag, entry.Statistics.chunk_count());
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
        Bootstrap_->VerifyPersistentStateRead();

        return ClusterCellStatisics_;
    }

    IChannelPtr GetMasterChannelOrThrow(TCellTag cellTag, EPeerKind peerKind) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto channel = FindMasterChannel(cellTag, peerKind);
        if (!channel) {
            THROW_ERROR_EXCEPTION("Unknown cell tag %v",
                cellTag);
        }
        return channel;
    }

    IChannelPtr FindMasterChannel(TCellTag cellTag, EPeerKind peerKind) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

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

    TMailbox* FindPrimaryMasterMailbox() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return PrimaryMasterMailbox_;
    }

    TFuture<void> SyncWithUpstream() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // This shortcut prevents waiting for the (no-op) batcher at primary cell leader.
        // XXX(babenko): tx cells
        if (IsPrimaryMaster()) {
            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            return hydraManager->SyncWithLeader();
        }

        return UpstreamSyncBatcher_->Run();
    }


    DEFINE_SIGNAL_OVERRIDE(void(TCellTag), ValidateSecondaryMasterRegistration);
    DEFINE_SIGNAL_OVERRIDE(void(TCellTag), ReplicateKeysToSecondaryMaster);
    DEFINE_SIGNAL_OVERRIDE(void(TCellTag), ReplicateValuesToSecondaryMaster);

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
            Save(context, Statistics);
        }

        void Load(NCellMaster::TLoadContext& context)
        {
            using NYT::Load;
            Load(context, Index);
            Load(context, Statistics);
        }
    };

    // NB: Must ensure stable order.
    std::map<TCellTag, TMasterEntry> RegisteredMasterMap_;
    TCellTagList RegisteredMasterCellTags_;
    EPrimaryRegisterState RegisterState_ = EPrimaryRegisterState::None;

    NProto::TCellStatistics LocalCellStatistics_;
    NProto::TCellStatistics ClusterCellStatisics_;

    TMailbox* PrimaryMasterMailbox_ = nullptr;
    THashMap<TCellTag, TMailbox*> CellTagToMasterMailbox_;

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
    THashMap<TCellTag, TString> MasterCellNameMap_;
    THashMap<TString, TCellTag> NameMasterCellMap_;

    static const TCellTagList EmptyCellTagList;

    const TIntrusivePtr<TAsyncBatcher<void>> UpstreamSyncBatcher_;
    NProfiling::TEventTimer UpstreamSyncTimer_ = CellMasterProfiler.Timer("/upstream_sync_time");

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        RegisteredMasterCellTags_.resize(RegisteredMasterMap_.size());

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        for (auto& [cellTag, entry] : RegisteredMasterMap_) {
            if (!IsValidCellTag(cellTag)) {
                THROW_ERROR_EXCEPTION("Unknown master cell tag %v", cellTag);
            }

            RegisteredMasterCellTags_[entry.Index] = cellTag;

            auto cellId = GetCellId(cellTag);
            auto* mailbox = hiveManager->GetMailbox(cellId);
            YT_VERIFY(CellTagToMasterMailbox_.emplace(cellTag, mailbox).second);
            if (cellTag == GetPrimaryCellTag()) {
                PrimaryMasterMailbox_ = mailbox;
            }

            YT_LOG_INFO("Master cell registered (CellTag: %v, CellIndex: %v)",
                cellTag,
                entry.Index);
        }

        RecomputeMasterCellRoles();
        RecomputeMasterCellNames();
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        RegisteredMasterMap_.clear();
        RegisteredMasterCellTags_.clear();
        RegisterState_ = EPrimaryRegisterState::None;
        CellTagToMasterMailbox_.clear();
        PrimaryMasterMailbox_ = nullptr;
        LocalCellStatistics_ = {};
        ClusterCellStatisics_ = {};
    }


    void LoadValues(TLoadContext& context)
    {
        using NYT::Load;
        Load(context, RegisteredMasterMap_);
        Load(context, RegisterState_);
        Load(context, LocalCellStatistics_);
        Load(context, ClusterCellStatisics_);
    }

    void SaveValues(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, RegisteredMasterMap_);
        Save(context, RegisterState_);
        Save(context, LocalCellStatistics_);
        Save(context, ClusterCellStatisics_);
    }

    void OnRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnRecoveryComplete();
    }

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        CellStatisticsGossipExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::MulticellGossip),
            BIND(&TMulticellManager::OnCellStatisticsGossip, MakeWeak(this)));
        CellStatisticsGossipExecutor_->Start();

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
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStartLeading();

        OnStartEpoch();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

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
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStartFollowing();

        OnStartEpoch();
    }

    void OnStopFollowing() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

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
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsPrimaryMaster());

        auto cellTag = FromProto<TCellTag>(request->cell_tag());
        if (!IsValidSecondaryCellTag(cellTag)) {
            YT_LOG_ALERT("Received registration request from an unknown secondary cell, ignored (CellTag: %v)",
                cellTag);
            return;
        }

        RegisterMasterMailbox(cellTag);

        try {
            if (MasterEntryExists(cellTag))  {
                THROW_ERROR_EXCEPTION("Attempted to re-register secondary master %v", cellTag);
            }

            ValidateSecondaryMasterRegistration_.Fire(cellTag);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error registering secondary master (CellTag: %v)",
                cellTag);
            NProto::TRspRegisterSecondaryMasterAtPrimary response;
            ToProto(response.mutable_error(), TError(ex));
            PostToMaster(response, cellTag, true);
            return;
        }

        RegisterMasterEntry(cellTag);

        ReplicateKeysToSecondaryMaster_.Fire(cellTag);
        ReplicateValuesToSecondaryMaster_.Fire(cellTag);

        for (const auto& [registeredCellTag, entry] : RegisteredMasterMap_) {
            if (registeredCellTag == cellTag) {
                continue;
            }

            {
                // Inform others about the new secondary.
                NProto::TReqRegisterSecondaryMasterAtSecondary request;
                request.set_cell_tag(ToProto<int>(cellTag));
                PostToMaster(request, registeredCellTag, true);
            }

            {
                // Inform the new secondary about others.
                NProto::TReqRegisterSecondaryMasterAtSecondary request;
                request.set_cell_tag(ToProto<int>(registeredCellTag));
                PostToMaster(request, cellTag, true);
            }
        }

        {
            NProto::TRspRegisterSecondaryMasterAtPrimary response;
            PostToMaster(response, cellTag, true);
        }
    }

    void HydraOnSecondaryMasterRegisteredAtPrimary(NProto::TRspRegisterSecondaryMasterAtPrimary* response) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        auto error = FromProto<TError>(response->error());
        if (!error.IsOK()) {
            YT_LOG_ERROR(error, "Error registering at primary master, will retry");
            RegisterState_ = EPrimaryRegisterState::None;
            return;
        }

        RegisterState_ = EPrimaryRegisterState::Registered;

        YT_LOG_INFO("Successfully registered at primary master");
    }

    void HydraRegisterSecondaryMasterAtSecondary(NProto::TReqRegisterSecondaryMasterAtSecondary* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        auto cellTag = FromProto<TCellTag>(request->cell_tag());
        if (!IsValidSecondaryCellTag(cellTag)) {
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
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        if (RegisterState_ != EPrimaryRegisterState::None) {
            return;
        }

        YT_LOG_INFO("Registering at primary master");

        RegisterState_ = EPrimaryRegisterState::Registering;
        RegisterMasterMailbox(GetPrimaryCellTag());
        RegisterMasterEntry(GetPrimaryCellTag());

        NProto::TReqRegisterSecondaryMasterAtPrimary request;
        request.set_cell_tag(ToProto<int>(GetCellTag()));
        PostToPrimaryMaster(request, true);
    }

    void HydraSetCellStatistics(NProto::TReqSetCellStatistics* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (IsPrimaryMaster()) {
            auto cellTag = FromProto<TCellTag>(request->cell_tag());

            if (cellTag == GetPrimaryCellTag()) {
                YT_LOG_INFO("Persisted primary cell statistics (%v)",
                    request->statistics());

                LocalCellStatistics_ = request->statistics();

                RecomputeClusterCellStatistics();
            } else {
                YT_LOG_INFO("Received cell statistics gossip message (CellTag: %v, %v)",
                    cellTag,
                    request->statistics());

                if (auto* entry = FindMasterEntry(cellTag)) { // Just in case.
                    entry->Statistics = request->statistics();
                }
            }
        } else {
            YT_LOG_INFO("Received cell statistics gossip message (%v)",
                request->statistics());
            ClusterCellStatisics_ = request->statistics();
        }
    }

    void HydraSetMulticellStatistics(NProto::TReqSetMulticellStatistics* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // NB: verifying this cell having the 'Cypress node host' role would
        // probably be a bit too fragile.
        YT_VERIFY(IsSecondaryMaster());

        YT_LOG_INFO("Received multicell statistics gossip message (%v)",
            request->statistics());

        for (const auto& cellStatistics : request->statistics()) {
            auto cellTag = FromProto<TCellTag>(cellStatistics.cell_tag());
            if (cellTag == GetCellTag()) {
                // No point in overwriting local statistics - they're persisted
                // periodically anyway.
                continue;
            }
            // Registering every secondary cell at every secondary cell may happen too late.
            if (auto* entry = FindMasterEntry(cellTag)) {
                entry->Statistics = cellStatistics.statistics();
            }
        }

        RecomputeClusterCellStatistics();
    }

    void HydraSyncHiveClocksAtMasters(NProto::TReqSyncHiveClocksAtMasters* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

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

    void RecomputeClusterCellStatistics()
    {
        ClusterCellStatisics_ = {};

        auto addCellStatistics = [&] (TCellTag cellTag, const NProto::TCellStatistics& statistics) {
            YT_ASSERT(statistics.online_node_count() == 0 || cellTag == GetPrimaryCellTag());

            ClusterCellStatisics_ += statistics;
            // TODO(shakurov): consider moving this into operator+.
            ClusterCellStatisics_.set_online_node_count(
                ClusterCellStatisics_.online_node_count() + statistics.online_node_count());
        };

        addCellStatistics(GetCellTag(), LocalCellStatistics_);
        for (const auto& [cellTag, entry] : RegisteredMasterMap_) {
            addCellStatistics(cellTag, entry.Statistics);
        }
    }

    bool IsValidSecondaryCellTag(TCellTag cellTag)
    {
        const auto& config = Bootstrap_->GetConfig();
        for (const auto& cellConfig : config->SecondaryMasters) {
            if (CellTagFromId(cellConfig->CellId) == cellTag) {
                return true;
            }
        }
        return false;
    }

    bool IsValidCellTag(TCellTag cellTag)
    {
        const auto& config = Bootstrap_->GetConfig();
        if (CellTagFromId(config->PrimaryMaster->CellId) == cellTag) {
            return true;
        }
        if (IsValidSecondaryCellTag(cellTag)) {
            return true;
        }
        return false;
    }


    void RegisterMasterMailbox(TCellTag cellTag)
    {
        if (CellTagToMasterMailbox_.contains(cellTag)) {
            return;
        }

        auto cellId = GetCellId(cellTag);
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        TMailbox* mailbox = hiveManager->GetOrCreateCellMailbox(cellId);

        YT_VERIFY(CellTagToMasterMailbox_.emplace(cellTag, mailbox).second);
        if (cellTag == GetPrimaryCellTag()) {
            PrimaryMasterMailbox_ = mailbox;
        }
    }

    void RegisterMasterEntry(TCellTag cellTag)
    {
        YT_VERIFY(FindMasterMailbox(cellTag));

        if (RegisteredMasterMap_.find(cellTag) != RegisteredMasterMap_.end()) {
            return;
        }

        YT_VERIFY(RegisteredMasterMap_.size() == RegisteredMasterCellTags_.size());
        int index = static_cast<int>(RegisteredMasterMap_.size());
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
        Bootstrap_->VerifyPersistentStateRead();

        return RegisteredMasterMap_.contains(cellTag);
    }

    TMailbox* FindMasterMailbox(TCellTag cellTag)
    {
        // Fast path.
        if (cellTag == PrimaryMasterCellTagSentinel) {
            return PrimaryMasterMailbox_;
        }

        // Slow path.
        auto it = CellTagToMasterMailbox_.find(cellTag);
        return it == CellTagToMasterMailbox_.end() ? nullptr : it->second;
    }


    void OnStartSecondaryMasterRegistration()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
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
            ->CommitAndLog(Logger));
    }

    void OnCellStatisticsGossip()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!IsLocalMasterCellRegistered()) {
            return;
        }

        auto localRequest = GetTransientLocalCellStatistics();

        YT_LOG_INFO("Sending cell statistics gossip message");

        if (IsPrimaryMaster()) {
            // Persist statistics locally.
            YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), localRequest)
                ->CommitAndLog(Logger));

            // Send statistics to secondary cells.
            auto allCellTags = GetRegisteredMasterCellTags();
            std::sort(allCellTags.begin(), allCellTags.end());

            auto portalCellTags = GetRoleMasterCells(EMasterCellRole::CypressNodeHost);
            YT_VERIFY(std::is_sorted(portalCellTags.begin(), portalCellTags.end()));

            TCellTagList nonPortalCellTags;
            std::set_difference(
                allCellTags.begin(),
                allCellTags.end(),
                portalCellTags.begin(),
                portalCellTags.end(),
                std::back_inserter(nonPortalCellTags));

            if (!portalCellTags.empty()) {
                auto multicellRequest = GetMulticellStatistics();
                PostToMasters(multicellRequest, portalCellTags, /*reliable*/ false);
            }

            if (!nonPortalCellTags.empty()) {
                auto clusterRequest = GetClusterCellStatistics();
                PostToMasters(clusterRequest, nonPortalCellTags, /*reliable*/ false);
            }
        } else {
            PostToPrimaryMaster(localRequest, /*reliable*/ false);
        }
    }

    void OnSyncHiveClocks()
    {
        NProto::TReqSyncHiveClocksAtMasters request;
        YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
            ->CommitAndLog(Logger));
    }

    NProto::TReqSetCellStatistics GetTransientLocalCellStatistics()
    {
        NProto::TReqSetCellStatistics result;
        result.set_cell_tag(ToProto<int>(GetCellTag()));
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

    NProto::TReqSetCellStatistics GetClusterCellStatistics()
    {
        YT_VERIFY(IsPrimaryMaster());

        NProto::TReqSetCellStatistics result;
        result.set_cell_tag(ToProto<int>(GetCellTag()));
        *result.mutable_statistics() = ClusterCellStatisics_;

        return result;
    }

    NProto::TReqSetMulticellStatistics GetMulticellStatistics()
    {
        YT_VERIFY(IsPrimaryMaster());

        NProto::TReqSetMulticellStatistics result;

        auto addCellStatistics = [&] (TCellTag cellTag, const NProto::TCellStatistics& statistics) {
            auto* cellStatistics = result.add_statistics();
            cellStatistics->set_cell_tag(ToProto<int>(cellTag));
            *cellStatistics->mutable_statistics() = statistics;
        };

        addCellStatistics(GetCellTag(), LocalCellStatistics_);
        for (auto& [cellTag, entry] : RegisteredMasterMap_) {
            addCellStatistics(cellTag, entry.Statistics);
        }

        return result;
    }

    static TFuture<void> DoSyncWithUpstream(const TWeakPtr<TMulticellManager>& weakThis)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto this_ = weakThis.Lock();
        if (!this_) {
            return MakeFuture(TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped"));
        }

        return this_->DoSyncWithUpstreamCore();
    }

    TFuture<void> DoSyncWithUpstreamCore()
    {
        VERIFY_THREAD_AFFINITY_ANY();
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
        VERIFY_THREAD_AFFINITY_ANY();

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
        const TCellTagList& cellTags,
        bool reliable)
    {
        TMailboxList mailboxes;
        for (auto cellTag : cellTags) {
            if (cellTag == PrimaryMasterCellTagSentinel) {
                cellTag = GetPrimaryCellTag();
            }
            if (auto* mailbox = FindMasterMailbox(cellTag)) {
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

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (CellStatisticsGossipExecutor_) {
            CellStatisticsGossipExecutor_->SetPeriod(GetDynamicConfig()->CellStatisticsGossipPeriod);
        }
        if (SyncHiveClocksExecutor_) {
            SyncHiveClocksExecutor_->SetPeriod(GetDynamicConfig()->SyncHiveClocksPeriod);
        }
        RecomputeMasterCellRoles();
        RecomputeMasterCellNames();
    }

    static std::vector<TError> GetAlerts(const TWeakPtr<TMulticellManager>& weakThis)
    {
        if (auto strongThis = weakThis.Lock()) {
            return strongThis->DoGetAlerts();
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
            [] (const std::pair<NYT::NObjectClient::TCellTag, NYT::TError>& elem) {
                return elem.second;
            });

        for (auto [cellTag, roles] : MasterCellRolesMap_) {
            if (roles == EMasterCellRoles::None) {
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
            YT_VERIFY(MasterCellNameMap_.emplace(cellTag, name).second);
            YT_VERIFY(NameMasterCellMap_.emplace(name, cellTag).second);
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

        if (config->RemoveSecondaryCellDefaultRoles) {
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

    TString ComputeMasterCellNameFromConfig(TCellTag cellTag)
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
