#include "multicell_manager.h"
#include "config.h"
#include "config_manager.h"
#include "bootstrap.h"
#include "private.h"
#include "automaton.h"
#include "serialize.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "helpers.h"

#include <yt/core/ytree/ypath_client.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/async_batcher.h>

#include <yt/core/rpc/retrying_channel.h>
#include <yt/core/rpc/dispatcher.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/server/lib/hive/hive_manager.h>
#include <yt/server/lib/hive/mailbox.h>
#include <yt/server/lib/hive/helpers.h>
#include <yt/server/lib/hive/proto/hive_manager.pb.h>

#include <yt/server/lib/hydra/mutation.h>

#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/user.h>

#include <yt/server/master/chunk_server/chunk_manager.h>

#include <yt/server/master/cell_master/proto/multicell_manager.pb.h>

namespace NYT::NCellMaster {

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
static const auto& Profiler = CellMasterProfiler;
static const auto RegisterRetryPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPrimaryRegisterState,
    (None)
    (Registering)
    (Registered)
);

class TMulticellManager::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(
        TMulticellManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::MulticellManager)
        , Config_(config)
        , UpstreamSyncBatcher_(New<TAsyncBatcher<void>>(
            BIND_DONT_CAPTURE_TRACE_CONTEXT(&TImpl::DoSyncWithUpstream, MakeWeak(this)),
            Config_->UpstreamSyncDelay))
    {
        YT_VERIFY(Config_);

        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterSecondaryMasterAtPrimary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraOnSecondaryMasterRegisteredAtPrimary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterSecondaryMasterAtSecondary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraStartSecondaryMasterRegistration, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraSetCellStatistics, Unretained(this)));

        RegisterLoader(
            "MulticellManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "MulticellManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
    }

    void Initialize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));
    }


    bool IsPrimaryMaster()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->IsPrimaryMaster();
    }

    bool IsSecondaryMaster()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->IsSecondaryMaster();
    }

    bool IsMulticell()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->IsMulticell();
    }

    TCellId GetCellId()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetCellId();
    }

    TCellId GetCellId(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return cellTag == PrimaryMasterCellTag
            ? GetPrimaryCellId()
            : ReplaceCellTagInId(GetPrimaryCellId(), cellTag);
    }

    TCellTag GetCellTag()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetCellTag();
    }

    TCellId GetPrimaryCellId()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetPrimaryCellId();
    }

    TCellTag GetPrimaryCellTag()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetPrimaryCellTag();
    }

    const TCellTagList& GetSecondaryCellTags()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetSecondaryCellTags();
    }


    void PostToMaster(
        const TCrossCellMessage& message,
        TCellTag cellTag,
        bool reliable)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto encapsulatedMessage = BuildHiveMessage(message);
        DoPostMessage(std::move(encapsulatedMessage), TCellTagList{cellTag}, reliable);
    }

    void PostToMasters(
        const TCrossCellMessage& message,
        const TCellTagList& cellTags,
        bool reliable)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (cellTags.empty()) {
            return;
        }

        auto encapsulatedMessage = BuildHiveMessage(message);
        DoPostMessage(std::move(encapsulatedMessage), cellTags, reliable);
    }

    void PostToSecondaryMasters(
        const TCrossCellMessage& message,
        bool reliable)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsPrimaryMaster());

        if (IsMulticell()) {
            PostToMasters(message, GetRegisteredMasterCellTags(), reliable);
        }
    }


    bool IsLocalMasterCellRegistered()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (IsPrimaryMaster()) {
            return true;
        }

        if (RegisterState_ == EPrimaryRegisterState::Registered) {
            return true;
        }

        return false;
    }

    bool IsRegisteredSecondaryMaster(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return FindMasterEntry(cellTag) != nullptr;
    }

    EMasterCellRoles GetMasterCellRoles(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto it = MasterCellRolesMap_.find(cellTag);
        return it == MasterCellRolesMap_.end() ? EMasterCellRoles::None : it->second;
    }

    const TCellTagList& GetRegisteredMasterCellTags()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return RegisteredMasterCellTags_;
    }

    int GetRegisteredMasterCellIndex(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return GetMasterEntry(cellTag)->Index;
    }


    TCellTag PickSecondaryChunkHostCell(double bias)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // List candidates.
        SmallVector<std::pair<TCellTag, i64>, MaxSecondaryMasterCells> candidates;
        auto maybeAddCandidate = [&] (TCellTag cellTag, i64 chunkCount) {
            if (cellTag == GetPrimaryCellTag()) {
                return;
            }
            if (None(GetMasterCellRoles(cellTag) & EMasterCellRoles::ChunkHost)) {
                return;
            }
            candidates.emplace_back(cellTag, chunkCount);
        };

        if (IsSecondaryMaster() && !IsMulticell()) {
            maybeAddCandidate(
                GetCellTag(),
                Bootstrap_->GetChunkManager()->Chunks().size());
        } else {
            for (const auto& [cellTag, entry] : RegisteredMasterMap_) {
                maybeAddCandidate(cellTag, entry.Statistics.chunk_count());
            }
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
        SmallVector<TCellTag, MaxSecondaryMasterCells> loCandidates;
        SmallVector<TCellTag, MaxSecondaryMasterCells> hiCandidates;
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
        ui64 random = mutationContext->RandomGenerator().Generate<ui64>() % totalTokens;
        return random < totalLoWeight
            ? loCandidates[random / weightPerLo]
            : hiCandidates[(random - totalLoWeight) / weightPerHi];
    }

    NProto::TCellStatistics ComputeClusterStatistics()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto result = GetLocalCellStatistics();
        for (const auto& [cellTag, entry] : RegisteredMasterMap_) {
            result += entry.Statistics;
        }
        return result;
    }


    IChannelPtr GetMasterChannelOrThrow(TCellTag cellTag, EPeerKind peerKind)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto channel = FindMasterChannel(cellTag, peerKind);
        if (!channel) {
            THROW_ERROR_EXCEPTION("Unknown cell tag %v",
                cellTag);
        }
        return channel;
    }

    IChannelPtr FindMasterChannel(TCellTag cellTag, EPeerKind peerKind)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto key = std::make_tuple(cellTag, peerKind);

        {
            TReaderGuard guard(MasterChannelCacheLock_);
            auto it = MasterChannelCache_.find(key);
            if (it != MasterChannelCache_.end()) {
                return it->second;
            }
        }


        const auto& cellDirectory = Bootstrap_->GetCellDirectory();
        auto cellId = GetCellId(cellTag);
        auto channel = cellDirectory->FindChannel(cellId, peerKind);
        if (!channel) {
            return nullptr;
        }

        channel = CreateRetryingChannel(Config_->MasterConnection, channel);
        channel = CreateDefaultTimeoutChannel(channel, Config_->MasterConnection->RpcTimeout);

        {
            // NB: Insertions are racy.
            TWriterGuard guard(MasterChannelCacheLock_);
            MasterChannelCache_.emplace(key, channel);
        }

        return channel;
    }

    TMailbox* FindPrimaryMasterMailbox()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return PrimaryMasterMailbox_;
    }

    TFuture<void> SyncWithUpstream()
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


    DEFINE_SIGNAL(void(TCellTag), ValidateSecondaryMasterRegistration);
    DEFINE_SIGNAL(void(TCellTag), ReplicateKeysToSecondaryMaster);
    DEFINE_SIGNAL(void(TCellTag), ReplicateValuesToSecondaryMaster);

private:
    const TMulticellManagerConfigPtr Config_;

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

    TMailbox* PrimaryMasterMailbox_ = nullptr;
    THashMap<TCellTag, TMailbox*> CellTagToMasterMailbox_;

    TPeriodicExecutorPtr RegisterAtPrimaryMasterExecutor_;
    TPeriodicExecutorPtr CellStatisticsGossipExecutor_;

    //! Caches master channels returned by FindMasterChannel and GetMasterChannelOrThrow.
    NConcurrency::TReaderWriterSpinLock MasterChannelCacheLock_;
    THashMap<std::tuple<TCellTag, EPeerKind>, IChannelPtr> MasterChannelCache_;

    THashMap<TCellTag, EMasterCellRoles> MasterCellRolesMap_;

    const TIntrusivePtr<TAsyncBatcher<void>> UpstreamSyncBatcher_;
    NProfiling::TAggregateGauge UpstreamSyncTimeGauge_{"/upstream_sync_time"};

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    virtual void OnAfterSnapshotLoaded()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        RegisteredMasterCellTags_.resize(RegisteredMasterMap_.size());

        const auto& hiveManager  = Bootstrap_->GetHiveManager();
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
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        RegisteredMasterMap_.clear();
        RegisteredMasterCellTags_.clear();
        RegisterState_ = EPrimaryRegisterState::None;
        CellTagToMasterMailbox_.clear();
        PrimaryMasterMailbox_ = nullptr;
    }


    void LoadValues(TLoadContext& context)
    {
        using NYT::Load;
        Load(context, RegisteredMasterMap_);
        Load(context, RegisterState_);
    }

    void SaveValues(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, RegisteredMasterMap_);
        Save(context, RegisterState_);
    }


    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        if (IsSecondaryMaster()) {
            RegisterAtPrimaryMasterExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
                BIND(&TImpl::OnStartSecondaryMasterRegistration, MakeWeak(this)),
                RegisterRetryPeriod);
            RegisterAtPrimaryMasterExecutor_->Start();

            CellStatisticsGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::MulticellGossip),
                BIND(&TImpl::OnCellStatisticsGossip, MakeWeak(this)));
            CellStatisticsGossipExecutor_->Start();
        }

        OnDynamicConfigChanged();
    }

    virtual void OnStartLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStartLeading();

        OnStartEpoch();
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        if (RegisterAtPrimaryMasterExecutor_) {
            RegisterAtPrimaryMasterExecutor_->Stop();
            RegisterAtPrimaryMasterExecutor_.Reset();
        }

        if (CellStatisticsGossipExecutor_) {
            CellStatisticsGossipExecutor_->Stop();
            CellStatisticsGossipExecutor_.Reset();
        }

        OnStopEpoch();
    }

    virtual void OnStartFollowing() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStartFollowing();

        OnStartEpoch();
    }

    virtual void OnStopFollowing() override
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
        TWriterGuard guard(MasterChannelCacheLock_);
        MasterChannelCache_.clear();
    }


    void HydraRegisterSecondaryMasterAtPrimary(NProto::TReqRegisterSecondaryMasterAtPrimary* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsPrimaryMaster());

        auto cellTag = request->cell_tag();
        if (!IsValidSecondaryCellTag(cellTag)) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Receieved registration request from an unknown secondary cell, ignored (CellTag: %v)",
                cellTag);
            return;
        }

        RegisterMasterMailbox(cellTag);

        try {
            if (FindMasterEntry(cellTag))  {
                THROW_ERROR_EXCEPTION("Attempted to re-register secondary master %v", cellTag);
            }

            ValidateSecondaryMasterRegistration_.Fire(cellTag);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING_UNLESS(IsRecovery(), ex, "Error registering secondary master (CellTag: %v)",
                cellTag);
            NProto::TRspRegisterSecondaryMasterAtPrimary response;
            ToProto(response.mutable_error(), TError(ex).Sanitize());
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
                request.set_cell_tag(cellTag);
                PostToMaster(request, registeredCellTag, true);
            }

            {
                // Inform the new secondary about others.
                NProto::TReqRegisterSecondaryMasterAtSecondary request;
                request.set_cell_tag(registeredCellTag);
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
            YT_LOG_ERROR_UNLESS(IsRecovery(), error, "Error registering at primary master, will retry");
            RegisterState_ = EPrimaryRegisterState::None;
            return;
        }

        RegisterState_ = EPrimaryRegisterState::Registered;

        YT_LOG_INFO_UNLESS(IsRecovery(), "Successfully registered at primary master");
    }

    void HydraRegisterSecondaryMasterAtSecondary(NProto::TReqRegisterSecondaryMasterAtSecondary* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        auto cellTag = request->cell_tag();
        if (!IsValidSecondaryCellTag(cellTag)) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Receieved registration request for an unknown secondary cell, ignored (CellTag: %v)",
                cellTag);
            return;
        }

        if (FindMasterEntry(cellTag))  {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Attempted to re-register secondary master, ignored (CellTag: %v)",
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

        YT_LOG_INFO_UNLESS(IsRecovery(), "Registering at primary master");

        RegisterState_ = EPrimaryRegisterState::Registering;
        RegisterMasterMailbox(GetPrimaryCellTag());
        RegisterMasterEntry(GetPrimaryCellTag());

        NProto::TReqRegisterSecondaryMasterAtPrimary request;
        request.set_cell_tag(GetCellTag());
        PostToMaster(request, PrimaryMasterCellTag, true);
    }

    void HydraSetCellStatistics(NProto::TReqSetCellStatistics* request) noexcept
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsPrimaryMaster());

        auto cellTag = request->cell_tag();
        YT_LOG_INFO_UNLESS(IsRecovery(), "Received cell statistics gossip message (CellTag: %v)",
            cellTag);

        auto* entry = GetMasterEntry(cellTag);
        entry->Statistics = request->statistics();
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
        auto* mailbox = hiveManager->GetOrCreateMailbox(cellId);

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

        auto [it, inserted] = RegisteredMasterMap_.insert(std::make_pair(cellTag, TMasterEntry()));
        YT_VERIFY(inserted);

        auto& entry = it->second;
        entry.Index = index;

        RecomputeMasterCellRoles();

        YT_LOG_INFO_UNLESS(IsRecovery(), "Master cell registered (CellTag: %v, CellIndex: %v)",
            cellTag,
            index);
    }

    TMasterEntry* FindMasterEntry(TCellTag cellTag)
    {
        auto it = RegisteredMasterMap_.find(cellTag);
        return it == RegisteredMasterMap_.end() ? nullptr : &it->second;
    }

    TMasterEntry* GetMasterEntry(TCellTag cellTag)
    {
        return &GetOrCrash(RegisteredMasterMap_, cellTag);
    }

    TMailbox* FindMasterMailbox(TCellTag cellTag)
    {
        // Fast path.
        if (cellTag == PrimaryMasterCellTag) {
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
        CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
            ->CommitAndLog(Logger);
    }

    void OnCellStatisticsGossip()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsSecondaryMaster());

        if (!IsLocalMasterCellRegistered()) {
            return;
        }

        YT_LOG_INFO("Sending cell statistics gossip message");

        NProto::TReqSetCellStatistics request;
        request.set_cell_tag(GetCellTag());
        *request.mutable_statistics() = GetLocalCellStatistics();
        PostToMaster(request, PrimaryMasterCellTag, false);
    }

    NProto::TCellStatistics GetLocalCellStatistics()
    {
        NProto::TCellStatistics result;
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        result.set_chunk_count(chunkManager->Chunks().GetSize());
        result.set_lost_vital_chunk_count(chunkManager->LostVitalChunks().size());
        return result;
    }


    static TFuture<void> DoSyncWithUpstream(const TWeakPtr<TImpl>& weakThis)
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
            BIND(&TImpl::OnUpstreamSyncReached, MakeStrong(this), timer)
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
        Profiler.Update(UpstreamSyncTimeGauge_, timer.GetElapsedValue());
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

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        hydraRequest.set_user_name(user->GetName());

        return NHiveServer::SerializeOutcomingMessage(hydraRequest);
    }

    void DoPostMessage(
        const TSerializedMessagePtr& message,
        const TCellTagList& cellTags,
        bool reliable)
    {
        TMailboxList mailboxes;
        for (auto cellTag : cellTags) {
            if (cellTag == PrimaryMasterCellTag) {
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

    void OnDynamicConfigChanged()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (CellStatisticsGossipExecutor_) {
            CellStatisticsGossipExecutor_->SetPeriod(GetDynamicConfig()->CellStatisticsGossipPeriod);
        }
        RecomputeMasterCellRoles();
    }


    void RecomputeMasterCellRoles()
    {
        MasterCellRolesMap_.clear();
        auto populateCellRoles = [&] (TCellTag cellTag) {
            MasterCellRolesMap_[cellTag] = ComputeMasterCellRolesFromConfig(cellTag);
        };
        populateCellRoles(GetCellTag());
        for (auto& [cellTag, entry] : RegisteredMasterMap_) {
            populateCellRoles(cellTag);
        }
    }

    EMasterCellRoles GetDefaultMasterCellRoles(TCellTag cellTag)
    {
        return cellTag == GetPrimaryCellTag()
            ? (EMasterCellRoles::CypressNodeHost |
               EMasterCellRoles::TransactionCoordinator |
               (IsMulticell() ? EMasterCellRoles::None : EMasterCellRoles::ChunkHost))
            : (EMasterCellRoles::CypressNodeHost | EMasterCellRoles::ChunkHost);
    }

    EMasterCellRoles ComputeMasterCellRolesFromConfig(TCellTag cellTag)
    {
        auto defaultRoles = GetDefaultMasterCellRoles(cellTag);
        return GetDynamicConfig()->CellRoles.Value(cellTag, defaultRoles);
    }
};

////////////////////////////////////////////////////////////////////////////////

TMulticellManager::TMulticellManager(
    TMulticellManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TMulticellManager::~TMulticellManager()
{ }

void TMulticellManager::Initialize()
{
    Impl_->Initialize();
}

bool TMulticellManager::IsPrimaryMaster()
{
    return Impl_->IsPrimaryMaster();
}

bool TMulticellManager::IsSecondaryMaster()
{
    return Impl_->IsSecondaryMaster();
}

bool TMulticellManager::IsMulticell()
{
    return Impl_->IsMulticell();
}

TCellId TMulticellManager::GetCellId()
{
    return Impl_->GetCellId();
}

TCellId TMulticellManager::GetCellId(TCellTag cellTag)
{
    return Impl_->GetCellId(cellTag);
}

TCellTag TMulticellManager::GetCellTag()
{
    return Impl_->GetCellTag();
}

TCellId TMulticellManager::GetPrimaryCellId()
{
    return Impl_->GetPrimaryCellId();
}

TCellTag TMulticellManager::GetPrimaryCellTag()
{
    return Impl_->GetPrimaryCellTag();
}

const TCellTagList& TMulticellManager::GetSecondaryCellTags()
{
    return Impl_->GetSecondaryCellTags();
}

void TMulticellManager::PostToMaster(
    const TCrossCellMessage& message,
    TCellTag cellTag,
    bool reliable)
{
    Impl_->PostToMaster(message, cellTag, reliable);
}

void TMulticellManager::PostToMasters(
    const TCrossCellMessage& message,
    const TCellTagList& cellTags,
    bool reliable)
{
    Impl_->PostToMasters(message, cellTags, reliable);
}

void TMulticellManager::PostToSecondaryMasters(
    const TCrossCellMessage& message,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(message, reliable);
}

bool TMulticellManager::IsLocalMasterCellRegistered()
{
    return Impl_->IsLocalMasterCellRegistered();
}

bool TMulticellManager::IsRegisteredMasterCell(TCellTag cellTag)
{
    return Impl_->IsRegisteredSecondaryMaster(cellTag);
}

EMasterCellRoles TMulticellManager::GetMasterCellRoles(TCellTag cellTag)
{
    return Impl_->GetMasterCellRoles(cellTag);
}

const TCellTagList& TMulticellManager::GetRegisteredMasterCellTags()
{
    return Impl_->GetRegisteredMasterCellTags();
}

int TMulticellManager::GetRegisteredMasterCellIndex(TCellTag cellTag)
{
    return Impl_->GetRegisteredMasterCellIndex(cellTag);
}

TCellTag TMulticellManager::PickSecondaryChunkHostCell(double bias)
{
    return Impl_->PickSecondaryChunkHostCell(bias);
}

NProto::TCellStatistics TMulticellManager::ComputeClusterStatistics()
{
    return Impl_->ComputeClusterStatistics();
}

IChannelPtr TMulticellManager::GetMasterChannelOrThrow(TCellTag cellTag, EPeerKind peerKind)
{
    return Impl_->GetMasterChannelOrThrow(cellTag, peerKind);
}

IChannelPtr TMulticellManager::FindMasterChannel(TCellTag cellTag, EPeerKind peerKind)
{
    return Impl_->FindMasterChannel(cellTag, peerKind);
}

TMailbox* TMulticellManager::FindPrimaryMasterMailbox()
{
    return Impl_->FindPrimaryMasterMailbox();
}

TFuture<void> TMulticellManager::SyncWithUpstream()
{
    return Impl_->SyncWithUpstream();
}

DELEGATE_SIGNAL(TMulticellManager, void(TCellTag), ValidateSecondaryMasterRegistration, *Impl_);
DELEGATE_SIGNAL(TMulticellManager, void(TCellTag), ReplicateKeysToSecondaryMaster, *Impl_);
DELEGATE_SIGNAL(TMulticellManager, void(TCellTag), ReplicateValuesToSecondaryMaster, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
