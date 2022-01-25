#include "chaos_manager.h"

#include "automaton.h"
#include "bootstrap.h"
#include "chaos_cell_synchronizer.h"
#include "chaos_slot.h"
#include "private.h"
#include "replication_card.h"
#include "slot_manager.h"

#include <yt/server/node/chaos_node/chaos_manager.pb.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/chaos_client/helpers.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/tablet_client/helpers.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NChaosNode {

using namespace NYson;
using namespace NYTree;
using namespace NHydra;
using namespace NClusterNode;
using namespace NHiveServer;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NChaosClient;
using namespace NTableClient;
using namespace NTabletClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TChaosManager
    : public IChaosManager
    , public TChaosAutomatonPart
{
public:
    TChaosManager(
        TChaosManagerConfigPtr config,
        IChaosSlotPtr slot,
        IBootstrap* bootstrap)
        : TChaosAutomatonPart(
            slot,
            bootstrap)
        , Config_(config)
        , OrchidService_(CreateOrchidService())
        , ChaosCellSynchronizer_(CreateChaosCellSynchronizer(Config_->ChaosCellSynchronizer, slot, bootstrap))
        , CommencerExecutor_(New<TPeriodicExecutor>(
            slot->GetAutomatonInvoker(NChaosNode::EAutomatonThreadQueue::EraCommencer),
            BIND(&TChaosManager::InvestigateStalledReplicationCards, MakeWeak(this)),
            Config_->EraCommencingPeriod))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "ChaosManager.Keys",
            BIND(&TChaosManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "ChaosManager.Values",
            BIND(&TChaosManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "ChaosManager.Keys",
            BIND(&TChaosManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ChaosManager.Values",
            BIND(&TChaosManager::SaveValues, Unretained(this)));

        RegisterMethod(BIND(&TChaosManager::HydraCreateReplicationCard, Unretained(this)));
        RegisterMethod(BIND(&TChaosManager::HydraUpdateCoordinatorCells, Unretained(this)));
        RegisterMethod(BIND(&TChaosManager::HydraCreateTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TChaosManager::HydraRemoveTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TChaosManager::HydraAlterTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TChaosManager::HydraUpdateReplicationProgress, Unretained(this)));
        RegisterMethod(BIND(&TChaosManager::HydraCommenceNewReplicationEra, Unretained(this)));
        RegisterMethod(BIND(&TChaosManager::HydraRspGrantShortcuts, Unretained(this)));
        RegisterMethod(BIND(&TChaosManager::HydraRspRevokeShortcuts, Unretained(this)));
        RegisterMethod(BIND(&TChaosManager::HydraSuspendCoordinator, Unretained(this)));
        RegisterMethod(BIND(&TChaosManager::HydraResumeCoordinator, Unretained(this)));
    }

    void Initialize() override
    { }

    IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }


    void CreateReplicationCard(const TCreateReplicationCardContextPtr& context) override
    {
        auto mutation = CreateMutation(HydraManager_, context);
        mutation->SetAllowLeaderForwarding(true);
        mutation->CommitAndReply(context);
    }

    void CreateTableReplica(const TCreateTableReplicaContextPtr& context) override
    {
        auto mutation = CreateMutation(HydraManager_, context);
        mutation->SetAllowLeaderForwarding(true);
        mutation->CommitAndReply(context);
    }

    void RemoveTableReplica(const TRemoveTableReplicaContextPtr& context) override
    {
        auto mutation = CreateMutation(HydraManager_, context);
        mutation->SetAllowLeaderForwarding(true);
        mutation->CommitAndReply(context);
    }

    void AlterTableReplica(const TAlterTableReplicaContextPtr& context) override
    {
        auto mutation = CreateMutation(HydraManager_, context);
        mutation->SetAllowLeaderForwarding(true);
        mutation->CommitAndReply(context);
    }

    void UpdateReplicationProgress(const TUpdateReplicationProgressContextPtr& context) override
    {
        auto mutation = CreateMutation(HydraManager_, context);
        mutation->SetAllowLeaderForwarding(true);
        mutation->CommitAndReply(context);
    }


    const std::vector<TCellId>& CoordinatorCellIds() override
    {
        return CoordinatorCellIds_;
    }

    bool IsCoordinatorSuspended(TCellId coordinatorCellId) override
    {
        return SuspendedCoordinators_.contains(coordinatorCellId);
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(ReplicationCard, TReplicationCard);

    TReplicationCard* GetReplicationCardOrThrow(TReplicationCardId replicationCardId) override
    {
        auto* replicationCard = ReplicationCardMap_.Find(replicationCardId);
        if (!replicationCard) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such replication card")
                << TErrorAttribute("replication_card_id", replicationCardId);
        }
        return replicationCard;
    }

private:
    class TReplicationCardOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TChaosManager> impl, IInvokerPtr invoker)
        {
            return New<TReplicationCardOrchidService>(std::move(impl))
                ->Via(invoker);
        }

        std::vector<TString> GetKeys(i64 limit) const override
        {
            std::vector<TString> keys;
            if (auto owner = Owner_.Lock()) {
                for (const auto& [replicationCardId, _] : owner->ReplicationCards()) {
                    if (std::ssize(keys) >= limit) {
                        break;
                    }
                    keys.push_back(ToString(replicationCardId));
                }
            }
            return keys;
        }

        i64 GetSize() const override
        {
            if (auto owner = Owner_.Lock()) {
                return owner->ReplicationCards().size();
            }
            return 0;
        }

        IYPathServicePtr FindItemService(TStringBuf key) const override
        {
            if (auto owner = Owner_.Lock()) {
                if (auto replicationCard = owner->FindReplicationCard(TReplicationCardId::FromString(key))) {
                    auto producer = BIND(&TChaosManager::BuildReplicationCardOrchidYson, owner, replicationCard);
                    return ConvertToNode(producer);
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<TChaosManager> Owner_;

        explicit TReplicationCardOrchidService(TWeakPtr<TChaosManager> impl)
            : Owner_(std::move(impl))
        { }

        DECLARE_NEW_FRIEND();
    };

    const TChaosManagerConfigPtr Config_;
    const IYPathServicePtr OrchidService_;
    const IChaosCellSynchronizerPtr ChaosCellSynchronizer_;
    const TPeriodicExecutorPtr CommencerExecutor_;

    TEntityMap<TReplicationCard> ReplicationCardMap_;
    std::vector<TCellId> CoordinatorCellIds_;
    THashMap<TCellId, TInstant> SuspendedCoordinators_;


    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(TSaveContext& context) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ReplicationCardMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Save;

        ReplicationCardMap_.SaveValues(context);
        Save(context, CoordinatorCellIds_);
        Save(context, SuspendedCoordinators_);
    }

    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ReplicationCardMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;

        ReplicationCardMap_.LoadValues(context);
        Load(context, CoordinatorCellIds_);
        Load(context, SuspendedCoordinators_);
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::Clear();

        ReplicationCardMap_.Clear();
        CoordinatorCellIds_.clear();
        SuspendedCoordinators_.clear();
    }


    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnLeaderActive();

        ChaosCellSynchronizer_->Start();
        CommencerExecutor_->Start();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnStopLeading();

        ChaosCellSynchronizer_->Stop();
        CommencerExecutor_->Stop();
    }


    void HydraCreateReplicationCard(
        const TCreateReplicationCardContextPtr& /*context*/,
        NChaosClient::NProto::TReqCreateReplicationCard* /*request*/,
        NChaosClient::NProto::TRspCreateReplicationCard* response)
    {
        auto replicationCardId = GenerateNewReplicationCardId();
        auto replicationCardHolder = std::make_unique<TReplicationCard>(replicationCardId);
        auto* replicationCard = replicationCardHolder.get();
        ReplicationCardMap_.Insert(replicationCardId, std::move(replicationCardHolder));

        ToProto(response->mutable_replication_card_id(), replicationCardId);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Created replication card (ReplicationCardId: %v, ReplicationCard: %v)",
            replicationCardId,
            *replicationCard);
    }

    void HydraCreateTableReplica(
        const TCreateTableReplicaContextPtr& /*context*/,
        NChaosClient::NProto::TReqCreateTableReplica* request,
        NChaosClient::NProto::TRspCreateTableReplica* response)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        const auto& clusterName = request->cluster_name();
        const auto& replicaPath = request->replica_path();
        auto contentType = FromProto<ETableReplicaContentType>(request->content_type());
        auto mode = FromProto<ETableReplicaMode>(request->mode());
        auto enabled = request->enabled();

        if (!IsStableReplicaMode(mode)) {
            THROW_ERROR_EXCEPTION("Invalid replica mode %Qlv", mode);
        }

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId);

        if (std::ssize(replicationCard->Replicas()) >= MaxReplicasPerReplicationCard) {
            THROW_ERROR_EXCEPTION("Replication card already has too many replicas")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("limit", MaxReplicasPerReplicationCard);
        }

        for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas()) {
            if (replicaInfo.ClusterName == clusterName && replicaInfo.ReplicaPath == replicaPath) {
                THROW_ERROR_EXCEPTION("Replica already exists")
                    << TErrorAttribute("replica_id", replicaId)
                    << TErrorAttribute("cluster_name", replicaInfo.ClusterName)
                    << TErrorAttribute("replica_path", replicaInfo.ReplicaPath);
            }
        }

        // TODO(savrus): validate that there is sync queue with relevant history.
        // We also need to be sure that old data is actually present at replicas.
        // One way to do that is to split removing process: a) first update progress at the replication card
        // and b) remove only data that is older than replication card progress says (e.g. data 'invisible' to other replicas)

        auto newReplicaId = GenerateNewReplicaId(replicationCard);

        auto& replicaInfo = EmplaceOrCrash(replicationCard->Replicas(), newReplicaId, TReplicaInfo())->second;
        replicaInfo.ClusterName = clusterName;
        replicaInfo.ReplicaPath = replicaPath;
        replicaInfo.ContentType = contentType;
        replicaInfo.State = enabled ? ETableReplicaState::Enabling : ETableReplicaState::Disabling;
        replicaInfo.Mode = mode;
        replicaInfo.ReplicationProgress = {
            .Segments = {{EmptyKey(), MinTimestamp}},
            .UpperKey = MaxKey()
        };
        replicaInfo.History.push_back({
            .Era = InitialReplicationEra,
            .Timestamp = MinTimestamp,
            .Mode = mode,
            .State = ETableReplicaState::Disabled
        });

        if (replicaInfo.State == ETableReplicaState::Enabling) {
            RevokeShortcuts(replicationCard);
        }

        ToProto(response->mutable_replica_id(), newReplicaId);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Created table replica (ReplicationCardId: %v, ReplicaId: %v)",
            replicationCardId,
            newReplicaId);
    }

    void HydraRemoveTableReplica(NChaosClient::NProto::TReqRemoveTableReplica* request)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto replicaId = FromProto<NChaosClient::TReplicaId>(request->replica_id());

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId);
        auto* replicaInfo = replicationCard->GetReplicaOrThrow(replicaId);

        if (replicaInfo->State != ETableReplicaState::Disabled) {
            THROW_ERROR_EXCEPTION("Could not remove replica since it is not disabled")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("replica_id", replicaId)
                << TErrorAttribute("state", replicaInfo->State);
        }

        EraseOrCrash(replicationCard->Replicas(), replicaId);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Removed table replica (ReplicationCardId: %v, ReplicaId: %v)",
            replicationCardId,
            replicaId);
    }

    void HydraAlterTableReplica(NChaosClient::NProto::TReqAlterTableReplica* request)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto replicaId = FromProto<NTableClient::TTableId>(request->replica_id());

        std::optional<ETableReplicaMode> mode;
        if (request->has_mode()) {
            mode = FromProto<ETableReplicaMode>(request->mode());
            if (!IsStableReplicaMode(*mode)) {
                THROW_ERROR_EXCEPTION("Invalid replica mode %Qlv", *mode);
            }
        }

        auto enabled = request->has_enabled()
            ? std::make_optional(request->enabled())
            : std::nullopt;

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId);
        auto* replicaInfo = replicationCard->GetReplicaOrThrow(replicaId);

        if (!IsStableReplicaMode(replicaInfo->Mode)) {
            THROW_ERROR_EXCEPTION("Replica mode is transitioning")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("replica_id", replicaId)
                << TErrorAttribute("mode", replicaInfo->Mode);
        }

        if (!IsStableReplicaState(replicaInfo->State)) {
            THROW_ERROR_EXCEPTION("Replica state is transitioning")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("replica_id", replicaId)
                << TErrorAttribute("state", replicaInfo->State);
        }

        bool revoke = false;

        if (mode && replicaInfo->Mode != *mode) {
            if (replicaInfo->Mode == ETableReplicaMode::Sync) {
                replicaInfo->Mode = ETableReplicaMode::SyncToAsync;
                revoke = true;
            } else if (replicaInfo->Mode == ETableReplicaMode::Async) {
                replicaInfo->Mode = ETableReplicaMode::AsyncToSync;
                revoke = true;
            }
        }

        bool currentlyEnabled = replicaInfo->State == ETableReplicaState::Enabled;
        if (enabled && *enabled != currentlyEnabled) {
            if (replicaInfo->State == ETableReplicaState::Disabled) {
                replicaInfo->State = ETableReplicaState::Enabling;
                revoke = true;
            } else if (replicaInfo->State == ETableReplicaState::Enabled) {
                replicaInfo->State = ETableReplicaState::Disabling;
                revoke = true;
            }
        }

        if (revoke) {
            RevokeShortcuts(replicationCard);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Altered table replica (ReplicationCardId: %v, ReplicaId: %v, Replica: %v)",
            replicationCardId,
            replicaId,
            *replicaInfo);
    }

    void HydraRspGrantShortcuts(NChaosNode::NProto::TRspGrantShortcuts* request)
    {
        auto coordinatorCellId = FromProto<TCellId>(request->coordinator_cell_id());
        bool suspended = request->suspended();
        std::vector<TReplicationCardId> replicationCardIds;

        for (const auto& shortcut : request->shortcuts()) {
            auto replicationCardId = FromProto<TReplicationCardId>(shortcut.replication_card_id());
            auto era = shortcut.era();

            auto* replicationCard = ReplicationCardMap_.Find(replicationCardId);
            if (!replicationCard) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Got grant shortcut response for an unknown replication card (ReplicationCardId: %v)",
                    replicationCardId);
                continue;
            }

            if (replicationCard->GetEra() != era) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Got grant shortcut response with invalid era (ReplicationCardId: %v, Era: %v, ResponseEra: %v)",
                    replicationCardId,
                    replicationCard->GetEra(),
                    era);
                continue;
            }

            replicationCardIds.push_back(replicationCardId);
            replicationCard->Coordinators()[coordinatorCellId].State = EShortcutState::Granted;
        }

        if (suspended) {
            SuspendCoordinator(coordinatorCellId);
        } else {
            ResumeCoordinator(coordinatorCellId);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Shortcuts granted (CoordinatorCellId: %v, Suspended: %v, ReplicationCardIds: %v)",
            coordinatorCellId,
            suspended,
            replicationCardIds);
    }

    void HydraRspRevokeShortcuts(NChaosNode::NProto::TRspRevokeShortcuts* request)
    {
        auto coordinatorCellId = FromProto<TCellId>(request->coordinator_cell_id());
        std::vector<TReplicationCardId> replicationCardIds;

        for (const auto& shortcut : request->shortcuts()) {
            auto replicationCardId = FromProto<TReplicationCardId>(shortcut.replication_card_id());
            auto era = shortcut.era();

            auto* replicationCard = ReplicationCardMap_.Find(replicationCardId);
            if (!replicationCard) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Got revoke shortcut response for an unknown replication card (ReplicationCardId: %v)",
                    replicationCardId);
                continue;
            }

            if (replicationCard->GetEra() != era) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Got revoke shortcut response with invalid era (ReplicationCardId: %v, Era: %v, ResponseEra: %v)",
                    replicationCardId,
                    replicationCard->GetEra(),
                    era);
                continue;
            }

            replicationCardIds.push_back(replicationCardId);
            EraseOrCrash(replicationCard->Coordinators(), coordinatorCellId);
            ScheduleNewEraIfReplicationCardIsReady(replicationCard);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Shortcuts revoked (CoordinatorCellId: %v, ReplicationCardIds: %v)",
            coordinatorCellId,
            replicationCardIds);
    }


    void RevokeShortcuts(TReplicationCard* replicationCard)
    {
        YT_VERIFY(HasMutationContext());

        const auto& hiveManager = Slot_->GetHiveManager();
        NChaosNode::NProto::TReqRevokeShortcuts req;
        ToProto(req.mutable_chaos_cell_id(), Slot_->GetCellId());
        auto* shortcut = req.add_shortcuts();
        ToProto(shortcut->mutable_replication_card_id(), replicationCard->GetId());
        shortcut->set_era(replicationCard->GetEra());

        for (auto& [cellId, coordinator] : replicationCard->Coordinators()) {
            if (coordinator.State == EShortcutState::Revoking) {
                continue;
            }

            coordinator.State = EShortcutState::Revoking;
            auto* mailbox = hiveManager->GetMailbox(cellId);
            hiveManager->PostMessage(mailbox, req);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Revoking shortcut (ReplicationCardId: %v, Era: %v CoordinatorCellId: %v)",
                replicationCard->GetId(),
                replicationCard->GetEra(),
                cellId);
        }
    }

    void GrantShortcuts(TReplicationCard* replicationCard, const std::vector<TCellId> coordinatorCellIds)
    {
        YT_VERIFY(HasMutationContext());

        const auto& hiveManager = Slot_->GetHiveManager();
        NChaosNode::NProto::TReqGrantShortcuts req;
        ToProto(req.mutable_chaos_cell_id(), Slot_->GetCellId());
        auto* shortcut = req.add_shortcuts();
        ToProto(shortcut->mutable_replication_card_id(), replicationCard->GetId());
        shortcut->set_era(replicationCard->GetEra());

        for (auto cellId : coordinatorCellIds) {
            // TODO(savrus) This could happen in case if coordinator cell id has been removed from CoordinatorCellIds_ and then added.
            // Need to make a better protocol.
            YT_VERIFY(!replicationCard->Coordinators().contains(cellId));

            replicationCard->Coordinators().insert(std::make_pair(cellId, TCoordinatorInfo{EShortcutState::Granting}));
            auto* mailbox = hiveManager->GetOrCreateMailbox(cellId);
            hiveManager->PostMessage(mailbox, req);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Granting shortcut to coordinator (ReplicationCardId: %v, Era: %v, CoordinatorCellId: %v",
                replicationCard->GetId(),
                replicationCard->GetEra(),
                cellId);
        }
    }

    void ScheduleNewEraIfReplicationCardIsReady(TReplicationCard* replicationCard)
    {
        if (!replicationCard->Coordinators().empty()) {
            return;
        }
        if (!IsLeader()) {
            return;
        }

        for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas()) {
            if (!IsStableReplicaMode(replicaInfo.Mode) || !IsStableReplicaState(replicaInfo.State)) {
                Bootstrap_->GetMasterConnection()->GetTimestampProvider()->GenerateTimestamps()
                    .Subscribe(BIND(
                        &TChaosManager::OnNewReplicationEraTimestampGenerated,
                        MakeWeak(this),
                        replicationCard->GetId(),
                        replicationCard->GetEra())
                        .Via(AutomatonInvoker_));
                break;
            }
        }
    }

    void OnNewReplicationEraTimestampGenerated(
        TReplicationCardId replicationCardId,
        TReplicationEra era,
        const TErrorOr<TTimestamp>& timestampOrError)
    {
        if (!timestampOrError.IsOK()) {
            YT_LOG_DEBUG(timestampOrError, "Error generating new era timestamp (ReplicationCardId: %v, Era: %v)",
                replicationCardId,
                era);
            return;
        }

        auto timestamp = timestampOrError.Value();
        YT_LOG_DEBUG("New era timestamp generated (ReplicationCardId: %v, Era: %v, Timestamp: %llx)",
            replicationCardId,
            era,
            timestamp);

        NChaosNode::NProto::TReqCommenceNewReplicationEra request;
        ToProto(request.mutable_replication_card_id(), replicationCardId);
        request.set_timestamp(timestamp);
        request.set_replication_era(era);
        CreateMutation(HydraManager_, request)
            ->CommitAndLog(Logger);
    }

    void HydraCommenceNewReplicationEra(NChaosNode::NProto::TReqCommenceNewReplicationEra* request)
    {
        auto timestamp = static_cast<TTimestamp>(request->timestamp());
        auto replicationCardId = FromProto<NChaosClient::TReplicationCardId>(request->replication_card_id());
        auto era = static_cast<TReplicationEra>(request->replication_era());

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId);
        if (replicationCard->GetEra() != era) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Replication card era mismatch (ReplicationCardId: %v, ExpectedEra: %v, ActualEra: %v)",
                era,
                replicationCard->GetEra(),
                replicationCardId);
            return;
        }

        DoCommenceNewReplicationEra(replicationCard, timestamp);
    }

    void DoCommenceNewReplicationEra(TReplicationCard *replicationCard, TTimestamp timestamp)
    {
        YT_VERIFY(HasMutationContext());

        auto hasSyncQueue = [&] {
            for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas()) {
                if (replicaInfo.ContentType == ETableReplicaContentType::Queue &&
                    (replicaInfo.Mode == ETableReplicaMode::Sync || replicaInfo.Mode == ETableReplicaMode::AsyncToSync))
                {
                    return true;
                }
            }
            return false;
        }();

        if (!hasSyncQueue) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Will not commence new replication era since there would be no sync queue replicas (ReplicationCard: %v)",
                *replicationCard);
            return;
        }

        auto newEra = replicationCard->GetEra() + 1;
        replicationCard->SetEra(newEra);

        for (auto& [replicaId, replicaInfo] : replicationCard->Replicas()) {
            bool updated = false;

            if (replicaInfo.Mode == ETableReplicaMode::SyncToAsync) {
                replicaInfo.Mode = ETableReplicaMode::Async;
                updated = true;
            } else if (replicaInfo.Mode == ETableReplicaMode::AsyncToSync) {
                replicaInfo.Mode = ETableReplicaMode::Sync;
                updated = true;
            }

            if (replicaInfo.State == ETableReplicaState::Disabling) {
                replicaInfo.State = ETableReplicaState::Disabled;
                updated = true;
            } else if (replicaInfo.State == ETableReplicaState::Enabling) {
                replicaInfo.State = ETableReplicaState::Enabled;
                updated = true;
            }

            if (updated) {
                // TODO(savrus) Implement history cleanup policy.
                replicaInfo.History.push_back({newEra, timestamp, replicaInfo.Mode, replicaInfo.State});
            }
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Start new replication era (ReplicationCard: %v, Era: %v, Timestamp: %llx)",
            *replicationCard,
            newEra,
            timestamp);

        GrantShortcuts(replicationCard, CoordinatorCellIds_);
    }

    void HydraSuspendCoordinator(NChaosNode::NProto::TReqSuspendCoordinator* request)
    {
        SuspendCoordinator(FromProto<TCellId>(request->coordinator_cell_id()));
    }

    void HydraResumeCoordinator(NChaosNode::NProto::TReqResumeCoordinator* request)
    {
        ResumeCoordinator(FromProto<TCellId>(request->coordinator_cell_id()));
    }

    void SuspendCoordinator(TCellId coordinatorCellId)
    {
        auto [_, inserted] = SuspendedCoordinators_.emplace(coordinatorCellId, GetCurrentMutationContext()->GetTimestamp());
        if (inserted) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Suspend coordinator (CoordinatorCellId: %v)",
                coordinatorCellId);
        }
    }

    void ResumeCoordinator(TCellId coordinatorCellId)
    {
        auto removed = SuspendedCoordinators_.erase(coordinatorCellId);
        if (removed > 0) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Resume coordinator (CoordinatorCellId: %v)",
                coordinatorCellId);
        }
    }


    void HydraUpdateCoordinatorCells(NChaosNode::NProto::TReqUpdateCoordinatorCells* request)
    {
        auto newCells = FromProto<std::vector<TCellId>>(request->add_coordinator_cell_ids());
        auto oldCells = FromProto<std::vector<TCellId>>(request->remove_coordinator_cell_ids());
        auto oldCellsSet = THashSet<TCellId>(oldCells.begin(), oldCells.end());
        auto newCellsSet = THashSet<TCellId>(newCells.begin(), newCells.end());
        std::vector<TCellId> removedCells;

        int current = 0;
        for (int index = 0; index < std::ssize(CoordinatorCellIds_); ++index) {
            const auto& cellId = CoordinatorCellIds_[index];

            if (auto it = newCellsSet.find(cellId)) {
                newCellsSet.erase(it);
            }

            if (!oldCellsSet.contains(cellId)) {
                if (current != index) {
                    CoordinatorCellIds_[current] = cellId;
                }
                ++current;
            } else {
                removedCells.push_back(cellId);
            }
        }

        CoordinatorCellIds_.resize(current);
        newCells = std::vector<TCellId>(newCellsSet.begin(), newCellsSet.end());
        std::sort(newCells.begin(), newCells.end());

        for (auto [_, replicationCard] : ReplicationCardMap_) {
            GrantShortcuts(replicationCard, newCells);
        }

        CoordinatorCellIds_.insert(CoordinatorCellIds_.end(), newCells.begin(), newCells.end());

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Updated coordinator cells (AddedCoordinatorCellIds: %v, RemovedCoordinatorCellIds: %v)",
            newCells,
            removedCells);
    }

    void HydraUpdateReplicationProgress(NChaosClient::NProto::TReqUpdateReplicationProgress* request)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto replicaId = FromProto<NTableClient::TTableId>(request->replica_id());
        auto newProgress = FromProto<TReplicationProgress>(request->replication_progress());

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId);
        auto* replicaInfo = replicationCard->GetReplicaOrThrow(replicaId);
        NChaosClient::UpdateReplicationProgress(&replicaInfo->ReplicationProgress, newProgress);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Replication progress updated (ReplicationCardId: %v, ReplicaId: %v, Progress: %v",
            replicationCardId,
            replicaId,
            replicaInfo->ReplicationProgress);
    }


    void InvestigateStalledReplicationCards()
    {
        for (const auto& [replicationCardId, replicationCard] : ReplicationCardMap_) {
            ScheduleNewEraIfReplicationCardIsReady(replicationCard);
        }
    }


    TReplicationCardId GenerateNewReplicationCardId()
    {
        return MakeReplicationCardId(Slot_->GenerateId(EObjectType::ReplicationCard));
    }

    TReplicaId GenerateNewReplicaId(TReplicationCard* replicationCard)
    {
        while (true) {
            auto index = replicationCard->GetCurrentReplicaIdIndex();
            // NB: Wrap-around is possible.
            replicationCard->SetCurrentReplicaIdIndex(index + 1);
            auto replicaId = MakeReplicaId(replicationCard->GetId(), index);
            if (!replicationCard->Replicas().contains(replicaId)) {
                return replicaId;
            }
        }
    }


    TCompositeMapServicePtr CreateOrchidService()
    {
        return New<TCompositeMapService>()
            ->AddAttribute(EInternedAttributeKey::Opaque, BIND([] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer)
                        .Value(true);
                }))
            ->AddChild("coordinators", IYPathService::FromMethod(
                &TChaosManager::BuildCoordinatorsOrchid,
                MakeWeak(this))
                ->Via(Slot_->GetAutomatonInvoker()))
            ->AddChild("suspended_coordinators", IYPathService::FromMethod(
                &TChaosManager::BuildSuspendedCoordinatorsOrchid,
                MakeWeak(this))
                ->Via(Slot_->GetAutomatonInvoker()))
            ->AddChild("replication_cards", TReplicationCardOrchidService::Create(MakeWeak(this), Slot_->GetGuardedAutomatonInvoker()));
    }

    void BuildCoordinatorsOrchid(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .DoListFor(CoordinatorCellIds_, [] (TFluentList fluent, const auto& coordinatorCellId) {
                fluent
                    .Item().Value(coordinatorCellId);
                });
    }

    void BuildSuspendedCoordinatorsOrchid(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .DoListFor(SuspendedCoordinators_, [] (TFluentList fluent, const auto& suspended) {
                fluent
                    .Item().BeginMap()
                        .Item("coordinator_cell_id").Value(suspended.first)
                        .Item("suspension_time").Value(suspended.second)
                    .EndMap();
                });
    }

    void BuildReplicationCardOrchidYson(TReplicationCard* card, IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("replication_card_id").Value(card->GetId())
                .Item("replicas").DoListFor(card->Replicas(), [] (TFluentList fluent, const auto& replicaInfo) {
                    Serialize(replicaInfo, fluent.GetConsumer());
                })
            .EndMap();
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TChaosManager, ReplicationCard, TReplicationCard, ReplicationCardMap_)

////////////////////////////////////////////////////////////////////////////////

IChaosManagerPtr CreateChaosManager(
    TChaosManagerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap)
{
    return New<TChaosManager>(
        config,
        slot,
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
