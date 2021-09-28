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
#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

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
            BIND(&TChaosManager::InvestigateStalledReplicationCards, MakeWeak(this))))
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
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnStopLeading();

        ChaosCellSynchronizer_->Stop();
    }


    TReplicationCard* GetReplicationCardOrThrow(TReplicationCardId replicationCardId)
    {
        auto* replicationCard = ReplicationCardMap_.Find(replicationCardId);
        if (!replicationCard) {
            THROW_ERROR_EXCEPTION("Replication card not found")
                << TErrorAttribute("replication_card_id", replicationCardId);
        }
        return replicationCard;
    }


    void HydraCreateReplicationCard(
        const TCreateReplicationCardContextPtr& /*context*/,
        NChaosClient::NProto::TReqCreateReplicationCard* /*request*/,
        NChaosClient::NProto::TRspCreateReplicationCard* response)
    {
        auto replicationCardId = Slot_->GenerateId(EObjectType::ReplicationCard);
        auto replicationCardHolder = std::make_unique<TReplicationCard>(replicationCardId);
        auto* replicationCard = replicationCardHolder.get();
        ReplicationCardMap_.Insert(replicationCardId, std::move(replicationCardHolder));

        auto replicationCardToken = TReplicationCardToken{Slot_->GetCellId(), replicationCardId};
        ToProto(response->mutable_replication_card_token(), replicationCardToken);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Created replication card (ReplicationCardId: %v, ReplicationCard: %v)",
            replicationCardId,
            *replicationCard);
    }

    void HydraCreateTableReplica(
        const TCreateTableReplicaContextPtr& /*context*/,
        NChaosClient::NProto::TReqCreateTableReplica* request,
        NChaosClient::NProto::TRspCreateTableReplica* response)
    {
        auto replicationCardToken = FromProto<NChaosClient::TReplicationCardToken>(request->replication_card_token());
        const auto& replicationCardId = replicationCardToken.ReplicationCardId;
        auto newReplica = FromProto<TReplicaInfo>(request->replica_info());

        if (newReplica.State == EReplicaState::Enabled) {
            newReplica.State = EReplicaState::Enabling;
        }

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId);
        for (const auto& replica : replicationCard->Replicas()) {
            if (replica.Cluster == newReplica.Cluster && replica.TablePath == newReplica.TablePath) {
                THROW_ERROR_EXCEPTION("Replica already exists")
                    << TErrorAttribute("cluster", replica.Cluster)
                    << TErrorAttribute("table_path", replica.TablePath)
                    << TErrorAttribute("replica_id", replica.ReplicaId);
            }
        }

        // TODO(savrus): validate that there is sync queue with relevant history.
        // We also need to be sure that old data is actually present at replicas.
        // One way to do that is to split removing process: a) first update progress at the replication card
        // and b) remove only data that is older than replication card progress says (e.g. data 'invisible' to other replicas)

        newReplica.ReplicaId = Slot_->GenerateId(EObjectType::ReplicationCardTableReplica);
        ToProto(response->mutable_replica_id(), newReplica.ReplicaId);

        if (newReplica.State == EReplicaState::Enabling) {
            RevokeShortcuts(replicationCard);
        }

        replicationCard->Replicas().push_back(std::move(newReplica));

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Created table replica (ReplicationCardId: %v, Replica: %v)",
            replicationCardId,
            newReplica);
    }

    void HydraRemoveTableReplica(NChaosClient::NProto::TReqRemoveTableReplica* request)
    {
        auto replicationCardToken = FromProto<NChaosClient::TReplicationCardToken>(request->replication_card_token());
        const auto& replicationCardId = replicationCardToken.ReplicationCardId;
        auto replicaId = FromProto<NChaosClient::TReplicaId>(request->replica_id());

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId);
        auto index = GetReplicaIndexOrThrow(replicationCard, replicaId);
        auto& replica = replicationCard->Replicas()[index];

        if (replica.State != EReplicaState::Disabled) {
            THROW_ERROR_EXCEPTION("Could not remove replica since it is not disabled")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("replica_id", replicaId)
                << TErrorAttribute("state", replica.State);
        }

        auto& replicas = replicationCard->Replicas();
        replicas.erase(replicas.begin() + index);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Removed table replica (ReplicationCardId: %v, ReplicaId: %v)",
            replicationCardId,
            replicaId);
    }

    void HydraAlterTableReplica(NChaosClient::NProto::TReqAlterTableReplica* request)
    {
        auto replicationCardToken = FromProto<NChaosClient::TReplicationCardToken>(request->replication_card_token());
        const auto& replicationCardId = replicationCardToken.ReplicationCardId;
        auto replicaId = FromProto<NTableClient::TTableId>(request->replica_id());
        std::optional<EReplicaMode> mode;
        std::optional<EReplicaState> state;
        std::optional<NYTree::TYPath> path;

        if (request->has_mode()) {
            mode = FromProto<EReplicaMode>(request->mode());
            YT_VERIFY(IsStableReplicaMode(*mode));
        }
        if (request->has_state()) {
            state = FromProto<EReplicaState>(request->state());
            YT_VERIFY(IsStableReplicaState(*state));
        }
        if (request->has_table_path()) {
            path = request->table_path();
        }

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId);
        auto& replica = GetReplicaOrThrow(replicationCard, replicaId);

        if (!IsStableReplicaMode(replica.Mode) || !IsStableReplicaState(replica.State)) {
            THROW_ERROR_EXCEPTION("Replica is transitioning")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("replica_id", replicaId)
                << TErrorAttribute("mode", replica.Mode)
                << TErrorAttribute("state", replica.State);
        }

        if (path && replica.State != EReplicaState::Disabled) {
            THROW_ERROR_EXCEPTION("Could not alter replica table path since it is not disabled")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("replica_id", replicaId)
                << TErrorAttribute("state", replica.State);
        }

        bool revoke = false;

        if (mode && replica.Mode != *mode) {
            if (replica.Mode == EReplicaMode::Sync) {
                replica.Mode = EReplicaMode::SyncToAsync;
                revoke = true;
            } else if (replica.Mode == EReplicaMode::Async) {
                replica.Mode = EReplicaMode::AsyncToSync;
                revoke = true;
            }
        }

        if (state && replica.State != *state) {
            if (replica.State == EReplicaState::Disabled) {
                replica.State = EReplicaState::Enabling;
                revoke = true;
            } else if (replica.State == EReplicaState::Enabled) {
                replica.State = EReplicaState::Disabling;
                revoke = true;
            }
        }

        if (path) {
            YT_VERIFY(replica.State == EReplicaState::Disabled);
            replica.TablePath = *path;
        }

        if (revoke) {
            RevokeShortcuts(replicationCard);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Altered table replica (ReplicationCardId: %v, Replica: %v)",
            replicationCardId,
            replica);
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
            YT_VERIFY(replicationCard->Coordinators().erase(coordinatorCellId) > 0);
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

        for (auto& [cellId, coordinator] : replicationCard->Coordinators()) {
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

        for (const auto& replica : replicationCard->Replicas()) {
            if (!IsStableReplicaMode(replica.Mode) || !IsStableReplicaState(replica.State)) {
                AutomatonInvoker_->Invoke(BIND(
                    &TChaosManager::GenerateNewReplicationEraTimestamp,
                    MakeStrong(this),
                    replicationCard->GetId(),
                    replicationCard->GetEra()));
                break;
            }
        }
    }

    void GenerateNewReplicationEraTimestamp(TReplicationCardId replicationCardId, TReplicationEra era)
    {
        auto timestampOrError = WaitFor(Bootstrap_->GetMasterConnection()->GetTimestampProvider()->GenerateTimestamps());
        if (timestampOrError.IsOK()) {
            NChaosNode::NProto::TReqCommenceNewReplicationEra request;
            ToProto(request.mutable_replication_card_id(), replicationCardId);
            request.set_timestamp(static_cast<ui64>(timestampOrError.Value()));
            request.set_replication_era(era);
            CreateMutation(HydraManager_, request)
                ->CommitAndLog(Logger);
        }
    }

    void HydraCommenceNewReplicationEra(NChaosNode::NProto::TReqCommenceNewReplicationEra* request)
    {
        auto timestamp = static_cast<TTimestamp>(request->timestamp());
        auto replicationCardId = FromProto<NChaosClient::TReplicationCardId>(request->replication_card_id());
        auto era = static_cast<TReplicationEra>(request->replication_era());

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId);
        if (replicationCard->GetEra() != era) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Replication card era mismatch: expected %v got %v (ReplicationCardId: %v)",
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

        auto newEra = replicationCard->GetEra() + 1;
        replicationCard->SetEra(newEra);

        for (auto& replica : replicationCard->Replicas()) {
            bool updated = false;

            if (replica.Mode == EReplicaMode::SyncToAsync) {
                replica.Mode = EReplicaMode::Async;
                updated = true;
            } else if (replica.Mode == EReplicaMode::AsyncToSync) {
                replica.Mode = EReplicaMode::Sync;
                updated = true;
            }

            if (replica.State == EReplicaState::Disabling) {
                replica.State = EReplicaState::Disabled;
                updated = true;
            } else if (replica.State == EReplicaState::Enabling) {
                replica.State = EReplicaState::Enabled;
                updated = true;
            }

            if (updated) {
                // TODO(savrus) Implement history cleanup policy.
                replica.History.push_back({newEra, timestamp, replica.Mode, replica.State});
            }
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Start new replication era (ReplicationCardId: %v, Era: %v, Timestamp: %v)",
            replicationCard->GetId(),
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
            } else if (!oldCellsSet.contains(cellId)) {
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
        auto replicationCardToken = FromProto<NChaosClient::TReplicationCardToken>(request->replication_card_token());
        auto replicaId = FromProto<NTableClient::TTableId>(request->replica_id());
        auto newProgress = FromProto<TReplicationProgress>(request->replication_progress());

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardToken.ReplicationCardId);
        auto& replica = GetReplicaOrThrow(replicationCard, replicaId);
        NChaosClient::UpdateReplicationProgress(&replica.ReplicationProgress, newProgress);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Replication progress updated (ReplicationCardId: %v, ReplicaId: %v, Progress: %v",
            replicationCardToken.ReplicationCardId,
            replicaId,
            replica.ReplicationProgress);
    }


    int GetReplicaIndexOrThrow(TReplicationCard* card, TReplicaId replicaId)
    {
        for (int index = 0; index < std::ssize(card->Replicas()); ++index) {
            if (card->Replicas()[index].ReplicaId == replicaId) {
                return index;
            }
        }

        THROW_ERROR_EXCEPTION("Replica not found")
            << TErrorAttribute("replication_card_id", card->GetId())
            << TErrorAttribute("replica_id", replicaId);
    }

    TReplicaInfo& GetReplicaOrThrow(TReplicationCard* card, TReplicaId replicaId)
    {
        return card->Replicas()[GetReplicaIndexOrThrow(card, replicaId)];
    }

    void InvestigateStalledReplicationCards()
    {
        for (const auto& [replicationCardId, replicationCard] : ReplicationCardMap_) {
            ScheduleNewEraIfReplicationCardIsReady(replicationCard);
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
                    .Item()
                        .BeginMap()
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
            .Item("replicas")
                .DoListFor(card->Replicas(), [&] (TFluentList fluent, const auto& replicaInfo) {
                    fluent
                        .Item()
                            .BeginMap()
                                .Item("cluster").Value(replicaInfo.Cluster)
                                .Item("table_path").Value(replicaInfo.TablePath)
                                .Item("content_type").Value(replicaInfo.ContentType)
                                .Item("mode").Value(replicaInfo.Mode)
                            .EndMap();
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
