#include "chaos_manager.h"

#include "automaton.h"
#include "bootstrap.h"
#include "chaos_cell_synchronizer.h"
#include "chaos_slot.h"
#include "foreign_migrated_replication_card_remover.h"
#include "migrated_replication_card_remover.h"
#include "private.h"
#include "replication_card.h"
#include "replication_card_collocation.h"
#include "replication_card_observer.h"
#include "slot_manager.h"

#include <yt/yt/server/node/chaos_node/transaction_manager.h>

#include <yt/server/node/chaos_node/chaos_manager.pb.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/hive/helpers.h>

#include <yt/yt/server/lib/tablet_server/config.h>
#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/chaos_client/helpers.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/tablet_client/helpers.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

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
using namespace NTabletServer;
using namespace NTabletNode;
using namespace NTransactionSupervisor;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr int MigrateLeftoversBatchSize = 128;

////////////////////////////////////////////////////////////////////////////////

class TChaosManager
    : public IChaosManager
    , public TChaosAutomatonPart
{
    DEFINE_SIGNAL_OVERRIDE(void(TReplicatedTableData), ReplicatedTableCreated);
    DEFINE_SIGNAL_OVERRIDE(void(TTableId), ReplicatedTableDestroyed);
    DEFINE_SIGNAL_OVERRIDE(void(TReplicaData), ReplicaCreated);
    DEFINE_SIGNAL_OVERRIDE(void(TTableReplicaId), ReplicaDestroyed);
    DEFINE_SIGNAL_OVERRIDE(void(TTableCollocationData), ReplicationCollocationCreated);
    DEFINE_SIGNAL_OVERRIDE(void(TTableCollocationId), ReplicationCollocationDestroyed);

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
            BIND(&TChaosManager::PeriodicCurrentTimestampPropagation, MakeWeak(this)),
            Config_->EraCommencingPeriod))
        , ReplicationCardObserver_(CreateReplicationCardObserver(Config_->ReplicationCardObserver, slot))
        , MigratedReplicationCardRemover_(CreateMigratedReplicationCardRemover(Config_->MigratedReplicationCardRemover, slot, bootstrap))
        , ForeignMigratedReplicationCardRemover_(CreateForeignMigratedReplicationCardRemover(
            Config_->ForeignMigratedReplicationCardRemover,
            slot,
            HydraManager_))
        , LeftoversMigrationExecutor_(New<TPeriodicExecutor>(
            slot->GetAutomatonInvoker(NChaosNode::EAutomatonThreadQueue::MigrationDepartment),
            BIND(&TChaosManager::PeriodicMigrateLeftovers, MakeWeak(this)),
            Config_->LeftoverMigrationPeriod))
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

        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraGenerateReplicationCardId, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraCreateReplicationCard, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraRemoveReplicationCard, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraAlterReplicationCard, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraChaosNodeRemoveReplicationCard, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraUpdateCoordinatorCells, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraCreateTableReplica, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraRemoveTableReplica, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraAlterTableReplica, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraUpdateTableReplicaProgress, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraCommenceNewReplicationEra, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraPropagateCurrentTimestamps, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraRspGrantShortcuts, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraRspRevokeShortcuts, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraSuspendCoordinator, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraResumeCoordinator, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraRemoveExpiredReplicaHistory, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraMigrateReplicationCards, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraResumeChaosCell, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraChaosNodeMigrateReplicationCards, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraChaosNodeConfirmReplicationCardMigration, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraCreateReplicationCardCollocation, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraChaosNodeRemoveMigratedReplicationCards, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Slot_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers<NChaosClient::NProto::TReqCreateReplicationCard>({
            .Prepare = BIND_NO_PROPAGATE(&TChaosManager::HydraPrepareCreateReplicationCard, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TChaosManager::HydraCommitCreateReplicationCard, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TChaosManager::HydraAbortCreateReplicationCard, Unretained(this)),
        });
    }

    IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }


    void GenerateReplicationCardId(const TCtxGenerateReplicationCardIdPtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosManager::HydraGenerateReplicationCardId,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void CreateReplicationCard(const TCtxCreateReplicationCardPtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosManager::HydraCreateReplicationCard,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void RemoveReplicationCard(const TCtxRemoveReplicationCardPtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosManager::HydraRemoveReplicationCard,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void AlterReplicationCard(const TCtxAlterReplicationCardPtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosManager::HydraAlterReplicationCard,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void CreateTableReplica(const TCtxCreateTableReplicaPtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosManager::HydraCreateTableReplica,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void RemoveTableReplica(const TCtxRemoveTableReplicaPtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosManager::HydraRemoveTableReplica,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void AlterTableReplica(const TCtxAlterTableReplicaPtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosManager::HydraAlterTableReplica,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void UpdateTableReplicaProgress(const TCtxUpdateTableReplicaProgressPtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosManager::HydraUpdateTableReplicaProgress,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void MigrateReplicationCards(const TCtxMigrateReplicationCardsPtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosManager::HydraMigrateReplicationCards,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void ResumeChaosCell(const TCtxResumeChaosCellPtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosManager::HydraResumeChaosCell,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    TFuture<void> ExecuteAlterTableReplica(const NChaosClient::NProto::TReqAlterTableReplica& request) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            request,
            &TChaosManager::HydraExecuteAlterTableReplica,
            this);
        return mutation
            ->CommitAndLog(Logger)
            .AsVoid();
    }

    void CreateReplicationCardCollocation(const TCtxCreateReplicationCardCollocationPtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosManager::HydraCreateReplicationCardCollocation,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
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
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(ReplicationCardCollocation, TReplicationCardCollocation);

    TReplicationCard* GetReplicationCardOrThrow(TReplicationCardId replicationCardId, bool allowMigrated=false) override
    {
        auto* replicationCard = ReplicationCardMap_.Find(replicationCardId);

        if (!replicationCard) {
            // Only replication card origin cell can answer if replication card exists.
            if (IsDomesticReplicationCard(replicationCardId)) {
                THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError, "No such replication card")
                    << TErrorAttribute("replication_card_id", replicationCardId);
            } else {
                THROW_ERROR_EXCEPTION(NChaosClient::EErrorCode::ReplicationCardNotKnown, "Replication card is not known")
                    << TErrorAttribute("replication_card_id", replicationCardId);
            }
        }

        if (!allowMigrated && IsReplicationCardMigrated(replicationCard)) {
            THROW_ERROR_EXCEPTION(NChaosClient::EErrorCode::ReplicationCardMigrated, "Replication card has been migrated")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("immigrated_to_cell_id", replicationCard->Migration().ImmigratedToCellId)
                << TErrorAttribute("immigration_time", replicationCard->Migration().ImmigrationTime);
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

        DECLARE_NEW_FRIEND()
    };

    const TChaosManagerConfigPtr Config_;
    const IYPathServicePtr OrchidService_;
    const IChaosCellSynchronizerPtr ChaosCellSynchronizer_;
    const TPeriodicExecutorPtr CommencerExecutor_;
    const IReplicationCardObserverPtr ReplicationCardObserver_;
    const IMigratedReplicationCardRemoverPtr MigratedReplicationCardRemover_;
    const IForeignMigratedReplicationCardRemoverPtr ForeignMigratedReplicationCardRemover_;
    const TPeriodicExecutorPtr LeftoversMigrationExecutor_;

    TEntityMap<TReplicationCard> ReplicationCardMap_;
    TEntityMap<TReplicationCardCollocation> CollocationMap_;
    std::vector<TCellId> CoordinatorCellIds_;
    THashMap<TCellId, TInstant> SuspendedCoordinators_;
    bool Suspended_ = false;

    bool NeedRecomputeReplicationCardState_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    TReplicationCardCollocation* GetReplicationCardCollocationOrThrow(TReplicationCardCollocationId collocationId)
    {
        auto* collocation = FindReplicationCardCollocation(collocationId);
        if (!collocation) {
            THROW_ERROR_EXCEPTION("No such replication card collocation")
                << TErrorAttribute("replication_card_collocation_id", collocationId);
        }
        return collocation;
    }

    void SaveKeys(TSaveContext& context) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ReplicationCardMap_.SaveKeys(context);
        CollocationMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Save;

        ReplicationCardMap_.SaveValues(context);
        CollocationMap_.SaveValues(context);
        Save(context, CoordinatorCellIds_);
        Save(context, SuspendedCoordinators_);
        Save(context, Suspended_);
        MigratedReplicationCardRemover_->Save(context);
    }

    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ReplicationCardMap_.LoadKeys(context);
        // COMPAT(savrus)
        if (context.GetVersion() >= EChaosReign::ReplicationCardCollocation) {
            CollocationMap_.LoadKeys(context);
        }
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;

        ReplicationCardMap_.LoadValues(context);
        // COMPAT(savrus)
        if (context.GetVersion() >= EChaosReign::ReplicationCardCollocation) {
            CollocationMap_.LoadValues(context);
        }
        Load(context, CoordinatorCellIds_);
        Load(context, SuspendedCoordinators_);
        // COMPAT(savrus)
        if (context.GetVersion() >= EChaosReign::ChaosCellSuspension) {
            Load(context, Suspended_);
        }
        // COMPAT(ponasenko-rs)
        if (context.GetVersion() >= EChaosReign::RemoveMigratedCards) {
            MigratedReplicationCardRemover_->Load(context);
        }

        NeedRecomputeReplicationCardState_ = context.GetVersion() < EChaosReign::Migration;
    }

    void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnAfterSnapshotLoaded();

        if (NeedRecomputeReplicationCardState_) {
            for (auto& [_, replicationCard] : ReplicationCardMap_) {
                bool alterInProgress = false;
                for (const auto& [_, replica] : replicationCard->Replicas()) {
                    if (!IsStableReplicaState(replica.State) || !IsStableReplicaMode(replica.Mode)) {
                        alterInProgress = true;
                        break;
                    }
                }

                replicationCard->SetState(alterInProgress
                    ? EReplicationCardState::RevokingShortcutsForAlter
                    : EReplicationCardState::Normal);
            }
        }
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::Clear();

        ReplicationCardMap_.Clear();
        CollocationMap_.Clear();
        CoordinatorCellIds_.clear();
        SuspendedCoordinators_.clear();
        NeedRecomputeReplicationCardState_ = false;
        MigratedReplicationCardRemover_->Clear();
    }


    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnLeaderActive();

        ChaosCellSynchronizer_->Start();
        CommencerExecutor_->Start();
        LeftoversMigrationExecutor_->Start();
        ReplicationCardObserver_->Start();
        MigratedReplicationCardRemover_->Start();
        ForeignMigratedReplicationCardRemover_->Start();
        Slot_->GetReplicatedTableTracker()->EnableTracking();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnStopLeading();

        ChaosCellSynchronizer_->Stop();
        YT_UNUSED_FUTURE(CommencerExecutor_->Stop());
        YT_UNUSED_FUTURE(LeftoversMigrationExecutor_->Stop());
        ReplicationCardObserver_->Stop();
        MigratedReplicationCardRemover_->Stop();
        ForeignMigratedReplicationCardRemover_->Stop();
        Slot_->GetReplicatedTableTracker()->DisableTracking();
    }

    void OnRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnRecoveryComplete();

        Slot_->GetReplicatedTableTracker()->Initialize();
    }

    void HydraGenerateReplicationCardId(
        const TCtxGenerateReplicationCardIdPtr& context,
        NChaosClient::NProto::TReqGenerateReplicationCardId* /*request*/,
        NChaosClient::NProto::TRspGenerateReplicationCardId* response)
    {
        auto replicationCardId = GenerateNewReplicationCardId();

        ToProto(response->mutable_replication_card_id(), replicationCardId);

        if (context) {
            context->SetResponseInfo("ReplicationCardId: %v",
                replicationCardId);
        }
    }

    void ValidateReplicationCardCreation(NChaosClient::NProto::TReqCreateReplicationCard* request)
    {
        if (Suspended_ && !FromProto<bool>(request->bypass_suspended())) {
            THROW_ERROR_EXCEPTION(NChaosClient::EErrorCode::ChaosCellSuspended, "Chaos cell %v is suspended",
                Slot_->GetCellId());
        }

        auto hintId = FromProto<TReplicationCardId>(request->hint_id());
        auto replicationCardId = hintId ? hintId : GenerateNewReplicationCardId();

        auto tableId = FromProto<TTableId>(request->table_id());
        if (tableId && TypeFromId(tableId) != EObjectType::ChaosReplicatedTable) {
            THROW_ERROR_EXCEPTION("Malformed chaos replicated table id %v",
                tableId);
        }

        if (!IsDomesticReplicationCard(replicationCardId)) {
            THROW_ERROR_EXCEPTION("Could not create replication card with id %v: expected cell tag %v, got %v",
                replicationCardId,
                CellTagFromId(Slot_->GetCellId()),
                CellTagFromId(replicationCardId));
        }
    }

    TReplicationCardId CreateReplicationCardImpl(NChaosClient::NProto::TReqCreateReplicationCard* request)
    {
        auto tableId = FromProto<TTableId>(request->table_id());
        auto hintId = FromProto<TReplicationCardId>(request->hint_id());
        auto replicationCardId = hintId ? hintId : GenerateNewReplicationCardId();
        auto options = request->has_replicated_table_options()
            ? SafeDeserializeReplicatedTableOptions(replicationCardId, TYsonString(request->replicated_table_options()))
            : New<TReplicatedTableOptions>();

        auto replicationCardHolder = std::make_unique<TReplicationCard>(replicationCardId);

        auto* replicationCard = replicationCardHolder.get();
        replicationCard->SetTableId(tableId);
        replicationCard->SetTablePath(request->table_path());
        replicationCard->SetTableClusterName(request->table_cluster_name());
        replicationCard->SetReplicatedTableOptions(std::move(options));

        ReplicationCardMap_.Insert(replicationCardId, std::move(replicationCardHolder));

        YT_LOG_DEBUG("Replication card created (ReplicationCardId: %v, ReplicationCard: %v)",
            replicationCardId,
            *replicationCard);

        BindReplicationCardToRTT(replicationCard);

        return replicationCardId;
    }

    void HydraCreateReplicationCard(
        const TCtxCreateReplicationCardPtr& context,
        NChaosClient::NProto::TReqCreateReplicationCard* request,
        NChaosClient::NProto::TRspCreateReplicationCard* response)
    {
        ValidateReplicationCardCreation(request);
        auto replicationCardId = CreateReplicationCardImpl(request);

        ToProto(response->mutable_replication_card_id(), replicationCardId);

        if (context) {
            context->SetResponseInfo("ReplicationCardId: %v",
                replicationCardId);
        }
    }

    void HydraPrepareCreateReplicationCard(
        TTransaction* /*transaction*/,
        NChaosClient::NProto::TReqCreateReplicationCard* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        if (request->has_replicated_table_options()) {
            auto options = ConvertTo<TReplicatedTableOptionsPtr>(TYsonString(request->replicated_table_options()));
            Y_UNUSED(options);
        }
        ValidateReplicationCardCreation(request);
    }

    void HydraCommitCreateReplicationCard(
        TTransaction* /*transaction*/,
        NChaosClient::NProto::TReqCreateReplicationCard* request,
        const TTransactionCommitOptions& /*options*/)
    {
        CreateReplicationCardImpl(request);
    }

    void HydraAbortCreateReplicationCard(
        TTransaction* /*transaction*/,
        NChaosClient::NProto::TReqCreateReplicationCard* /*request*/,
        const TTransactionAbortOptions& /*options*/)
    { }

    void HydraAlterReplicationCard(
        const TCtxAlterReplicationCardPtr& /*context*/,
        NChaosClient::NProto::TReqAlterReplicationCard* request,
        NChaosClient::NProto::TRspAlterReplicationCard* /*response*/)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());

        auto options = request->has_replicated_table_options()
            ? SafeDeserializeReplicatedTableOptions(replicationCardId, TYsonString(request->replicated_table_options()))
            : TReplicatedTableOptionsPtr();
        auto enableTracker = request->has_enable_replicated_table_tracker()
            ? std::make_optional(request->enable_replicated_table_tracker())
            : std::nullopt;
        auto collocationId = request->has_replication_card_collocation_id()
            ? std::make_optional(FromProto<TReplicationCardCollocationId>(request->replication_card_collocation_id()))
            : std::nullopt;

        if (options && enableTracker) {
            THROW_ERROR_EXCEPTION(
                "Cannot alter replication card %v: only one of \"replicated_table_options\" "
                "and \"enable_replicated_table_tracker\" could be specified",
                replicationCardId);
        }

        auto replicationCard = GetReplicationCardOrThrow(replicationCardId);
        replicationCard->ValidateCollocationNotMigrating();

        if (collocationId && *collocationId) {
            auto* collocation = GetReplicationCardCollocationOrThrow(*collocationId);
            collocation->ValidateNotMigrating();
        }

        YT_LOG_DEBUG(
            "Alter replication card "
            "(ReplicationCardId: %v, ReplicatedTableOptions: %v, EnableReplicatedTableTracker: %v)",
            replicationCardId,
            options
                ? TStringBuf("null")
                : ConvertToYsonString(options, EYsonFormat::Text).AsStringBuf(),
            enableTracker);

        if (options) {
            replicationCard->SetReplicatedTableOptions(options);
            ReplicatedTableCreated_.Fire(TReplicatedTableData{
                .Id = replicationCardId,
                .Options = options,
            });
        }
        if (enableTracker) {
            replicationCard->GetReplicatedTableOptions()->EnableReplicatedTableTracker = *enableTracker;
            ReplicatedTableCreated_.Fire(TReplicatedTableData{
                .Id = replicationCardId,
                .Options = replicationCard->GetReplicatedTableOptions(),
            });
        }
        if (collocationId) {
            auto* collocation = FindReplicationCardCollocation(*collocationId);
            UpdateReplicationCardCollocation(
                replicationCard,
                collocation);
        }
    }

    void HydraRemoveReplicationCard(
        const TCtxRemoveReplicationCardPtr& /*context*/,
        NChaosClient::NProto::TReqRemoveReplicationCard* request,
        NChaosClient::NProto::TRspRemoveReplicationCard* /*response*/)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        bool isHiveMutation = IsHiveMutation();
        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId, isHiveMutation);

        if (IsReplicationCardMigrated(replicationCard)) {
            YT_VERIFY(isHiveMutation);
            MigratedReplicationCardRemover_->EnqueueRemoval(replicationCardId);
            return;
        }

        replicationCard->ValidateCollocationNotMigrating();

        RevokeShortcuts(replicationCard);

        if (!IsDomesticReplicationCard(replicationCardId)) {
            const auto& hiveManager = Slot_->GetHiveManager();
            NChaosNode::NProto::TReqRemoveReplicationCard req;
            ToProto(req.mutable_replication_card_id(), replicationCard->GetId());

            auto* mailbox = hiveManager->GetCellMailbox(replicationCard->Migration().OriginCellId);
            hiveManager->PostMessage(mailbox, req);

            YT_LOG_DEBUG("Removing migrated replication card at origin cell (ReplicationCardId: %v, OriginCellId: %v)",
                replicationCardId,
                replicationCard->Migration().OriginCellId);
        }

        UpdateReplicationCardCollocation(
            replicationCard,
            /*collocation*/ nullptr);
        UnbindReplicationCardFromRTT(replicationCard);
        ReplicationCardMap_.Remove(replicationCardId);
        MigratedReplicationCardRemover_->ConfirmRemoval(replicationCardId);

        YT_LOG_DEBUG("Replication card removed (ReplicationCardId: %v)",
            replicationCardId);
    }

    void HydraChaosNodeRemoveReplicationCard(NChaosNode::NProto::TReqRemoveReplicationCard *request)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto* replicationCard = FindReplicationCard(replicationCardId);

        if (!replicationCard) {
            YT_LOG_ALERT("Trying to remove emigrated replication card but it does not exist"
                "(ReplicationCardId: %v)",
                replicationCardId);
            return;
        }

        if (!IsReplicationCardMigrated(replicationCard)) {
            YT_LOG_ALERT("Trying to remove emigrated replication card in unexpected state "
                "(ReplicationCardId: %v, ReplicationCardState: %v)",
                replicationCardId,
                replicationCard->GetState());

            return;
        }

        if (!IsDomesticReplicationCard(replicationCardId)) {
            YT_LOG_ALERT("Trying to remove emigrated replication card but it is not domestic "
                "(ReplicationCardId: %v, OriginCellId: %v)",
                replicationCardId,
                replicationCard->Migration().OriginCellId);

            return;
        }

        if (auto* collocation = replicationCard->GetCollocation()) {
            YT_LOG_ALERT("Removing migrated replication card with non-zero collocation (ReplicationCardId: %v, CollocationId: %v)",
                replicationCardId,
                collocation->GetId());

            UpdateReplicationCardCollocation(
                replicationCard,
                /*collocation*/ nullptr);
        }

        ReplicationCardMap_.Remove(replicationCardId);
        MigratedReplicationCardRemover_->ConfirmRemoval(replicationCardId);

        YT_LOG_DEBUG("Replication card removed (ReplicationCardId: %v)",
            replicationCardId);
    }

    void HydraChaosNodeRemoveMigratedReplicationCards(NChaosNode::NProto::TReqRemoveMigratedReplicationCards* request)
    {
        int cardsRemoved = 0;
        for (const auto& protoMigratedCard : request->migrated_cards()) {
            auto replicationCardId = FromProto<TReplicationCardId>(protoMigratedCard.replication_card_id());
            auto migrationTimestamp = FromProto<TTimestamp>(protoMigratedCard.migration_timestamp());

            const auto* replicationCard = ReplicationCardMap_.Find(replicationCardId);
            if (!replicationCard || IsDomesticReplicationCard(replicationCardId) ||
                !IsReplicationCardMigrated(replicationCard) || replicationCard->GetCurrentTimestamp() != migrationTimestamp)
            {
                continue;
            }

            ReplicationCardMap_.Remove(replicationCardId);
            ++cardsRemoved;
        }

        YT_LOG_DEBUG(
            "Removed foreign migrated replication cards (Requested: %v, Removed: %v)",
            request->migrated_cards_size(),
            cardsRemoved);
    }

    void HydraCreateTableReplica(
        const TCtxCreateTableReplicaPtr& context,
        NChaosClient::NProto::TReqCreateTableReplica* request,
        NChaosClient::NProto::TRspCreateTableReplica* response)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        const auto& clusterName = request->cluster_name();
        const auto& replicaPath = request->replica_path();
        auto contentType = FromProto<ETableReplicaContentType>(request->content_type());
        auto mode = FromProto<ETableReplicaMode>(request->mode());
        auto enabled = request->enabled();
        auto catchup = request->catchup();
        auto replicationProgress = request->has_replication_progress()
            ? std::make_optional(FromProto<TReplicationProgress>(request->replication_progress()))
            : std::nullopt;
        auto enableReplicatedTableTracker = request->has_enable_replicated_table_tracker()
            ? request->enable_replicated_table_tracker()
            : false;

        if (!IsStableReplicaMode(mode)) {
            THROW_ERROR_EXCEPTION("Invalid replica mode %v", mode);
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

        if (!catchup && replicationProgress) {
            THROW_ERROR_EXCEPTION("Replication progress specified while replica is not to be caught up")
                << TErrorAttribute("replication_progress", *replicationProgress);
        }

        if (!replicationProgress) {
            replicationProgress = TReplicationProgress{
                .Segments = {{EmptyKey(), MinTimestamp}},
                .UpperKey = MaxKey()
            };
        }

        auto isWaitingReplica = [&] {
            for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas()) {
                if (!replicaInfo.History.empty() &&
                    IsReplicationProgressGreaterOrEqual(*replicationProgress, replicaInfo.ReplicationProgress))
                {
                    return true;
                }
            }
            return false;
        };

        // Validate that old data is actually present at queues.
        // To do this we check that at least one replica is as far behind as the new one (as should be in case of replica copying).
        // This is correct since a) data replica first updates its progress at the replication card
        // b) queue only removes data that is older than overall replication card progress (e.g. data 'invisible' to other replicas)

        if (catchup && replicationCard->GetEra() != InitialReplicationEra && !isWaitingReplica()) {
            THROW_ERROR_EXCEPTION("Could not create replica since all other replicas already left it behind")
                << TErrorAttribute("replication_progress", *replicationProgress);
        }

        auto newReplicaId = GenerateNewReplicaId(replicationCard);

        auto& replicaInfo = EmplaceOrCrash(replicationCard->Replicas(), newReplicaId, TReplicaInfo())->second;
        replicaInfo.ClusterName = clusterName;
        replicaInfo.ReplicaPath = replicaPath;
        replicaInfo.ContentType = contentType;
        replicaInfo.State = enabled ? ETableReplicaState::Enabling : ETableReplicaState::Disabled;
        replicaInfo.Mode = mode;
        replicaInfo.ReplicationProgress = std::move(*replicationProgress);
        replicaInfo.EnableReplicatedTableTracker = enableReplicatedTableTracker;

        if (catchup) {
            replicaInfo.History.push_back({
                .Era = replicationCard->GetEra(),
                .Timestamp = MinTimestamp,
                .Mode = mode,
                .State = enabled && replicationCard->GetEra() == InitialReplicationEra
                    ? ETableReplicaState::Enabled
                    : ETableReplicaState::Disabled
            });
        }

        YT_LOG_DEBUG("Table replica created (ReplicationCardId: %v, ReplicaId: %v, ReplicaInfo: %v)",
            replicationCardId,
            newReplicaId,
            replicaInfo);

        if (replicaInfo.State == ETableReplicaState::Enabling) {
            UpdateReplicationCardState(replicationCard, EReplicationCardState::RevokingShortcutsForAlter);
        }

        ReplicaCreated_.Fire(TReplicaData{
            .TableId = replicationCardId,
            .Id = newReplicaId,
            .Mode = mode,
            .Enabled = enabled,
            .ClusterName = clusterName,
            .TablePath = replicaPath,
            .TrackingEnabled = enableReplicatedTableTracker,
            .ContentType = contentType
        });

        ToProto(response->mutable_replica_id(), newReplicaId);

        if (context) {
            context->SetResponseInfo("ReplicaId: %v",
                newReplicaId);
        }
    }

    void HydraRemoveTableReplica(
        const TCtxRemoveTableReplicaPtr& /*context*/,
        NChaosClient::NProto::TReqRemoveTableReplica* request,
        NChaosClient::NProto::TRspRemoveTableReplica* /*response*/)
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

        ReplicaDestroyed_.Fire(replicaId);

        YT_LOG_DEBUG("Table replica removed (ReplicationCardId: %v, ReplicaId: %v)",
            replicationCardId,
            replicaId);
    }

    void HydraAlterTableReplica(
        const TCtxAlterTableReplicaPtr& /*context*/,
        NChaosClient::NProto::TReqAlterTableReplica* request,
        NChaosClient::NProto::TRspAlterTableReplica* /*response*/)
    {
        HydraExecuteAlterTableReplica(request);
    }

    void HydraExecuteAlterTableReplica(NChaosClient::NProto::TReqAlterTableReplica* request)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto replicaId = FromProto<TTableId>(request->replica_id());
        auto mode = request->has_mode()
            ? std::make_optional(FromProto<ETableReplicaMode>(request->mode()))
            : std::nullopt;
        auto enabled = request->has_enabled()
            ? std::make_optional(request->enabled())
            : std::nullopt;
        auto enableReplicatedTableTracker = request->has_enable_replicated_table_tracker()
            ? std::make_optional(request->enable_replicated_table_tracker())
            : std::nullopt;

        if (mode && !IsStableReplicaMode(*mode)) {
            THROW_ERROR_EXCEPTION("Invalid replica mode %Qlv", *mode);
        }

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId);
        auto* replicaInfo = replicationCard->GetReplicaOrThrow(replicaId);

        // COMPAT(savrus)
        if (GetCurrentMutationContext()->Request().Reign < ToUnderlying(EChaosReign::AllowAlterInCataclysm)) {
            if (!IsStableReplicaMode(replicaInfo->Mode)) {
                THROW_ERROR_EXCEPTION("Replica mode is transitioning")
                    << TErrorAttribute("replication_card_id", replicationCardId)
                    << TErrorAttribute("replica_id", replicaId)
                    << TErrorAttribute("mode", replicaInfo->Mode);
            }
        }

        // COMPAT(savrus)
        if (GetCurrentMutationContext()->Request().Reign < ToUnderlying(EChaosReign::AllowAlterInCataclysm)) {
            if (!IsStableReplicaState(replicaInfo->State)) {
                THROW_ERROR_EXCEPTION("Replica state is transitioning")
                    << TErrorAttribute("replication_card_id", replicationCardId)
                    << TErrorAttribute("replica_id", replicaId)
                    << TErrorAttribute("state", replicaInfo->State);
            }
        }

        if (replicationCard->GetState() == EReplicationCardState::RevokingShortcutsForMigration) {
            THROW_ERROR_EXCEPTION("Replication card is migrating")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("replica_id", replicaId)
                << TErrorAttribute("state", replicationCard->GetState());
        }

        bool revoke = false;

        if (mode && replicaInfo->Mode != *mode) {
            // COMPAT(savrus)
            if (GetCurrentMutationContext()->Request().Reign >= ToUnderlying(EChaosReign::AllowAlterInCataclysm)) {
                switch (*mode) {
                    case ETableReplicaMode::Sync:
                        replicaInfo->Mode = ETableReplicaMode::AsyncToSync;
                        break;

                    case ETableReplicaMode::Async:
                        replicaInfo->Mode = ETableReplicaMode::SyncToAsync;
                        break;

                    default:
                        YT_ABORT();
                }
            } else {
                switch (replicaInfo->Mode) {
                    case ETableReplicaMode::Sync:
                        replicaInfo->Mode = ETableReplicaMode::SyncToAsync;
                        break;

                    case ETableReplicaMode::Async:
                        replicaInfo->Mode = ETableReplicaMode::AsyncToSync;
                        break;

                    default:
                        YT_ABORT();
                }
            }

            revoke = true;
            FireTableReplicaCreatedOrUpdated(replicationCardId, replicaId, *replicaInfo);
        }

        bool currentlyEnabled = replicaInfo->State == ETableReplicaState::Enabled;
        if (enabled && *enabled != currentlyEnabled) {
            // COMPAT(savrus)
            if (GetCurrentMutationContext()->Request().Reign >= ToUnderlying(EChaosReign::AllowAlterInCataclysm)) {
                replicaInfo->State = *enabled
                    ? ETableReplicaState::Enabling
                    : ETableReplicaState::Disabling;
            } else {
                switch (replicaInfo->State) {
                    case ETableReplicaState::Disabled:
                        replicaInfo->State = ETableReplicaState::Enabling;
                        break;

                    case ETableReplicaState::Enabled:
                        replicaInfo->State = ETableReplicaState::Disabling;
                        break;

                    default:
                        YT_ABORT();
                }
            }

            revoke = true;
            FireTableReplicaCreatedOrUpdated(replicationCardId, replicaId, *replicaInfo);
        }

        if (enableReplicatedTableTracker && replicaInfo->EnableReplicatedTableTracker != *enableReplicatedTableTracker) {
            replicaInfo->EnableReplicatedTableTracker = *enableReplicatedTableTracker;
            FireTableReplicaCreatedOrUpdated(replicationCardId, replicaId, *replicaInfo);
        }

        YT_LOG_DEBUG("Table replica altered (ReplicationCardId: %v, ReplicaId: %v, Replica: %v)",
            replicationCardId,
            replicaId,
            *replicaInfo);

        if (revoke) {
            UpdateReplicationCardState(replicationCard, EReplicationCardState::RevokingShortcutsForAlter);
        }
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
                YT_LOG_WARNING("Got grant shortcut response for an unknown replication card (ReplicationCardId: %v)",
                    replicationCardId);
                continue;
            }

            if (replicationCard->GetEra() != era) {
                YT_LOG_ALERT("Got grant shortcut response with invalid era (ReplicationCardId: %v, Era: %v, ResponseEra: %v)",
                    replicationCardId,
                    replicationCard->GetEra(),
                    era);
                continue;
            }

            if (auto it = replicationCard->Coordinators().find(coordinatorCellId); !it || it->second.State != EShortcutState::Granting) {
                YT_LOG_WARNING("Got grant shortcut response but shortcut is not waiting for it"
                    "(ReplicationCardId: %v, Era: %v, CoordinatorCellId: %v, ShortcutState: %v)",
                    replicationCardId,
                    era,
                    coordinatorCellId,
                    it ? std::make_optional(it->second.State) : std::nullopt);

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

        YT_LOG_DEBUG("Shortcuts granted (CoordinatorCellId: %v, Suspended: %v, ReplicationCardIds: %v)",
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
                YT_LOG_WARNING("Got revoke shortcut response for an unknown replication card (ReplicationCardId: %v)",
                    replicationCardId);
                continue;
            }

            if (replicationCard->GetEra() != era) {
                YT_LOG_ALERT("Got revoke shortcut response with invalid era (ReplicationCardId: %v, Era: %v, ResponseEra: %v)",
                    replicationCardId,
                    replicationCard->GetEra(),
                    era);
                continue;
            }

            if (auto it = replicationCard->Coordinators().find(coordinatorCellId); it && it->second.State != EShortcutState::Revoking) {
                YT_LOG_WARNING("Got revoke shortcut response but shortcut is not waiting for it"
                    "(ReplicationCardId: %v, Era: %v CoordinatorCellId: %v, ShortcutState: %v)",
                    replicationCard->GetId(),
                    replicationCard->GetEra(),
                    coordinatorCellId,
                    it->second.State);

                continue;
            }

            replicationCardIds.push_back(replicationCardId);
            EraseOrCrash(replicationCard->Coordinators(), coordinatorCellId);
            HandleReplicationCardStateTransition(replicationCard);
        }

        YT_LOG_DEBUG("Shortcuts revoked (CoordinatorCellId: %v, ReplicationCardIds: %v)",
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

        for (auto [cellId, coordinator] : GetValuesSortedByKey(replicationCard->Coordinators())) {
            if (coordinator->State == EShortcutState::Revoking) {
                YT_LOG_DEBUG("Will not revoke shortcut since it already is revoking "
                    "(ReplicationCardId: %v, Era: %v CoordinatorCellId: %v)",
                    replicationCard->GetId(),
                    replicationCard->GetEra(),
                    cellId);

                continue;
            }

            coordinator->State = EShortcutState::Revoking;
            auto* mailbox = hiveManager->GetCellMailbox(cellId);
            hiveManager->PostMessage(mailbox, req);

            YT_LOG_DEBUG("Revoking shortcut (ReplicationCardId: %v, Era: %v CoordinatorCellId: %v)",
                replicationCard->GetId(),
                replicationCard->GetEra(),
                cellId);
        }

        YT_LOG_DEBUG("Finished revoking shortcuts (ReplicationCardId: %v, Era; %v)",
            replicationCard->GetId(),
            replicationCard->GetEra());
    }

    void GrantShortcuts(TReplicationCard* replicationCard, const std::vector<TCellId> coordinatorCellIds, bool strict = true)
    {
        YT_VERIFY(HasMutationContext());

        const auto& hiveManager = Slot_->GetHiveManager();
        NChaosNode::NProto::TReqGrantShortcuts req;
        ToProto(req.mutable_chaos_cell_id(), Slot_->GetCellId());
        auto* shortcut = req.add_shortcuts();
        ToProto(shortcut->mutable_replication_card_id(), replicationCard->GetId());
        shortcut->set_era(replicationCard->GetEra());

        std::vector<TCellId> suspendedCoordinators;

        for (auto cellId : coordinatorCellIds) {
            // COMPAT(savrus)
            if (GetCurrentMutationContext()->Request().Reign >= ToUnderlying(EChaosReign::RevokeFromSuspended)) {
                if (IsCoordinatorSuspended(cellId)) {
                    suspendedCoordinators.push_back(cellId);
                    continue;
                }
            }

            // TODO(savrus) This could happen in case if coordinator cell id has been removed from CoordinatorCellIds_ and then added.
            // Need to make a better protocol (YT-16072).
            if (replicationCard->Coordinators().contains(cellId)) {
                if (strict) {
                    YT_LOG_ALERT("Will not grant shortcut since it already is in replication card "
                        "(ReplicationCardId: %v, Era: %v, CoordinatorCellId: %v, CoordinatorState: %v)",
                        replicationCard->GetId(),
                        replicationCard->GetEra(),
                        cellId,
                        replicationCard->Coordinators()[cellId].State);
                }

                continue;
            }

            replicationCard->Coordinators().insert(std::pair(cellId, TCoordinatorInfo{EShortcutState::Granting}));
            auto* mailbox = hiveManager->GetOrCreateCellMailbox(cellId);
            hiveManager->PostMessage(mailbox, req);

            YT_LOG_DEBUG("Granting shortcut to coordinator (ReplicationCardId: %v, Era: %v, CoordinatorCellId: %v",
                replicationCard->GetId(),
                replicationCard->GetEra(),
                cellId);
        }

        YT_LOG_DEBUG("Finished granting shortcuts (ReplicationCardId: %v, Era; %v, SuspendedCoordinators: %v)",
            replicationCard->GetId(),
            replicationCard->GetEra(),
            suspendedCoordinators);
    }

    void HydraMigrateReplicationCards(
        const TCtxMigrateReplicationCardsPtr& /*context*/,
        NChaosClient::NProto::TReqMigrateReplicationCards* request,
        NChaosClient::NProto::TRspMigrateReplicationCards* /*response*/)
    {
        auto migrateToCellId = FromProto<TCellId>(request->migrate_to_cell_id());
        auto replicationCardIds = FromProto<std::vector<TReplicationCardId>>(request->replication_card_ids());
        bool migrateAllReplicationCards = request->migrate_all_replication_cards();
        bool suspendChaosCell = request->suspend_chaos_cell();
        bool requireSuspension = request->require_suspension();

        if (requireSuspension && !Suspended_) {
            THROW_ERROR_EXCEPTION("Request cannot be processed in non-suspended state");
        }

        if (std::find(CoordinatorCellIds_.begin(), CoordinatorCellIds_.end(), migrateToCellId) == CoordinatorCellIds_.end()) {
            THROW_ERROR_EXCEPTION("Trying to migrate replication card to unknown cell %v",
                migrateToCellId);
        }

        if (migrateAllReplicationCards) {
            if (!replicationCardIds.empty()) {
                THROW_ERROR_EXCEPTION("Replication card ids and migrate all replication cards cannot be specified simultaneously");
            } else {
                for (auto* replicationCard : GetValuesSortedByKey(ReplicationCardMap_)) {
                    if (replicationCard->IsReadyToMigrate()) {
                        replicationCardIds.push_back(replicationCard->GetId());
                    }
                }
            }
        }

        THashSet<TReplicationCardCollocation*> collocations;
        for (auto replicationCardId : replicationCardIds) {
            auto replicationCard = GetReplicationCardOrThrow(replicationCardId);
            if (!replicationCard->IsReadyToMigrate()) {
                THROW_ERROR_EXCEPTION("Trying to migrate replication card %v while it is in %v state",
                    replicationCardId,
                    replicationCard->GetState());
            }
            if (auto* collocation = replicationCard->GetCollocation()) {
                collocations.insert(replicationCard->GetCollocation());
            }
        }

        THashSet<TReplicationCardId> replicationCardIdsSet(replicationCardIds.begin(), replicationCardIds.end());
        for (auto* collocation : GetValuesSortedByKey(collocations)) {
            collocation->ValidateNotMigrating();

            for (auto* replicationCard : GetValuesSortedByKey(collocation->ReplicationCards())) {
                if (!replicationCardIdsSet.contains(replicationCard->GetId())) {
                    THROW_ERROR_EXCEPTION("Trying to move incomplete collocation %v: replication card %v is absent",
                        collocation->GetId(),
                        replicationCard->GetId());
                }
            }
        }

        if (suspendChaosCell) {
            YT_LOG_DEBUG("Suspending chaos cell");

            Suspended_ = true;
        }

        for (auto* collocation : collocations) {
            collocation->SetState(EReplicationCardCollocationState::Emmigrating);
            ReplicationCollocationDestroyed_.Fire(collocation->GetId());
        }

        for (auto replicationCardId : replicationCardIds) {
            auto replicationCard = GetReplicationCardOrThrow(replicationCardId);
            replicationCard->Migration().ImmigratedToCellId = migrateToCellId;
            UpdateReplicationCardState(replicationCard, EReplicationCardState::RevokingShortcutsForMigration);
            UnbindReplicationCardFromRTT(replicationCard);
        }
    }

    void HydraChaosNodeMigrateReplicationCards(NChaosNode::NProto::TReqMigrateReplicationCards* request)
    {
        auto emigratedFromCellId = FromProto<TCellId>(request->emigrated_from_cell_id());
        auto now = GetCurrentMutationContext()->GetTimestamp();

        for (const auto& protoMigrationCard : request->migration_cards()) {
            const auto& protoReplicationCard = protoMigrationCard.replication_card();

            auto collocationId = FromProto<TReplicationCardCollocationId>(protoReplicationCard.replication_card_collocation_id());
            auto collocationSize = protoMigrationCard.replication_card_collocation_size();
            auto* collocation = FindReplicationCardCollocation(collocationId);

            YT_LOG_ALERT_IF(collocation && collocation->GetSize() != collocationSize,
                "Replication collocation size differ (CollocationId: %v, ExpectedSize: %v, ActualSize: %v)",
                collocationId,
                collocation->GetSize(),
                collocationSize);

            if (!collocation && collocationId) {
                collocation = CreateReplicationCardCollocation(
                    collocationId,
                    EReplicationCardCollocationState::Immigrating,
                    collocationSize);
            }

            auto replicationCardId = FromProto<TReplicationCardId>(protoMigrationCard.replication_card_id());
            auto* replicationCard = FindReplicationCard(replicationCardId);
            if (!replicationCard) {
                if (IsDomesticReplicationCard(replicationCardId)) {
                    // Seems like card has been removed.
                    YT_LOG_DEBUG("Unexpected replication card returned from emmigration (ReplicationCardId: %v)",
                        replicationCardId);
                    continue;
                }

                auto replicationCardHolder = std::make_unique<TReplicationCard>(replicationCardId);
                replicationCard = replicationCardHolder.get();

                ReplicationCardMap_.Insert(replicationCardId, std::move(replicationCardHolder));

                YT_LOG_DEBUG("Replication card created for immigration (ReplicationCardId: %v)",
                    replicationCardId);
            }

            auto options = protoReplicationCard.has_replicated_table_options()
                ? SafeDeserializeReplicatedTableOptions(replicationCardId, TYsonString(protoReplicationCard.replicated_table_options()))
                : New<TReplicatedTableOptions>();

            replicationCard->SetTableId(FromProto<TTableId>(protoReplicationCard.table_id()));
            replicationCard->SetTablePath(protoReplicationCard.table_path());
            replicationCard->SetTableClusterName(protoReplicationCard.table_cluster_name());
            replicationCard->SetEra(protoReplicationCard.era());
            replicationCard->SetReplicatedTableOptions(std::move(options));

            YT_VERIFY(replicationCard->Coordinators().empty());

            replicationCard->Replicas().clear();
            for (auto protoReplica : protoReplicationCard.replicas()) {
                auto replicaId = FromProto<TReplicaId>(protoReplica.id());
                auto replicaInfo = FromProto<TReplicaInfo>(protoReplica.info());
                EmplaceOrCrash(replicationCard->Replicas(), replicaId, replicaInfo);
            }

            auto& migration = replicationCard->Migration();
            if (IsDomesticReplicationCard(replicationCardId)) {
                migration.ImmigratedToCellId = TCellId();
                migration.ImmigrationTime = TInstant();
            } else {
                migration.OriginCellId = FromProto<TCellId>(protoMigrationCard.origin_cell_id());
                migration.EmigratedFromCellId = emigratedFromCellId;
                migration.EmigrationTime = now;
            }

            replicationCard->SetState(EReplicationCardState::GeneratingTimestampForNewEra);

            YT_LOG_DEBUG("Replication card migration started (ReplicationCardId: %v, Domestic: %v, ReplicationCard: %v)",
                replicationCardId,
                IsDomesticReplicationCard(replicationCardId),
                *replicationCard);

            UpdateReplicationCardCollocation(
                replicationCard,
                collocation,
                /*migration*/ true);

            if (!collocation) {
                BindReplicationCardToRTT(replicationCard);
            }

            HandleReplicationCardStateTransition(replicationCard);
        }

        if (!request->has_migration_token()) {
            return;
        }

        std::vector<TReplicationCardId> replicationCardIds;
        for (const auto& protoMigrationCard : request->migration_cards()) {
            replicationCardIds.push_back(FromProto<TReplicationCardId>(protoMigrationCard.replication_card_id()));
        }

        auto migrationToken = FromProto<NObjectClient::TObjectId>(request->migration_token());

        NChaosNode::NProto::TReqConfirmReplicationCardMigration rsp;
        ToProto(rsp.mutable_replication_card_ids(), replicationCardIds);
        ToProto(rsp.mutable_migration_token(), migrationToken);

        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = hiveManager->GetOrCreateCellMailbox(emigratedFromCellId);
        hiveManager->PostMessage(mailbox, rsp);
    }

    void HydraChaosNodeConfirmReplicationCardMigration(NChaosNode::NProto::TReqConfirmReplicationCardMigration* request)
    {
        NObjectClient::TObjectId expectedMigrationToken = FromProto<NObjectClient::TObjectId>(request->migration_token());
        for (const auto& protoReplicationCardId : request->replication_card_ids()) {
            auto* replicationCard = ReplicationCardMap_.Find(FromProto<TReplicationCardId>(protoReplicationCardId));

            if (!replicationCard ||
                replicationCard->GetState() != EReplicationCardState::AwaitingMigrationConfirmation ||
                replicationCard->GetMigrationToken() != expectedMigrationToken)
            {
                continue;
            }

            replicationCard->SetState(EReplicationCardState::Migrated);
        }
    }


    void MigrateReplicationCard(TReplicationCard* replicationCard)
    {
        YT_VERIFY(HasMutationContext());
        YT_VERIFY(replicationCard->Coordinators().empty());
        auto immigratedToCellId = replicationCard->Migration().ImmigratedToCellId;

        auto replicationCardId = replicationCard->GetId();
        YT_LOG_DEBUG("Migrating replication card to different cell "
            "(ReplicationCardId: %v, ImmigratedToCellId: %v, Domestic: %v)",
            replicationCardId,
            immigratedToCellId,
            IsDomesticReplicationCard(replicationCardId));

        NChaosNode::NProto::TReqMigrateReplicationCards req;
        ToProto(req.mutable_emigrated_from_cell_id(), Slot_->GetCellId());
        auto protoMigrationCard = req.add_migration_cards();
        auto originCellId = IsDomesticReplicationCard(replicationCardId)
            ? Slot_->GetCellId()
            : replicationCard->Migration().OriginCellId;
        ToProto(protoMigrationCard->mutable_origin_cell_id(), originCellId);
        ToProto(protoMigrationCard->mutable_replication_card_id(), replicationCardId);
        auto* protoReplicationCard = protoMigrationCard->mutable_replication_card();

        ToProto(protoReplicationCard->mutable_table_id(), replicationCard->GetTableId());
        protoReplicationCard->set_table_path(replicationCard->GetTablePath());
        protoReplicationCard->set_table_cluster_name(replicationCard->GetTableClusterName());
        protoReplicationCard->set_era(replicationCard->GetEra());
        protoReplicationCard->set_replicated_table_options(
            ConvertToYsonString(replicationCard->GetReplicatedTableOptions()).ToString());

        if (auto* collocation = replicationCard->GetCollocation()) {
            ToProto(protoReplicationCard->mutable_replication_card_collocation_id(), collocation->GetId());
            protoMigrationCard->set_replication_card_collocation_size(collocation->GetSize());
        }

        TReplicationCardFetchOptions fetchOptions{
            .IncludeProgress = true,
            .IncludeHistory = true
        };

        for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas()) {
            auto* protoEntry = protoReplicationCard->add_replicas();
            ToProto(protoEntry->mutable_id(), replicaId);
            ToProto(protoEntry->mutable_info(), replicaInfo, fetchOptions);
        }

        auto migrationToken = Slot_->GenerateId(EObjectType::Null);
        ToProto(req.mutable_migration_token(), migrationToken);

        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = hiveManager->GetOrCreateCellMailbox(immigratedToCellId);
        hiveManager->PostMessage(mailbox, req);

        // COMPAT(ponasenko-rs)
        if (GetCurrentMutationContext()->Request().Reign < ToUnderlying(EChaosReign::ConfirmMigrations)) {
            replicationCard->SetState(EReplicationCardState::Migrated);
        } else {
            replicationCard->SetState(EReplicationCardState::AwaitingMigrationConfirmation);
        }

        replicationCard->Migration().ImmigrationTime = GetCurrentMutationContext()->GetTimestamp();
        replicationCard->SetMigrationToken(migrationToken);

        if (auto* collocation = replicationCard->GetCollocation()) {
            UpdateReplicationCardCollocation(
                replicationCard,
                /*collocation*/ nullptr,
                /*migration*/ true);
        }
    }

    bool IsDomesticReplicationCard(TReplicationCardId replicationCardId)
    {
        return CellTagFromId(replicationCardId) == CellTagFromId(Slot_->GetCellId());
    }

    bool IsReplicationCardMigrated(const TReplicationCard* replicationCard)
    {
        return replicationCard->IsMigrated();
    }

    void HydraResumeChaosCell(
        const TCtxResumeChaosCellPtr& /*context*/,
        NChaosClient::NProto::TReqResumeChaosCell* /*request*/,
        NChaosClient::NProto::TRspResumeChaosCell* /*response*/)
    {
        YT_LOG_DEBUG("Resuming chaos cell");

        Suspended_ = false;
    }

    void PeriodicMigrateLeftovers()
    {
        if (!Suspended_) {
            return;
        }

        std::vector<TReplicationCardId> cardIdsToMigrate;
        for (const auto& [id, replicationCard] : ReplicationCardMap_) {
            if (replicationCard->IsReadyToMigrate()) {
                cardIdsToMigrate.push_back(id);
            }

            if (std::ssize(cardIdsToMigrate) >= MigrateLeftoversBatchSize) {
                break;
            }
        }

        if (cardIdsToMigrate.empty()) {
            return;
        }

        const auto& connection = Bootstrap_->GetConnection();
        const auto& cellDirectory = connection->GetCellDirectory();

        auto siblingCellTag = GetSiblingChaosCellTag(CellTagFromId(Slot_->GetCellId()));
        auto descriptor = cellDirectory->FindDescriptorByCellTag(siblingCellTag);
        if (!descriptor) {
            THROW_ERROR_EXCEPTION("Unable to identify sibling cell to migrate replication cards into")
                << TErrorAttribute("chaos_cell_id", Slot_->GetCellId())
                << TErrorAttribute("sibling_cell_tag", siblingCellTag);
        }

        NChaosClient::NProto::TReqMigrateReplicationCards req;
        ToProto(req.mutable_replication_card_ids(), cardIdsToMigrate);
        ToProto(req.mutable_migrate_to_cell_id(), descriptor->CellId);
        req.set_require_suspension(true);

        YT_UNUSED_FUTURE(CreateMutation(HydraManager_, req)
            ->CommitAndLog(Logger));
    }

    void PeriodicCurrentTimestampPropagation()
    {
        if (!IsLeader()) {
            return;
        }

        Slot_->GetTimestampProvider()->GenerateTimestamps()
            .Subscribe(BIND(
                &TChaosManager::OnCurrentTimestampPropagationGenerated,
                MakeWeak(this))
                .Via(AutomatonInvoker_));
    }

    void OnCurrentTimestampPropagationGenerated(const TErrorOr<TTimestamp>& timestampOrError)
    {
        if (!IsLeader()) {
            return;
        }

        if (!timestampOrError.IsOK()) {
            YT_LOG_DEBUG(timestampOrError, "Error generating new current timestamp");
            return;
        }

        auto timestamp = timestampOrError.Value();
        YT_LOG_DEBUG("New current timestamp generated (Timestamp: %v)",
            timestamp);

        NChaosNode::NProto::TReqPropagateCurrentTimestamp request;
        request.set_timestamp(timestamp);
        YT_UNUSED_FUTURE(CreateMutation(HydraManager_, request)
            ->CommitAndLog(Logger));
    }

    void HydraPropagateCurrentTimestamps(NChaosNode::NProto::TReqPropagateCurrentTimestamp* request)
    {
        auto timestamp = request->timestamp();

        YT_LOG_DEBUG("Started periodic current timestamp propagation (Timestamp: %v)",
            timestamp);

        for (auto* replicationCard : GetValuesSortedByKey(ReplicationCardMap_)) {
            if (IsReplicationCardMigrated(replicationCard)) {
                continue;
            }

            MaybeCommenceNewReplicationEra(replicationCard, timestamp);
        }

        YT_LOG_DEBUG("Finished periodic current timestamp propagation (Timestamp: %v)",
            timestamp);
    }

    void UpdateReplicationCardState(TReplicationCard* replicationCard, EReplicationCardState newState)
    {
        switch (newState) {
            case EReplicationCardState::RevokingShortcutsForMigration:
                YT_VERIFY(replicationCard->IsReadyToMigrate());
                replicationCard->SetState(EReplicationCardState::RevokingShortcutsForMigration);
                RevokeShortcuts(replicationCard);
                HandleReplicationCardStateTransition(replicationCard);
                break;

            case EReplicationCardState::RevokingShortcutsForAlter:
                if (replicationCard->GetState() == EReplicationCardState::Normal) {
                    replicationCard->SetState(EReplicationCardState::RevokingShortcutsForAlter);
                    RevokeShortcuts(replicationCard);
                    HandleReplicationCardStateTransition(replicationCard);
                } else {
                    YT_LOG_DEBUG("Skipping replication card state update (ReplicationCardId: %v, State: %v, NewState: %v)",
                        replicationCard->GetId(),
                        replicationCard->GetState(),
                        newState);
                }
                break;

            default:
                YT_ABORT();
        }
    }

    void HandleReplicationCardStateTransition(TReplicationCard* replicationCard)
    {
        while (true) {
            switch (replicationCard->GetState()) {
                case EReplicationCardState::RevokingShortcutsForMigration:
                    if (replicationCard->Coordinators().empty()) {
                        MigrateReplicationCard(replicationCard);
                    }
                    return;

                case EReplicationCardState::RevokingShortcutsForAlter:
                    if (replicationCard->Coordinators().empty()) {
                        replicationCard->SetState(EReplicationCardState::GeneratingTimestampForNewEra);
                        continue;
                    }
                    return;

                case EReplicationCardState::GeneratingTimestampForNewEra:
                    GenerateTimestampForNewEra(replicationCard);
                    return;

                case EReplicationCardState::Normal:
                    return;

                default:
                    YT_ABORT();
            }
        }
    }

    void GenerateTimestampForNewEra(TReplicationCard* replicationCard)
    {
        if (!IsLeader()) {
            return;
        }

        Slot_->GetTimestampProvider()->GenerateTimestamps()
            .Subscribe(BIND(
                &TChaosManager::OnNewReplicationEraTimestampGenerated,
                MakeWeak(this),
                replicationCard->GetId(),
                replicationCard->GetEra())
                .Via(AutomatonInvoker_));
    }

    void OnNewReplicationEraTimestampGenerated(
        TReplicationCardId replicationCardId,
        TReplicationEra era,
        const TErrorOr<TTimestamp>& timestampOrError)
    {
        if (!IsLeader()) {
            return;
        }

        if (!timestampOrError.IsOK()) {
            YT_LOG_DEBUG(timestampOrError, "Error generating new era timestamp (ReplicationCardId: %v, Era: %v)",
                replicationCardId,
                era);
            return;
        }

        auto timestamp = timestampOrError.Value();
        YT_LOG_DEBUG("New era timestamp generated (ReplicationCardId: %v, Era: %v, Timestamp: %v)",
            replicationCardId,
            era,
            timestamp);

        NChaosNode::NProto::TReqCommenceNewReplicationEra request;
        ToProto(request.mutable_replication_card_id(), replicationCardId);
        request.set_timestamp(timestamp);
        request.set_replication_era(era);
        YT_UNUSED_FUTURE(CreateMutation(HydraManager_, request)
            ->CommitAndLog(Logger));
    }

    void HydraCommenceNewReplicationEra(NChaosNode::NProto::TReqCommenceNewReplicationEra* request)
    {
        auto timestamp = static_cast<TTimestamp>(request->timestamp());
        auto replicationCardId = FromProto<NChaosClient::TReplicationCardId>(request->replication_card_id());
        auto era = static_cast<TReplicationEra>(request->replication_era());

        auto* replicationCard = FindReplicationCard(replicationCardId);
        if (!replicationCard) {
            YT_LOG_DEBUG("Will not commence new replication era because replication card is not found (ReplicationCardId: %v)",
                replicationCardId);
            return;
        }

        if (IsReplicationCardMigrated(replicationCard)) {
            YT_LOG_DEBUG("Will not commence new replication card era since replication card has been migrated (ReplicationCardId: %v)",
                replicationCardId);
            return;
        }

        if (replicationCard->GetEra() != era) {
            YT_LOG_DEBUG("Will not commence new replication card era because of era mismatch (ReplicationCardId: %v, ExpectedEra: %v, ActualEra: %v)",
                era,
                replicationCard->GetEra(),
                replicationCardId);
            return;
        }

        MaybeCommenceNewReplicationEra(replicationCard, timestamp);
    }

    void MaybeCommenceNewReplicationEra(TReplicationCard *replicationCard, TTimestamp timestamp)
    {
        YT_VERIFY(HasMutationContext());

        bool willUpdate = timestamp > replicationCard->GetCurrentTimestamp();
        YT_LOG_DEBUG("Updating replication card current timestamp "
            "(ReplicationCardId: %v, Era: %v, State: %v, CurrentTimestamp: %v, NewTimestamp: %v, WillUpdate: %v)",
            replicationCard->GetId(),
            replicationCard->GetEra(),
            replicationCard->GetState(),
            replicationCard->GetCurrentTimestamp(),
            timestamp,
            willUpdate);

        if (!willUpdate) {
            return;
        }

        replicationCard->SetCurrentTimestamp(timestamp);

        if (replicationCard->GetState() != EReplicationCardState::GeneratingTimestampForNewEra) {
            return;
        }

        auto hasSyncQueue = [&] {
            for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas()) {
                if (replicaInfo.ContentType == ETableReplicaContentType::Queue &&
                    GetTargetReplicaState(replicaInfo.State) == ETableReplicaState::Enabled &&
                    GetTargetReplicaMode(replicaInfo.Mode) == ETableReplicaMode::Sync)
                {
                    return true;
                }
            }
            return false;
        }();

        if (!hasSyncQueue) {
            YT_LOG_DEBUG("Will not commence new replication era since there would be no sync queue replicas (ReplicationCard: %v)",
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
                if (replicaInfo.History.empty()) {
                    replicaInfo.ReplicationProgress = TReplicationProgress{
                        .Segments = {{EmptyKey(), timestamp}},
                        .UpperKey = MaxKey()
                    };
                }

                if (replicaInfo.History.empty() ||
                    replicaInfo.History.back().Mode != replicaInfo.Mode ||
                    replicaInfo.History.back().State != replicaInfo.State)
                {
                    replicaInfo.History.push_back({newEra, timestamp, replicaInfo.Mode, replicaInfo.State});
                }
            }
        }

        replicationCard->SetState(EReplicationCardState::Normal);

        YT_LOG_DEBUG("Starting new replication era (ReplicationCard: %v, Era: %v, Timestamp: %v)",
            *replicationCard,
            newEra,
            timestamp);

        GrantShortcuts(replicationCard, CoordinatorCellIds_);
    }

    void HydraSuspendCoordinator(NChaosNode::NProto::TReqSuspendCoordinator* request)
    {
        auto coordinatorCellId = FromProto<TCellId>(request->coordinator_cell_id());
        SuspendCoordinator(coordinatorCellId);

        // COMPAT(savrus)
        if (GetCurrentMutationContext()->Request().Reign < ToUnderlying(EChaosReign::RevokeFromSuspended)) {
            return;
        }

        NChaosNode::NProto::TReqRevokeShortcuts req;
        ToProto(req.mutable_chaos_cell_id(), Slot_->GetCellId());

        for (auto* replicationCard : GetValuesSortedByKey(ReplicationCardMap_)) {
            if (replicationCard->GetState() != EReplicationCardState::Normal) {
                continue;
            }

            if (auto it = replicationCard->Coordinators().find(coordinatorCellId);
                it && (it->second.State == EShortcutState::Granted || it->second.State == EShortcutState::Granting))
            {
                auto* shortcut = req.add_shortcuts();
                ToProto(shortcut->mutable_replication_card_id(), replicationCard->GetId());
                shortcut->set_era(replicationCard->GetEra());

                it->second.State = EShortcutState::Revoking;
            }
        }

        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = hiveManager->GetCellMailbox(coordinatorCellId);
        hiveManager->PostMessage(mailbox, req);
    }

    void HydraResumeCoordinator(NChaosNode::NProto::TReqResumeCoordinator* request)
    {
        auto coordinatorCellId = FromProto<TCellId>(request->coordinator_cell_id());
        ResumeCoordinator(coordinatorCellId);

        // COMPAT(savrus)
        if (GetCurrentMutationContext()->Request().Reign < ToUnderlying(EChaosReign::RevokeFromSuspended)) {
            return;
        }

        NChaosNode::NProto::TReqGrantShortcuts req;
        ToProto(req.mutable_chaos_cell_id(), Slot_->GetCellId());

        for (auto* replicationCard : GetValuesSortedByKey(ReplicationCardMap_)) {
            if (replicationCard->GetState() != EReplicationCardState::Normal) {
                continue;
            }

            if (auto it = replicationCard->Coordinators().find(coordinatorCellId);
                !it || (it->second.State == EShortcutState::Revoked || it->second.State == EShortcutState::Revoking))
            {
                auto* shortcut = req.add_shortcuts();
                ToProto(shortcut->mutable_replication_card_id(), replicationCard->GetId());
                shortcut->set_era(replicationCard->GetEra());

                if (it) {
                    it->second.State = EShortcutState::Granting;
                } else {
                    replicationCard->Coordinators().insert(std::pair(coordinatorCellId, TCoordinatorInfo{EShortcutState::Granting}));
                }
            }
        }

        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = hiveManager->GetCellMailbox(coordinatorCellId);
        hiveManager->PostMessage(mailbox, req);
    }

    void SuspendCoordinator(TCellId coordinatorCellId)
    {
        auto [_, inserted] = SuspendedCoordinators_.emplace(coordinatorCellId, GetCurrentMutationContext()->GetTimestamp());
        if (inserted) {
            YT_LOG_DEBUG("Coordinator suspended (CoordinatorCellId: %v)",
                coordinatorCellId);
        }
    }

    void ResumeCoordinator(TCellId coordinatorCellId)
    {
        auto removed = SuspendedCoordinators_.erase(coordinatorCellId);
        if (removed > 0) {
            YT_LOG_DEBUG("Coordinator resumed (CoordinatorCellId: %v)",
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

        for (auto* replicationCard : GetValuesSortedByKey(ReplicationCardMap_)) {
            if (replicationCard->GetState() == EReplicationCardState::Normal) {
                GrantShortcuts(replicationCard, newCells, /*strict*/ false);
            }
        }

        CoordinatorCellIds_.insert(CoordinatorCellIds_.end(), newCells.begin(), newCells.end());

        YT_LOG_DEBUG("Coordinator cells updated (AddedCoordinatorCellIds: %v, RemovedCoordinatorCellIds: %v)",
            newCells,
            removedCells);
    }

    void HydraUpdateTableReplicaProgress(
        const TCtxUpdateTableReplicaProgressPtr& /*context*/,
        NChaosClient::NProto::TReqUpdateTableReplicaProgress* request,
        NChaosClient::NProto::TRspUpdateTableReplicaProgress* /*response*/)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        auto replicaId = FromProto<TTableId>(request->replica_id());
        auto newProgress = FromProto<TReplicationProgress>(request->replication_progress());
        auto force = request->force();

        auto* replicationCard = GetReplicationCardOrThrow(replicationCardId);
        auto* replicaInfo = replicationCard->GetReplicaOrThrow(replicaId);

        if (replicaInfo->History.empty()) {
            THROW_ERROR_EXCEPTION("Replication progress update is prohibited because replica history has not been started yet")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("replica_id", replicaId);
        }

        YT_LOG_DEBUG("Updating replication progress (ReplicationCardId: %v, ReplicaId: %v, Force: %v, OldProgress: %v, NewProgress: %v)",
            replicationCardId,
            replicaId,
            force,
            replicaInfo->ReplicationProgress,
            newProgress);

        if (force) {
            replicaInfo->ReplicationProgress = std::move(newProgress);
        } else {
            NChaosClient::UpdateReplicationProgress(&replicaInfo->ReplicationProgress, newProgress);
        }

        YT_LOG_DEBUG("Replication progress updated (ReplicationCardId: %v, ReplicaId: %v, Progress: %v)",
            replicationCardId,
            replicaId,
            replicaInfo->ReplicationProgress);
    }

    void HydraRemoveExpiredReplicaHistory(NProto::TReqRemoveExpiredReplicaHistory *request)
    {
        auto expires = FromProto<std::vector<TExpiredReplicaHistory>>(request->expired_replica_histories());

        for (const auto [replicaId, retainTimestamp] : expires) {
            auto replicationCardId = ReplicationCardIdFromReplicaId(replicaId);
            auto* replicationCard = FindReplicationCard(replicationCardId);
            if (!replicationCard || IsReplicationCardMigrated(replicationCard)) {
                continue;
            }

            auto* replica = replicationCard->FindReplica(replicaId);
            if (!replica) {
                continue;
            }

            auto historyIndex = replica->FindHistoryItemIndex(retainTimestamp);
            if (historyIndex > 0) {
                replica->History.erase(
                    replica->History.begin(),
                    replica->History.begin() + historyIndex);

                YT_LOG_DEBUG("Forsaken old replica history items (ReplicationCardId: %v, ReplicaId: %v, RetainTimestamp: %v, HistoryItemIndex: %v)",
                    replicationCardId,
                    replicaId,
                    retainTimestamp,
                    historyIndex);
            }
        }
    }

    void HydraCreateReplicationCardCollocation(
        const TCtxCreateReplicationCardCollocationPtr& context,
        NChaosClient::NProto::TReqCreateReplicationCardCollocation* request,
        NChaosClient::NProto::TRspCreateReplicationCardCollocation* response)
    {
        auto replicationCardIds = FromProto<std::vector<TReplicationCardId>>(request->replication_card_ids());

        std::vector<TReplicationCard*> replicationCards;
        for (auto replicationCardId : replicationCardIds) {
            replicationCards.push_back(GetReplicationCardOrThrow(replicationCardId));
        }

        for (auto replicationCard : replicationCards) {
            if (auto* collocation = replicationCard->GetCollocation()) {
                THROW_ERROR_EXCEPTION("Replication card %v already belongs to collocation %v",
                    replicationCard->GetId(),
                    collocation->GetId());
            }
        }

        auto* collocation = CreateReplicationCardCollocation(
            MakeReplicationCardCollocationId(Slot_->GenerateId(EObjectType::ReplicationCardCollocation)),
            EReplicationCardCollocationState::Normal,
            /*size*/ 0);

        collocation->ReplicationCards() = TReplicationCardCollocation::TReplicationCards(replicationCards.begin(), replicationCards.end());
        collocation->SetSize(collocation->ReplicationCards().size());

        for (auto replicationCard : replicationCards) {
            replicationCard->SetCollocation(collocation);
        }

        FireReplicationCardCollocationUpdated(collocation);

        YT_LOG_DEBUG("Created replication card collocation (CollocationId: %v, ReplicationCardIds: %v)",
            collocation->GetId(),
            collocation->GetReplicationCardIds());

        ToProto(response->mutable_replication_card_collocation_id(), collocation->GetId());

        if (context) {
            context->SetResponseInfo("ReplicationCardCollocationId: %v",
                collocation->GetId());
        }
    }

    void UpdateReplicationCardCollocation(
        TReplicationCard* replicationCard,
        TReplicationCardCollocation* collocation,
        bool migration = false)
    {
        if (collocation == replicationCard->GetCollocation()) {
            return;
        }

        auto* oldCollocation = replicationCard->GetCollocation();

        if (migration) {
            YT_LOG_ALERT_IF(collocation && replicationCard->GetCollocation(),
                "Replication card collocation exchange during migration "
                "(ReplicationCardId: %v, OldCollocationId: %v, NewCollocationId: %v)",
                replicationCard->GetId(),
                replicationCard->GetCollocation()->GetId(),
                collocation->GetId());

            YT_LOG_ALERT_IF(collocation && collocation->GetState() != EReplicationCardCollocationState::Immigrating,
                "Unexpected replication card collocation state during migration "
                "(ReplicationCardId: %v, NewCollocationId: %v, NewCollocationState: %v)",
                replicationCard->GetId(),
                collocation->GetId(),
                collocation->GetState());

            YT_LOG_ALERT_IF(oldCollocation && oldCollocation->GetState() != EReplicationCardCollocationState::Emmigrating,
                "Unexpected replication card collocation state during migration "
                "(ReplicationCardId: %v, OldCollocationId: %v, OldCollocationState: %v)",
                replicationCard->GetId(),
                oldCollocation->GetId(),
                oldCollocation->GetState());
        }

        if (oldCollocation) {
            EraseOrCrash(oldCollocation->ReplicationCards(), replicationCard);
            if (!migration) {
                oldCollocation->SetSize(oldCollocation->GetSize() - 1);

                if (oldCollocation->ReplicationCards().empty()) {
                    ReplicationCollocationDestroyed_.Fire(oldCollocation->GetId());
                } else {
                    ReplicationCollocationCreated_.Fire(TTableCollocationData{
                        .Id = oldCollocation->GetId(),
                        .TableIds = oldCollocation->GetReplicationCardIds()
                    });
                }
            }
            if (oldCollocation->ReplicationCards().empty()) {
                CollocationMap_.Remove(oldCollocation->GetId());
            }
        }

        if (collocation) {
            EmplaceOrCrash(collocation->ReplicationCards(), replicationCard);
            if (!migration) {
                collocation->SetSize(collocation->GetSize() + 1);
                ReplicationCollocationCreated_.Fire(TTableCollocationData{
                    .Id = collocation->GetId(),
                    .TableIds = collocation->GetReplicationCardIds()
                });
            } else if (std::ssize(collocation->ReplicationCards()) == collocation->GetSize()) {
                collocation->SetState(EReplicationCardCollocationState::Normal);
                BindReplicationCardCollocationToRTT(collocation);
            }
        }

        replicationCard->SetCollocation(collocation);
    }

    TReplicationCardCollocation* CreateReplicationCardCollocation(
        TReplicationCardCollocationId collocationId,
        EReplicationCardCollocationState state,
        int size)
    {
        auto collocationHolder = std::make_unique<TReplicationCardCollocation>(collocationId);
        auto* collocation = collocationHolder.get();
        CollocationMap_.Insert(collocationId, std::move(collocationHolder));
        collocation->SetState(state);
        collocation->SetSize(size);
        return collocation;
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

    void FireReplicationCardCollocationUpdated(TReplicationCardCollocation* collocation)
    {
        ReplicationCollocationCreated_.Fire(TTableCollocationData{
            .Id = collocation->GetId(),
            .TableIds = collocation->GetReplicationCardIds()
        });
    }

    void BindReplicationCardCollocationToRTT(TReplicationCardCollocation* collocation)
    {
        for (auto* replicationCard : collocation->ReplicationCards()) {
            BindReplicationCardToRTT(replicationCard);
        }

        FireReplicationCardCollocationUpdated(collocation);
    }

    void BindReplicationCardToRTT(TReplicationCard* replicationCard)
    {
        ReplicatedTableCreated_.Fire(TReplicatedTableData{
            .Id = replicationCard->GetId(),
            .Options = replicationCard->GetReplicatedTableOptions()
        });

        for (const auto& [replicaId, replicaInfo] : replicationCard->Replicas()) {
            FireTableReplicaCreatedOrUpdated(replicationCard->GetId(), replicaId, replicaInfo);
        }
    }

    void UnbindReplicationCardFromRTT(TReplicationCard* replicationCard)
    {
        for (const auto& [replicaId, _] : replicationCard->Replicas()) {
            ReplicaDestroyed_.Fire(replicaId);
        }

        ReplicatedTableDestroyed_.Fire(replicationCard->GetId());
    }

    TEnumIndexedArray<EReplicationCardState, int> CountReplicationCardStates() const
    {
        TEnumIndexedArray<EReplicationCardState, int> counts;
        for (const auto& [_, replicationCard] : ReplicationCardMap_) {
            counts[replicationCard->GetState()]++;
        }

        return counts;
    }

    TCompositeMapServicePtr CreateOrchidService()
    {
        return New<TCompositeMapService>()
            ->AddAttribute(EInternedAttributeKey::Opaque, BIND([] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer)
                        .Value(true);
                }))
            ->AddChild("internal", IYPathService::FromMethod(
                &TChaosManager::BuildInternalOrchid,
                MakeWeak(this))
                ->Via(Slot_->GetAutomatonInvoker()))
            ->AddChild("coordinators", IYPathService::FromMethod(
                &TChaosManager::BuildCoordinatorsOrchid,
                MakeWeak(this))
                ->Via(Slot_->GetAutomatonInvoker()))
            ->AddChild("suspended_coordinators", IYPathService::FromMethod(
                &TChaosManager::BuildSuspendedCoordinatorsOrchid,
                MakeWeak(this))
                ->Via(Slot_->GetAutomatonInvoker()))
            ->AddChild("replication_card_collocations", IYPathService::FromMethod(
                &TChaosManager::BuildReplicationCardCollocationsOrchid,
                MakeWeak(this))
                ->Via(Slot_->GetAutomatonInvoker()))
            ->AddChild("replication_cards", TReplicationCardOrchidService::Create(MakeWeak(this), Slot_->GetGuardedAutomatonInvoker()));
    }

    void BuildInternalOrchid(IYsonConsumer* consumer) const
    {
        // NB: Transactions account for both 2pc replication card creation and coordinator transactions.
        const auto& transactionManager = Slot_->GetTransactionManager();

        auto replicationCardStateCounts = CountReplicationCardStates();
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("suspended").Value(Suspended_
                    && replicationCardStateCounts[EReplicationCardState::Migrated] == ReplicationCardMap_.GetSize()
                    && transactionManager->Transactions().empty())
                .Item("replication_card_states").DoMapFor(
                    TEnumTraits<EReplicationCardState>::GetDomainValues(),
                    [&] (TFluentMap fluent, const auto& state) {
                        fluent
                            .Item(CamelCaseToUnderscoreCase(ToString(state))).Value(replicationCardStateCounts[state]);
                    })
            .EndMap();
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
        const auto& migration = card->Migration();
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("replication_card_id").Value(card->GetId())
                .Item("era").Value(card->GetEra())
                .Item("state").Value(card->GetState())
                .Item("coordinators").DoMapFor(card->Coordinators(), [] (TFluentMap fluent, const auto& pair) {
                    fluent
                        .Item(ToString(pair.first)).Value(pair.second.State);
                })
                .Item("replicas").DoListFor(card->Replicas(), [] (TFluentList fluent, const auto& replicaInfo) {
                    Serialize(replicaInfo, fluent.GetConsumer());
                })
                .Item("migration")
                    .BeginMap()
                        .DoIf(static_cast<bool>(migration.OriginCellId), [&] (TFluentMap fluent) {
                            fluent.Item("origin_cell_id").Value(migration.OriginCellId);
                        })
                        .DoIf(static_cast<bool>(migration.ImmigratedToCellId), [&] (TFluentMap fluent) {
                            fluent.Item("immigrated_to_cell_id").Value(migration.ImmigratedToCellId);
                        })
                        .DoIf(static_cast<bool>(migration.EmigratedFromCellId), [&] (TFluentMap fluent) {
                            fluent.Item("emmgrated_from_cell_id").Value(migration.EmigratedFromCellId);
                        })
                        .DoIf(static_cast<bool>(migration.ImmigrationTime), [&] (TFluentMap fluent) {
                            fluent.Item("immigration_time").Value(migration.ImmigrationTime);
                        })
                        .DoIf(static_cast<bool>(migration.EmigrationTime), [&] (TFluentMap fluent) {
                            fluent.Item("emmigration_time").Value(migration.EmigrationTime);
                        })
                    .EndMap()
            .EndMap();
    }

    void BuildReplicationCardCollocationsOrchid(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .DoMapFor(CollocationMap_, [] (TFluentMap fluent, const auto& pair) {
                const auto [collocationId, collocation] = pair;
                fluent
                    .Item(ToString(collocationId))
                        .BeginMap()
                            .Item("state").Value(collocation->GetState())
                            .Item("size").Value(collocation->GetSize())
                            .Item("replication_card_ids")
                                .DoListFor(collocation->ReplicationCards(), [] (TFluentList fluent, const auto* replicationCard) {
                                    fluent
                                        .Item().Value(ToString(replicationCard->GetId()));
                                })
                        .EndMap();
                });
    }

    TReplicatedTableOptionsPtr SafeDeserializeReplicatedTableOptions(TReplicationCardId replicationCardId, const TYsonString& serializedOptions)
    {
        try {
            return ConvertTo<TReplicatedTableOptionsPtr>(serializedOptions);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Failed to parse replicated table options (ReplicationCardId: %v)",
                replicationCardId);
        }
        return New<TReplicatedTableOptions>();
    }

    void FireTableReplicaCreatedOrUpdated(
        TReplicationCardId replicationCardId,
        TReplicaId replicaId,
        const TReplicaInfo& replicaInfo)
    {
        ReplicaCreated_.Fire(TReplicaData{
            .TableId = replicationCardId,
            .Id = replicaId,
            .Mode = GetTargetReplicaMode(replicaInfo.Mode),
            .Enabled = GetTargetReplicaState(replicaInfo.State) == ETableReplicaState::Enabled,
            .ClusterName = replicaInfo.ClusterName,
            .TablePath = replicaInfo.ReplicaPath,
            .TrackingEnabled = replicaInfo.EnableReplicatedTableTracker,
            .ContentType = replicaInfo.ContentType
        });
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TChaosManager, ReplicationCard, TReplicationCard, ReplicationCardMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TChaosManager, ReplicationCardCollocation, TReplicationCardCollocation, CollocationMap_);

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
