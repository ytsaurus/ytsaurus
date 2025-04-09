#include "coordinator_manager.h"

#include "automaton.h"
#include "chaos_manager.h"
#include "chaos_slot.h"
#include "private.h"
#include "replication_card.h"
#include "shortcut_snapshot_store.h"
#include "transaction.h"
#include "transaction_manager.h"

#include <yt/yt/server/node/chaos_node/chaos_manager.pb.h>

#include <yt/yt/server/lib/hive/helpers.h>
#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <util/generic/algorithm.h>

namespace NYT::NChaosNode {

using namespace NYson;
using namespace NYTree;
using namespace NHiveServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NChaosClient;
using namespace NTransactionClient;
using namespace NTransactionSupervisor;
using namespace NServer;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TCoordinatorManager
    : public ICoordinatorManager
    , public TChaosAutomatonPart
{
public:
    TCoordinatorManager(
        TCoordinatorManagerConfigPtr config,
        IChaosSlotPtr slot,
        IBootstrap* bootstrap)
        : TChaosAutomatonPart(
            slot,
            bootstrap)
        , Config_(config)
        , SnapshotStore_(slot->GetShortcutSnapshotStore())
        , OrchidService_(CreateOrchidService())
    {
        YT_ASSERT_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "CoordinatorManager.Values",
            BIND_NO_PROPAGATE(&TCoordinatorManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "CoordinatorManager.Values",
            BIND_NO_PROPAGATE(&TCoordinatorManager::SaveValues, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TCoordinatorManager::HydraReqSuspend, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TCoordinatorManager::HydraReqResume, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TCoordinatorManager::HydraReqGrantShortcuts, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TCoordinatorManager::HydraReqRevokeShortcuts, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Slot_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers<NChaosClient::NProto::TReqReplicatedCommit>({
            .Prepare = BIND_NO_PROPAGATE(&TCoordinatorManager::HydraPrepareReplicatedCommit, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TCoordinatorManager::HydraCommitReplicatedCommit, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TCoordinatorManager::HydraAbortReplicatedCommit, Unretained(this)),
        });
    }

    IYPathServicePtr GetOrchidService() override
    {
        return OrchidService_;
    }

    void SuspendCoordinator(TSuspendCoordinatorContextPtr context) override
    {
        auto mutation = CreateMutation(HydraManager_, context);
        mutation->SetAllowLeaderForwarding(true);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void ResumeCoordinator(TResumeCoordinatorContextPtr context) override
    {
        auto mutation = CreateMutation(HydraManager_, context);
        mutation->SetAllowLeaderForwarding(true);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

private:
    class TShortcutOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TCoordinatorManager> owner, IInvokerPtr invoker)
        {
            return New<TShortcutOrchidService>(std::move(owner))
                ->Via(invoker);
        }

        std::vector<std::string> GetKeys(i64 limit) const override
        {
            std::vector<std::string> keys;
            if (auto owner = Owner_.Lock()) {
                for (const auto& [chaosObjectId, shortcut] : owner->Shortcuts_) {
                    if (std::ssize(keys) >= limit) {
                        break;
                    }
                    keys.push_back(ToString(chaosObjectId));
                }
            }
            return keys;
        }

        i64 GetSize() const override
        {
            if (auto owner = Owner_.Lock()) {
                return owner->Shortcuts_.size();
            }
            return 0;
        }

        IYPathServicePtr FindItemService(const std::string& key) const override
        {
            if (auto owner = Owner_.Lock()) {
                if (auto shortcut = owner->FindShortcut(TChaosObjectId::FromString(key))) {
                    auto producer = BIND(&TCoordinatorManager::BuildChaosObjectOrchidYson, owner, shortcut);
                    return ConvertToNode(producer);
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<TCoordinatorManager> Owner_;

        explicit TShortcutOrchidService(TWeakPtr<TCoordinatorManager> owner)
            : Owner_(std::move(owner))
        { }

        DECLARE_NEW_FRIEND()
    };

    struct TShortcut
    {
        TCellId CellId;
        TReplicationEra Era;
        EShortcutState State;
        THashSet<TTransactionId> AliveTransactions;

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, CellId);
            Persist(context, Era);
            Persist(context, State);
            Persist(context, AliveTransactions);
        }
    };

    const TCoordinatorManagerConfigPtr Config_;
    const IShortcutSnapshotStorePtr SnapshotStore_;

    using TShortcutMap = THashMap<NChaosClient::TChaosObjectId, TShortcut>;
    TShortcutMap Shortcuts_;
    bool Suspended_ = false;

    const IYPathServicePtr OrchidService_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveValues(TSaveContext& context) const
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        using NYT::Save;
        Save(context, Shortcuts_);
        Save(context, Suspended_);
    }

    void LoadValues(TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;
        Load(context, Shortcuts_);
        Load(context, Suspended_);
    }

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::Clear();

        Shortcuts_.clear();
        Suspended_ = false;
        SnapshotStore_->Clear();
    }

    void OnAfterSnapshotLoaded() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnAfterSnapshotLoaded();

        for (const auto& [chaosObjectId, shortcut] : Shortcuts_) {
            SnapshotStore_->UpdateShortcut(chaosObjectId, {shortcut.Era});
        }
    }

    TShortcut* FindShortcut(TChaosObjectId chaosObjectId)
    {
        auto it = Shortcuts_.find(chaosObjectId);
        return it ? &it->second : nullptr;
    }

    void HydraReqSuspend(NChaosClient::NProto::TReqSuspendCoordinator* /*request*/)
    {
        if (Suspended_) {
            return;
        }

        auto cellIds = GetMetadataCellIds();
        for (auto cellId : cellIds) {
            NChaosNode::NProto::TReqSuspendCoordinator req;
            ToProto(req.mutable_coordinator_cell_id(), Slot_->GetCellId());

            const auto& hiveManager = Slot_->GetHiveManager();
            if (auto mailbox = hiveManager->FindMailbox(cellId)) {
                hiveManager->PostMessage(mailbox, req);
            }
        }

        Suspended_ = true;

        YT_LOG_DEBUG("Coordinator suspended (SuspendedAtCells: %v)",
            cellIds);
    }

    void HydraReqResume(NChaosClient::NProto::TReqResumeCoordinator* /*request*/)
    {
        if (!Suspended_) {
            return;
        }

        auto cellIds = GetMetadataCellIds();
        for (auto cellId : cellIds) {
            NChaosNode::NProto::TReqResumeCoordinator req;
            ToProto(req.mutable_coordinator_cell_id(), Slot_->GetCellId());

            const auto& hiveManager = Slot_->GetHiveManager();
            if (auto mailbox = hiveManager->FindMailbox(cellId)) {
                hiveManager->PostMessage(mailbox, req);
            }
        }

        Suspended_ = false;

        YT_LOG_DEBUG("Coordinator resumed (ResumedAtCells: %v)",
            cellIds);
    }


    void HydraReqGrantShortcuts(NChaosNode::NProto::TReqGrantShortcuts* request)
    {
        auto chaosCellId = FromProto<TCellId>(request->chaos_cell_id());
        std::vector<std::pair<TChaosObjectId, TReplicationEra>> grantedShortcuts;

        NChaosNode::NProto::TRspGrantShortcuts rsp;
        ToProto(rsp.mutable_coordinator_cell_id(), Slot_->GetCellId());
        rsp.set_suspended(Suspended_);

        for (const auto& protoShortcut : request->shortcuts()) {
            auto chaosObjectId = FromProto<NChaosClient::TChaosObjectId>(protoShortcut.chaos_object_id());
            auto era = protoShortcut.era();

            if (auto it = Shortcuts_.find(chaosObjectId)) {
                YT_LOG_ALERT("Granting shortcut while shortcut already present "
                    "(ChaosCellId: %v, ChaosObjectId: %v, Type: %v, OldEra: %v, OldState: %v)",
                    chaosCellId,
                    chaosObjectId,
                    TypeFromId(chaosObjectId),
                    it->second.Era,
                    it->second.State);

                YT_VERIFY(it->second.AliveTransactions.empty());
            }

            InsertShortcut(chaosObjectId, {chaosCellId, era, EShortcutState::Granted, {}});
            grantedShortcuts.emplace_back(chaosObjectId, era);

            auto* rspShortcut = rsp.add_shortcuts();
            ToProto(rspShortcut->mutable_chaos_object_id(), chaosObjectId);
            rspShortcut->set_era(era);
        }

        const auto& hiveManager = Slot_->GetHiveManager();
        auto mailbox = hiveManager->GetMailbox(chaosCellId);
        hiveManager->PostMessage(mailbox, rsp);

        YT_LOG_DEBUG("Shortcuts granted (Shortcuts: %v)",
            MakeFormattableView(grantedShortcuts, [] (auto* builder, const auto& grantedShortcut) {
                builder->AppendFormat("<%v, %v>", grantedShortcut.first, grantedShortcut.second);
            }));
    }

    void HydraReqRevokeShortcuts(NChaosNode::NProto::TReqRevokeShortcuts* request)
    {
        auto chaosCellId = FromProto<TCellId>(request->chaos_cell_id());
        std::vector<std::pair<TChaosObjectId, TReplicationEra>> revokedShortcuts;
        std::vector<std::pair<TChaosObjectId, TReplicationEra>> inactiveShortcuts;

        for (auto protoShortcut : request->shortcuts()) {
            auto chaosObjectId = FromProto<NChaosClient::TChaosObjectId>(protoShortcut.chaos_object_id());
            auto era = protoShortcut.era();

            if (!Shortcuts_.contains(chaosObjectId)) {
                YT_LOG_ALERT("Revoking unknown shortcut (ChaosCellId: %v, ChaosObjectId: %v, Type: %v, Era: %v)",
                    chaosCellId,
                    chaosObjectId,
                    TypeFromId(chaosObjectId),
                    era);
                continue;
            }

            auto& shortcut = Shortcuts_[chaosObjectId];

            if (shortcut.Era != era) {
                YT_LOG_ALERT("Revoking shortcut with invalid era "
                    "(ChaosCellId: %v, ChaosObjectId: %v, Type: %v, ShortcutEra: %v, RequestedEra: %v)",
                    chaosCellId,
                    chaosObjectId,
                    TypeFromId(chaosObjectId),
                    shortcut.Era,
                    era);
            }

            revokedShortcuts.emplace_back(chaosObjectId, shortcut.Era);

            if (!shortcut.AliveTransactions.empty()) {
                shortcut.State = EShortcutState::Revoking;
            } else {
                inactiveShortcuts.emplace_back(chaosObjectId, shortcut.Era);
                EraseShortcut(chaosObjectId);
            }
        }

        if (!inactiveShortcuts.empty()) {
            SendRevokeShortcutsResponse(chaosCellId, inactiveShortcuts);
        }

        YT_LOG_DEBUG("Shortcuts revoked (Shortcuts: %v, Inactive: %v)",
            MakeFormattableView(revokedShortcuts, [] (auto* builder, const auto& shortcut) {
                builder->AppendFormat("<%v, %v>", shortcut.first, shortcut.second);
            }),
            MakeFormattableView(inactiveShortcuts, [] (auto* builder, const auto& shortcut) {
                builder->AppendFormat("<%v, %v>", shortcut.first, shortcut.second);
            }));
    }

    void HydraPrepareReplicatedCommit(
        TTransaction* transaction,
        NChaosClient::NProto::TReqReplicatedCommit* request,
        const TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.Persistent);

        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        TReplicationEra era = request->replication_era();

        auto it = Shortcuts_.find(replicationCardId);
        if (it == Shortcuts_.end()) {
            THROW_ERROR_EXCEPTION("Shortcut for replication card is not found")
                << TErrorAttribute("replication_card_id", replicationCardId);
        }
        if (it->second.State != EShortcutState::Granted) {
            THROW_ERROR_EXCEPTION("Shortcut for replication card has been revoked")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("shortcut_state", it->second.State);
        }
        if (it->second.Era != era) {
            THROW_ERROR_EXCEPTION("Shortcut for replication card has different era")
                << TErrorAttribute("replication_card_id", replicationCardId)
                << TErrorAttribute("shortcut_era", it->second.Era)
                << TErrorAttribute("replication_card_era", era);
        }
        InsertOrCrash(it->second.AliveTransactions, transaction->GetId());

        auto chaosLeaseIds = FromProto<std::vector<TChaosLeaseId>>(request->prerequisite_ids());
        for (const auto& chaosLeaseId : chaosLeaseIds) {
            auto it = Shortcuts_.find(chaosLeaseId);
            if (it == Shortcuts_.end()) {
                THROW_ERROR_EXCEPTION("Shortcut for chaos lease is not found")
                    << TErrorAttribute("chaos_lease_id", chaosLeaseId);
            }
            if (it->second.State != EShortcutState::Granted) {
                THROW_ERROR_EXCEPTION("Shortcut is not in 'Granted' state")
                    << TErrorAttribute("chaos_lease_id", chaosLeaseId)
                    << TErrorAttribute("shortcut_state", it->second.State);
            }
        }

        for (const auto& chaosLeaseId : chaosLeaseIds) {
            auto it = GetIteratorOrCrash(Shortcuts_, chaosLeaseId);
            InsertOrCrash(it->second.AliveTransactions, transaction->GetId());
        }

        YT_LOG_DEBUG("Replication batch prepared (ReplicationCardId: %v, TransactionId: %v, ChaosLeaseIds: %v)",
            replicationCardId,
            transaction->GetId(),
            chaosLeaseIds);
    }

    void HydraCommitReplicatedCommit(
        TTransaction* transaction,
        NChaosClient::NProto::TReqReplicatedCommit* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        DiscardAliveTransaction(replicationCardId, transaction->GetId(), false);
        for (const auto& protoPrerequisiteId : request->prerequisite_ids()) {
            auto chaosLeaseId = FromProto<TChaosLeaseId>(protoPrerequisiteId);
            DiscardAliveTransaction(chaosLeaseId, transaction->GetId(), false);
        }

        YT_LOG_DEBUG("Replication batch committed (ReplicationCardId: %v, TransactionId: %v)",
            replicationCardId,
            transaction->GetId());
    }

    void HydraAbortReplicatedCommit(
        TTransaction* transaction,
        NChaosClient::NProto::TReqReplicatedCommit* request,
        const TTransactionAbortOptions& /*options*/)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        DiscardAliveTransaction(replicationCardId, transaction->GetId(), true);
        for (const auto& prerequisiteId : request->prerequisite_ids()) {
            auto chaosLeaseId = FromProto<TChaosLeaseId>(prerequisiteId);
            DiscardAliveTransaction(chaosLeaseId, transaction->GetId(), true);
        }

        YT_LOG_DEBUG("Replication batch aborted (ReplicationCardId: %v, TransactionId: %v)",
            replicationCardId,
            transaction->GetId());
    }

    void DiscardAliveTransaction(TChaosObjectId chaosObjectId, TTransactionId transactionId, bool isAbort)
    {
        auto it = Shortcuts_.find(chaosObjectId);
        if (it == Shortcuts_.end()) {
            YT_LOG_DEBUG("Trying to decrease transaction count for absent shortcut "
                "(ChaosObjectId: %v, Type: %v, TransactionId: %v)",
                chaosObjectId,
                TypeFromId(chaosObjectId),
                transactionId);
            YT_VERIFY(isAbort);
            return;
        }

        if (!it->second.AliveTransactions.contains(transactionId)) {
            YT_LOG_DEBUG("Trying to decrease transaction count for transaction absent shortcut "
                "(ChaosObjectId: %v, Type: %v, TransactionId: %v)",
                chaosObjectId,
                TypeFromId(chaosObjectId),
                transactionId);
            YT_VERIFY(isAbort);
            return;
        }

        it->second.AliveTransactions.erase(transactionId);

        if (it->second.AliveTransactions.empty() && it->second.State == EShortcutState::Revoking) {
            auto chaosCellId = it->second.CellId;
            auto era = it->second.Era;

            SendRevokeShortcutsResponse(chaosCellId, {{chaosObjectId, era}});
            EraseShortcut(chaosObjectId);

            YT_LOG_DEBUG("Shortcut revoked (ChaosObjectId: %v, Type: %v, Era: %v)",
                chaosObjectId,
                TypeFromId(chaosObjectId),
                era);
        }
    }

    void SendRevokeShortcutsResponse(
        TCellId chaosCellId,
        const std::vector<std::pair<TReplicationCardId, TReplicationEra>>& shortcuts)
    {
        NChaosNode::NProto::TRspRevokeShortcuts rsp;
        ToProto(rsp.mutable_coordinator_cell_id(), Slot_->GetCellId());

        for (const auto& [chaosObjectId, era] : shortcuts) {
            auto* shortcut = rsp.add_shortcuts();
            ToProto(shortcut->mutable_chaos_object_id(), chaosObjectId);
            shortcut->set_era(era);
        }

        const auto& hiveManager = Slot_->GetHiveManager();
        auto mailbox = hiveManager->GetMailbox(chaosCellId);
        hiveManager->PostMessage(mailbox, rsp);
    }

    void InsertShortcut(TChaosObjectId chaosObjectId, TShortcut shortcut)
    {
        Shortcuts_[chaosObjectId] = shortcut;
        SnapshotStore_->UpdateShortcut(chaosObjectId, {shortcut.Era});
    }

    void EraseShortcut(TChaosObjectId chaosObjectId)
    {
        Shortcuts_.erase(chaosObjectId);
        SnapshotStore_->RemoveShortcut(chaosObjectId);
    }


    TCompositeMapServicePtr CreateOrchidService()
    {
        return New<TCompositeMapService>()
            ->AddAttribute(EInternedAttributeKey::Opaque, BIND([] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer)
                        .Value(true);
                }))
            ->AddChild("internal", IYPathService::FromMethod(
                &TCoordinatorManager::BuildInternalOrchid,
                MakeWeak(this))
                ->Via(Slot_->GetAutomatonInvoker()))
            ->AddChild("shortcuts", TShortcutOrchidService::Create(MakeWeak(this), Slot_->GetGuardedAutomatonInvoker()));
    }

    void BuildInternalOrchid(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("suspended").Value(Suspended_)
            .EndMap();
    }

    void BuildChaosObjectOrchidYson(TShortcut* shortcut, IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("chaos_cell_id").Value(shortcut->CellId)
                .Item("era").Value(shortcut->Era)
                .Item("state").Value(shortcut->State)
                .Item("alive_transaction_ids")
                    .DoListFor(shortcut->AliveTransactions, [&] (TFluentList fluent, const auto& transactionId) {
                        fluent
                            .Item().Value(transactionId);
                    })
            .EndMap();
    }

    std::vector<TCellId> GetMetadataCellIds()
    {
        std::vector<TCellId> cells;
        const auto& chaosManager = Slot_->GetChaosManager();
        cells = chaosManager->CoordinatorCellIds();
        SortUnique(cells);
        return cells;
    }
};

////////////////////////////////////////////////////////////////////////////////

ICoordinatorManagerPtr CreateCoordinatorManager(
    TCoordinatorManagerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap)
{
    return New<TCoordinatorManager>(
        std::move(config),
        std::move(slot),
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
