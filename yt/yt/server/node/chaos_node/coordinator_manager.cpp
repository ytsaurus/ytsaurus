#include "coordinator_manager.h"

#include "automaton.h"
#include "chaos_slot.h"
#include "chaos_manager.h"
#include "private.h"
#include "shortcut_snapshot_store.h"
#include "transaction.h"
#include "transaction_manager.h"

#include <yt/yt/server/node/chaos_node/chaos_manager.pb.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>
#include <yt/yt/server/lib/hive/helpers.h>

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
using namespace NChaosClient;
using namespace NTransactionClient;
using namespace NTransactionSupervisor;

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
        VERIFY_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "CoordinatorManager.Values",
            BIND(&TCoordinatorManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "CoordinatorManager.Values",
            BIND(&TCoordinatorManager::SaveValues, Unretained(this)));

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

        std::vector<TString> GetKeys(i64 limit) const override
        {
            std::vector<TString> keys;
            if (auto owner = Owner_.Lock()) {
                for (const auto& [replicationCardId, shortcut] : owner->Shortcuts_) {
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
                return owner->Shortcuts_.size();
            }
            return 0;
        }

        IYPathServicePtr FindItemService(TStringBuf key) const override
        {
            if (auto owner = Owner_.Lock()) {
                if (auto shortcut = owner->FindShortcut(TReplicationCardId::FromString(key))) {
                    auto producer = BIND(&TCoordinatorManager::BuildReplicationCardOrchidYson, owner, shortcut);
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

    using TShortcutMap = THashMap<NChaosClient::TReplicationCardId, TShortcut>;
    TShortcutMap Shortcuts_;
    bool Suspended_ = false;

    const IYPathServicePtr OrchidService_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveValues(TSaveContext& context) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Save;
        Save(context, Shortcuts_);
        Save(context, Suspended_);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;
        Load(context, Shortcuts_);
        Load(context, Suspended_);
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::Clear();

        Shortcuts_.clear();
        Suspended_ = false;
        SnapshotStore_->Clear();
    }

    void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnAfterSnapshotLoaded();

        for (const auto& [replicationCardId, shortcut] : Shortcuts_) {
            SnapshotStore_->UpdateShortcut(replicationCardId, {shortcut.Era});
        }
    }

    TShortcut* FindShortcut(TReplicationCardId replicationCardId)
    {
        auto it = Shortcuts_.find(replicationCardId);
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
            if (auto* mailbox = hiveManager->FindCellMailbox(cellId)) {
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
            if (auto* mailbox = hiveManager->FindCellMailbox(cellId)) {
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
        std::vector<std::pair<TReplicationCardId, TReplicationEra>> grantedShortcuts;

        NChaosNode::NProto::TRspGrantShortcuts rsp;
        ToProto(rsp.mutable_coordinator_cell_id(), Slot_->GetCellId());
        rsp.set_suspended(Suspended_);

        for (const auto& protoShortcut : request->shortcuts()) {
            auto replicationCardId = FromProto<NChaosClient::TReplicationCardId>(protoShortcut.replication_card_id());
            auto era = protoShortcut.era();

            if (auto it = Shortcuts_.find(replicationCardId)) {
                YT_LOG_ALERT("Granting shortcut while shortcut already present (ChaosCellId: %v, ReplicationCardId: %v, OldEra: %v, OldState: %v)",
                    chaosCellId,
                    replicationCardId,
                    it->second.Era,
                    it->second.State);

                YT_VERIFY(it->second.AliveTransactions.empty());
            }

            InsertShortcut(replicationCardId, {chaosCellId, era, EShortcutState::Granted, {}});
            grantedShortcuts.emplace_back(replicationCardId, era);

            auto* rspShortcut = rsp.add_shortcuts();
            ToProto(rspShortcut->mutable_replication_card_id(), replicationCardId);
            rspShortcut->set_era(era);
        }

        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = hiveManager->GetCellMailbox(chaosCellId);
        hiveManager->PostMessage(mailbox, rsp);

        YT_LOG_DEBUG("Shortcuts granted (Shortcuts: %v)",
            MakeFormattableView(grantedShortcuts, [] (auto* builder, const auto& grantedShortcut) {
                builder->AppendFormat("<%v, %v>", grantedShortcut.first, grantedShortcut.second);
            }));
    }

    void HydraReqRevokeShortcuts(NChaosNode::NProto::TReqRevokeShortcuts* request)
    {
        auto chaosCellId = FromProto<TCellId>(request->chaos_cell_id());
        std::vector<std::pair<TReplicationCardId, TReplicationEra>> revokedShortcuts;
        std::vector<std::pair<TReplicationCardId, TReplicationEra>> inactiveShortcuts;

        for (auto protoShortcut : request->shortcuts()) {
            auto replicationCardId = FromProto<NChaosClient::TReplicationCardId>(protoShortcut.replication_card_id());
            auto era = protoShortcut.era();

            if (!Shortcuts_.contains(replicationCardId)) {
                YT_LOG_ALERT("Revoking unknown shortcut (ChaosCellId: %v, ReplicationCardId: %v, Era: %v)",
                    chaosCellId,
                    replicationCardId,
                    era);
                continue;
            }

            auto& shortcut = Shortcuts_[replicationCardId];

            if (shortcut.Era != era) {
                YT_LOG_ALERT("Revoking shortcut with invalid era (ChaosCellId: %v, ReplicationCardId: %v, ShortcutEra: %v, RequestedEra: %v)",
                    chaosCellId,
                    replicationCardId,
                    shortcut.Era,
                    era);
            }

            revokedShortcuts.emplace_back(replicationCardId, shortcut.Era);

            if (!shortcut.AliveTransactions.empty()) {
                shortcut.State = EShortcutState::Revoking;
            } else {
                inactiveShortcuts.emplace_back(replicationCardId, shortcut.Era);
                EraseShortcut(replicationCardId);
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

        YT_LOG_DEBUG("Replication batch prepared (ReplicationCardId: %v, TransactionId: %v)",
            replicationCardId,
            transaction->GetId());
    }

    void HydraCommitReplicatedCommit(
        TTransaction* transaction,
        NChaosClient::NProto::TReqReplicatedCommit* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());
        DiscardAliveTransaction(replicationCardId, transaction->GetId(), false);

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

        YT_LOG_DEBUG("Replication batch aborted (ReplicationCardId: %v, TransactionId: %v)",
            replicationCardId,
            transaction->GetId());
    }

    void DiscardAliveTransaction(TReplicationCardId replicationCardId, TTransactionId transactionId, bool isAbort)
    {
        auto it = Shortcuts_.find(replicationCardId);
        if (it == Shortcuts_.end()) {
            YT_LOG_DEBUG("Trying to decrease transaction count for absent shortcut (ReplicationCardId: %v, TransactionId: %v)",
                replicationCardId,
                transactionId);
            YT_VERIFY(isAbort);
            return;
        }

        if (!it->second.AliveTransactions.contains(transactionId)) {
            YT_LOG_DEBUG("Trying to decrease transaction count for transaction absent shortcut (ReplicationCardId: %v, TransactionId: %v)",
                replicationCardId,
                transactionId);
            YT_VERIFY(isAbort);
            return;
        }

        it->second.AliveTransactions.erase(transactionId);

        if (it->second.AliveTransactions.empty() && it->second.State == EShortcutState::Revoking) {
            auto chaosCellId = it->second.CellId;
            auto era = it->second.Era;

            SendRevokeShortcutsResponse(chaosCellId, {{replicationCardId, era}});
            EraseShortcut(replicationCardId);

            YT_LOG_DEBUG("Shortcut revoked (ReplicationCardId: %v, Era: %v)",
                replicationCardId,
                era);
        }
    }

    void SendRevokeShortcutsResponse(
        TCellId chaosCellId,
        const std::vector<std::pair<TReplicationCardId, TReplicationEra>>& shortcuts)
    {
        NChaosNode::NProto::TRspRevokeShortcuts rsp;
        ToProto(rsp.mutable_coordinator_cell_id(), Slot_->GetCellId());

        for (const auto& [replicationCardId, era] : shortcuts) {
            auto* shortcut = rsp.add_shortcuts();
            ToProto(shortcut->mutable_replication_card_id(), replicationCardId);
            shortcut->set_era(era);
        }

        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = hiveManager->GetCellMailbox(chaosCellId);
        hiveManager->PostMessage(mailbox, rsp);
    }

    void InsertShortcut(TReplicationCardId replicationCardId, TShortcut shortcut)
    {
        Shortcuts_[replicationCardId] = shortcut;
        SnapshotStore_->UpdateShortcut(replicationCardId, {shortcut.Era});
    }

    void EraseShortcut(TReplicationCardId replicationCardId)
    {
        Shortcuts_.erase(replicationCardId);
        SnapshotStore_->RemoveShortcut(replicationCardId);
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

    void BuildReplicationCardOrchidYson(TShortcut* shortcut, IYsonConsumer* consumer)
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

        // COMPAT(savrus)
        if (GetCurrentMutationContext()->Request().Reign < ToUnderlying(EChaosReign::RevokeFromSuspended)) {
            for (const auto& shortcut : Shortcuts_) {
                cells.push_back(shortcut.second.CellId);
            }
        } else {
            const auto& chaosManager = Slot_->GetChaosManager();
            cells = chaosManager->CoordinatorCellIds();
        }

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
