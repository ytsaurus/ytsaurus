#include "coordinator_manager.h"

#include "automaton.h"
#include "chaos_slot.h"
#include "private.h"
#include "transaction.h"
#include "transaction_manager.h"

#include <yt/yt/server/node/chaos_node/chaos_manager.pb.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>
#include <yt/yt/server/lib/hive/helpers.h>

#include <yt/yt/server/lib/chaos_node/proto/chaos_manager.pb.h>

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
using namespace NHydra;
using namespace NChaosClient;
using namespace NTransactionClient;

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

        RegisterMethod(BIND(&TCoordinatorManager::HydraReqSuspend, Unretained(this)));
        RegisterMethod(BIND(&TCoordinatorManager::HydraReqResume, Unretained(this)));
        RegisterMethod(BIND(&TCoordinatorManager::HydraReqGrantShortcuts, Unretained(this)));
        RegisterMethod(BIND(&TCoordinatorManager::HydraReqRevokeShortcuts, Unretained(this)));
    }

    IYPathServicePtr GetOrchidService() override
    {
        return OrchidService_;
    }

    void SuspendCoordinator(TSuspendCoordinatorContextPtr context) override
    {
        auto mutation = CreateMutation(HydraManager_, context);
        mutation->SetAllowLeaderForwarding(true);
        mutation->CommitAndReply(context);
    }

    void ResumeCoordinator(TResumeCoordinatorContextPtr context) override
    {
        auto mutation = CreateMutation(HydraManager_, context);
        mutation->SetAllowLeaderForwarding(true);
        mutation->CommitAndReply(context);
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

        DECLARE_NEW_FRIEND();
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

        for (auto cellId : GetMetadataCellIds()) {
            NChaosNode::NProto::TReqSuspendCoordinator req;
            ToProto(req.mutable_coordinator_cell_id(), Slot_->GetCellId());

            const auto& hiveManager = Slot_->GetHiveManager();
            auto* mailbox = hiveManager->GetMailbox(cellId);
            hiveManager->PostMessage(mailbox, req);
        }

        Suspended_ = true;
    }

    void HydraReqResume(NChaosClient::NProto::TReqResumeCoordinator* /*request*/)
    {
        if (!Suspended_) {
            return;
        }

        for (auto cellId : GetMetadataCellIds()) {
            NChaosNode::NProto::TReqResumeCoordinator req;
            ToProto(req.mutable_coordinator_cell_id(), Slot_->GetCellId());

            const auto& hiveManager = Slot_->GetHiveManager();
            auto* mailbox = hiveManager->GetMailbox(cellId);
            hiveManager->PostMessage(mailbox, req);
        }

        Suspended_ = false;
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
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Granting shortcut while shortcut already present (ChaosCellId: %v, ReplicationCardId: %v, OldEra: %v, OldState: %v",
                    chaosCellId,
                    replicationCardId,
                    it->second.Era,
                    it->second.State);

                YT_VERIFY(it->second.AliveTransactions.empty());
            }

            Shortcuts_[replicationCardId] = TShortcut{chaosCellId, era, EShortcutState::Granted, {}};
            grantedShortcuts.emplace_back(replicationCardId, era);

            auto* rspShortcut = rsp.add_shortcuts();
            ToProto(rspShortcut->mutable_replication_card_id(), replicationCardId);
            rspShortcut->set_era(era);
        }

        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(chaosCellId);
        hiveManager->PostMessage(mailbox, rsp);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Shortcuts granted (Shortcuts: %v)",
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
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Revoking unknown shortcut (ChaosCellId: %v, ReplicationCardId: %v, Era: %v)",
                    chaosCellId,
                    replicationCardId,
                    era);
                continue;
            }

            auto& shortcut = Shortcuts_[replicationCardId];

            if (shortcut.Era != era) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Revoking shortcut with invalid era (ChaosCellId: %v, ReplicationCardId: %v, ShortcutEra: %v, RequestedEra: %v)",
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
                 Shortcuts_.erase(replicationCardId);
            }
        }

        if (!inactiveShortcuts.empty()) {
            NChaosNode::NProto::TRspRevokeShortcuts rsp;
            ToProto(rsp.mutable_coordinator_cell_id(), Slot_->GetCellId());

            for (const auto& [replicationCardId, era] : inactiveShortcuts) {
                auto* shortcut = rsp.add_shortcuts();
                ToProto(shortcut->mutable_replication_card_id(), replicationCardId);
                shortcut->set_era(era);
            }

            const auto& hiveManager = Slot_->GetHiveManager();
            auto* mailbox = hiveManager->GetMailbox(chaosCellId);
            hiveManager->PostMessage(mailbox, rsp);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Shortcuts revoked (Shortcuts: %v, Inactive: %v)",
            MakeFormattableView(revokedShortcuts, [] (auto* builder, const auto& shortcut) {
                builder->AppendFormat("<%v, %v>", shortcut.first, shortcut.second);
            }),
            MakeFormattableView(inactiveShortcuts, [] (auto* builder, const auto& shortcut) {
                builder->AppendFormat("<%v, %v>", shortcut.first, shortcut.second);
            }));
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
        for (const auto& shortcut : Shortcuts_) {
            cells.push_back(shortcut.second.CellId);
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
