#include "chaos_lease_manager.h"

#include "automaton.h"
#include "bootstrap.h"
#include "chaos_manager.h"
#include "chaos_lease.h"
#include "chaos_slot.h"

#include <yt/server/node/chaos_node/chaos_manager.pb.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/server/lib/hive/helpers.h>
#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/client/chaos_client/helpers.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NChaosNode {

using namespace NYTree;
using namespace NHydra;
using namespace NObjectClient;
using namespace NChaosClient;
using namespace NTransactionSupervisor;
using namespace NYson;
using namespace NConcurrency;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

TError CreateForbiddenStateTransitionError(
    EChaosLeaseManagerState currentState,
    EChaosLeaseManagerState nextState,
    EChaosLeaseManagerState expectedCurrentState)
{
    return TError("Chaos lease state transition is forbidden")
        << TErrorAttribute("current_state", currentState)
        << TErrorAttribute("next_state", nextState)
        << TErrorAttribute("expected_current_state", expectedCurrentState);
}

TError CreateChaosLeaseNotKnownError(TChaosLeaseId chaosLeaseId)
{
    return TError(NYTree::EErrorCode::ResolveError, "No such chaos lease")
            << TErrorAttribute("chaos_lease_id", chaosLeaseId);
}

TError CreateChaosCellIsNotEnabledError(EChaosLeaseManagerState state)
{
    return TError(
        NChaosClient::EErrorCode::ChaosCellIsNotEnabled,
        "Chaos cell is not enabled, retry later")
        << TErrorAttribute("state", state);
}

[[noreturn]] void ThrowForbiddenStateTransition(
    EChaosLeaseManagerState currentState,
    EChaosLeaseManagerState nextState,
    EChaosLeaseManagerState expectedCurrentState)
{
    THROW_ERROR(CreateForbiddenStateTransitionError(currentState, nextState, expectedCurrentState));
}

[[noreturn]] void ThrowChaosLeaseNotKnown(TChaosLeaseId chaosLeaseId)
{
    THROW_ERROR(CreateChaosLeaseNotKnownError(chaosLeaseId));
}

[[noreturn]] void ThrowCellIsNotEnabled(EChaosLeaseManagerState state)
{
    THROW_ERROR(CreateChaosCellIsNotEnabledError(state));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TChaosLeaseManager
    : public IChaosLeaseManager
    , public TChaosAutomatonPart
{
public:
    TChaosLeaseManager(
        TChaosLeaseManagerConfigPtr config,
        IChaosSlotPtr slot,
        IBootstrap* bootstrap)
        : TChaosAutomatonPart(
            slot,
            bootstrap)
        , Config_(std::move(config))
        , OrchidService_(CreateOrchidService())
        , ChaosLeaseTracker_(CreateTransactionLeaseTracker(
            Bootstrap_->GetTransactionLeaseTrackerThreadPool(),
            Logger))
        , State_(IsEvenCellTag()
            ? EChaosLeaseManagerState::Enabled
            : EChaosLeaseManagerState::Disabled)
    {
        YT_ASSERT_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "ChaosLeaseManager.Keys",
            BIND_NO_PROPAGATE(&TChaosLeaseManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "ChaosLeaseManager.Values",
            BIND_NO_PROPAGATE(&TChaosLeaseManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "ChaosLeaseManager.Keys",
            BIND_NO_PROPAGATE(&TChaosLeaseManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ChaosLeaseManager.Values",
            BIND_NO_PROPAGATE(&TChaosLeaseManager::SaveValues, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TChaosLeaseManager::HydraCreateChaosLease, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosLeaseManager::HydraRemoveChaosLease, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosLeaseManager::HydraChaosNodeSetState, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosLeaseManager::HydraChaosNodeMigrateChaosLeases, Unretained(this)));

    }

    void Initialize() override
    { }

    IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }

    ITransactionLeaseTrackerPtr GetChaosLeaseTracker() const override
    {
        return ChaosLeaseTracker_;
    }

    EChaosLeaseManagerState GetState() const override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        return State_;
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(ChaosLease, TChaosLease);

    void CreateChaosLease(const TCtxCreateChaosLeasePtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosLeaseManager::HydraCreateChaosLease,
            this);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void RemoveChaosLease(const TCtxRemoveChaosLeasePtr& context) override
    {
        auto mutation = CreateMutation(
            HydraManager_,
            context,
            &TChaosLeaseManager::HydraRemoveChaosLease,
            this);

        auto chaosLeaseId = FromProto<TChaosLeaseId>(context->Request().chaos_lease_id());

        auto removeFuture = mutation->Commit().AsVoid().Apply(BIND([this, this_ = MakeStrong(this), chaosLeaseId] {
                auto* chaosLease = FindChaosLease(chaosLeaseId);
                if (!chaosLease) {
                    return OKFuture;
                }

                return chaosLease->RemovePromise().ToFuture();
            })
            .AsyncVia(Slot_->GetEpochAutomatonInvoker()));

        context->ReplyFrom(removeFuture);
    }

    TFuture<void> PingChaosLease(TChaosLeaseId chaosLeaseId, bool pingAncestors) override
    {
        return ChaosLeaseTracker_->PingTransaction(chaosLeaseId, pingAncestors);
    }

    TChaosLease* GetChaosLeaseOrThrow(TChaosLeaseId chaosLeaseId) const override
    {
        ValidateEnabledState();

        auto* chaosLease = FindChaosLease(chaosLeaseId);
        if (!chaosLease) {
            ThrowChaosLeaseNotKnown(chaosLeaseId);
        }

        return chaosLease;
    }

    void InsertChaosLease(std::unique_ptr<TChaosLease> chaosLeaseHolder) override
    {
        auto chaosLeaseId = chaosLeaseHolder->GetId();
        auto* chaosLease = ChaosLeaseMap_.Insert(chaosLeaseId, std::move(chaosLeaseHolder));
        ChaosLeaseTracker_->RegisterTransaction(
            chaosLeaseId,
            chaosLease->GetParentId(),
            chaosLease->GetTimeout(),
            std::nullopt,
            BIND(&TChaosLeaseManager::OnLeaseExpired, MakeWeak(this))
                .Via(Slot_->GetEpochAutomatonInvoker()));
    }

    void HydraChaosNodeSetState(NChaosNode::NProto::TReqSetState* request)
    {
        auto nextState = FromProto<EChaosLeaseManagerState>(request->next_state());
        auto expectedCurrentState = FromProto<EChaosLeaseManagerState>(request->expected_current_state());
        try {
            MakeStateTransition(expectedCurrentState, nextState);
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Unexpected state transition");
        }
    }

    void HydraChaosNodeMigrateChaosLeases(NChaosNode::NProto::TReqMigrateChaosLeases* request)
    {
        std::vector<TChaosLeaseId> createdLeaseIds;
        std::vector<TChaosLeaseId> nestedLeaseIds;
        for (const auto& protoChaosLease : request->chaos_leases()) {
            auto chaosLeaseId = FromProto<TChaosLeaseId>(protoChaosLease.chaos_lease_id());
            createdLeaseIds.push_back(chaosLeaseId);
            nestedLeaseIds.push_back(chaosLeaseId);
            auto* chaosLease = FindChaosLease(chaosLeaseId);
            if (!chaosLease) {
                auto chaosLeaseHolder = std::make_unique<TChaosLease>(chaosLeaseId);
                chaosLease = ChaosLeaseMap_.Insert(chaosLeaseId, std::move(chaosLeaseHolder));
                YT_LOG_DEBUG("Chaos lease created for immigration (ChaosLeaseId: %v)",
                    chaosLeaseId);
            }

            YT_VERIFY(chaosLease->Coordinators().empty());
            chaosLease->SetParentId(FromProto<TChaosLeaseId>(protoChaosLease.parent_chaos_lease_id()));
            chaosLease->SetRootId(FromProto<TChaosLeaseId>(protoChaosLease.root_chaos_lease_id()));
            chaosLease->SetTimeout(FromProto<TDuration>(protoChaosLease.timeout()));
            for (auto protoNestedChaosLeaseId : protoChaosLease.nested_chaos_lease_ids()) {
                auto nestedLeaseId = FromProto<TChaosLeaseId>(protoNestedChaosLeaseId);
                chaosLease->NestedLeaseIds().push_back(nestedLeaseId);
                nestedLeaseIds.push_back(nestedLeaseId);
            }

            chaosLease->SetState(EChaosLeaseState::Normal);
            ChaosLeaseTracker_->RegisterTransaction(
                chaosLeaseId,
                chaosLease->GetParentId(),
                chaosLease->GetTimeout(),
                std::nullopt,
                BIND(&TChaosLeaseManager::OnLeaseExpired, MakeWeak(this))
                    .Via(Slot_->GetEpochAutomatonInvoker()));

            YT_LOG_DEBUG("Chaos lease migrated (ChaosLeaseId: %v)",
                chaosLeaseId);
        }

        std::sort(createdLeaseIds.begin(), createdLeaseIds.end());
        std::sort(nestedLeaseIds.begin(), nestedLeaseIds.end());
        YT_VERIFY(std::includes(createdLeaseIds.begin(), createdLeaseIds.end(), nestedLeaseIds.begin(), nestedLeaseIds.end()));
    }

private:
    class TChaosLeaseOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TChaosLeaseManager> impl, IInvokerPtr invoker)
        {
            return New<TChaosLeaseOrchidService>(std::move(impl))
                ->Via(invoker);
        }

        std::vector<std::string> GetKeys(i64 limit) const override
        {
            std::vector<std::string> keys;
            if (auto owner = Owner_.Lock()) {
                for (const auto& [chaosLeaseId, _] : owner->ChaosLeases()) {
                    if (std::ssize(keys) >= limit) {
                        break;
                    }
                    keys.push_back(ToString(chaosLeaseId));
                }
            }
            return keys;
        }

        i64 GetSize() const override
        {
            if (auto owner = Owner_.Lock()) {
                return owner->ChaosLeases().size();
            }
            return 0;
        }

        IYPathServicePtr FindItemService(const std::string& key) const override
        {
            if (auto owner = Owner_.Lock()) {
                if (auto* chaosLease = owner->FindChaosLease(TChaosLeaseId::FromString(key))) {
                    auto producer = BIND(&TChaosLeaseManager::BuildChaosLeaseOrchidYson, owner, chaosLease);
                    return ConvertToNode(producer);
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<TChaosLeaseManager> Owner_;

        explicit TChaosLeaseOrchidService(TWeakPtr<TChaosLeaseManager> impl)
            : Owner_(std::move(impl))
        { }

        DECLARE_NEW_FRIEND()
    };

    const TChaosLeaseManagerConfigPtr Config_;
    const IYPathServicePtr OrchidService_;

    const ITransactionLeaseTrackerPtr ChaosLeaseTracker_;

    TEntityMap<TChaosLease> ChaosLeaseMap_;
    EChaosLeaseManagerState State_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void SaveKeys(TSaveContext& context) const
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        ChaosLeaseMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        using NYT::Save;

        ChaosLeaseMap_.SaveValues(context);
        Save(context, State_);
    }

    void LoadKeys(TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (context.GetVersion() >= EChaosReign::IntroduceChaosLeaseManager) {
            ChaosLeaseMap_.LoadKeys(context);
        }
    }

    void LoadValues(TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;

        if (context.GetVersion() >= EChaosReign::IntroduceChaosLeaseManager) {
            ChaosLeaseMap_.LoadValues(context);
            Load(context, State_);
        }
    }

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::Clear();

        ChaosLeaseTracker_->Stop();
        ChaosLeaseMap_.Clear();
    }


    void OnLeaderActive() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnLeaderActive();

        ChaosLeaseTracker_->Start();

        // Recreate chaos leases.
        for (const auto& [chaosLeaseId, chaosLease] : ChaosLeaseMap_) {
            ChaosLeaseTracker_->RegisterTransaction(
                chaosLeaseId,
                chaosLease->GetParentId(),
                chaosLease->GetTimeout(),
                std::nullopt,
                BIND(&TChaosLeaseManager::OnLeaseExpired, MakeWeak(this))
                    .Via(Slot_->GetEpochAutomatonInvoker()));
        }
    }

    void OnStopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnStopLeading();

        ChaosLeaseTracker_->Stop();

        for (const auto& [chaosLeaseId, chaosLease] : ChaosLeaseMap_) {
            if (chaosLease->RemovePromise()) {
                auto error = TError(NRpc::EErrorCode::Unavailable, "Hydra peer has stopped");
                chaosLease->RemovePromise().TrySet(error);
                chaosLease->RemovePromise() = NewPromise<void>();
            }
        }
    }

    void DoRemoveChaosLease(TChaosLeaseId chaosLeaseId)
    {
        ChaosLeaseTracker_->UnregisterTransaction(chaosLeaseId);

        auto chaosLeaseHolder = ChaosLeaseMap_.Release(chaosLeaseId);
        chaosLeaseHolder->RemovePromise().Set();
    }

    void HandleChaosLeaseStateTransition(TChaosLease* chaosLease) override
    {
        if (chaosLease->GetState() == EChaosLeaseState::RevokingShortcutsForRemoval && chaosLease->Coordinators().empty()) {
            YT_LOG_DEBUG("Chaos lease removed after revoking all shortcuts (ChaosObjectId: %v, Type: %v)",
                chaosLease->GetId(),
                TypeFromId(chaosLease->GetId()));

            DoRemoveChaosLease(chaosLease->GetId());
        }
    }

    void TraverseLeaseSubtree(TChaosLease* chaosLease, std::vector<TChaosLease*>* traversedLeases)
    {
        traversedLeases->push_back(chaosLease);
        for (int index = std::ssize(*traversedLeases) - 1; index < std::ssize(*traversedLeases); ++index) {
            auto* chaosLease = (*traversedLeases)[index];

            for (auto nestedChaosLeaseId : chaosLease->NestedLeaseIds()) {
                auto* nestedChaosLease = GetChaosLease(nestedChaosLeaseId);

                traversedLeases->push_back(nestedChaosLease);
            }
        }
    }

    void MigrateAllChaosLeases()
    {
        THashSet<TChaosLeaseId> seenRoots;
        std::vector<TChaosLease*> currentBatch;

        for (auto* chaosLease : GetValuesSortedByKey(ChaosLeaseMap_)) {
            if (chaosLease->IsRoot()) {
                InsertOrCrash(seenRoots, chaosLease->GetId());
                TraverseLeaseSubtree(chaosLease, &currentBatch);

                if (std::ssize(currentBatch) >= Config_->MaxMigrationBatchSize) {
                    MigrateChaosLeases(std::exchange(currentBatch, {}));
                }
            }
        }

        if (!currentBatch.empty()) {
            MigrateChaosLeases(std::move(currentBatch));
        }
    }

    void MigrateChaosLeases(const std::vector<TChaosLease*>& chaosLeases)
    {
        YT_VERIFY(HasMutationContext());

        NChaosNode::NProto::TReqMigrateChaosLeases req;

        for (const auto* chaosLease : chaosLeases) {
            YT_LOG_DEBUG("Migrating chaos lease to the sibling cell (ChaosLeaseId: %v)",
                chaosLease->GetId());

            auto protoChaosLease = req.add_chaos_leases();
            ToProto(protoChaosLease->mutable_chaos_lease_id(), chaosLease->GetId());
            protoChaosLease->set_timeout(ToProto(chaosLease->GetTimeout()));
            ToProto(protoChaosLease->mutable_root_chaos_lease_id(), chaosLease->GetRootId());

            for (auto nestedLeaseId : chaosLease->NestedLeaseIds()) {
                ToProto(protoChaosLease->add_nested_chaos_lease_ids(), nestedLeaseId);
            }
        }

        const auto& hiveManager = Slot_->GetHiveManager();
        auto mailbox = hiveManager->GetOrCreateCellMailbox(GetKnownSiblingCellIdOrThrow());
        hiveManager->PostMessage(mailbox, req);
    }


    void MakeStateTransition(EChaosLeaseManagerState expectedCurrentState, EChaosLeaseManagerState nextState) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (State_ != expectedCurrentState) {
            ThrowForbiddenStateTransition(State_, nextState, expectedCurrentState);
        }

        auto siblingCellId = GetKnownSiblingCellIdOrThrow();

        YT_LOG_INFO("Changing chaos lease manager state (CurrentState: %v, NextState: %v)",
            State_,
            nextState);

        switch (State_) {
            case EChaosLeaseManagerState::Enabling:
                YT_VERIFY(nextState == EChaosLeaseManagerState::Enabled);

                ChaosLeaseTracker_->Start();
                break;

            case EChaosLeaseManagerState::Disabling: {
                YT_VERIFY(nextState == EChaosLeaseManagerState::Disabled);

                NChaosNode::NProto::TReqSetState req;
                req.set_next_state(ToProto(EChaosLeaseManagerState::Enabled));
                req.set_expected_current_state(ToProto(EChaosLeaseManagerState::Enabling));
                const auto& hiveManager = Slot_->GetHiveManager();
                auto mailbox = hiveManager->GetOrCreateCellMailbox(siblingCellId);
                hiveManager->PostMessage(mailbox, req);

                Clear();
                break;
            }

            case EChaosLeaseManagerState::Enabled: {
                YT_VERIFY(nextState == EChaosLeaseManagerState::Disabling);

                ChaosLeaseTracker_->Stop();

                NChaosNode::NProto::TReqSetState req;
                req.set_next_state(ToProto(EChaosLeaseManagerState::Enabling));
                req.set_expected_current_state(ToProto(EChaosLeaseManagerState::Disabled));
                const auto& hiveManager = Slot_->GetHiveManager();
                auto mailbox = hiveManager->GetOrCreateCellMailbox(siblingCellId);
                hiveManager->PostMessage(mailbox, req);

                MigrateAllChaosLeases();

                State_ = nextState;
                MakeStateTransition(EChaosLeaseManagerState::Disabling, EChaosLeaseManagerState::Disabled);
                return;
            }

            case EChaosLeaseManagerState::Disabled:
                YT_VERIFY(nextState == EChaosLeaseManagerState::Enabling);
                break;
        }

        State_ = nextState;
    }

    void HydraCreateChaosLease(
        const TCtxCreateChaosLeasePtr& context,
        NChaosClient::NProto::TReqCreateChaosLease* request,
        NChaosClient::NProto::TRspCreateChaosLease* response)
    {
        ValidateEnabledState();

        auto chaosLeaseId = GenerateNewChaosLeaseId();
        auto timeout = FromProto<TDuration>(request->timeout());
        auto parentId = FromProto<TChaosLeaseId>(request->parent_id());

        auto chaosLeaseHolder = std::make_unique<TChaosLease>(chaosLeaseId);
        chaosLeaseHolder->SetState(EChaosLeaseState::Normal);
        if (parentId) {
            auto parent = GetChaosLeaseOrThrow(parentId);

            if (parent->GetState() == EChaosLeaseState::RevokingShortcutsForRemoval) {
                THROW_ERROR_EXCEPTION("Failed to create chaos lease since its parent is being removed")
                    << TErrorAttribute("parent_chaos_lease_id", parentId);
            }

            chaosLeaseHolder->SetParentId(parentId);
            chaosLeaseHolder->SetRootId(parent->GetRootId() ? parent->GetRootId() : parentId);
            parent->NestedLeaseIds().push_back(chaosLeaseId);
        }

        chaosLeaseHolder->SetTimeout(timeout);
        auto* chaosLease = ChaosLeaseMap_.Insert(chaosLeaseId, std::move(chaosLeaseHolder));

        ChaosLeaseTracker_->RegisterTransaction(
            chaosLeaseId,
            parentId,
            timeout,
            std::nullopt,
            BIND(&TChaosLeaseManager::OnLeaseExpired, MakeWeak(this))
                .Via(Slot_->GetEpochAutomatonInvoker()));

        YT_LOG_DEBUG("Created chaos lease (LeaseId: %v)",
            chaosLeaseId);

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->GrantShortcuts(chaosLease, chaosManager->CoordinatorCellIds());

        ToProto(response->mutable_chaos_lease_id(), chaosLeaseId);

        if (context) {
            context->SetResponseInfo("ChaosLeaseId: %v",
                chaosLeaseId);
        }
    }

    void OnLeaseExpired(TChaosLeaseId chaosLeaseId)
    {
        NChaosClient::NProto::TReqRemoveChaosLease request;
        ToProto(request.mutable_chaos_lease_id(), chaosLeaseId);
        auto mutation = CreateMutation(HydraManager_, request);

        YT_UNUSED_FUTURE(mutation->Commit());
    }

    void HydraRemoveChaosLease(
        const TCtxRemoveChaosLeasePtr& /*context*/,
        NChaosClient::NProto::TReqRemoveChaosLease* request,
        NChaosClient::NProto::TRspRemoveChaosLease* /*response*/)
    {
        ValidateEnabledState();

        auto rootChaosLeaseId = FromProto<TChaosLeaseId>(request->chaos_lease_id());
        auto* rootChaosLease = GetChaosLeaseOrThrow(rootChaosLeaseId);

        if (rootChaosLease->GetState() == EChaosLeaseState::RevokingShortcutsForRemoval) {
            return;
        }

        std::vector<TChaosLease*> chaosLeases;
        TraverseLeaseSubtree(rootChaosLease, &chaosLeases);
        for (auto* chaosLease : chaosLeases) {
            chaosLease->RemovePromise() = NewPromise<void>();
            chaosLease->SetState(EChaosLeaseState::RevokingShortcutsForRemoval);
        }

        const auto& chaosManager = Slot_->GetChaosManager();
        chaosManager->RevokeShortcuts(TRange<TChaosObjectBase*>(reinterpret_cast<TChaosObjectBase**>(chaosLeases.data()), chaosLeases.size()));

        // NB: Chaos leases with active shortcuts will receive a hive message from the coordinators about
        //     revocation, but for leases without coordinators HandleChaosLeaseStateTransition will never be triggered.
        for (auto* chaosLease : chaosLeases) {
            if (chaosLease->Coordinators().empty()) {
                HandleChaosLeaseStateTransition(chaosLease);
            }
        }
    }

    TChaosLeaseId GenerateNewChaosLeaseId()
    {
        return MakeChaosLeaseId(Slot_->GenerateId(EObjectType::ChaosLease));
    }

    TCompositeMapServicePtr CreateOrchidService()
    {
        return New<TCompositeMapService>()
            ->AddChild("internal", IYPathService::FromMethod(
                &TChaosLeaseManager::BuildInternalOrchid,
                MakeWeak(this))
                ->Via(Slot_->GetAutomatonInvoker()))
            ->AddChild("chaos_leases", TChaosLeaseOrchidService::Create(
                MakeWeak(this),
                Slot_->GetGuardedAutomatonInvoker()));
    }

    void BuildInternalOrchid(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("state").Value(CamelCaseToUnderscoreCase(ToString(State_)))
            .EndMap();
    }

    TCellId GetKnownSiblingCellIdOrThrow()
    {
        const auto& chaosManager = Slot_->GetChaosManager();
        auto siblingChaosCellTag = GetSiblingChaosCellTag(CellTagFromId(Slot_->GetCellId()));
        for (const auto& cellId : chaosManager->CoordinatorCellIds()) {
            if (CellTagFromId(cellId) == siblingChaosCellTag) {
                return cellId;
            }
        }

        THROW_ERROR_EXCEPTION("Sibling cell %v is not found",
            siblingChaosCellTag);
    }

    void ValidateEnabledState() const
    {
        if (State_ != EChaosLeaseManagerState::Enabled) {
            ThrowCellIsNotEnabled(State_);
        }
    }

    bool IsEvenCellTag() const
    {
        return CellTagFromId(Slot_->GetCellId()).Underlying() % 2 == 0;
    }

    void BuildChaosLeaseOrchidYson(TChaosLease* chaosLease, IYsonConsumer* consumer)
    {
        auto lastPingTime = NConcurrency::WaitFor(ChaosLeaseTracker_->GetLastPingTime(chaosLease->GetId()))
            .ValueOrThrow();

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("chaos_lease_id").Value(chaosLease->GetId())
                .Item("state").Value(chaosLease->GetState())
                .Item("coordinators").DoMapFor(chaosLease->Coordinators(), [] (TFluentMap fluent, const auto& pair) {
                    fluent
                        .Item(ToString(pair.first)).Value(pair.second.State);
                })
                .Item("timeout").Value(chaosLease->GetTimeout())
                .Item("parent_id").Value(chaosLease->GetParentId())
                .Item("root_id").Value(chaosLease->GetRootId())
                .Item("last_ping_time").Value(lastPingTime)
                .Item("nested_lease_ids").Value(chaosLease->NestedLeaseIds())
            .EndMap();
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TChaosLeaseManager, ChaosLease, TChaosLease, ChaosLeaseMap_);

////////////////////////////////////////////////////////////////////////////////

IChaosLeaseManagerPtr CreateChaosLeaseManager(
    TChaosLeaseManagerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap)
{
    return New<TChaosLeaseManager>(
        std::move(config),
        slot,
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
