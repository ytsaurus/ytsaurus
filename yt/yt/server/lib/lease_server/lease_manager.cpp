#include "lease_manager.h"

#include "config.h"
#include "serialize.h"
#include "private.h"

#include <yt/yt/server/lib/lease_server/proto/lease_manager.pb.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/entity_map.h>
#include <yt/yt/server/lib/hydra/hydra_service.h>

#include <yt/yt/ytlib/lease_client/lease_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NLeaseServer {

using namespace NConcurrency;
using namespace NHiveServer;
using namespace NHydra;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TLeaseManager;

////////////////////////////////////////////////////////////////////////////////

class TLease final
    : public ILease
    , public NHydra::TEntityBase
    , public TRefTracked<TLease>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, PersistentRefCounter);
    DEFINE_BYVAL_RW_PROPERTY(int, TransientRefCounter);

    DEFINE_BYVAL_RW_PROPERTY(TCellId, OwnerCellId);

public:
    TLease(
        TLeaseId id,
        TLeaseManager* owner)
        : Id_(id)
        , Owner_(owner)
    { }

    // ILease implementation.

    TLeaseId GetId() const override
    {
        return Id_;
    }

    ELeaseState GetState() const override
    {
        return State_;
    }

    int RefPersistently(bool force) override;
    int UnrefPersistently() override;
    ILeaseGuardPtr GetPersistentLeaseGuard(bool force) override;
    ILeaseGuardPtr GetPersistentLeaseGuardOnLoad() override;

    int RefTransiently(bool force) override;
    int UnrefTransiently(int leaderAutomatonTerm) override;
    ILeaseGuardPtr GetTransientLeaseGuard(bool force) override;

    void SetState(ELeaseState newState)
    {
        YT_VERIFY(HasHydraContext());

        State_ = newState;
    }

    void Persist(const TStreamPersistenceContext& context)
    {
        using NYT::Persist;

        Persist(context, State_);
        Persist(context, PersistentRefCounter_);
    }

private:
    class TPersistentLeaseGuard
        : public ILeaseGuard
    {
    public:
        TPersistentLeaseGuard(
            ILease* lease,
            ILeaseManagerPtr leaseManager,
            bool force,
            bool onLoad)
            : LeaseManager_(std::move(leaseManager))
        {
            // NB: May throw, so we store Lease_
            // after successful ref.
            if (!onLoad) {
                lease->RefPersistently(force);
            }

            LeaseId_ = lease->GetId();
        }

        ~TPersistentLeaseGuard()
        {
            if (LeaseId_) {
                auto* lease = LeaseManager_->FindLease(LeaseId_);
                // Lease was forcefully revoked.
                if (!lease) {
                    return;
                }

                lease->UnrefPersistently();
                LeaseId_ = {};
            }
        }

        TLeaseId GetLeaseId() const override
        {
            return LeaseId_;
        }

        bool IsPersistent() const override
        {
            return true;
        }

        bool IsValid() const override
        {
            return LeaseManager_->FindLease(LeaseId_);
        }

    private:
        const ILeaseManagerPtr LeaseManager_;

        TLeaseId LeaseId_;
    };

    class TTransientLeaseGuard
        : public ILeaseGuard
    {
    public:
        TTransientLeaseGuard(
            ILease* lease,
            ILeaseManagerPtr leaseManager,
            bool force,
            int leaderAutomatonTerm)
            : LeaseManager_(std::move(leaseManager))
        {
            // NB: May throw, so we store Lease_
            // after successful ref.
            lease->RefTransiently(force);

            LeaseId_ = lease->GetId();
            LeaderAutomatonTerm_ = leaderAutomatonTerm;
        }

        ~TTransientLeaseGuard()
        {
            if (LeaseId_) {
                auto* lease = LeaseManager_->FindLease(LeaseId_);
                // Lease was forcefully revoked.
                if (!lease) {
                    return;
                }

                lease->UnrefTransiently(LeaderAutomatonTerm_);
                LeaseId_ = {};
            }
        }

        TLeaseId GetLeaseId() const override
        {
            return LeaseId_;
        }

        bool IsPersistent() const override
        {
            return false;
        }

        bool IsValid() const override
        {
            return
                LeaderAutomatonTerm_ == LeaseManager_->GetLeaderAutomatonTerm() &&
                LeaseManager_->FindLease(LeaseId_);
        }

    private:
        const ILeaseManagerPtr LeaseManager_;

        TLeaseId LeaseId_;
        int LeaderAutomatonTerm_;
    };

    const TLeaseId Id_;
    TWeakPtr<TLeaseManager> Owner_;

    ELeaseState State_ = ELeaseState::Unknown;
};

DECLARE_ENTITY_TYPE(TLease, TLeaseId, NObjectClient::TDirectObjectIdHash);

////////////////////////////////////////////////////////////////////////////////

bool TLeaseGuardComparer::operator()(const ILeaseGuardPtr& lhs, const ILeaseGuardPtr& rhs) const
{
    return Compare(lhs, rhs);
}

bool TLeaseGuardComparer::Compare(const ILeaseGuardPtr& lhs, const ILeaseGuardPtr& rhs)
{
    return lhs->GetLeaseId() < rhs->GetLeaseId();
}

////////////////////////////////////////////////////////////////////////////////

class TLeaseManager
    : public ILeaseManager
    , public TCompositeAutomatonPart
    , public THydraServiceBase
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(TLeaseId leaseId, TCellId cellId), LeaseRevoked);

public:
    TLeaseManager(
        TLeaseManagerConfigPtr config,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        NHiveServer::IHiveManagerPtr hiveManager,
        IInvokerPtr automatonInvoker,
        TCellId selfCellId,
        IUpstreamSynchronizerPtr upstreamSynchronizer,
        IAuthenticatorPtr authenticator)
        : TCompositeAutomatonPart(
            hydraManager,
            std::move(automaton),
            automatonInvoker)
        , THydraServiceBase(
            hydraManager,
            hydraManager->CreateGuardedAutomatonInvoker(automatonInvoker),
            NLeaseClient::TLeaseServiceProxy::GetDescriptor(),
            LeaseManagerLogger,
            selfCellId,
            std::move(upstreamSynchronizer),
            std::move(authenticator))
        , Config_(std::move(config))
        , HiveManager_(std::move(hiveManager))
        , LeaseMap_(TLeaseEntityMapTraits(this))
    {
        RegisterLoader(
            "LeaseManager.Keys",
            BIND(&TLeaseManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "LeaseManager.Values",
            BIND(&TLeaseManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "LeaseManager.Keys",
            BIND(&TLeaseManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "LeaseManager.Values",
            BIND(&TLeaseManager::SaveValues, Unretained(this)));

        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TLeaseManager::HydraRegisterLease, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TLeaseManager::HydraRevokeLease, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TLeaseManager::HydraRemoveLeases, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TLeaseManager::HydraOnLeaseRevoked, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND_NO_PROPAGATE(&TLeaseManager::HydraToggleLeaseRefCounter, Unretained(this)));

        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(IssueLease));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(RevokeLease));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(ReferenceLease));
        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(UnreferenceLease));
    }

    // ILeaseManager implementation.
    ILease* FindLease(TLeaseId leaseId) const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return LeaseMap_.Find(leaseId);
    }

    ILease* GetLease(TLeaseId leaseId) const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* lease = FindLease(leaseId);
        YT_VERIFY(lease);

        return lease;
    }

    ILease* GetLeaseOrThrow(TLeaseId leaseId) const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* lease = FindLease(leaseId);
        if (!lease) {
            THROW_ERROR_EXCEPTION("No such lease %v", leaseId);
        }

        return lease;
    }

    void SetDecommission(bool decommission) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (decommission == Decommission_) {
            return;
        }

        if (decommission) {
            for (auto [leaseId, lease] : LeaseMap_) {
                lease->SetState(ELeaseState::Revoking);

                if (IsLeader()) {
                    MaybeRemoveLease(lease);
                }
            }

            YT_LOG_INFO("Lease manager is decommissioned");
        } else {
            YT_LOG_INFO("Lease manager is no longer decommissioned");
        }

        Decommission_ = decommission;
    }

    bool IsFullyDecommissioned() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return Decommission_ && LeaseMap_.empty();
    }

    std::optional<int> GetLeaderAutomatonTerm() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return LeaderAutomatonTerm_;
    }

    IServicePtr GetRpcService() override
    {
        return this;
    }

    void BuildOrchid(NYson::IYsonConsumer* consumer) const override
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("leases").DoMapFor(LeaseMap_, [&] (TFluentMap fluent, const std::pair<TCellId, TLease*>& pair) {
                    auto* lease = pair.second;
                    fluent
                        .Item(ToString(lease->GetId())).BeginMap()
                            .Item("state").Value(lease->GetState())
                            .Item("persistent_ref_counter").Value(lease->GetPersistentRefCounter())
                            .Item("transient_ref_counter").Value(lease->GetTransientRefCounter())
                        .EndMap();
                })
            .EndMap();
    }

private:
    const TLeaseManagerConfigPtr Config_;

    const IHiveManagerPtr HiveManager_;

    friend class TLease;

    class TLeaseEntityMapTraits
    {
    public:
        explicit TLeaseEntityMapTraits(TLeaseManager* leaseManager)
            : LeaseManager_(leaseManager)
        { }

        std::unique_ptr<TLease> Create(TLeaseId leaseId) const
        {
            return std::make_unique<TLease>(leaseId, LeaseManager_);
        }

    private:
        TLeaseManager* const LeaseManager_;
    };
    TEntityMap<TLease, TLeaseEntityMapTraits> LeaseMap_;

    bool Decommission_ = false;

    THashSet<TLeaseId> LeaseIdsToRemove_;

    TPeriodicExecutorPtr LeaseRemovalExecutor_;

    std::optional<int> LeaderAutomatonTerm_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void LoadKeys(TLoadContext& context)
    {
        LeaseMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        LeaseMap_.LoadValues(context);

        Load(context, Decommission_);
    }

    void SaveKeys(TSaveContext& context)
    {
        LeaseMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context)
    {
        LeaseMap_.SaveValues(context);

        Save(context, Decommission_);
    }

    void OnLeaderActive() override
    {
        YT_VERIFY(!LeaseRemovalExecutor_);
        LeaseRemovalExecutor_ = New<TPeriodicExecutor>(
            EpochAutomatonInvoker_,
            BIND(&TLeaseManager::ScheduleRemoveLeases, MakeWeak(this)),
            Config_->LeaseRemovalPeriod);
        LeaseRemovalExecutor_->Start();

        LeaderAutomatonTerm_ = TCompositeAutomatonPart::HydraManager_->GetAutomatonTerm();

        for (auto [leaseId, lease] : LeaseMap_) {
            MaybeRemoveLease(lease);
        }
    }

    void OnStopLeading() override
    {
        LeaseIdsToRemove_.clear();

        if (LeaseRemovalExecutor_) {
            Y_UNUSED(LeaseRemovalExecutor_->Stop());
            LeaseRemovalExecutor_.Reset();
        }

        for (auto [leaseId, lease] : LeaseMap_) {
            lease->SetTransientRefCounter(0);
        }

        LeaderAutomatonTerm_ = std::nullopt;
    }

    void HydraRegisterLease(NProto::TReqRegisterLease* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto leaseId = FromProto<TLeaseId>(request->lease_id());
        auto ownerCellId = NHiveServer::GetHiveMutationSenderId();

        if (Decommission_) {
            YT_LOG_DEBUG(
                "Lease manager is decommissioned, ignoring lease registration "
                "(LeaseId: %v, OwnerCellId: %v)",
                leaseId,
                ownerCellId);
            return;
        }

        auto leaseHolder = std::make_unique<TLease>(leaseId, this);
        auto* lease = LeaseMap_.Insert(leaseId, std::move(leaseHolder));
        lease->SetState(ELeaseState::Active);
        lease->SetOwnerCellId(ownerCellId);

        YT_LOG_DEBUG(
            "Lease registered (LeaseId: %v, OwnerCellId: %v)",
            leaseId,
            ownerCellId);
    }

    void HydraRevokeLease(NProto::TReqRevokeLease* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto leaseId = FromProto<TLeaseId>(request->lease_id());
        auto force = request->force();
        auto* lease = LeaseMap_.Find(leaseId);
        if (!lease) {
            YT_LOG_DEBUG(
                "Requested to remove non-existent lease, ignored (LeaseId: %v)",
                leaseId);
            return;
        }

        YT_LOG_DEBUG(
            "Revoking lease (LeaseId: %v, Force: %v)",
            leaseId,
            force);

        if (force) {
            DoRemoveLease(lease);
        } else {
            lease->SetState(ELeaseState::Revoking);

            if (IsLeader()) {
                MaybeRemoveLease(lease);
            }
        }
    }

    void HydraOnLeaseRevoked(NProto::TReqConfirmLeaseRevocation* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto leaseId = FromProto<TLeaseId>(request->lease_id());
        auto cellId = GetHiveMutationSenderId();

        YT_LOG_DEBUG(
            "Lease was revoked (LeaseId: %v, CellId: %v)",
            leaseId,
            cellId);

        LeaseRevoked_.Fire(leaseId, cellId);
    }

    void HydraRemoveLeases(NProto::TReqRemoveLeases* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        for (const auto& protoLeaseId : request->lease_ids()) {
            auto leaseId = FromProto<TLeaseId>(protoLeaseId);
            auto* lease = LeaseMap_.Find(leaseId);
            if (!lease) {
                YT_LOG_DEBUG(
                    "Requested to remove a non-existing lease, ignored (LeaseId: %v)",
                    leaseId);

                // NB: Technically should not be needed, but protects us from bugs.
                LeaseIdsToRemove_.erase(leaseId);
                continue;
            }

            if (lease->GetState() != ELeaseState::Revoking) {
                YT_LOG_ALERT("Requested to remove lease which is not in \"revoking\" state, ignored "
                    "(LeaseId: %v, LeaseState: %v)",
                    lease->GetId(),
                    lease->GetState());

                LeaseIdsToRemove_.erase(leaseId);
                continue;
            }

            if (lease->GetPersistentRefCounter() != 0) {
                YT_LOG_ALERT("Requested to remove lease with non-zero persistent refcounter, ignored "
                    "(LeaseId: %v, PersistentRefCounter: %v)",
                    lease->GetId(),
                    lease->GetPersistentRefCounter());

                LeaseIdsToRemove_.erase(leaseId);
                continue;
            }

            // NB: This value is transient, so can only read this value in mutation handler.
            YT_VERIFY(lease->GetTransientRefCounter() == 0);

            DoRemoveLease(lease);
        }
    }

    void HydraToggleLeaseRefCounter(NProto::TReqToggleLeaseRefCounter* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto leaseId = FromProto<TLeaseId>(request->lease_id());
        auto force = request->force();

        auto* lease = GetLeaseOrThrow(leaseId);

        if (request->reference()) {
            lease->RefPersistently(force);
        } else {
            lease->UnrefPersistently();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NLeaseClient::NProto, IssueLease)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto leaseId = FromProto<TLeaseId>(request->lease_id());
        context->SetRequestInfo("LeaseId: %v", leaseId);

        NProto::TReqRegisterLease req;
        ToProto(req.mutable_lease_id(), leaseId);
        auto mutation = CreateMutation(TCompositeAutomatonPart::HydraManager_, req);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NLeaseClient::NProto, RevokeLease)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto leaseId = FromProto<TLeaseId>(request->lease_id());
        auto force = request->force();
        context->SetRequestInfo("LeaseId: %v, Force: %v",
            leaseId,
            force);

        NProto::TReqRevokeLease req;
        ToProto(req.mutable_lease_id(), leaseId);
        req.set_force(force);
        auto mutation = CreateMutation(TCompositeAutomatonPart::HydraManager_, req);
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NLeaseClient::NProto, ReferenceLease)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto leaseId = FromProto<TLeaseId>(request->lease_id());
        auto persistent = request->persistent();
        auto force = request->force();
        context->SetRequestInfo("LeaseId: %v, Persistent: %v, Force: %v",
            leaseId,
            persistent,
            force);

        if (persistent) {
            NProto::TReqToggleLeaseRefCounter req;
            ToProto(req.mutable_lease_id(), leaseId);
            req.set_reference(true);
            req.set_force(force);
            auto mutation = CreateMutation(TCompositeAutomatonPart::HydraManager_, req);
            YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
        } else {
            auto* lease = GetLeaseOrThrow(leaseId);
            lease->RefTransiently(force);
            context->Reply();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NLeaseClient::NProto, UnreferenceLease)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto leaseId = FromProto<TLeaseId>(request->lease_id());
        auto persistent = request->persistent();
        context->SetRequestInfo("LeaseId: %v, Persistent: %v",
            leaseId,
            persistent);

        if (persistent) {
            NProto::TReqToggleLeaseRefCounter req;
            ToProto(req.mutable_lease_id(), leaseId);
            req.set_reference(false);
            auto mutation = CreateMutation(TCompositeAutomatonPart::HydraManager_, req);
            YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
        } else {
            auto* lease = GetLeaseOrThrow(leaseId);
            lease->UnrefTransiently(*GetLeaderAutomatonTerm());
            context->Reply();
        }
    }

    int RefLeasePersistently(TLease* lease, bool force)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!force && lease->GetState() != ELeaseState::Active) {
            THROW_ERROR_EXCEPTION("Non-active lease cannot be referenced persistently")
                << TErrorAttribute("lease_id", lease->GetId())
                << TErrorAttribute("lease_state", lease->GetState());
        }

        auto persistentRefCounter = lease->GetPersistentRefCounter() + 1;
        lease->SetPersistentRefCounter(persistentRefCounter);

        YT_LOG_DEBUG(
            "Lease referenced persistently "
            "(LeaseId: %v, PersistentRefCounter: %v -> %v)",
            lease->GetId(),
            persistentRefCounter - 1,
            persistentRefCounter);

        return persistentRefCounter;
    }

    int UnrefLeasePersistently(TLease* lease)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        auto persistentRefCounter = lease->GetPersistentRefCounter() - 1;
        YT_VERIFY(persistentRefCounter >= 0);
        lease->SetPersistentRefCounter(persistentRefCounter);

        YT_LOG_DEBUG(
            "Lease unreferenced persistently "
            "(LeaseId: %v, PersistentRefCounter: %v -> %v)",
            lease->GetId(),
            persistentRefCounter + 1,
            persistentRefCounter);

        if (IsLeader()) {
            MaybeRemoveLease(lease);
        }

        return persistentRefCounter;
    }

    int RefLeaseTransiently(TLease* lease, bool force)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());
        YT_VERIFY(LeaderAutomatonTerm_);

        if (!force && lease->GetState() != ELeaseState::Active) {
            THROW_ERROR_EXCEPTION("Non-active lease cannot be referenced transiently")
                << TErrorAttribute("lease_id", lease->GetId())
                << TErrorAttribute("lease_state", lease->GetState());
        }

        auto transientRefCounter = lease->GetTransientRefCounter() + 1;
        lease->SetTransientRefCounter(transientRefCounter);

        YT_LOG_DEBUG(
            "Lease referenced transiently "
            "(LeaseId: %v, TransientRefCounter: %v -> %v)",
            lease->GetId(),
            transientRefCounter - 1,
            transientRefCounter);

        return transientRefCounter;
    }

    int UnrefLeaseTransiently(TLease* lease, int leaderAutomatonTerm)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Leader epoch changed during lease holding.
        if (leaderAutomatonTerm != LeaderAutomatonTerm_) {
            return lease->GetTransientRefCounter();
        }

        auto transientRefCounter = lease->GetTransientRefCounter() - 1;
        lease->SetTransientRefCounter(transientRefCounter);

        YT_LOG_DEBUG(
            "Lease unreferenced transiently "
            "(LeaseId: %v, TransientRefCounter: %v -> %v)",
            lease->GetId(),
            transientRefCounter + 1,
            transientRefCounter);

        MaybeRemoveLease(lease);

        return transientRefCounter;
    }

    void MaybeRemoveLease(TLease* lease)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (lease->GetState() == ELeaseState::Revoking &&
            lease->GetPersistentRefCounter() == 0 &&
            lease->GetTransientRefCounter() == 0)
        {
            YT_LOG_DEBUG(
                "Lease is no longer needed, adding lease to removal queue (LeaseId: %v)",
                lease->GetId());
            LeaseIdsToRemove_.insert(lease->GetId());
            LeaseRemovalExecutor_->ScheduleOutOfBand();
        }
    }

    void DoRemoveLease(TLease* lease)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        // NB: Lease may die below.
        auto leaseId = lease->GetId();
        auto ownerCellId = lease->GetOwnerCellId();
        LeaseMap_.Release(leaseId);

        LeaseIdsToRemove_.erase(leaseId);

        // In tests, lease creation mutations are non-Hive,
        // so owner cell id may be null.
        if (ownerCellId) {
            NProto::TReqConfirmLeaseRevocation message;
            ToProto(message.mutable_lease_id(), leaseId);

            auto* mailbox = HiveManager_->GetOrCreateCellMailbox(ownerCellId);
            HiveManager_->PostMessage(mailbox, message);
        }

        YT_LOG_DEBUG(
            "Lease removed (LeaseId: %v)",
            leaseId);
    }

    void ScheduleRemoveLeases()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        std::vector<TLeaseId> leaseIdsToRemove;
        for (auto leaseId : LeaseIdsToRemove_) {
            leaseIdsToRemove.push_back(leaseId);
            if (std::ssize(leaseIdsToRemove) > Config_->MaxLeasesPerRemoval) {
                break;
            }
        }

        if (leaseIdsToRemove.empty()) {
            return;
        }

        NProto::TReqRemoveLeases request;
        ToProto(request.mutable_lease_ids(), leaseIdsToRemove);
        auto mutation = CreateMutation(TCompositeAutomatonPart::HydraManager_, request);

        YT_LOG_DEBUG("Scheduled lease removal (LeaseCount: %v)",
            leaseIdsToRemove.size());

        auto error = WaitFor(mutation->Commit());

        YT_LOG_DEBUG_UNLESS(error.IsOK(), error, "Failed to remove leases");
    }
};

////////////////////////////////////////////////////////////////////////////////

int TLease::RefPersistently(bool force)
{
    auto owner = Owner_.Lock();
    YT_VERIFY(owner);

    return owner->RefLeasePersistently(this, force);
}

int TLease::UnrefPersistently()
{
    auto owner = Owner_.Lock();
    YT_VERIFY(owner);

    return owner->UnrefLeasePersistently(this);
}

ILeaseGuardPtr TLease::GetPersistentLeaseGuard(bool force)
{
    auto owner = Owner_.Lock();
    YT_VERIFY(owner);

    return New<TPersistentLeaseGuard>(
        this,
        std::move(owner),
        force,
        /*onLoad*/ false);
}

ILeaseGuardPtr TLease::GetPersistentLeaseGuardOnLoad()
{
    auto owner = Owner_.Lock();
    YT_VERIFY(owner);

    return New<TPersistentLeaseGuard>(
        this,
        std::move(owner),
        /*force*/ false,
        /*onLoad*/ true);
}

int TLease::RefTransiently(bool force)
{
    auto owner = Owner_.Lock();
    YT_VERIFY(owner);

    return owner->RefLeaseTransiently(this, force);
}

int TLease::UnrefTransiently(int leaderAutomatonTerm)
{
    auto owner = Owner_.Lock();
    YT_VERIFY(owner);

    return owner->UnrefLeaseTransiently(this, leaderAutomatonTerm);
}

ILeaseGuardPtr TLease::GetTransientLeaseGuard(bool force)
{
    auto owner = Owner_.Lock();
    YT_VERIFY(owner);

    auto leaderAutomatonTerm = *owner->GetLeaderAutomatonTerm();
    return New<TTransientLeaseGuard>(
        this,
        std::move(owner),
        force,
        leaderAutomatonTerm);
}

////////////////////////////////////////////////////////////////////////////////

ILeaseManagerPtr CreateLeaseManager(
    TLeaseManagerConfigPtr config,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    IHiveManagerPtr hiveManager,
    IInvokerPtr automatonInvoker,
    TCellId selfCellId,
    IUpstreamSynchronizerPtr upstreamSynchronizer,
    IAuthenticatorPtr authenticator)
{
    return New<TLeaseManager>(
        std::move(config),
        std::move(hydraManager),
        std::move(automaton),
        std::move(hiveManager),
        std::move(automatonInvoker),
        selfCellId,
        std::move(upstreamSynchronizer),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseServer
