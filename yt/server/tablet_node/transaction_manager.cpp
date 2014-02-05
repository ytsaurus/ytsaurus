#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "config.h"
#include "automaton.h"
#include "tablet_slot.h"
#include "private.h"

#include <core/misc/lease_manager.h>

#include <core/concurrency/thread_affinity.h>

#include <core/ytree/fluent.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>

#include <server/hive/transaction_supervisor.h>

namespace NYT {
namespace NTabletNode {

using namespace NYTree;
using namespace NYson;
using namespace NTransactionClient;
using namespace NHydra;
using namespace NCellNode;
using namespace NTabletClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TTabletAutomatonPart
{
    DEFINE_SIGNAL(void(TTransaction*), TransactionStarted);
    DEFINE_SIGNAL(void(TTransaction*), TransactionPrepared);
    DEFINE_SIGNAL(void(TTransaction*), TransactionCommitted);
    DEFINE_SIGNAL(void(TTransaction*), TransactionAborted);

public:
    TImpl(
        TTransactionManagerConfigPtr config,
        TTabletSlot* slot,
        NCellNode::TBootstrap* bootstrap)
        : TTabletAutomatonPart(
            slot,
            bootstrap)
        , Config_(config)
    {
        VERIFY_INVOKER_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        Slot_->GetAutomaton()->RegisterPart(this);

        RegisterLoader(
            "TransactionManager.Keys",
            BIND(&TImpl::LoadKeys, MakeStrong(this)));
        RegisterLoader(
            "TransactionManager.Values",
            BIND(&TImpl::LoadValues, MakeStrong(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "TransactionManager.Keys",
            BIND(&TImpl::SaveKeys, MakeStrong(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "TransactionManager.Values",
            BIND(&TImpl::SaveValues, MakeStrong(this)));

        RegisterMethod(BIND(&TImpl::HydraStartTransaction, Unretained(this)));
    }


    TDuration GetActualTimeout(TNullable<TDuration> timeout)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return std::min(
            timeout.Get(Config_->DefaultTransactionTimeout),
            Config_->MaxTransactionTimeout);
    }

    TMutationPtr CreateStartTransactionMutation(TReqStartTransaction request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return CreateMutation(
            Slot_->GetHydraManager(),
            request,
            this,
            &TImpl::HydraStartTransaction);
    }

    TTransaction* GetTransactionOrThrow(const TTransactionId& id)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = FindTransaction(id);
        if (!transaction) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such transction %s",
                ~ToString(id));
        }
        return transaction;
    }

    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        BuildYsonFluently(consumer)
            .DoMapFor(TransactionMap_, [&] (TFluentMap fluent, const std::pair<TTransactionId, TTransaction*>& pair) {
                auto* transaction = pair.second;
                fluent
                    .Item(ToString(transaction->GetId())).BeginMap()
                        .Item("timeout").Value(transaction->GetTimeout())
                        .Item("start_time").Value(transaction->GetStartTime())
                        .Item("state").Value(transaction->GetState())
                        .Item("start_timestamp").Value(transaction->GetStartTimestamp())
                        .Item("prepare_timestamp").Value(transaction->GetPrepareTimestamp())
                        // Omit CommitTimestamp, it's typically null.
                        .Item("locked_row_count").Value(transaction->LockedRows().size())
                    .EndMap();
            });
    }


    // ITransactionManager implementation.
    void PrepareTransactionCommit(
        const TTransactionId& transactionId,
        bool persistent,
        TTimestamp prepareTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        if (transaction->GetState() != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION("Transaction is not active");
        }

        transaction->SetPrepareTimestamp(prepareTimestamp);
        transaction->SetState(
            persistent
            ? ETransactionState::PersistentlyPrepared
            : ETransactionState::TransientlyPrepared);

        TransactionPrepared_.Fire(transaction);

        LOG_DEBUG("Transaction prepared (TransactionId: %s, Presistent: %s, PrepareTimestamp: %" PRIu64 ")",
            ~ToString(transactionId),
            ~FormatBool(persistent),
            prepareTimestamp);
    }

    void CommitTransaction(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        auto state = transaction->GetState();
        if (state != ETransactionState::Active &&
            state != ETransactionState::TransientlyPrepared &&
            state != ETransactionState::PersistentlyPrepared)
        {
            THROW_ERROR_EXCEPTION("Transaction %s is in %s state",
                ~ToString(transaction->GetId()),
                ~FormatEnum(state).Quote());
        }

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetCommitTimestamp(commitTimestamp);
        transaction->SetState(ETransactionState::Committed);

        TransactionCommitted_.Fire(transaction);

        FinishTransaction(transaction);

        LOG_INFO_UNLESS(IsRecovery(), "Transaction committed (TransactionId: %s, CommitTimestamp: %" PRIu64 ")",
            ~ToString(transactionId),
            commitTimestamp);
    }

    void AbortTransaction(
        const TTransactionId& transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        if (transaction->GetState() == ETransactionState::PersistentlyPrepared) {
            THROW_ERROR_EXCEPTION("Cannot abort a persistently prepared transaction");
        }

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetState(ETransactionState::Aborted);

        TransactionAborted_.Fire(transaction);

        FinishTransaction(transaction);

        LOG_INFO_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %s)",
            ~ToString(transactionId));
    }

    void PingTransaction(
        const TTransactionId& transactionId,
        const NHive::NProto::TReqPingTransaction& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        
        if (transaction->GetState() != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION("Transaction is not active");
        }

        auto it = LeaseMap_.find(transaction->GetId());
        YCHECK(it != LeaseMap_.end());

        auto timeout = transaction->GetTimeout();

        TLeaseManager::RenewLease(it->second, timeout);

        LOG_DEBUG("Transaction pinged (TransactionId: %s, Timeout: %" PRIu64 ")",
            ~ToString(transaction->GetId()),
            timeout.MilliSeconds());
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction, TTransactionId);

private:
    TTransactionManagerConfigPtr Config_;

    TEntityMap<TTransactionId, TTransaction> TransactionMap_;
    yhash_map<TTransactionId, TLeaseManager::TLease> LeaseMap_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    
    void CreateLease(const TTransaction* transaction, TDuration timeout)
    {
        auto lease = TLeaseManager::CreateLease(
            timeout,
            BIND(&TImpl::OnTransactionExpired, MakeStrong(this), transaction->GetId())
                .Via(Slot_->GetEpochAutomatonInvoker()));
        YCHECK(LeaseMap_.insert(std::make_pair(transaction->GetId(), lease)).second);
    }

    void CloseLease(const TTransaction* transaction)
    {
        auto it = LeaseMap_.find(transaction->GetId());
        YCHECK(it != LeaseMap_.end());
        TLeaseManager::CloseLease(it->second);
        LeaseMap_.erase(it);
    }

    void OnTransactionExpired(const TTransactionId& id)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = FindTransaction(id);
        if (transaction->GetState() != ETransactionState::Active)
            return;

        LOG_INFO("Transaction lease expired (TransactionId: %s)",
            ~ToString(id));

        auto transactionSupervisor = Slot_->GetTransactionSupervisor();

        NHive::NProto::TReqAbortTransaction req;
        ToProto(req.mutable_transaction_id(), transaction->GetId());

        transactionSupervisor
            ->CreateAbortTransactionMutation(req)
            ->Commit();
    }

    void FinishTransaction(TTransaction* transaction)
    {
        transaction->SetFinished();
        TransactionMap_.Remove(transaction->GetId());
    }


    // Hydra handlers.
    void HydraStartTransaction(const TReqStartTransaction& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto startTimestamp = TTimestamp(request.start_timestamp());
        auto timeout = TDuration::MilliSeconds(request.timeout());

        auto* transaction = new TTransaction(transactionId);
        TransactionMap_.Insert(transactionId, transaction);

        transaction->SetTimeout(timeout);
        transaction->SetStartTimestamp(startTimestamp);
        transaction->SetState(ETransactionState::Active);

        LOG_DEBUG("Transaction started (TransactionId: %s, StartTimestamp: %" PRIu64 ", Timeout: %" PRIu64 ")",
            ~ToString(transactionId),
            startTimestamp,
            timeout.MilliSeconds());

        if (IsLeader()) {
            CreateLease(transaction, timeout);
        }
    }


    virtual void OnLeaderActive() override
    {
        // Recreate leases for all active transactions.
        for (const auto& pair : TransactionMap_) {
            const auto* transaction = pair.second;
            if (transaction->GetState() == ETransactionState::Active) {
                auto actualTimeout = GetActualTimeout(transaction->GetTimeout());
                CreateLease(transaction, actualTimeout);
            }
        }
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Reset all leases.
        for (const auto& pair : LeaseMap_) {
            TLeaseManager::CloseLease(pair.second);
        }
        LeaseMap_.clear();

        // Reset all transiently prepared transactions back into active state.
        // Mark all transactions are finished to release pending readers.
        for (const auto& pair : TransactionMap_) {
            auto* transaction = pair.second;
            transaction->SetState(transaction->GetPersistentState());
            transaction->ResetFinished();
        }
    }


    void SaveKeys(TSaveContext& context)
    {
        TransactionMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context)
    {
        TransactionMap_.SaveValues(context);
    }

    void OnBeforeSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TransactionMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TransactionMap_.LoadValues(context);
    }

    void DoClear()
    {
        TransactionMap_.Clear();
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TTransactionManager::TImpl, Transaction, TTransaction, TTransactionId, TransactionMap_)

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TTransactionManagerConfigPtr config,
    TTabletSlot* slot,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        slot,
        bootstrap))
{ }

TTransactionManager::~TTransactionManager()
{ }

TDuration TTransactionManager::GetActualTimeout(TNullable<TDuration> timeout)
{
    return Impl_->GetActualTimeout(timeout);
}

TMutationPtr TTransactionManager::CreateStartTransactionMutation(
    const TReqStartTransaction& request)
{
    return Impl_->CreateStartTransactionMutation(request);
}

TTransaction* TTransactionManager::GetTransactionOrThrow(const TTransactionId& id)
{
    return Impl_->GetTransactionOrThrow(id);
}

void TTransactionManager::BuildOrchidYson(IYsonConsumer* consumer)
{
    Impl_->BuildOrchidYson(consumer);
}

void TTransactionManager::PrepareTransactionCommit(
    const TTransactionId& transactionId,
    bool persistent,
    TTimestamp prepareTimestamp)
{
    Impl_->PrepareTransactionCommit(
        transactionId,
        persistent,
        prepareTimestamp);
}

void TTransactionManager::CommitTransaction(
    const TTransactionId& transactionId,
    TTimestamp commitTimestamp)
{
    Impl_->CommitTransaction(transactionId, commitTimestamp);
}

void TTransactionManager::AbortTransaction(const TTransactionId& transactionId)
{
    Impl_->AbortTransaction(transactionId);
}

void TTransactionManager::PingTransaction(
    const TTransactionId& transactionId,
    const NHive::NProto::TReqPingTransaction& request)
{
    Impl_->PingTransaction(transactionId, request);
}

DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionStarted, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionPrepared, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionCommitted, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionAborted, *Impl_);
DELEGATE_ENTITY_MAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
