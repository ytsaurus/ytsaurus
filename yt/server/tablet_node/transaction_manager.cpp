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

#include <core/logging/log.h>

#include <ytlib/transaction_client/helpers.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>

#include <server/hive/transaction_supervisor.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTransactionClient;
using namespace NHydra;
using namespace NCellNode;
using namespace NTabletClient::NProto;
using namespace NHive::NProto;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TTabletAutomatonPart
{
public:
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
        , Logger(TabletNodeLogger)
    {
        VERIFY_INVOKER_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        Logger.AddTag("CellId: %v", Slot_->GetCellGuid());

        Slot_->GetAutomaton()->RegisterPart(this);

        RegisterLoader(
            "TransactionManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TransactionManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "TransactionManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "TransactionManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

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
                "No such transaction %v",
                id);
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
            transaction->ThrowInvalidState();
        }

        transaction->SetPrepareTimestamp(prepareTimestamp);
        transaction->SetState(persistent
            ? ETransactionState::PersistentCommitPrepared
            : ETransactionState::TransientCommitPrepared);

        TransactionPrepared_.Fire(transaction);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction commit prepared (TransactionId: %v, Presistent: %v, PrepareTimestamp: %v)",
            transactionId,
            persistent,
            prepareTimestamp);
    }

    void PrepareTransactionAbort(const TTransactionId& transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        if (transaction->GetState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        transaction->SetState(ETransactionState::TransientAbortPrepared);

        LOG_DEBUG("Transaction abort prepared (TransactionId: %v)",
            transactionId);
    }

    void CommitTransaction(const TTransactionId& transactionId, TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        if (transaction->GetState() != ETransactionState::Active &&
            transaction->GetState() != ETransactionState::TransientCommitPrepared &&
            transaction->GetState() != ETransactionState::PersistentCommitPrepared)
        {
            transaction->ThrowInvalidState();
        }

        if (IsLeader()) {
            CloseLeases(transaction);
        }

        transaction->SetCommitTimestamp(commitTimestamp);
        transaction->SetState(ETransactionState::Committed);

        TransactionCommitted_.Fire(transaction);

        FinishTransaction(transaction);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction committed (TransactionId: %v, CommitTimestamp: %v)",
            transactionId,
            commitTimestamp);
    }

    void AbortTransaction(const TTransactionId& transactionId, bool force)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        if (transaction->GetState() == ETransactionState::PersistentCommitPrepared && !force) {
            transaction->ThrowInvalidState();
        }

        if (IsLeader()) {
            CloseLeases(transaction);
        }

        transaction->SetState(ETransactionState::Aborted);

        TransactionAborted_.Fire(transaction);

        FinishTransaction(transaction);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %v)",
            transactionId);
    }

    void PingTransaction(const TTransactionId& transactionId, const TReqPingTransaction& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        if (transaction->GetState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }


        auto timeout = transaction->GetTimeout();
        TLeaseManager::RenewLease(transaction->GetLease(), timeout);

        LOG_DEBUG("Transaction pinged (TransactionId: %v, Timeout: %v)",
            transaction->GetId(),
            timeout);
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction, TTransactionId);

private:
    TTransactionManagerConfigPtr Config_;

    TEntityMap<TTransactionId, TTransaction> TransactionMap_;

    NLog::TLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    
    void CreateLeases(TTransaction* transaction, TDuration timeout)
    {
        auto invoker = Slot_->GetEpochAutomatonInvoker();

        auto lease = TLeaseManager::CreateLease(
            timeout,
            BIND(&TImpl::OnTransactionExpired, MakeStrong(this), transaction->GetId())
                .Via(invoker));
        transaction->SetLease(lease);

        auto startInstants = TimestampToInstant(transaction->GetStartTimestamp());
        auto deadline = startInstants.first + Config_->MaxTransactionDuration;
        auto cookie = TDelayedExecutor::Submit(
            BIND(&TImpl::OnTransactionTimedOut, MakeStrong(this), transaction->GetId())
                .Via(invoker),
            deadline);
        transaction->SetTimeoutCookie(cookie);
    }

    void CloseLeases(TTransaction* transaction)
    {
        TLeaseManager::CloseLease(transaction->GetLease());
        transaction->SetLease(NullLease);

        TDelayedExecutor::Cancel(transaction->GetTimeoutCookie());
        transaction->SetTimeoutCookie(NullDelayedExecutorCookie);
    }


    void OnTransactionExpired(const TTransactionId& id)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = FindTransaction(id);
        if (!transaction)
            return;
        if (transaction->GetState() != ETransactionState::Active)
            return;

        LOG_DEBUG("Transaction lease expired (TransactionId: %v)",
            id);

        auto transactionSupervisor = Slot_->GetTransactionSupervisor();
        transactionSupervisor->AbortTransaction(id).Subscribe(BIND([=] (TError error) {
            if (!error.IsOK()) {
                LOG_DEBUG(error, "Error aborting expired transaction (TransactionId: %v)",
                    id);
            }
        }));
    }

    void OnTransactionTimedOut(const TTransactionId& id)
    {
        auto* transaction = FindTransaction(id);
        if (!transaction)
            return;
        if (transaction->GetState() != ETransactionState::Active)
            return;

        LOG_DEBUG("Transaction timed out (TransactionId: %v)",
            id);

        auto transactionSupervisor = Slot_->GetTransactionSupervisor();
        transactionSupervisor->AbortTransaction(id).Subscribe(BIND([=] (TError error) {
            if (!error.IsOK()) {
                LOG_DEBUG(error, "Error aborting timed out transaction (TransactionId: %v)",
                    id);
            }
        }));
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
        if (TransactionMap_.Contains(transactionId)) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction is already started, request ignored (TransactionId: %v)",
                transactionId);
            return;
        }

        auto startTimestamp = TTimestamp(request.start_timestamp());
        auto timeout = TDuration::MilliSeconds(request.timeout());

        auto* transaction = new TTransaction(transactionId);
        TransactionMap_.Insert(transactionId, transaction);

        transaction->SetTimeout(timeout);
        transaction->SetStartTimestamp(startTimestamp);
        transaction->SetState(ETransactionState::Active);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction started (TransactionId: %v, StartTimestamp: %v, Timeout: %v)",
            transactionId,
            startTimestamp,
            timeout);

        if (IsLeader()) {
            CreateLeases(transaction, timeout);
        }
    }


    virtual void OnLeaderActive() override
    {
        // Recreate leases for all active transactions.
        for (const auto& pair : TransactionMap_) {
            auto* transaction = pair.second;
            if (transaction->GetState() == ETransactionState::Active ||
                transaction->GetState() == ETransactionState::PersistentCommitPrepared)
            {
                auto actualTimeout = GetActualTimeout(transaction->GetTimeout());
                CreateLeases(transaction, actualTimeout);
            }
        }
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Reset all transiently prepared transactions back into active state.
        // Mark all transactions are finished to release pending readers.
        for (const auto& pair : TransactionMap_) {
            auto* transaction = pair.second;
            transaction->SetState(transaction->GetPersistentState());
            transaction->ResetFinished();
            CloseLeases(transaction);
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

void TTransactionManager::PrepareTransactionAbort(const TTransactionId& transactionId)
{
    Impl_->PrepareTransactionAbort(transactionId);    
}

void TTransactionManager::CommitTransaction(const TTransactionId& transactionId, TTimestamp commitTimestamp)
{
    Impl_->CommitTransaction(transactionId, commitTimestamp);
}

void TTransactionManager::AbortTransaction(const TTransactionId& transactionId, bool force)
{
    Impl_->AbortTransaction(transactionId, force);
}

void TTransactionManager::PingTransaction(const TTransactionId& transactionId, const TReqPingTransaction& request)
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
