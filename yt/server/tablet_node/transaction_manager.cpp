#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "config.h"
#include "automaton.h"
#include "tablet_slot.h"
#include "private.h"

#include <core/misc/lease_manager.h>

#include <core/concurrency/thread_affinity.h>

#include <ytlib/tablet_client/tablet_service.pb.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>

#include <server/hive/transaction_supervisor.h>

namespace NYT {
namespace NTabletNode {

using namespace NTransactionClient;
//using namespace NTransactionClient::NProto;
using namespace NHydra;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TTabletAutomatonPart
{
    DEFINE_SIGNAL(void(TTransaction*), TransactionStarted);
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
        , Config(config)
    {
        VERIFY_INVOKER_AFFINITY(Slot->GetAutomatonInvoker(), AutomatonThread);

        Slot->GetAutomaton()->RegisterPart(this);

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

    DECLARE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction, TTransactionId);


    // ITransactionManager implementation.
    TTransactionId StartTransaction(
        TTimestamp startTimestamp,
        const NHive::NProto::TReqStartTransaction& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& requestExt = request.GetExtension(NTabletClient::NProto::TReqStartTransactionExt::start_transaction_ext);

        auto id = FromProto<TTransactionId>(requestExt.transaction_id());

        auto timeout =
            requestExt.has_timeout()
            ? TNullable<TDuration>(TDuration::MilliSeconds(requestExt.timeout()))
            : Null;

        auto* transaction = new TTransaction(id);
        TransactionMap.Insert(id, transaction);

        auto actualTimeout = GetActualTimeout(timeout);
        transaction->SetTimeout(actualTimeout);
        transaction->SetState(ETransactionState::Active);
        transaction->SetStartTimestamp(startTimestamp);

        LOG_DEBUG("Transaction started (TransactionId: %s, StartTimestamp: %" PRId64 ", Timeout: %" PRId64 ")",
            ~ToString(id),
            startTimestamp,
            actualTimeout.MilliSeconds());

        if (IsLeader()) {
            CreateLease(transaction, actualTimeout);
        }

        return id;
    }

    void PrepareTransactionCommit(
        const TTransactionId& transactionId,
        bool persistent,
        TTimestamp prepareTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

    }

    void CommitTransaction(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

    }

    void AbortTransaction(
        const TTransactionId& transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

    }

    void PingTransaction(
        const TTransactionId& transactionId,
        const NHive::NProto::TReqPingTransaction& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

    }

private:
    TTransactionManagerConfigPtr Config;

    TEntityMap<TTransactionId, TTransaction> TransactionMap;
    yhash_map<TTransactionId, TLeaseManager::TLease> LeaseMap;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    TDuration GetActualTimeout(TNullable<TDuration> timeout)
    {
        return std::min(
            timeout.Get(Config->DefaultTransactionTimeout),
            Config->MaxTransactionTimeout);
    }
    
    void CreateLease(const TTransaction* transaction, TDuration timeout)
    {
        /*
        auto metaStateFacade = Bootstrap->GetMetaStateFacade();
        auto lease = TLeaseManager::CreateLease(
            timeout,
            BIND(&TThis::OnTransactionExpired, MakeStrong(this), transaction->GetId())
                .Via(metaStateFacade->GetEpochInvoker()));
        YCHECK(LeaseMap.insert(std::make_pair(transaction->GetId(), lease)).second);
        */
    }

    void OnTransactionExpired(const TTransactionId& id)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = FindTransaction(id);
        if (transaction->GetState() != ETransactionState::Active)
            return;

        LOG_INFO("Transaction lease expired (TransactionId: %s)",
            ~ToString(id));

        auto transactionSupervisor = Slot->GetTransactionSupervisor();

        NHive::NProto::TReqAbortTransaction req;
        ToProto(req.mutable_transaction_id(), transaction->GetId());

        transactionSupervisor
            ->CreateAbortTransactionMutation(req)
            ->OnSuccess(BIND([=] () {
                LOG_INFO("Transaction expiration commit success (TransactionId: %s)",
                    ~ToString(id));
            }))
            ->OnError(BIND([=] (const TError& error) {
                LOG_ERROR(error, "Transaction expiration commit failed (TransactionId: %s)",
                    ~ToString(id));
            }))
            ->Commit();
    }


    virtual void OnLeaderActive() override
    {
        for (const auto& pair : TransactionMap) {
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

        for (const auto& pair : LeaseMap) {
            TLeaseManager::CloseLease(pair.second);
        }
        LeaseMap.clear();

        for (const auto& pair : TransactionMap) {
            auto* transaction = pair.second;
            if (transaction->GetState() == ETransactionState::TransientPrepared) {
                transaction->SetState(ETransactionState::Active);
            }
        }
    }


    void SaveKeys(TSaveContext& context)
    {
        TransactionMap.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context)
    {
        TransactionMap.SaveValues(context);
    }

    void OnBeforeSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TransactionMap.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TransactionMap.LoadValues(context);
    }

    void DoClear()
    {
        TransactionMap.Clear();
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TTransactionManager::TImpl, Transaction, TTransaction, TTransactionId, TransactionMap)

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TTransactionManagerConfigPtr config,
    TTabletSlot* slot,
    TBootstrap* bootstrap)
    : Impl(New<TImpl>(
        config,
        slot,
        bootstrap))
{ }

TTransaction* TTransactionManager::GetTransactionOrThrow(const TTransactionId& id)
{
    return Impl->GetTransactionOrThrow(id);
}

void TTransactionManager::PrepareTransactionCommit(
    const TTransactionId& transactionId,
    bool persistent,
    TTimestamp prepareTimestamp)
{
    Impl->PrepareTransactionCommit(
        transactionId,
        persistent,
        prepareTimestamp);
}

TTransactionId TTransactionManager::StartTransaction(
    TTimestamp startTimestamp,
    const NHive::NProto::TReqStartTransaction& request)
{
    return Impl->StartTransaction(startTimestamp, request);
}

void TTransactionManager::CommitTransaction(
    const TTransactionId& transactionId,
    TTimestamp commitTimestamp)
{
    Impl->CommitTransaction(transactionId, commitTimestamp);
}

void TTransactionManager::AbortTransaction(const TTransactionId& transactionId)
{
    Impl->AbortTransaction(transactionId);
}

void TTransactionManager::PingTransaction(
    const TTransactionId& transactionId,
    const NHive::NProto::TReqPingTransaction& request)
{
    Impl->PingTransaction(transactionId, request);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, *Impl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
