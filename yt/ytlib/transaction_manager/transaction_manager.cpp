#include "stdafx.h"
#include "transaction_manager.h"

namespace NYT {
namespace NTransaction {

using namespace NMetaState;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TransactionLogger;

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    const TConfig& config,
    TMetaStateManager::TPtr metaStateManager,
    TCompositeMetaState::TPtr metaState)
    : TMetaStatePart(metaStateManager, metaState)
    , Config(config)
    // Some random number.
    , TransactionIdGenerator(0x5fab718461fda630)
{
    YASSERT(~metaStateManager != NULL);
    YASSERT(~metaState != NULL);

    RegisterMethod(this, &TThis::StartTransaction);
    RegisterMethod(this, &TThis::CommitTransaction);
    RegisterMethod(this, &TThis::AbortTransaction);

    metaState->RegisterPart(this);

    VERIFY_INVOKER_AFFINITY(metaStateManager->GetStateInvoker(), StateThread);
}

TMetaChange<TTransactionId>::TPtr
TTransactionManager::InitiateStartTransaction()
{
    TMsgStartTransaction message;

    return CreateMetaChange(
        MetaStateManager,
        message,
        &TThis::StartTransaction,
        TPtr(this));
}

TTransactionId TTransactionManager::StartTransaction(const TMsgStartTransaction& message)
{
    UNUSED(message);
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = TransactionIdGenerator.Next();

    auto* transaction = new TTransaction(id);
    TransactionMap.Insert(id, transaction);
    
    if (IsLeader()) {
        CreateLease(*transaction);
    }

    OnTransactionStarted_.Fire(*transaction);

    LOG_INFO_IF(!IsRecovery(), "Transaction started (TransactionId: %s)",
        ~id.ToString());

    return id;
}

NMetaState::TMetaChange<TVoid>::TPtr
TTransactionManager::InitiateCommitTransaction(const TTransactionId& id)
{
    TMsgCommitTransaction message;
    message.SetTransactionId(id.ToProto());
    return CreateMetaChange(
        MetaStateManager,
        message,
        &TThis::CommitTransaction,
        TPtr(this));
}

TVoid TTransactionManager::CommitTransaction(const TMsgCommitTransaction& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = TTransactionId::FromProto(message.GetTransactionId());

    auto& transaction = TransactionMap.GetForUpdate(id);

    if (IsLeader()) {
        CloseLease(transaction);
    }

    OnTransactionCommitted_.Fire(transaction);

    TransactionMap.Remove(id);

    LOG_INFO_IF(!IsRecovery(), "Transaction committed (TransactionId: %s)",
        ~id.ToString());

    return TVoid();
}

NMetaState::TMetaChange<TVoid>::TPtr
TTransactionManager::InitiateAbortTransaction(const TTransactionId& id)
{
    TMsgAbortTransaction message;
    message.SetTransactionId(id.ToProto());
    return CreateMetaChange(
        MetaStateManager,
        message,
        &TThis::AbortTransaction,
        TPtr(this));
}

TVoid TTransactionManager::AbortTransaction(const TMsgAbortTransaction& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = TTransactionId::FromProto(message.GetTransactionId());

    auto& transaction = TransactionMap.GetForUpdate(id);

    if (IsLeader()) {
        CloseLease(transaction);
    }

    OnTransactionAborted_.Fire(transaction);

    TransactionMap.Remove(id);

    LOG_INFO_IF(!IsRecovery(), "Transaction aborted (TransactionId: %s)",
        ~id.ToString());

    return TVoid();
}

void TTransactionManager::RenewLease(const TTransactionId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto it = LeaseMap.find(id);
    YASSERT(it != LeaseMap.end());
    TLeaseManager::Get()->RenewLease(it->Second());
}

Stroka TTransactionManager::GetPartName() const
{
    return "TransactionManager";
}

TFuture<TVoid>::TPtr TTransactionManager::Save(TOutputStream* stream, IInvoker::TPtr invoker)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto transactionIdGenerator = TransactionIdGenerator;
    invoker->Invoke(FromFunctor([=] ()
        {
            ::Save(stream, transactionIdGenerator);
        }));

    return TransactionMap.Save(invoker, stream);
}

TFuture<TVoid>::TPtr TTransactionManager::Load(TInputStream* stream, IInvoker::TPtr invoker)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TPtr thisPtr = this;
    invoker->Invoke(FromFunctor([=] ()
        {
            ::Load(stream, thisPtr->TransactionIdGenerator);
        }));

    TransactionMap.Load(invoker, stream);

    return
        FromMethod(&TThis::OnLoaded, thisPtr)
        ->AsyncVia(invoker)
        ->Do();
}

TVoid TTransactionManager::OnLoaded()
{
    if (IsLeader()) {
        FOREACH (const auto& pair, TransactionMap) {
            CreateLease(*pair.Second());
        }
    }

    return TVoid();
}

void TTransactionManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionIdGenerator.Reset();
    TransactionMap.Clear();
}

void TTransactionManager::OnStopLeading()
{
    FOREACH (const auto& pair, LeaseMap) {
        TLeaseManager::Get()->CloseLease(pair.Second());
    }
    LeaseMap.clear();
}

void TTransactionManager::CreateLease(const TTransaction& transaction)
{
    auto lease = TLeaseManager::Get()->CreateLease(
        Config.TransactionTimeout,
        FromMethod(&TThis::OnTransactionExpired, TPtr(this), transaction.GetId())
        ->Via(MetaStateManager->GetEpochStateInvoker()));
    YVERIFY(LeaseMap.insert(MakePair(transaction.GetId(), lease)).Second());
}

void TTransactionManager::CloseLease(const TTransaction& transaction)
{
    auto it = LeaseMap.find(transaction.GetId());
    YASSERT(it != LeaseMap.end());
    TLeaseManager::Get()->CloseLease(it->Second());
    LeaseMap.erase(it);
}

void TTransactionManager::OnTransactionExpired(const TTransactionId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (FindTransaction(id) == NULL)
        return;

    InitiateAbortTransaction(id)->Commit();
}

METAMAP_ACCESSORS_IMPL(TTransactionManager, Transaction, TTransaction, TTransactionId, TransactionMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
