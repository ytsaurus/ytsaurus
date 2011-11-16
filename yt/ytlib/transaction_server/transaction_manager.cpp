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

    metaState->RegisterLoader(
        "TransactionManager.1",
        FromMethod(&TTransactionManager::Load, TPtr(this)));
    metaState->RegisterSaver(
        "TransactionManager.1",
        FromMethod(&TTransactionManager::Save, TPtr(this)));

    metaState->RegisterPart(this);

    VERIFY_INVOKER_AFFINITY(metaStateManager->GetStateInvoker(), StateThread);
}

TMetaChange<TTransactionId>::TPtr
TTransactionManager::InitiateStartTransaction()
{
    TMsgStartTransaction message;

    return CreateMetaChange(
        ~MetaStateManager,
        message,
        &TThis::StartTransaction,
        this);
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
        ~MetaStateManager,
        message,
        &TThis::CommitTransaction,
        this);
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
        ~MetaStateManager,
        message,
        &TThis::AbortTransaction,
        this);
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

TFuture<TVoid>::TPtr TTransactionManager::Save(const TCompositeMetaState::TSaveContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* output = context.Output;
    auto invoker = context.Invoker;

    auto transactionIdGenerator = TransactionIdGenerator;
    invoker->Invoke(FromFunctor([=] ()
        {
            ::Save(output, transactionIdGenerator);
        }));

    return TransactionMap.Save(invoker, output);
}

void TTransactionManager::Load(TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ::Load(input, TransactionIdGenerator);
    TransactionMap.Load(input);
}

void TTransactionManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionIdGenerator.Reset();
    TransactionMap.Clear();
}

void TTransactionManager::OnLeaderRecoveryComplete()
{
    FOREACH (const auto& pair, TransactionMap) {
        CreateLease(*pair.Second());
    }
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
