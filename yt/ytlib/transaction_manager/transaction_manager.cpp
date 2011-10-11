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
{
    metaState->RegisterPart(this);

    VERIFY_INVOKER_AFFINITY(metaStateManager->GetStateInvoker(), StateThread);
}

TTransactionId TTransactionManager::StartTransaction(const TMsgCreateTransaction& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = TTransactionId::FromProto(message.GetTransactionId());

    auto* transaction = new TTransaction(id);
    YVERIFY(TransactionMap.Insert(id, transaction));
    
    if (IsLeader()) {
        CreateLease(*transaction);
    }

    OnTransactionStarted_.Fire(*transaction);

    LOG_INFO("Transaction started (TransactionId: %s)",
        ~id.ToString());

    return id;
}

NYT::TVoid TTransactionManager::CommitTransaction(const TMsgCommitTransaction& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = TTransactionId::FromProto(message.GetTransactionId());

    auto& transaction = TransactionMap.GetForUpdate(id);

    if (IsLeader()) {
        CloseLease(transaction);
    }

    OnTransactionCommitted_.Fire(transaction);

    YASSERT(TransactionMap.Remove(id));

    LOG_INFO("Transaction committed (TransactionId: %s)",
        ~id.ToString());

    return TVoid();
}

NYT::TVoid TTransactionManager::AbortTransaction(const TMsgAbortTransaction& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = TTransactionId::FromProto(message.GetTransactionId());

    auto& transaction = TransactionMap.GetForUpdate(id);

    if (IsLeader()) {
        CloseLease(transaction);
    }

    OnTransactionAborted_.Fire(transaction);

    YASSERT(TransactionMap.Remove(id));

    LOG_INFO("Transaction aborted (TransactionId: %s)",
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

TParamSignal<TTransaction&>& TTransactionManager::OnTransactionStarted()
{
    return OnTransactionStarted_;
}

TParamSignal<TTransaction&>& TTransactionManager::OnTransactionCommitted()
{
    return OnTransactionCommitted_;
}

TParamSignal<TTransaction&>& TTransactionManager::OnTransactionAborted()
{
    return OnTransactionAborted_;
}

Stroka TTransactionManager::GetPartName() const
{
    return "TransactionManager";
}

TFuture<TVoid>::TPtr TTransactionManager::Save(TOutputStream* stream, IInvoker::TPtr invoker)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return TransactionMap.Save(invoker, stream);
}

TFuture<TVoid>::TPtr TTransactionManager::Load(TInputStream* stream, IInvoker::TPtr invoker)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return TransactionMap.Load(invoker, stream);
}

void TTransactionManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.Clear();
}

void TTransactionManager::OnStartLeading()
{
    TMetaStatePart::OnStartLeading();

    YASSERT(LeaseMap.empty());

    FOREACH (const auto& pair, TransactionMap) {
        CreateLease(*pair.Second());
    }
}

void TTransactionManager::OnStopLeading()
{
    TMetaStatePart::OnStopLeading();

    FOREACH (const auto& pair, LeaseMap) {
        TLeaseManager::Get()->CloseLease(pair.Second());
    }
    LeaseMap.clear();
}

void TTransactionManager::CreateLease( const TTransaction& transaction )
{
    auto lease = TLeaseManager::Get()->CreateLease(
        Config.TransactionTimeout,
        FromMethod(&TThis::OnTransactionExpired, TPtr(this), transaction.GetId())
        ->Via(MetaStateManager->GetEpochStateInvoker()));
    YVERIFY(LeaseMap.insert(MakePair(transaction.GetId(), lease)).Second());
}

void TTransactionManager::CloseLease( const TTransaction& transaction )
{
    auto it = LeaseMap.find(transaction.GetId());
    YASSERT(it != LeaseMap.end());
    TLeaseManager::Get()->CloseLease(it->Second());
    LeaseMap.erase(it);
}

void TTransactionManager::OnTransactionExpired(const TTransactionId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto it = LeaseMap.find(id);
    if (it == LeaseMap.end())
        return;

    LeaseMap.erase(it);

    NProto::TMsgAbortTransaction message;
    message.SetTransactionId(id.ToProto());

    CommitChange(
        message,
        FromMethod(&TThis::AbortTransaction, this));
}

METAMAP_ACCESSORS_IMPL(TTransactionManager, Transaction, TTransaction, TTransactionId, TransactionMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
