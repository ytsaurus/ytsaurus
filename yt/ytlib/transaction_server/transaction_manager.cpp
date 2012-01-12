#include "stdafx.h"
#include "transaction_manager.h"
#include "common.h"
#include "transaction.h"
#include "transaction_proxy.h"

#include <object_server/type_handler_detail.h>

namespace NYT {
namespace NTransactionServer {

using namespace NObjectServer;
using namespace NMetaState;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTypeHandler
    : public TObjectTypeHandlerBase<TTransaction>
{
public:
    TTypeHandler(TTransactionManager* owner)
        : TObjectTypeHandlerBase(&owner->TransactionMap)
        , Owner(owner)
    { }

    virtual EObjectType GetType()
    {
        return EObjectType::Transaction;
    }

    virtual IObjectProxy::TPtr GetProxy(const TTransactionId& id)
    {
        return New<TTransactionProxy>(Owner, id);
    }

private:
    TTransactionManager* Owner;

};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TConfig* config,
    IMetaStateManager* metaStateManager,
    TCompositeMetaState* metaState,
    TObjectManager* objectManager)
    : TMetaStatePart(metaStateManager, metaState)
    , Config(config)
    , ObjectManager(objectManager)
{
    YASSERT(metaStateManager);
    YASSERT(metaState);
    YASSERT(objectManager);

    RegisterMethod(this, &TThis::DoStartTransaction);
    RegisterMethod(this, &TThis::DoCommitTransaction);
    RegisterMethod(this, &TThis::DoAbortTransaction);

    metaState->RegisterLoader(
        "TransactionManager.1",
        FromMethod(&TTransactionManager::Load, TPtr(this)));
    metaState->RegisterSaver(
        "TransactionManager.1",
        FromMethod(&TTransactionManager::Save, TPtr(this)));

    metaState->RegisterPart(this);

    objectManager->RegisterHandler(~New<TTypeHandler>(this));

    VERIFY_INVOKER_AFFINITY(metaStateManager->GetStateInvoker(), StateThread);
}

TMetaChange<TTransactionId>::TPtr
TTransactionManager::InitiateStartTransaction()
{
    TMsgStartTransaction message;

    return CreateMetaChange(
        ~MetaStateManager,
        message,
        &TThis::DoStartTransaction,
        this);
}

TTransaction& TTransactionManager::Start()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = ObjectManager->GenerateId(EObjectType::Transaction);

    auto* transaction = new TTransaction(id);
    TransactionMap.Insert(id, transaction);
    // Every active transaction has a fake reference to it.
    ObjectManager->RefObject(id);
    
    if (IsLeader()) {
        CreateLease(*transaction);
    }

    transaction->SetState(ETransactionState::Active);
    LOG_INFO_IF(!IsRecovery(), "Transaction started (TransactionId: %s)",
        ~id.ToString());

    OnTransactionStarted_.Fire(*transaction);

    return *transaction;
}

TTransactionId TTransactionManager::DoStartTransaction(const TMsgStartTransaction& message)
{
    UNUSED(message);

    auto& transaction = Start();
    return transaction.GetId();
}

NMetaState::TMetaChange<TVoid>::TPtr
TTransactionManager::InitiateCommitTransaction(const TTransactionId& id)
{
    TMsgCommitTransaction message;
    message.set_transaction_id(id.ToProto());
    return CreateMetaChange(
        ~MetaStateManager,
        message,
        &TThis::DoCommitTransaction,
        this);
}

void TTransactionManager::Commit(TTransaction& transaction)
{
    auto id = transaction.GetId();

    if (IsLeader()) {
        CloseLease(transaction);
    }

    transaction.SetState(ETransactionState::Committed);
    LOG_INFO_IF(!IsRecovery(), "Transaction committed (TransactionId: %s)",
        ~id.ToString());

    OnTransactionCommitted_.Fire(transaction);

    ObjectManager->UnrefObject(id);
}

TVoid TTransactionManager::DoCommitTransaction(const TMsgCommitTransaction& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = TTransactionId::FromProto(message.transaction_id());

    auto& transaction = TransactionMap.GetForUpdate(id);
    Commit(transaction);
    return TVoid();
}

NMetaState::TMetaChange<TVoid>::TPtr
TTransactionManager::InitiateAbortTransaction(const TTransactionId& id)
{
    TMsgAbortTransaction message;
    message.set_transaction_id(id.ToProto());
    return CreateMetaChange(
        ~MetaStateManager,
        message,
        &TThis::DoAbortTransaction,
        this);
}

void TTransactionManager::Abort(TTransaction& transaction)
{
    auto id = transaction.GetId();

    if (IsLeader()) {
        CloseLease(transaction);
    }

    transaction.SetState(ETransactionState::Aborted);
    LOG_INFO_IF(!IsRecovery(), "Transaction aborted (TransactionId: %s)",
        ~id.ToString());

    OnTransactionAborted_.Fire(transaction);

    ObjectManager->UnrefObject(id);
}

TVoid TTransactionManager::DoAbortTransaction(const TMsgAbortTransaction& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = TTransactionId::FromProto(message.transaction_id());
    auto& transaction = TransactionMap.GetForUpdate(id);
    Abort(transaction);
    return TVoid();
}

void TTransactionManager::RenewLease(const TTransactionId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto it = LeaseMap.find(id);
    YASSERT(it != LeaseMap.end());
    TLeaseManager::RenewLease(it->Second());
}

TFuture<TVoid>::TPtr TTransactionManager::Save(const TCompositeMetaState::TSaveContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* output = context.Output;
    auto invoker = context.Invoker;
    return TransactionMap.Save(invoker, output);
}

void TTransactionManager::Load(TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.Load(input);
}

void TTransactionManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

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
        TLeaseManager::CloseLease(pair.Second());
    }
    LeaseMap.clear();
}

void TTransactionManager::CreateLease(const TTransaction& transaction)
{
    auto lease = TLeaseManager::CreateLease(
        Config->TransactionTimeout,
        ~FromMethod(&TThis::OnTransactionExpired, TPtr(this), transaction.GetId())
        ->Via(MetaStateManager->GetEpochStateInvoker()));
    YVERIFY(LeaseMap.insert(MakePair(transaction.GetId(), lease)).Second());
}

void TTransactionManager::CloseLease(const TTransaction& transaction)
{
    auto it = LeaseMap.find(transaction.GetId());
    YASSERT(it != LeaseMap.end());
    TLeaseManager::CloseLease(it->Second());
    LeaseMap.erase(it);
}

void TTransactionManager::OnTransactionExpired(const TTransactionId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (!FindTransaction(id))
        return;

    LOG_INFO("Transaction expired (TransactionId: %s)",
        ~id.ToString());

    InitiateAbortTransaction(id)->Commit();
}

DEFINE_METAMAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, TransactionMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
