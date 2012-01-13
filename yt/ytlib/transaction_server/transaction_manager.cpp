#include "stdafx.h"
#include "transaction_manager.h"
#include "common.h"
#include "transaction.h"
#include "transaction_proxy.h"
#include "transaction_ypath_proxy.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/object_server/type_handler_detail.h>
#include <ytlib/cypress/cypress_manager.h>

namespace NYT {
namespace NTransactionServer {

using namespace NObjectServer;
using namespace NMetaState;
using namespace NProto;
using namespace NYTree;

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

    virtual IObjectProxy::TPtr FindProxy(const TTransactionId& id)
    {
        return New<TTransactionProxy>(Owner, id);
    }

    virtual TObjectId CreateFromManifest(NYTree::IMapNode* manifest)
    {
        UNUSED(manifest);
        return Owner->Start().GetId();
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

    // Kill the fake reference.
    ObjectManager->UnrefObject(id);
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

    // Kill the fake reference.
    ObjectManager->UnrefObject(id);
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

    LOG_INFO("Transaction expired (TransactionId: %s)", ~id.ToString());

    auto req = TTransactionYPathProxy::Abort();
    ExecuteVerb(~req, ~CypressManager->CreateProcessor(id));
}

void TTransactionManager::SetCypressManager(NCypress::TCypressManager* cypressManager)
{
    CypressManager = cypressManager;
}

DEFINE_METAMAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, TransactionMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
