#include "stdafx.h"
#include "transaction_manager.h"
#include "common.h"
#include "transaction.h"
#include "transaction_ypath_proxy.h"
#include "transaction_ypath.pb.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/object_server/type_handler_detail.h>
#include <ytlib/cypress/cypress_manager.h>
#include <ytlib/cypress/cypress_service_proxy.h>

namespace NYT {
namespace NTransactionServer {

using namespace NObjectServer;
using namespace NMetaState;
using namespace NProto;
using namespace NYTree;
using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransactionProxy
    : public NObjectServer::TObjectProxyBase<TTransaction>
{
public:
    TTransactionProxy(TTransactionManager* owner, const TTransactionId& id)
        : TBase(id, &owner->TransactionMap, TransactionServerLogger.GetCategory())
        , Owner(owner)
    { }

    virtual bool IsLogged(NRpc::IServiceContext* context) const
    {
        Stroka verb = context->GetVerb();
        if (verb == "Commit" ||
            verb == "Abort")
        {
            return true;
        }
        return TBase::IsLogged(context);;
    }

private:
    typedef TObjectProxyBase<TTransaction> TBase;

    TTransactionManager::TPtr Owner;

    void DoInvoke(NRpc::IServiceContext* context)
    {
        Stroka verb = context->GetVerb();
        if (verb == "Commit") {
            CommitThunk(context);
        } else if (verb == "Abort") {
            AbortThunk(context);
        } else if (verb == "RenewLease") {
            RenewLeaseThunk(context);
        } else {
            TBase::DoInvoke(context);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Commit)
    {
        UNUSED(request);
        UNUSED(response);

        Owner->Commit(GetImplForUpdate());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Abort)
    {
        UNUSED(request);
        UNUSED(response);

        Owner->Abort(GetImplForUpdate());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, RenewLease)
    {
        UNUSED(request);
        UNUSED(response);

        Owner->RenewLease(GetId());
        context->Reply();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransactionTypeHandler
    : public TObjectTypeHandlerBase<TTransaction>
{
public:
    TTransactionTypeHandler(TTransactionManager* owner)
        : TObjectTypeHandlerBase(&owner->TransactionMap)
        , Owner(owner)
    { }

    virtual EObjectType GetType()
    {
        return EObjectType::Transaction;
    }

    virtual TObjectId CreateFromManifest(NYTree::IMapNode* manifestNode)
    {
        auto manifest = New<TTransactionManifest>();
        manifest->LoadAndValidate(manifestNode);

        if (manifest->ParentId != NullTransactionId && !Owner->FindTransaction(manifest->ParentId)) {
            // TODO(babenko): use TServiceException here
            ythrow yexception() << Sprintf("No such parent transaction (ParentId: %s)",
                ~manifest->ParentId.ToString());
        }

        return Owner->Start(~manifest).GetId();
    }

private:
    TTransactionManager* Owner;

    virtual IObjectProxy::TPtr CreateProxy(const TObjectId& id)
    {
        return New<TTransactionProxy>(Owner, id);
    }
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

    objectManager->RegisterHandler(~New<TTransactionTypeHandler>(this));

    VERIFY_INVOKER_AFFINITY(metaStateManager->GetStateInvoker(), StateThread);
}

TTransaction& TTransactionManager::Start(TTransactionManifest* manifest)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(manifest);

    auto id = ObjectManager->GenerateId(EObjectType::Transaction);

    auto* transaction = new TTransaction(id);
    TransactionMap.Insert(id, transaction);
    // Every active transaction has a fake reference to it.
    ObjectManager->RefObject(id);
    
    if (manifest->ParentId != NullTransactionId) {
        transaction->SetParentId(manifest->ParentId);
        auto& parent = GetTransactionForUpdate(manifest->ParentId);
        YVERIFY(parent.NestedTransactionIds().insert(id).second);
        ObjectManager->RefObject(id);
    }

    if (IsLeader()) {
        CreateLease(*transaction);
    }

    transaction->SetState(ETransactionState::Active);

    OnTransactionStarted_.Fire(*transaction);

    LOG_INFO_IF(!IsRecovery(), "Transaction started (TransactionId: %s, ParentId: %s)",
        ~id.ToString(),
        ~manifest->ParentId.ToString());

    return *transaction;
}

void TTransactionManager::Commit(TTransaction& transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = transaction.GetId();

    if (!transaction.NestedTransactionIds().empty()) {
        ythrow yexception() << "Cannot commit since the transaction has nested transactions in progress";
    }

    if (IsLeader()) {
        CloseLease(transaction);
    }

    transaction.SetState(ETransactionState::Committed);

    OnTransactionCommitted_.Fire(transaction);

    FinishTransaction(transaction);

    LOG_INFO_IF(!IsRecovery(), "Transaction committed (TransactionId: %s)", ~id.ToString());
}

void TTransactionManager::Abort(TTransaction& transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = transaction.GetId();

    FOREACH (const auto& nestedId, transaction.NestedTransactionIds()) {
        ObjectManager->UnrefObject(nestedId);
        Abort(GetTransactionForUpdate(nestedId));
    }
    transaction.NestedTransactionIds().clear();

    if (IsLeader()) {
        CloseLease(transaction);
    }

    transaction.SetState(ETransactionState::Aborted);

    OnTransactionAborted_.Fire(transaction);

    FinishTransaction(transaction);

    LOG_INFO_IF(!IsRecovery(), "Transaction aborted (TransactionId: %s)", ~id.ToString());
}

void TTransactionManager::FinishTransaction(TTransaction& transaction)
{
    auto id = transaction.GetId();

    if (transaction.GetParentId() != NullTransactionId) {
        auto& parent = GetTransactionForUpdate(transaction.GetParentId());
        YVERIFY(parent.NestedTransactionIds().erase(id) == 1);
        ObjectManager->UnrefObject(id);
    }

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

    auto req = TTransactionYPathProxy::Abort(FromObjectId(id));
    ExecuteVerb(~req, ~CypressManager->CreateProcessor());
}

void TTransactionManager::SetCypressManager(NCypress::TCypressManager* cypressManager)
{
    CypressManager = cypressManager;
}

DEFINE_METAMAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, TransactionMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
