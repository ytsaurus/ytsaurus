#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "transaction_ypath_proxy.h"

#include <ytlib/transaction_server/transaction_ypath.pb.h>
#include <ytlib/cell_master/load_context.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/config.h>
#include <ytlib/misc/string.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/cypress/cypress_manager.h>
#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/object_server/type_handler_detail.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NMetaState;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("TransactionServer");

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransactionProxy
    : public NObjectServer::TUnversionedObjectProxyBase<TTransaction>
{
public:
    TTransactionProxy(TTransactionManager* owner, const TTransactionId& id)
        : TBase(owner->Bootstrap, id, &owner->TransactionMap)
        , Owner(owner)
    {
        Logger = NTransactionServer::Logger;
    }

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Commit);
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Abort);
        DECLARE_YPATH_SERVICE_WRITE_METHOD(CreateObject);
        DECLARE_YPATH_SERVICE_WRITE_METHOD(ReleaseObject);
        return TBase::IsWriteRequest(context);
    }

private:
    typedef TUnversionedObjectProxyBase<TTransaction> TBase;

    TTransactionManager::TPtr Owner;

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
    {
        attributes->push_back("state");
        attributes->push_back("parent_id");
        attributes->push_back("nested_transaction_ids");
        attributes->push_back("created_object_ids");
        TBase::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
    {
        const auto& transaction = GetTypedImpl();
        
        if (name == "state") {
            BuildYsonFluently(consumer)
                .Scalar(FormatEnum(transaction.GetState()));
            return true;
        }

        if (name == "parent_id") {
            BuildYsonFluently(consumer)
                .Scalar(GetObjectId(transaction.GetParent()).ToString());
            return true;
        }

        if (name == "nested_transaction_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction.NestedTransactions(), [=] (TFluentList fluent, TTransaction* nestedTransaction) {
                    fluent.Item().Scalar(nestedTransaction->GetId().ToString());
                });
            return true;
        }

        if (name == "created_object_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction.CreatedObjectIds(), [=] (TFluentList fluent, TTransactionId id) {
                    fluent.Item().Scalar(id.ToString());
                });
            return true;
        }

        return TBase::GetSystemAttribute(name, consumer);
    }

    virtual void DoInvoke(NRpc::IServiceContextPtr context)
    {
        DISPATCH_YPATH_SERVICE_METHOD(Commit);
        DISPATCH_YPATH_SERVICE_METHOD(Abort);
        DISPATCH_YPATH_SERVICE_METHOD(RenewLease);
        DISPATCH_YPATH_SERVICE_METHOD(CreateObject);
        DISPATCH_YPATH_SERVICE_METHOD(ReleaseObject);
        TBase::DoInvoke(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Commit)
    {
        UNUSED(request);
        UNUSED(response);

        Owner->Commit(GetTypedImpl());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Abort)
    {
        UNUSED(request);
        UNUSED(response);

        Owner->Abort(GetTypedImpl());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, RenewLease)
    {
        UNUSED(request);
        UNUSED(response);

        Owner->RenewLease(GetId());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, CreateObject)
    {
        auto type = EObjectType(request->type());

        context->SetRequestInfo("TransactionId: %s, Type: %s",
            ~GetId().ToString(),
            ~type.ToString());

        auto* transaction = GetId() == NullTransactionId ? NULL : &GetTypedImpl();

        auto objectManager = Owner->Bootstrap->GetObjectManager();
        auto handler = objectManager->FindHandler(type);
        if (!handler) {
            ythrow yexception() << Sprintf("Unknown object type %s", ~type.ToString());
        }

        if (handler->IsTransactionRequired() && !transaction) {
            ythrow yexception() << Sprintf("Cannot create an instance of %s outside",
                ~FormatEnum(type));
        }

        auto objectId = handler->Create(
            transaction,
            request,
            response);

        *response->mutable_object_id() = objectId.ToProto();

        auto attributeKeys = request->Attributes().List();
        if (!attributeKeys.empty()) {
            // Copy attributes. Quick and dirty.
            auto* attributeSet = objectManager->FindAttributes(objectId);
            if (!attributeSet) {
                attributeSet = objectManager->CreateAttributes(objectId);
            }
            
            FOREACH (const auto& key, attributeKeys) {
                YVERIFY(attributeSet->Attributes().insert(MakePair(
                    key,
                    request->Attributes().GetYson(key))).second);
            }
        }

        if (transaction) {
            YVERIFY(transaction->CreatedObjectIds().insert(objectId).second);
            objectManager->RefObject(objectId);
        }

        context->SetResponseInfo("ObjectId: %s", ~objectId.ToString());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionServer::NProto, ReleaseObject)
    {
        UNUSED(response);

        auto objectId = TObjectId::FromProto(request->object_id());

        context->SetRequestInfo("ObjectId: %s", ~objectId.ToString());

        auto& transaction = GetTypedImpl();
        if (transaction.CreatedObjectIds().erase(objectId) != 1) {
            ythrow yexception() << "Transaction does not own the object";
        }

        auto objectManager = Owner->Bootstrap->GetObjectManager();
        objectManager->UnrefObject(objectId);

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransactionTypeHandler
    : public TObjectTypeHandlerBase<TTransaction>
{
public:
    explicit TTransactionTypeHandler(TTransactionManager* owner)
        : TObjectTypeHandlerBase(owner->Bootstrap, &owner->TransactionMap)
        , Owner(owner)
    { }

    virtual EObjectType GetType()
    {
        return EObjectType::Transaction;
    }

    virtual TObjectId Create(
        TTransaction* parent,
        TReqCreateObject* request,
        TRspCreateObject* response)
    {
        UNUSED(response);

        auto timeout = request->Attributes().Find<TDuration>("timeout");
        auto& transaction = Owner->Start(parent, timeout);
        return transaction.GetId();
    }

    virtual IObjectProxy::TPtr GetProxy(
        const TObjectId& id,
        NTransactionServer::TTransaction* transaction)
    {
        UNUSED(transaction);
        return New<TTransactionProxy>(Owner, id);
    }

    virtual bool IsTransactionRequired() const
    {
        return false;
    }

private:
    TTransactionManager* Owner;
};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TTransactionManagerConfigPtr config,
    TBootstrap* bootstrap)
    : TMetaStatePart(
        ~bootstrap->GetMetaStateManager(),
        ~bootstrap->GetMetaState())
    , Config(config)
    , Bootstrap(bootstrap)
{
    YASSERT(config);
    YASSERT(bootstrap);
    VERIFY_INVOKER_AFFINITY(bootstrap->GetStateInvoker(), StateThread);

    TLoadContext context(Bootstrap);

    auto metaState = Bootstrap->GetMetaState();
    metaState->RegisterLoader(
        "TransactionManager.Keys.1",
        BIND(&TTransactionManager::LoadKeys, MakeStrong(this)));
    metaState->RegisterLoader(
        "TransactionManager.Values.1",
        BIND(&TTransactionManager::LoadValues, MakeStrong(this), context));
    metaState->RegisterSaver(
        "TransactionManager.Keys.1",
        BIND(&TTransactionManager::SaveKeys, MakeStrong(this)),
        ESavePhase::Keys);
    metaState->RegisterSaver(
        "TransactionManager.Values.1",
        BIND(&TTransactionManager::SaveValues, MakeStrong(this)),
        ESavePhase::Values);

    metaState->RegisterPart(this);
}

void TTransactionManager::Init()
{
    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->RegisterHandler(~New<TTransactionTypeHandler>(this));
}

TTransaction& TTransactionManager::Start(TTransaction* parent, TNullable<TDuration> timeout)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto objectManager = Bootstrap->GetObjectManager();
    auto id = objectManager->GenerateId(EObjectType::Transaction);

    auto* transaction = new TTransaction(id);
    TransactionMap.Insert(id, transaction);

    // Every active transaction has a fake reference to it.
    objectManager->RefObject(id);
    
    if (parent) {
        transaction->SetParent(parent);
        YVERIFY(parent->NestedTransactions().insert(transaction).second);
        objectManager->RefObject(id);
    }

    if (IsLeader()) {
        CreateLease(*transaction, timeout);
    }

    transaction->SetState(ETransactionState::Active);

    TransactionStarted_.Fire(*transaction);

    LOG_INFO_IF(!IsRecovery(), "Started transaction %s (ParentId: %s)",
        ~id.ToString(),
        ~GetObjectId(parent).ToString());

    return *transaction;
}

void TTransactionManager::Commit(TTransaction& transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (transaction.GetState() != ETransactionState::Active) {
        ythrow yexception() << "Cannot commit an inactive transaction";
    }

    auto id = transaction.GetId();

    if (!transaction.NestedTransactions().empty()) {
        ythrow yexception() << "Cannot commit since the transaction has nested transactions in progress";
    }

    if (IsLeader()) {
        CloseLease(transaction);
    }

    transaction.SetState(ETransactionState::Committed);

    TransactionCommitted_.Fire(transaction);

    FinishTransaction(transaction);

    LOG_INFO_IF(!IsRecovery(), "Committed transaction %s", ~id.ToString());
}

void TTransactionManager::Abort(TTransaction& transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (transaction.GetState() != ETransactionState::Active) {
        ythrow yexception() << "Cannot abort an inactive transaction";
    }

    auto id = transaction.GetId();

    // Make a copy, the set will be modified.
    auto nestedTransactions = transaction.NestedTransactions();
    FOREACH (auto* nestedTransaction, nestedTransactions) {
        Abort(*nestedTransaction);
    }
    YASSERT(transaction.NestedTransactions().empty());

    if (IsLeader()) {
        CloseLease(transaction);
    }

    transaction.SetState(ETransactionState::Aborted);

    TransactionAborted_.Fire(transaction);

    FinishTransaction(transaction);

    LOG_INFO_IF(!IsRecovery(), "Aborted transaction %s", ~id.ToString());
}

void TTransactionManager::FinishTransaction(TTransaction& transaction)
{
    auto objectManager = Bootstrap->GetObjectManager();

    auto* parent = transaction.GetParent();
    if (parent) {
        YVERIFY(parent->NestedTransactions().erase(&transaction) == 1);
        objectManager->UnrefObject(&transaction);
    }

    // Kill the fake reference.
    objectManager->UnrefObject(&transaction);
}

void TTransactionManager::RenewLease(const TTransaction& transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (transaction.GetState() != ETransactionState::Active) {
        ythrow yexception() << "Cannot renew lease for an inactive transaction";
    }

    auto it = LeaseMap.find(transaction.GetId());
    YASSERT(it != LeaseMap.end());
    TLeaseManager::RenewLease(it->second);
}

void TTransactionManager::SaveKeys(TOutputStream* output)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.SaveKeys(output);
}

void TTransactionManager::SaveValues(TOutputStream* output)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.SaveValues(output);
}

void TTransactionManager::LoadKeys(TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.LoadKeys(input);
}

void TTransactionManager::LoadValues(TLoadContext context, TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.LoadValues(context, input);
}

void TTransactionManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.Clear();
}

void TTransactionManager::OnLeaderRecoveryComplete()
{
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& pair, TransactionMap) {
        const auto& id = pair.first;
        const auto& transaction = *pair.second;
        auto proxy = objectManager->GetProxy(id, NULL);
        auto timeout = proxy->Attributes().Find<TDuration>("timeout");
        CreateLease(transaction, timeout);
    }
}

void TTransactionManager::OnStopLeading()
{
    FOREACH (const auto& pair, LeaseMap) {
        TLeaseManager::CloseLease(pair.second);
    }
    LeaseMap.clear();
}

void TTransactionManager::CreateLease(const TTransaction& transaction, TNullable<TDuration> timeout)
{
    auto actualTimeout = Min(
        timeout.Get(Config->DefaultTransactionTimeout),
        Config->MaximumTransactionTimeout);
    auto lease = TLeaseManager::CreateLease(
        actualTimeout,
        BIND(&TThis::OnTransactionExpired, MakeStrong(this), transaction.GetId())
        .Via(
            Bootstrap->GetStateInvoker(),
            Bootstrap->GetMetaStateManager()->GetEpochContext()));
    YVERIFY(LeaseMap.insert(MakePair(transaction.GetId(), lease)).second);
}

void TTransactionManager::CloseLease(const TTransaction& transaction)
{
    auto it = LeaseMap.find(transaction.GetId());
    YASSERT(it != LeaseMap.end());
    TLeaseManager::CloseLease(it->second);
    LeaseMap.erase(it);
}

void TTransactionManager::OnTransactionExpired(const TTransactionId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto objectManager = Bootstrap->GetObjectManager();
    auto proxy = objectManager->FindProxy(id);
    if (!proxy)
        return;

    LOG_INFO("Transaction %s has expired", ~id.ToString());

    auto req = TTransactionYPathProxy::Abort();
    ExecuteVerb(proxy, req);
}

IObjectProxy::TPtr TTransactionManager::GetRootTransactionProxy()
{
    return New<TTransactionProxy>(this, NullTransactionId);
}

DEFINE_METAMAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, TransactionMap)

std::vector<TTransaction*> TTransactionManager::GetTransactionPath(TTransaction* transaction) const
{
    std::vector<TTransaction*> path;
    path.push_back(transaction);
    auto current = transaction;
    while (current != NULL) {
        current = current->GetParent();
        path.push_back(current);
    }
    return path;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
