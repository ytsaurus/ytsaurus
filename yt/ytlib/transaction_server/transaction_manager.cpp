#include "stdafx.h"
#include "transaction_manager.h"
#include "common.h"
#include "transaction.h"
#include "transaction_ypath_proxy.h"
#include "transaction_ypath.pb.h"

#include <ytlib/misc/string.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/cypress/cypress_manager.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/object_server/type_handler_detail.h>

namespace NYT {
namespace NTransactionServer {

using namespace NObjectServer;
using namespace NMetaState;
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
        : TBase(~owner->ObjectManager, id, &owner->TransactionMap, TransactionServerLogger.GetCategory())
        , Owner(owner)
    { }

    virtual bool IsWriteRequest(NRpc::IServiceContext* context) const
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Commit);
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Abort);
        DECLARE_YPATH_SERVICE_WRITE_METHOD(CreateObject);
        DECLARE_YPATH_SERVICE_WRITE_METHOD(ReleaseObject);
        return TBase::IsWriteRequest(context);
    }

private:
    typedef TObjectProxyBase<TTransaction> TBase;

    TTransactionManager::TPtr Owner;

    virtual void GetSystemAttributeNames(yvector<Stroka>* names)
    {
        names->push_back("state");
        names->push_back("parent_id");
        names->push_back("nested_transaction_ids");
        names->push_back("created_object_ids");
        TBase::GetSystemAttributeNames(names);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
    {
        const auto& transaction = GetTypedImpl();
        
        if (name == "state") {
            BuildYsonFluently(consumer)
                .Scalar(CamelCaseToUnderscoreCase(transaction.GetState().ToString()));
            return true;
        }

        if (name == "parent_id") {
            BuildYsonFluently(consumer)
                .Scalar(transaction.GetParentId().ToString());
            return true;
        }

        if (name == "nested_transaction_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction.NestedTransactionIds(), [=] (TFluentList fluent, TTransactionId id)
                    {
                        fluent.Item().Scalar(id.ToString());
                    });
            return true;
        }

        if (name == "created_object_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction.CreatedObjectIds(), [=] (TFluentList fluent, TTransactionId id)
            {
                fluent.Item().Scalar(id.ToString());
            });
            return true;
        }

        return TBase::GetSystemAttribute(name, consumer);
    }

    virtual void DoInvoke(NRpc::IServiceContext* context)
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

        Owner->Commit(GetTypedImplForUpdate());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Abort)
    {
        UNUSED(request);
        UNUSED(response);

        Owner->Abort(GetTypedImplForUpdate());
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
        // TODO(babenko): validate type
        auto type = EObjectType(request->type());

        context->SetRequestInfo("TransactionId: %s, Type: %s",
            ~GetId().ToString(),
            ~type.ToString());

        auto handler = Owner->ObjectManager->GetHandler(type);

        NYTree::INode::TPtr manifestNode =
            request->has_manifest()
            ? DeserializeFromYson(request->manifest())
            : GetEphemeralNodeFactory()->CreateMap();

        if (manifestNode->GetType() != NYTree::ENodeType::Map) {
            ythrow yexception() << "Manifest must be a map";
        }

        if (handler->IsTransactionRequired() && GetId() == NullTransactionId) {
            ythrow yexception() << Sprintf("Cannot create an instance outside of a transaction (Type: %s)",
                ~type.ToString());
        }

        auto objectId = handler->CreateFromManifest(
            GetId(),
            ~manifestNode->AsMap());

        if (GetId() != NullTransactionId) {
            auto& transaction = GetTypedImplForUpdate();
            YVERIFY(transaction.CreatedObjectIds().insert(objectId).second);
            Owner->ObjectManager->RefObject(objectId);
        }

        response->set_object_id(objectId.ToProto());

        context->SetResponseInfo("ObjectId: %s", ~objectId.ToString());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionServer::NProto, ReleaseObject)
    {
        UNUSED(response);

        auto objectId = TObjectId::FromProto(request->object_id());

        context->SetRequestInfo("ObjectId: %s", ~objectId.ToString());

        auto& transaction = GetTypedImplForUpdate();
        if (transaction.CreatedObjectIds().erase(objectId) != 1) {
            ythrow yexception() << Sprintf("Transaction does not own the object (ObjectId: %s)", ~objectId.ToString());
        }

        Owner->ObjectManager->UnrefObject(objectId);

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransactionTypeHandler
    : public TObjectTypeHandlerBase<TTransaction>
{
public:
    TTransactionTypeHandler(TTransactionManager* owner)
        : TObjectTypeHandlerBase(~owner->ObjectManager, &owner->TransactionMap)
        , Owner(owner)
    { }

    virtual EObjectType GetType()
    {
        return EObjectType::Transaction;
    }

    virtual TObjectId CreateFromManifest(
        const TTransactionId& transactionId,
        IMapNode* manifestNode)
    {
        auto manifest = New<TTransactionManifest>();
        manifest->LoadAndValidate(manifestNode);

        auto* parent =
            transactionId == NullTransactionId
            ? NULL
            : &Owner->GetTransactionForUpdate(transactionId);

        return Owner->Start(parent, ~manifest).GetId();
    }

    virtual bool IsTransactionRequired() const
    {
        return false;
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

TTransaction& TTransactionManager::Start(TTransaction* parent, TTransactionManifest* manifest)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(manifest);

    auto id = ObjectManager->GenerateId(EObjectType::Transaction);

    auto* transaction = new TTransaction(id);
    TransactionMap.Insert(id, transaction);
    // Every active transaction has a fake reference to it.
    ObjectManager->RefObject(id);
    
    if (parent) {
        transaction->SetParentId(parent->GetId());
        YVERIFY(parent->NestedTransactionIds().insert(id).second);
        ObjectManager->RefObject(id);
    }

    if (IsLeader()) {
        CreateLease(*transaction, manifest->Timeout);
    }

    transaction->SetState(ETransactionState::Active);

    OnTransactionStarted_.Fire(*transaction);

    LOG_INFO_IF(!IsRecovery(), "Transaction started (TransactionId: %s, ParentId: %s)",
        ~id.ToString(),
        parent ? ~parent->GetId().ToString() : "None");

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

    FOREACH (const auto& createdId, transaction.CreatedObjectIds()) {
        ObjectManager->UnrefObject(createdId);
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
        // TODO(roizner): This timeout is probably incorrect
        CreateLease(*pair.Second(), Config->DefaultTransactionTimeout);
    }
}

void TTransactionManager::OnStopLeading()
{
    FOREACH (const auto& pair, LeaseMap) {
        TLeaseManager::CloseLease(pair.Second());
    }
    LeaseMap.clear();
}

void TTransactionManager::CreateLease(
    const TTransaction& transaction,
    TDuration timeout)
{
    if (timeout == TDuration::Zero()) {
        timeout = Config->DefaultTransactionTimeout;
    }
    auto lease = TLeaseManager::CreateLease(
        timeout,
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

TObjectManager* TTransactionManager::GetObjectManager() const
{
    return ~ObjectManager;
}

IObjectProxy::TPtr TTransactionManager::GetRootTransactionProxy()
{
    return New<TTransactionProxy>(this, NullTransactionId);
}

DEFINE_METAMAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, TransactionMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
