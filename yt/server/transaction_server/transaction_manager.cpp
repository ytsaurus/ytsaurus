#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"

#include <ytlib/misc/string.h>

#include <ytlib/transaction_client/transaction_ypath.pb.h>
#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/fluent.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/object_server/type_handler_detail.h>

#include <server/cell_master/serialization_context.h>
#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>
#include <server/cell_master/meta_state_facade.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NMetaState;
using namespace NYTree;
using namespace NCypressServer;
using namespace NTransactionClient;

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
        // NB: RenewLease is not logged and thus is not considered to be a write
        // request. It can only be served at leaders though, so its handler explicitly
        // checks the status.
        return TBase::IsWriteRequest(context);
    }

private:
    typedef TUnversionedObjectProxyBase<TTransaction> TBase;

    TTransactionManager::TPtr Owner;

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
    {
        attributes->push_back("state");
        attributes->push_back("parent_id");
        attributes->push_back("start_time");
        attributes->push_back("nested_transaction_ids");
        attributes->push_back("created_object_ids");
        attributes->push_back("created_node_ids");
        attributes->push_back("branched_node_ids");
        attributes->push_back("locked_node_ids");
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer)
    {
        const auto* transaction = GetTypedImpl();
        
        if (key == "state") {
            BuildYsonFluently(consumer)
                .Scalar(FormatEnum(transaction->GetState()));
            return true;
        }

        if (key == "parent_id") {
            BuildYsonFluently(consumer)
                .Scalar(GetObjectId(transaction->GetParent()).ToString());
            return true;
        }

        if (key == "start_time") {
            BuildYsonFluently(consumer)
                .Scalar(ToString(transaction->GetStartTime()));
            return true;
        }

        if (key == "nested_transaction_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->NestedTransactions(), [=] (TFluentList fluent, TTransaction* nestedTransaction) {
                    fluent.Item().Scalar(nestedTransaction->GetId().ToString());
                });
            return true;
        }

        if (key == "created_object_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->CreatedObjectIds(), [=] (TFluentList fluent, const TTransactionId& id) {
                    fluent.Item().Scalar(id.ToString());
                });
            return true;
        }

        if (key == "created_node_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->CreatedNodes(), [=] (TFluentList fluent, const ICypressNode* node) {
                    fluent.Item().Scalar(node->GetId().ObjectId.ToString());
                });
            return true;
        }

        if (key == "branched_node_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->BranchedNodes(), [=] (TFluentList fluent, const ICypressNode* node) {
                    fluent.Item().Scalar(node->GetId().ObjectId.ToString());
            });
            return true;
        }

        if (key == "locked_node_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->LockedNodes(), [=] (TFluentList fluent, const ICypressNode* node) {
                    fluent.Item().Scalar(node->GetId().ObjectId.ToString());
            });
            return true;
        }

        return TBase::GetSystemAttribute(key, consumer);
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

    void ValidateTransactionNotNull()
    {
        if (GetId() == NullTransactionId) {
            THROW_ERROR_EXCEPTION("Non-null transaction must be provided");
        }
    }

    void ValidateTransactionIsActive(const TTransaction* transaction)
    {
        if (!transaction->IsActive()) {
            THROW_ERROR_EXCEPTION("Transaction is not active: %s",
                ~transaction->GetId().ToString());
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, Commit)
    {
        UNUSED(request);
        UNUSED(response);

        ValidateTransactionNotNull();
        auto* transaction = GetTypedImpl();
        ValidateTransactionIsActive(transaction);

        context->SetRequestInfo("");
        Owner->Commit(transaction);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, Abort)
    {
        UNUSED(request);
        UNUSED(response);

        ValidateTransactionNotNull();
        auto* transaction = GetTypedImpl();
        ValidateTransactionIsActive(transaction);

        context->SetRequestInfo("");
        Owner->Abort(transaction);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, RenewLease)
    {
        UNUSED(response);

        bool renewAncestors = request->renew_ancestors();
        context->SetRequestInfo("RenewAncestors: %s", ~FormatBool(renewAncestors));

        ValidateLeaderStatus();

        ValidateTransactionNotNull();

        auto* transaction = GetTypedImpl();
        ValidateTransactionIsActive(transaction);

        Owner->RenewLease(transaction, renewAncestors);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, CreateObject)
    {
        auto type = EObjectType(request->type());

        context->SetRequestInfo("TransactionId: %s, Type: %s",
            ~GetId().ToString(),
            ~type.ToString());

        auto* transaction = GetId() == NullTransactionId ? NULL : GetTypedImpl();

        if (transaction) {
            ValidateTransactionIsActive(transaction);
        }

        auto objectManager = Owner->Bootstrap->GetObjectManager();
        auto handler = objectManager->FindHandler(type);
        if (!handler) {
            THROW_ERROR_EXCEPTION("Unknown object type: %s",
                ~type.ToString());
        }

        if (handler->IsTransactionRequired() && !transaction) {
            THROW_ERROR_EXCEPTION("Cannot create an instance of %s outside of a transaction",
                ~FormatEnum(type).Quote());
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
                YCHECK(attributeSet->Attributes().insert(MakePair(
                    key,
                    request->Attributes().GetYson(key))).second);
            }
        }

        if (transaction) {
            YCHECK(transaction->CreatedObjectIds().insert(objectId).second);
            objectManager->RefObject(objectId);
        }

        context->SetResponseInfo("ObjectId: %s", ~objectId.ToString());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, ReleaseObject)
    {
        UNUSED(response);

        auto objectId = TObjectId::FromProto(request->object_id());
        context->SetRequestInfo("ObjectId: %s", ~objectId.ToString());

        ValidateTransactionNotNull();
        auto* transaction = GetTypedImpl();
        ValidateTransactionIsActive(transaction);

        if (transaction->CreatedObjectIds().erase(objectId) != 1) {
            THROW_ERROR_EXCEPTION("Object %s does not belong to transaction %s",
                ~objectId.ToString(),
                ~transaction->GetId().ToString());
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
        auto* transaction = Owner->Start(parent, timeout);
        return transaction->GetId();
    }

    virtual IObjectProxyPtr GetProxy(
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
        bootstrap->GetMetaStateFacade()->GetManager(),
        bootstrap->GetMetaStateFacade()->GetState())
    , Config(config)
    , Bootstrap(bootstrap)
{
    YCHECK(config);
    YCHECK(bootstrap);
    VERIFY_INVOKER_AFFINITY(bootstrap->GetMetaStateFacade()->GetInvoker(), StateThread);

    {
        NCellMaster::TLoadContext context;
        context.SetBootstrap(Bootstrap);

        RegisterLoader(
            "TransactionManager.Keys",
            SnapshotVersionValidator(),
            BIND(&TTransactionManager::LoadKeys, MakeStrong(this)),
            context);
        RegisterLoader(
            "TransactionManager.Values",
            SnapshotVersionValidator(),
            BIND(&TTransactionManager::LoadValues, MakeStrong(this)),
            context);
    }
    {
        NCellMaster::TSaveContext context;

        RegisterSaver(
            ESavePriority::Keys,
            "TransactionManager.Keys",
            CurrentSnapshotVersion,
            BIND(&TTransactionManager::SaveKeys, MakeStrong(this)),
            context);
        RegisterSaver(
            ESavePriority::Values,
            "TransactionManager.Values",
            CurrentSnapshotVersion,
            BIND(&TTransactionManager::SaveValues, MakeStrong(this)),
            context);
    }
}

void TTransactionManager::Init()
{
    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->RegisterHandler(New<TTransactionTypeHandler>(this));
}

TTransaction* TTransactionManager::Start(TTransaction* parent, TNullable<TDuration> timeout)
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
        YCHECK(parent->NestedTransactions().insert(transaction).second);
        objectManager->RefObject(id);
    }

    auto actualTimeout = GetActualTimeout(timeout);
    if (IsLeader()) {
        CreateLease(transaction, actualTimeout);
    }

    transaction->SetState(ETransactionState::Active);

    auto* mutationContext = Bootstrap
        ->GetMetaStateFacade()
        ->GetManager()
        ->GetMutationContext();
    transaction->SetStartTime(mutationContext->GetTimestamp());

    TransactionStarted_.Fire(transaction);

    LOG_INFO_UNLESS(IsRecovery(), "Transaction started (TransactionId: %s, ParentId: %s, Timeout: %" PRIu64 ")",
        ~id.ToString(),
        ~GetObjectId(parent).ToString(),
        actualTimeout.MilliSeconds());

    return transaction;
}

void TTransactionManager::Commit(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YCHECK(transaction->IsActive());

    auto id = transaction->GetId();

    if (!transaction->NestedTransactions().empty()) {
        THROW_ERROR_EXCEPTION("Cannot commit transaction %s since it has %d active nested transaction(s)",
            ~ToString(id),
            static_cast<int>(transaction->NestedTransactions().size()));
    }

    if (IsLeader()) {
        CloseLease(transaction);
    }

    transaction->SetState(ETransactionState::Committed);

    TransactionCommitted_.Fire(transaction);

    FinishTransaction(transaction);

    LOG_INFO_UNLESS(IsRecovery(), "Transaction committed (TransactionId: %s)", ~id.ToString());
}

void TTransactionManager::Abort(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YCHECK(transaction->IsActive());

    auto id = transaction->GetId();

    // Sort nested transactions by id to ensure consist unref order.
    std::vector<TTransaction*> nestedTransactions(
        transaction->NestedTransactions().begin(),
        transaction->NestedTransactions().end());
    std::sort(
        nestedTransactions.begin(),
        nestedTransactions.end(),
        [] (const TTransaction* lhs, const TTransaction* rhs) {
            return lhs->GetId() < rhs->GetId();
        });
    FOREACH (auto* nestedTransaction, nestedTransactions) {
        Abort(nestedTransaction);
    }
    YCHECK(transaction->NestedTransactions().empty());

    if (IsLeader()) {
        CloseLease(transaction);
    }

    transaction->SetState(ETransactionState::Aborted);

    TransactionAborted_.Fire(transaction);

    FinishTransaction(transaction);

    LOG_INFO_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %s)", ~id.ToString());
}

void TTransactionManager::FinishTransaction(TTransaction* transaction)
{
    auto objectManager = Bootstrap->GetObjectManager();

    auto* parent = transaction->GetParent();
    if (parent) {
        YCHECK(parent->NestedTransactions().erase(transaction) == 1);
        objectManager->UnrefObject(transaction);
    }

    // Kill the fake reference.
    objectManager->UnrefObject(transaction);
}

void TTransactionManager::RenewLease(const TTransaction* transaction, bool renewAncestors)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    DoRenewLease(transaction);

    if (renewAncestors) {
        auto parentTransaction = transaction->GetParent();
        while (parentTransaction) {
            DoRenewLease(parentTransaction);
            parentTransaction = parentTransaction->GetParent();
        }
    }
}

void TTransactionManager::DoRenewLease(const TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(transaction->IsActive());

    auto it = LeaseMap.find(transaction->GetId());
    YCHECK(it != LeaseMap.end());
    TLeaseManager::RenewLease(it->second);
}

void TTransactionManager::SaveKeys(const NCellMaster::TSaveContext& context)
{
    TransactionMap.SaveKeys(context);
}

void TTransactionManager::SaveValues(const NCellMaster::TSaveContext& context)
{
    TransactionMap.SaveValues(context);
}

void TTransactionManager::LoadKeys(const NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.LoadKeys(context);
}

void TTransactionManager::LoadValues(const NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.LoadValues(context);
}

void TTransactionManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.Clear();
}

TDuration TTransactionManager::GetActualTimeout(TNullable<TDuration> timeout)
{
    return Min(
        timeout.Get(Config->DefaultTransactionTimeout),
        Config->MaxTransactionTimeout);
}

void TTransactionManager::OnActiveQuorumEstablished()
{
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& pair, TransactionMap) {
        const auto& id = pair.first;
        const auto* transaction = pair.second;
        if (transaction->GetState() == ETransactionState::Active) {
            auto proxy = objectManager->GetProxy(id, NULL);
            auto timeout = proxy->Attributes().Find<TDuration>("timeout");
            CreateLease(transaction, GetActualTimeout(timeout));
        }
    }
}

void TTransactionManager::OnStopLeading()
{
    FOREACH (const auto& pair, LeaseMap) {
        TLeaseManager::CloseLease(pair.second);
    }
    LeaseMap.clear();
}

void TTransactionManager::CreateLease(const TTransaction* transaction, TDuration timeout)
{
    auto metaStateFacade = Bootstrap->GetMetaStateFacade();
    auto lease = TLeaseManager::CreateLease(
        timeout,
        BIND(&TThis::OnTransactionExpired, MakeStrong(this), transaction->GetId())
            .Via(metaStateFacade->GetEpochInvoker()));
    YCHECK(LeaseMap.insert(MakePair(transaction->GetId(), lease)).second);
}

void TTransactionManager::CloseLease(const TTransaction* transaction)
{
    auto it = LeaseMap.find(transaction->GetId());
    YCHECK(it != LeaseMap.end());
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

    LOG_INFO("Transaction has expired (TransactionId: %s)", ~id.ToString());

    auto req = TTransactionYPathProxy::Abort();
    ExecuteVerb(proxy, req).Subscribe(BIND([=] (TTransactionYPathProxy::TRspAbortPtr rsp) {
        if (rsp->IsOK()) {
            LOG_INFO("Transaction expiration commit success (TransactionId: %s)",
                ~id.ToString());
        } else {
            LOG_ERROR(*rsp, "Transaction expiration commit failed (TransactionId: %s)",
                ~id.ToString());
        }
    }));
}

IObjectProxyPtr TTransactionManager::GetRootTransactionProxy()
{
    return New<TTransactionProxy>(this, NullTransactionId);
}

std::vector<TTransaction*> TTransactionManager::GetTransactionPath(TTransaction* transaction) const
{
    std::vector<TTransaction*> result;
    while (true) {
        result.push_back(transaction);
        if (!transaction) {
            break;
        }
        transaction = transaction->GetParent();
    }
    return result;
}

DEFINE_METAMAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, TransactionMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
