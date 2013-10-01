#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "private.h"

#include <core/misc/string.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/fluent.h>
#include <core/ytree/attributes.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/object_server/object.h>
#include <server/object_server/type_handler_detail.h>
#include <server/object_server/attribute_set.h>

#include <server/cypress_server/node.h>

#include <server/cell_master/serialization_context.h>
#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>
#include <server/cell_master/meta_state_facade.h>

#include <server/security_server/account.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCypressServer;
using namespace NMetaState;
using namespace NYTree;
using namespace NYson;
using namespace NCypressServer;
using namespace NTransactionClient;
using namespace NSecurityServer;
using namespace NTransactionClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransactionProxy
    : public NObjectServer::TNonversionedObjectProxyBase<TTransaction>
{
public:
    TTransactionProxy(NCellMaster::TBootstrap* bootstrap, TTransaction* transaction)
        : TBase(bootstrap, transaction)
    {
        Logger = TransactionServerLogger;
    }

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Commit);
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Abort);
        DECLARE_YPATH_SERVICE_WRITE_METHOD(UnstageObject);
        // NB: Ping is not logged and thus is not considered to be a write
        // request. It can only be served at leaders though, so its handler explicitly
        // checks the status.
        return TBase::IsWriteRequest(context);
    }

private:
    typedef TNonversionedObjectProxyBase<TTransaction> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        attributes->push_back("state");
        attributes->push_back("timeout");
        attributes->push_back("uncommitted_accounting_enabled");
        attributes->push_back("staged_accounting_enabled");
        attributes->push_back("parent_id");
        attributes->push_back("start_time");
        attributes->push_back("nested_transaction_ids");
        attributes->push_back("staged_object_ids");
        attributes->push_back("staged_node_ids");
        attributes->push_back("branched_node_ids");
        attributes->push_back("locked_node_ids");
        attributes->push_back("lock_ids");
        attributes->push_back("resource_usage");
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* transaction = GetThisTypedImpl();

        if (key == "state") {
            BuildYsonFluently(consumer)
                .Value(transaction->GetState());
            return true;
        }

        if (key == "timeout") {
            BuildYsonFluently(consumer)
                .Value(transaction->GetTimeout());
            return true;
        }

        if (key == "uncommitted_accounting_enabled") {
            BuildYsonFluently(consumer)
                .Value(transaction->GetUncommittedAccountingEnabled());
            return true;
        }

        if (key == "staged_accounting_enabled") {
            BuildYsonFluently(consumer)
                .Value(transaction->GetStagedAccountingEnabled());
            return true;
        }

        if (key == "parent_id") {
            BuildYsonFluently(consumer)
                .Value(GetObjectId(transaction->GetParent()));
            return true;
        }

        if (key == "start_time") {
            BuildYsonFluently(consumer)
                .Value(ToString(transaction->GetStartTime()));
            return true;
        }

        if (key == "nested_transaction_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->NestedTransactions(), [=] (TFluentList fluent, TTransaction* nestedTransaction) {
                    fluent.Item().Value(nestedTransaction->GetId());
                });
            return true;
        }

        if (key == "staged_object_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->StagedObjects(), [=] (TFluentList fluent, const TObjectBase* object) {
                    fluent.Item().Value(object->GetId());
                });
            return true;
        }

        if (key == "staged_node_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->StagedNodes(), [=] (TFluentList fluent, const TCypressNodeBase* node) {
                    fluent.Item().Value(node->GetId());
                });
            return true;
        }

        if (key == "branched_node_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->BranchedNodes(), [=] (TFluentList fluent, const TCypressNodeBase* node) {
                    fluent.Item().Value(node->GetId());
            });
            return true;
        }

        if (key == "locked_node_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->LockedNodes(), [=] (TFluentList fluent, const TCypressNodeBase* node) {
                    fluent.Item().Value(node->GetId());
                });
            return true;
        }

        if (key == "lock_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->Locks(), [=] (TFluentList fluent, const TLock* lock) {
                    fluent.Item().Value(lock->GetId());
            });
            return true;
        }

        if (key == "resource_usage") {
            BuildYsonFluently(consumer)
                .DoMapFor(transaction->AccountResourceUsage(), [=] (TFluentMap fluent, const TTransaction::TAccountResourcesMap::value_type& pair) {
                    const auto* account = pair.first;
                    const auto& usage = pair.second;
                    fluent.Item(account->GetName()).Value(usage);
                });
            return true;
        }

        return TBase::GetSystemAttribute(key, consumer);
    }

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Commit);
        DISPATCH_YPATH_SERVICE_METHOD(Abort);
        DISPATCH_YPATH_SERVICE_METHOD(Ping);
        DISPATCH_YPATH_SERVICE_METHOD(UnstageObject);
        return TBase::DoInvoke(context);
    }

    void ValidateTransactionIsActive(const TTransaction* transaction)
    {
        if (!transaction->IsActive()) {
            THROW_ERROR_EXCEPTION("Transaction is not active: %s",
                ~ToString(transaction->GetId()));
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, Commit)
    {
        UNUSED(request);
        UNUSED(response);

        context->SetRequestInfo("");

        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

        auto* transaction = GetThisTypedImpl();
        ValidateTransactionIsActive(transaction);

        auto transactionManager = Bootstrap->GetTransactionManager();
        transactionManager->CommitTransaction(transaction);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, Abort)
    {
        UNUSED(request);
        UNUSED(response);

        context->SetRequestInfo("");

        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

        auto* transaction = GetThisTypedImpl();
        ValidateTransactionIsActive(transaction);

        auto transactionManager = Bootstrap->GetTransactionManager();
        transactionManager->AbortTransaction(transaction);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, Ping)
    {
        UNUSED(response);

        bool pingAncestors = request->ping_ancestors();
        context->SetRequestInfo("PingAncestors: %s", ~FormatBool(pingAncestors));

        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
        ValidateActiveLeader();

        auto* transaction = GetThisTypedImpl();
        ValidateTransactionIsActive(transaction);

        auto transactionManager = Bootstrap->GetTransactionManager();
        transactionManager->PingTransaction(transaction, pingAncestors);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, UnstageObject)
    {
        UNUSED(response);

        auto objectId = FromProto<TObjectId>(request->object_id());
        bool recursive = request->recursive();
        context->SetRequestInfo("ObjectId: %s, Recursive: %s",
            ~ToString(objectId),
            ~FormatBool(recursive));

        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

        auto* transaction = GetThisTypedImpl();
        ValidateTransactionIsActive(transaction);

        auto objectManager = Bootstrap->GetObjectManager();
        auto* object = objectManager->GetObjectOrThrow(objectId);

        auto transactionManager = Bootstrap->GetTransactionManager();
        transactionManager->UnstageObject(transaction, object, recursive);

        context->Reply();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransactionTypeHandler
    : public TObjectTypeHandlerWithMapBase<TTransaction>
{
public:
    explicit TTransactionTypeHandler(TTransactionManager* owner)
        : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->TransactionMap)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Transaction;
    }

    virtual TNullable<TTypeCreationOptions> GetCreationOptions() const override
    {
        return TTypeCreationOptions(
            EObjectTransactionMode::Optional,
            EObjectAccountMode::Forbidden,
            false);
    }

    virtual TNonversionedObjectBase* Create(
        TTransaction* parent,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObjects* request,
        TRspCreateObjects* response) override
    {
        UNUSED(account);
        UNUSED(response);

        const auto* requestExt = &request->GetExtension(TReqCreateTransactionExt::create_transaction_ext);
        auto timeout =
            requestExt->has_timeout()
            ? TNullable<TDuration>(TDuration::MilliSeconds(requestExt->timeout()))
            : Null;

        auto transactionManager = Bootstrap->GetTransactionManager();
        auto* transaction = transactionManager->StartTransaction(parent, timeout);
        transaction->SetUncommittedAccountingEnabled(requestExt->enable_uncommitted_accounting());
        transaction->SetStagedAccountingEnabled(requestExt->enable_staged_accounting());
        return transaction;
    }

private:
    virtual Stroka DoGetName(TTransaction* transaction) override
    {
        return Sprintf("transaction %s", ~ToString(transaction->GetId()));
    }

    virtual IObjectProxyPtr DoGetProxy(TTransaction* transaction, TTransaction* dummyTransaction) override
    {
        UNUSED(dummyTransaction);
        return New<TTransactionProxy>(Bootstrap, transaction);
    }

    virtual TAccessControlDescriptor* DoFindAcd(TTransaction* transaction) override
    {
        return &transaction->Acd();
    }

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
            ESerializationPriority::Keys,
            "TransactionManager.Keys",
            GetCurrentSnapshotVersion(),
            BIND(&TTransactionManager::SaveKeys, MakeStrong(this)),
            context);
        RegisterSaver(
            ESerializationPriority::Values,
            "TransactionManager.Values",
            GetCurrentSnapshotVersion(),
            BIND(&TTransactionManager::SaveValues, MakeStrong(this)),
            context);
    }
}

void TTransactionManager::Inititialize()
{
    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->RegisterHandler(New<TTransactionTypeHandler>(this));
}

TTransaction* TTransactionManager::StartTransaction(TTransaction* parent, TNullable<TDuration> timeout)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto objectManager = Bootstrap->GetObjectManager();
    auto id = objectManager->GenerateId(EObjectType::Transaction);

    auto* transaction = new TTransaction(id);
    TransactionMap.Insert(id, transaction);

    // Every active transaction has a fake reference to itself.
    objectManager->RefObject(transaction);

    if (parent) {
        transaction->SetParent(parent);
        YCHECK(parent->NestedTransactions().insert(transaction).second);
        objectManager->RefObject(transaction);
    } else {
        YCHECK(TopmostTransactions_.insert(transaction).second);
    }

    auto actualTimeout = GetActualTimeout(timeout);
    transaction->SetTimeout(actualTimeout);

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
        ~ToString(id),
        ~ToString(GetObjectId(parent)),
        actualTimeout.MilliSeconds());

    return transaction;
}

void TTransactionManager::CommitTransaction(TTransaction* transaction)
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

    LOG_INFO_UNLESS(IsRecovery(), "Transaction committed (TransactionId: %s)", ~ToString(id));
}

void TTransactionManager::AbortTransaction(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YCHECK(transaction->IsActive());

    auto nestedTransactions = transaction->NestedTransactions();
    FOREACH (auto* nestedTransaction, nestedTransactions) {
        AbortTransaction(nestedTransaction);
    }
    YCHECK(transaction->NestedTransactions().empty());

    if (IsLeader()) {
        CloseLease(transaction);
    }

    transaction->SetState(ETransactionState::Aborted);

    TransactionAborted_.Fire(transaction);

    FinishTransaction(transaction);

    LOG_INFO_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %s)",
        ~ToString(transaction->GetId()));
}

void TTransactionManager::FinishTransaction(TTransaction* transaction)
{
    auto objectManager = Bootstrap->GetObjectManager();

    FOREACH (auto* object, transaction->StagedObjects()) {
        auto handler = objectManager->GetHandler(object);
        handler->Unstage(object, transaction, false);
        objectManager->UnrefObject(object);
    }
    transaction->StagedObjects().clear();

    FOREACH (auto* node, transaction->StagedNodes()) {
        objectManager->UnrefObject(node);
    }
    transaction->StagedNodes().clear();

    auto* parent = transaction->GetParent();
    if (parent) {
        YCHECK(parent->NestedTransactions().erase(transaction) == 1);
        objectManager->UnrefObject(transaction);
        transaction->SetParent(nullptr);
    } else {
        YCHECK(TopmostTransactions_.erase(transaction) == 1);
    }

    // Kill the fake reference thus destroying the object.
    objectManager->UnrefObject(transaction);
}

void TTransactionManager::PingTransaction(const TTransaction* transaction, bool pingAncestors)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    DoPingTransaction(transaction);

    if (pingAncestors) {
        auto parentTransaction = transaction->GetParent();
        while (parentTransaction) {
            DoPingTransaction(parentTransaction);
            parentTransaction = parentTransaction->GetParent();
        }
    }
}

void TTransactionManager::DoPingTransaction(const TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(transaction->IsActive());

    auto it = LeaseMap.find(transaction->GetId());
    YCHECK(it != LeaseMap.end());

    auto timeout = transaction->GetTimeout();

    TLeaseManager::RenewLease(it->second, timeout);

    LOG_DEBUG("Transaction pinged (TransactionId: %s, Timeout: %" PRIu64 ")",
        ~ToString(transaction->GetId()),
        timeout.MilliSeconds());
}

void TTransactionManager::SaveKeys(NCellMaster::TSaveContext& context)
{
    TransactionMap.SaveKeys(context);
}

void TTransactionManager::SaveValues(NCellMaster::TSaveContext& context)
{
    TransactionMap.SaveValues(context);
}

void TTransactionManager::OnBeforeLoaded()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    DoClear();
}

void TTransactionManager::LoadKeys(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.LoadKeys(context);
    // COMPAT(babenko)
    if (context.GetVersion() < 24) {
        YCHECK(TransactionMap.GetSize() == 0);
    }
}

void TTransactionManager::LoadValues(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    TransactionMap.LoadValues(context);
}

void TTransactionManager::OnAfterLoaded()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    // Reconstruct TopmostTransactions.
    TopmostTransactions_.clear();
    FOREACH (const auto& pair, TransactionMap) {
        auto* transaction = pair.second;
        if (IsObjectAlive(transaction) && !transaction->GetParent()) {
            YCHECK(TopmostTransactions_.insert(transaction).second);
        }
    }
}

void TTransactionManager::DoClear()
{
    TransactionMap.Clear();
    TopmostTransactions_.clear();
}

void TTransactionManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    DoClear();
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
        const auto* transaction = pair.second;
        if (transaction->GetState() == ETransactionState::Active) {
            auto actualTimeout = GetActualTimeout(transaction->GetTimeout());
            CreateLease(transaction, actualTimeout);
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
    YCHECK(LeaseMap.insert(std::make_pair(transaction->GetId(), lease)).second);
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

    auto* transaction = FindTransaction(id);
    if (!transaction || !transaction->IsActive())
        return;

    auto objectManager = Bootstrap->GetObjectManager();
    auto proxy = objectManager->GetProxy(transaction);

    LOG_INFO("Transaction lease expired (TransactionId: %s)", ~ToString(id));

    auto req = TTransactionYPathProxy::Abort();
    ExecuteVerb(proxy, req).Subscribe(BIND([=] (TTransactionYPathProxy::TRspAbortPtr rsp) {
        if (rsp->IsOK()) {
            LOG_INFO("Transaction expiration commit success (TransactionId: %s)",
                ~ToString(id));
        } else {
            LOG_ERROR(*rsp, "Transaction expiration commit failed (TransactionId: %s)",
                ~ToString(id));
        }
    }));
}

TTransactionPath TTransactionManager::GetTransactionPath(TTransaction* transaction) const
{
    TTransactionPath result;
    while (true) {
        result.push_back(transaction);
        if (!transaction) {
            break;
        }
        transaction = transaction->GetParent();
    }
    return result;
}

void TTransactionManager::StageObject(TTransaction* transaction, TObjectBase* object)
{
    auto objectManager = Bootstrap->GetObjectManager();
    YCHECK(transaction->StagedObjects().insert(object).second);
    objectManager->RefObject(object);
}

void TTransactionManager::UnstageObject(
    TTransaction* transaction,
    TObjectBase* object,
    bool recursive)
{
    if (transaction->StagedObjects().erase(object) == 0) {
        THROW_ERROR_EXCEPTION("Object %s does not belong to transaction %s",
            ~ToString(object->GetId()),
            ~ToString(transaction->GetId()));
    }

    auto objectManager = Bootstrap->GetObjectManager();

    auto handler = objectManager->GetHandler(object);
    handler->Unstage(object, transaction, recursive);

    objectManager->UnrefObject(object);
}

void TTransactionManager::StageNode(TTransaction* transaction, TCypressNodeBase* node)
{
    auto objectManager = Bootstrap->GetObjectManager();
    transaction->StagedNodes().push_back(node);
    objectManager->RefObject(node);
}

DEFINE_METAMAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, TransactionMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
