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

#include <server/cell_master/serialize.h>
#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>
#include <server/cell_master/hydra_facade.h>

#include <server/security_server/account.h>
#include <server/security_server/security_manager.h>

#include <server/hive/transaction_supervisor.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCypressServer;
using namespace NHydra;
using namespace NYTree;
using namespace NYson;
using namespace NCypressServer;
using namespace NTransactionClient;
using namespace NSecurityServer;
using namespace NTransactionClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransactionProxy
    : public TNonversionedObjectProxyBase<TTransaction>
{
public:
    TTransactionProxy(NCellMaster::TBootstrap* bootstrap, TTransaction* transaction)
        : TBase(bootstrap, transaction)
    { }

private:
    typedef TNonversionedObjectProxyBase<TTransaction> TBase;

    virtual NLog::TLogger CreateLogger() const override
    {
        return TransactionServerLogger;
    }

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

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
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
                .Value(transaction->GetStartTime());
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

        return TBase::GetBuiltinAttribute(key, consumer);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransactionTypeHandler
    : public TObjectTypeHandlerWithMapBase<TTransaction>
{
public:
    explicit TTransactionTypeHandler(TTransactionManager* owner)
        : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->TransactionMap)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Transaction;
    }

    virtual TNullable<TTypeCreationOptions> GetCreationOptions() const override
    {
        return TTypeCreationOptions(
            EObjectTransactionMode::Optional,
            EObjectAccountMode::Forbidden);
    }

    virtual TNonversionedObjectBase* Create(
        TTransaction* parent,
        TAccount* /*account*/,
        IAttributeDictionary* attributes,
        TReqCreateObjects* request,
        TRspCreateObjects* /*response*/) override
    {
        const auto* requestExt = &request->GetExtension(TReqStartTransactionExt::create_transaction_ext);
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
        return Format("transaction %v", transaction->GetId());
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
    : TMasterAutomatonPart(bootstrap)
    , Config(config)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(), AutomatonThread);

    RegisterLoader(
        "TransactionManager.Keys",
        BIND(&TTransactionManager::LoadKeys, Unretained(this)));
    RegisterLoader(
        "TransactionManager.Values",
        BIND(&TTransactionManager::LoadValues, Unretained(this)));

    RegisterSaver(
        ESerializationPriority::Keys,
        "TransactionManager.Keys",
        BIND(&TTransactionManager::SaveKeys, Unretained(this)));
    RegisterSaver(
        ESerializationPriority::Values,
        "TransactionManager.Values",
        BIND(&TTransactionManager::SaveValues, Unretained(this)));
}

void TTransactionManager::Initialize()
{
    auto objectManager = Bootstrap_->GetObjectManager();
    objectManager->RegisterHandler(New<TTransactionTypeHandler>(this));
}

TTransaction* TTransactionManager::StartTransaction(TTransaction* parent, TNullable<TDuration> timeout)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto objectManager = Bootstrap_->GetObjectManager();
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

    auto* mutationContext = Bootstrap_
        ->GetHydraFacade()
        ->GetHydraManager()
        ->GetMutationContext();
    transaction->SetStartTime(mutationContext->GetTimestamp());

    TransactionStarted_.Fire(transaction);

    LOG_DEBUG_UNLESS(IsRecovery(), "Transaction started (TransactionId: %v, ParentId: %v, Timeout: %v)",
        id,
        GetObjectId(parent),
        actualTimeout);

    return transaction;
}

void TTransactionManager::CommitTransaction(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (transaction->GetState() != ETransactionState::Active &&
        transaction->GetState() != ETransactionState::TransientCommitPrepared &&
        transaction->GetState() != ETransactionState::PersistentCommitPrepared)
    {
        transaction->ThrowInvalidState();
    }

    // NB: Save it for logging.
    auto id = transaction->GetId();

    if (IsLeader()) {
        CloseLease(transaction);
    }

    transaction->SetState(ETransactionState::Committed);

    TransactionCommitted_.Fire(transaction);

    FinishTransaction(transaction);

    LOG_DEBUG_UNLESS(IsRecovery(), "Transaction committed (TransactionId: %v)",
        id);
}

void TTransactionManager::AbortTransaction(TTransaction* transaction, bool force)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (transaction->GetState() == ETransactionState::PersistentCommitPrepared && !force) {
        transaction->ThrowInvalidState();
    }

    auto securityManager = Bootstrap_->GetSecurityManager();
    securityManager->ValidatePermission(transaction, EPermission::Write);

    auto id = transaction->GetId();

    auto nestedTransactions = transaction->NestedTransactions();
    for (auto* nestedTransaction : nestedTransactions) {
        AbortTransaction(nestedTransaction, force);
    }
    YCHECK(transaction->NestedTransactions().empty());

    if (IsLeader()) {
        CloseLease(transaction);
    }

    transaction->SetState(ETransactionState::Aborted);

    TransactionAborted_.Fire(transaction);

    FinishTransaction(transaction);

    LOG_INFO_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %v, Force: %v)",
        id,
        force);
}

void TTransactionManager::FinishTransaction(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto objectManager = Bootstrap_->GetObjectManager();

    for (auto* object : transaction->StagedObjects()) {
        auto handler = objectManager->GetHandler(object);
        handler->Unstage(object, false);
        objectManager->UnrefObject(object);
    }
    transaction->StagedObjects().clear();

    for (auto* node : transaction->StagedNodes()) {
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

void TTransactionManager::PingTransaction(TTransaction* transaction, bool pingAncestors)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (transaction->GetState() != ETransactionState::Active) {
        transaction->ThrowInvalidState();
    }

    // TODO(babenko): validate permissions?

    DoPingTransaction(transaction);

    if (pingAncestors) {
        auto parentTransaction = transaction->GetParent();
        while (parentTransaction) {
            DoPingTransaction(parentTransaction);
            parentTransaction = parentTransaction->GetParent();
        }
    }
}

void TTransactionManager::DoPingTransaction(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(transaction->GetState() == ETransactionState::Active);

    auto timeout = transaction->GetTimeout();

    TLeaseManager::RenewLease(transaction->GetLease(), timeout);

    LOG_DEBUG("Transaction pinged (TransactionId: %v, Timeout: %v)",
        transaction->GetId(),
        timeout);
}

TTransaction* TTransactionManager::GetTransactionOrThrow(const TTransactionId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* transaction = FindTransaction(id);
    if (!IsObjectAlive(transaction)) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "No such transaction %v",
            id);
    }
    return transaction;
}

void TTransactionManager::SaveKeys(NCellMaster::TSaveContext& context)
{
    TransactionMap.SaveKeys(context);
}

void TTransactionManager::SaveValues(NCellMaster::TSaveContext& context)
{
    TransactionMap.SaveValues(context);
}

void TTransactionManager::OnBeforeSnapshotLoaded()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    DoClear();
}

void TTransactionManager::LoadKeys(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TransactionMap.LoadKeys(context);
}

void TTransactionManager::LoadValues(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TransactionMap.LoadValues(context);
}

void TTransactionManager::OnAfterSnapshotLoaded()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    // Reconstruct TopmostTransactions.
    TopmostTransactions_.clear();
    for (const auto& pair : TransactionMap) {
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
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    DoClear();
}

TDuration TTransactionManager::GetActualTimeout(TNullable<TDuration> timeout)
{
    return std::min(
        timeout.Get(Config->DefaultTransactionTimeout),
        Config->MaxTransactionTimeout);
}

void TTransactionManager::OnLeaderActive()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    for (const auto& pair : TransactionMap) {
        auto* transaction = pair.second;
        if (transaction->GetState() == ETransactionState::Active ||
            transaction->GetState() == ETransactionState::PersistentCommitPrepared)
        {
            auto actualTimeout = GetActualTimeout(transaction->GetTimeout());
            CreateLease(transaction, actualTimeout);
        }
    }
}

void TTransactionManager::OnStopLeading()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    // Reset all transiently prepared transactions back into active state.
    for (const auto& pair : TransactionMap) {
        auto* transaction = pair.second;
        transaction->SetState(transaction->GetPersistentState());
        CloseLease(transaction);
    }
}

void TTransactionManager::CreateLease(TTransaction* transaction, TDuration timeout)
{
    auto hydraFacade = Bootstrap_->GetHydraFacade();
    auto lease = TLeaseManager::CreateLease(
        timeout,
        BIND(&TThis::OnTransactionExpired, MakeStrong(this), transaction->GetId())
            .Via(hydraFacade->GetEpochAutomatonInvoker()));
    transaction->SetLease(lease);
}

void TTransactionManager::CloseLease(TTransaction* transaction)
{
    TLeaseManager::CloseLease(transaction->GetLease());
    transaction->SetLease(NullLease);
}

void TTransactionManager::OnTransactionExpired(const TTransactionId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* transaction = FindTransaction(id);
    if (!IsObjectAlive(transaction))
        return;
    if (transaction->GetState() != ETransactionState::Active)
        return;

    LOG_DEBUG("Transaction lease expired (TransactionId: %v)", id);

    auto transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
    transactionSupervisor->AbortTransaction(id).Subscribe(BIND([=] (const TError& error) {
        if (!error.IsOK()) {
            LOG_DEBUG(error, "Error aborting expired transaction (TransactionId: %v)",
                id);
        }
    }));
}

TTransactionPath TTransactionManager::GetTransactionPath(TTransaction* transaction) const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

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
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto objectManager = Bootstrap_->GetObjectManager();
    YCHECK(transaction->StagedObjects().insert(object).second);
    objectManager->RefObject(object);
}

void TTransactionManager::UnstageObject(TObjectBase* object, bool recursive)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto objectManager = Bootstrap_->GetObjectManager();
    auto handler = objectManager->GetHandler(object);
    auto* transaction = handler->GetStagingTransaction(object);

    handler->Unstage(object, recursive);

    if (transaction) {
        YCHECK(transaction->StagedObjects().erase(object) == 1);
        objectManager->UnrefObject(object);
    }
}

void TTransactionManager::StageNode(TTransaction* transaction, TCypressNodeBase* node)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto objectManager = Bootstrap_->GetObjectManager();
    transaction->StagedNodes().push_back(node);
    objectManager->RefObject(node);
}

void TTransactionManager::PrepareTransactionCommit(
    const TTransactionId& transactionId,
    bool persistent,
    TTimestamp prepareTimestamp)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* transaction = GetTransactionOrThrow(transactionId);
    if (transaction->GetState() != ETransactionState::Active) {
        transaction->ThrowInvalidState();
    }

    auto securityManager = Bootstrap_->GetSecurityManager();
    securityManager->ValidatePermission(transaction, EPermission::Write);

    if (!transaction->NestedTransactions().empty()) {
        THROW_ERROR_EXCEPTION("Cannot commit transaction %v since it has %v active nested transaction(s)",
            transaction->GetId(),
            transaction->NestedTransactions().size());
    }

    transaction->SetState(persistent
        ? ETransactionState::PersistentCommitPrepared
        : ETransactionState::TransientCommitPrepared);

    LOG_DEBUG_UNLESS(IsRecovery(), "Transaction commit prepared (TransactionId: %v, Persistent: %lv)",
        transactionId,
        persistent);
}

void TTransactionManager::PrepareTransactionAbort(const TTransactionId& transactionId, bool force)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* transaction = GetTransactionOrThrow(transactionId);
    if (transaction->GetState() != ETransactionState::Active && !force) {
        transaction->ThrowInvalidState();
    }

    transaction->SetState(ETransactionState::TransientAbortPrepared);

    LOG_DEBUG("Transaction abort prepared (TransactionId: %v)",
        transactionId);
}

void TTransactionManager::CommitTransaction(
    const TTransactionId& transactionId,
    TTimestamp /*commitTimestamp*/)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    // NB: Transaction must exist.
    auto* transaction = GetTransaction(transactionId);
    CommitTransaction(transaction);
}

void TTransactionManager::AbortTransaction(
    const TTransactionId& transactionId,
    bool force)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* transaction = GetTransactionOrThrow(transactionId);

    AbortTransaction(transaction, force);
}

void TTransactionManager::PingTransaction(
    const TTransactionId& transactionId,
    const NHive::NProto::TReqPingTransaction& request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& requestExt = request.GetExtension(TReqPingTransactionExt::ping_transaction_ext);
    auto* transaction = GetTransactionOrThrow(transactionId);

    PingTransaction(transaction, requestExt.ping_ancestors());
}

DEFINE_ENTITY_MAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, TransactionMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
