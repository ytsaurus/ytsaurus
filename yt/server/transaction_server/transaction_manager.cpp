#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "config.h"
#include "private.h"

#include <core/misc/string.h>
#include <core/misc/id_generator.h>
#include <core/misc/lease_manager.h>

#include <core/concurrency/thread_affinity.h>

#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/fluent.h>
#include <core/ytree/attributes.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/mutation.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/object_server/object.h>
#include <server/object_server/type_handler_detail.h>
#include <server/object_server/attribute_set.h>

#include <server/cypress_server/node.h>

#include <server/cell_master/automaton.h>
#include <server/cell_master/serialize.h>
#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/multicell_manager.h>

#include <server/security_server/account.h>
#include <server/security_server/user.h>
#include <server/security_server/security_manager.h>

#include <server/hive/transaction_supervisor.h>

#include <server/transaction_server/transaction_manager.pb.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
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

    virtual NLogging::TLogger CreateLogger() const override
    {
        return TransactionServerLogger;
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* transaction = GetThisTypedImpl();

        descriptors->push_back("state");
        descriptors->push_back(TAttributeDescriptor("timeout")
            .SetPresent(transaction->GetTimeout().HasValue()));
        descriptors->push_back("uncommitted_accounting_enabled");
        descriptors->push_back("staged_accounting_enabled");
        descriptors->push_back("parent_id");
        descriptors->push_back("start_time");
        descriptors->push_back("nested_transaction_ids");
        descriptors->push_back("staged_object_ids");
        descriptors->push_back("staged_node_ids");
        descriptors->push_back("branched_node_ids");
        descriptors->push_back("locked_node_ids");
        descriptors->push_back("lock_ids");
        descriptors->push_back("resource_usage");
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* transaction = GetThisTypedImpl();

        if (key == "state") {
            BuildYsonFluently(consumer)
                .Value(transaction->GetState());
            return true;
        }

        if (key == "timeout" && transaction->GetTimeout()) {
            BuildYsonFluently(consumer)
                .Value(*transaction->GetTimeout());
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
    explicit TTransactionTypeHandler(TImpl* owner);

    virtual EObjectReplicationFlags GetReplicationFlags() const override
    {
        return
            EObjectReplicationFlags::ReplicateCreate |
            EObjectReplicationFlags::ReplicateAttributes;
    }

    virtual TCellTag GetReplicationCellTag(const TObjectBase* /*object*/) override
    {
        return AllSecondaryMastersCellTag;
    }

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

    virtual TNonversionedObjectBase* CreateObject(
        const TObjectId& hintId,
        TTransaction* parent,
        TAccount* account,
        IAttributeDictionary* attributes,
        const TObjectCreationExtensions& extensions) override;

private:
    TImpl* const Owner_;


    virtual Stroka DoGetName(TTransaction* transaction) override
    {
        return Format("transaction %v", transaction->GetId());
    }

    virtual IObjectProxyPtr DoGetProxy(TTransaction* transaction, TTransaction* /*dummyTransaction*/) override
    {
        return New<TTransactionProxy>(Bootstrap_, transaction);
    }

    virtual TAccessControlDescriptor* DoFindAcd(TTransaction* transaction) override
    {
        return &transaction->Acd();
    }

    virtual void DoPopulateObjectReplicationRequest(
        const TTransaction* transaction,
        NObjectServer::NProto::TReqCreateForeignObject* request) override
    {
        if (transaction->GetParent()) {
            ToProto(request->mutable_transaction_id(), transaction->GetParent()->GetId());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TMasterAutomatonPart
{
public:
    //! Raised when a new transaction is started.
    DEFINE_SIGNAL(void(TTransaction*), TransactionStarted);

    //! Raised when a transaction is committed.
    DEFINE_SIGNAL(void(TTransaction*), TransactionCommitted);

    //! Raised when a transaction is aborted.
    DEFINE_SIGNAL(void(TTransaction*), TransactionAborted);

    DEFINE_BYREF_RO_PROPERTY(yhash_set<TTransaction*>, TopmostTransactions);

    DECLARE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction, TTransactionId);

public:
    TImpl(
        TTransactionManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap)
        , Config_(config)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(), AutomatonThread);

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraPrepareTransactionCommit, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraCommitTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraAbortTransaction, Unretained(this)));

        RegisterLoader(
            "TransactionManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TransactionManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TransactionManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TransactionManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
    }

    void Initialize()
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(this));

        auto multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->SubscribeSecondaryMasterRegistered(BIND(&TImpl::OnSecondaryMasterRegistered, MakeWeak(this)));
    }

    TTransaction* StartTransaction(
        TTransaction* parent,
        TNullable<TDuration> timeout,
        const TObjectId& hintId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (parent && parent->GetPersistentState() != ETransactionState::Active) {
            parent->ThrowInvalidState();
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto transactionId = objectManager->GenerateId(EObjectType::Transaction, hintId);

        auto transactionHolder = std::make_unique<TTransaction>(transactionId);
        auto* transaction = TransactionMap_.Insert(transactionId, std::move(transactionHolder));

        // Every active transaction has a fake reference to itself.
        YCHECK(transaction->RefObject() == 1);

        if (parent) {
            transaction->SetParent(parent);
            YCHECK(parent->NestedTransactions().insert(transaction).second);
            objectManager->RefObject(transaction);
        } else {
            YCHECK(TopmostTransactions_.insert(transaction).second);
        }

        transaction->SetState(ETransactionState::Active);

        if (Bootstrap_->IsPrimaryMaster()) {
            if (timeout) {
                transaction->SetTimeout(std::min(*timeout, Config_->MaxTransactionTimeout));
            }
            if (IsLeader()) {
                CreateLease(transaction);
            }
        }

        // NB: This is not quite correct for replicated transactions but we don't care.
        const auto* mutationContext = GetCurrentMutationContext();
        transaction->SetStartTime(mutationContext->GetTimestamp());

        auto securityManager = Bootstrap_->GetSecurityManager();
        transaction->Acd().SetOwner(securityManager->GetRootUser());

        TransactionStarted_.Fire(transaction);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction started (TransactionId: %v, ParentId: %v, Timeout: %v)",
            transactionId,
            GetObjectId(parent),
            transaction->GetTimeout());

        return transaction;
    }

    void CommitTransaction(
        TTransaction* transaction,
        TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::Committed)
            return;

        if (state != ETransactionState::Active &&
            state != ETransactionState::PersistentCommitPrepared)
        {
            transaction->ThrowInvalidState();
        }

        // NB: Save it for logging.
        auto transactionId = transaction->GetId();

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetState(ETransactionState::Committed);

        TransactionCommitted_.Fire(transaction);

        FinishTransaction(transaction);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction committed (TransactionId: %v, CommitTimestamp: %v)",
            transactionId,
            commitTimestamp);
    }

    void AbortTransaction(TTransaction* transaction, bool force)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::Aborted)
            return;

        if (state == ETransactionState::PersistentCommitPrepared && !force ||
            state == ETransactionState::Committed)
        {
            transaction->ThrowInvalidState();
        }

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(transaction, EPermission::Write);

        auto transactionId = transaction->GetId();

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
            transactionId,
            force);
    }

    void PingTransaction(TTransaction* transaction, bool pingAncestors)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* currentTransaction = transaction;
        while (currentTransaction) {
            if (currentTransaction->GetState() == ETransactionState::Active) {
                DoPingTransaction(currentTransaction);
            }

            if (!pingAncestors)
                break;

            currentTransaction = currentTransaction->GetParent();
        }
    }

    TTransaction* GetTransactionOrThrow(const TTransactionId& transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such transaction %v",
                transactionId);
        }
        return transaction;
    }

    void StageObject(TTransaction* transaction, TObjectBase* object)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap_->GetObjectManager();
        YCHECK(transaction->StagedObjects().insert(object).second);
        objectManager->RefObject(object);
    }

    void UnstageObject(TObjectBase* object, bool recursive)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap_->GetObjectManager();
        const auto& handler = objectManager->GetHandler(object);
        auto* transaction = handler->GetStagingTransaction(object);

        handler->UnstageObject(object, recursive);

        if (transaction) {
            YCHECK(transaction->StagedObjects().erase(object) == 1);
            objectManager->UnrefObject(object);
        }
    }

    void StageNode(TTransaction* transaction, TCypressNodeBase* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YASSERT(trunkNode->IsTrunk());

        auto objectManager = Bootstrap_->GetObjectManager();
        transaction->StagedNodes().push_back(trunkNode);
        objectManager->RefObject(trunkNode);
    }


    // ITransactionManager implementation.
    void PrepareTransactionCommit(
        const TTransactionId& transactionId,
        bool persistent,
        TTimestamp prepareTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        // Allow preparing transactions in Active and TransientCommitPrepared (for persistent mode) states.
        auto state = persistent ? transaction->GetPersistentState() : transaction->GetState();
        if (state != ETransactionState::Active &&
            (!persistent || state != ETransactionState::TransientCommitPrepared))
        {
            transaction->ThrowInvalidState();
        }

        if (!transaction->NestedTransactions().empty()) {
            THROW_ERROR_EXCEPTION("Cannot commit transaction %v since it has %v active nested transaction(s)",
                transaction->GetId(),
                transaction->NestedTransactions().size());
        }

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(transaction, EPermission::Write);

        transaction->SetState(persistent
            ? ETransactionState::PersistentCommitPrepared
            : ETransactionState::TransientCommitPrepared);

        if (state == ETransactionState::Active) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction commit prepared (TransactionId: %v, Persistent: %v)",
                transactionId,
                persistent);
        }

        if (persistent && Bootstrap_->IsPrimaryMaster()) {
            NProto::TReqPrepareTransactionCommit request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_prepare_timestamp(prepareTimestamp);
            auto multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToSecondaryMasters(request);
        }
    }

    void PrepareTransactionAbort(const TTransactionId& transactionId, bool force)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        auto state = transaction->GetState();
        if (state != ETransactionState::Active && !force) {
            transaction->ThrowInvalidState();
        }

        if (state == ETransactionState::Active) {
            transaction->SetState(ETransactionState::TransientAbortPrepared);

            LOG_DEBUG("Transaction abort prepared (TransactionId: %v)",
                transactionId);
        }
    }

    void CommitTransaction(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // NB: Transaction must exist.
        auto* transaction = GetTransaction(transactionId);
        CommitTransaction(transaction, commitTimestamp);

        if (Bootstrap_->IsPrimaryMaster()) {
            NProto::TReqCommitTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_commit_timestamp(commitTimestamp);
            auto multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToSecondaryMasters(request);
        }
    }

    void AbortTransaction(
        const TTransactionId& transactionId,
        bool force)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);

        AbortTransaction(transaction, force);

        if (Bootstrap_->IsPrimaryMaster()) {
            NProto::TReqAbortTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_force(force);
            auto multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToSecondaryMasters(request);
        }
    }

    void PingTransaction(
        const TTransactionId& transactionId,
        const NHive::NProto::TReqPingTransaction& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& requestExt = request.GetExtension(TReqPingTransactionExt::ping_transaction_ext);
        auto* transaction = GetTransactionOrThrow(transactionId);

        PingTransaction(transaction, requestExt.ping_ancestors());
    }

private:
    friend class TTransactionTypeHandler;

    const TTransactionManagerConfigPtr Config_;

    NHydra::TEntityMap<TTransactionId, TTransaction> TransactionMap_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    // Primary-secondary replication only.
    void HydraPrepareTransactionCommit(const NProto::TReqPrepareTransactionCommit& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto prepareTimestamp = request.prepare_timestamp();
        PrepareTransactionCommit(transactionId, true, prepareTimestamp);
    }

    void HydraCommitTransaction(const NProto::TReqCommitTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto commitTimestamp = request.commit_timestamp();
        CommitTransaction(transactionId, commitTimestamp);
    }

    void HydraAbortTransaction(const NProto::TReqAbortTransaction& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        bool force = request.force();
        AbortTransaction(transactionId, force);
    }


    void FinishTransaction(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap_->GetObjectManager();

        for (auto* object : transaction->StagedObjects()) {
            const auto& handler = objectManager->GetHandler(object);
            handler->UnstageObject(object, false);
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

    void DoPingTransaction(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(transaction->GetState() == ETransactionState::Active);

        auto timeout = transaction->GetTimeout();
        if (timeout) {
            TLeaseManager::RenewLease(transaction->GetLease(), *timeout);
        }

        LOG_DEBUG("Transaction pinged (TransactionId: %v, Timeout: %v)",
            transaction->GetId(),
            timeout);
    }


    void SaveKeys(NCellMaster::TSaveContext& context)
    {
        TransactionMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context)
    {
        TransactionMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TransactionMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TransactionMap_.LoadValues(context);
    }


    void OnAfterSnapshotLoaded()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Reconstruct TopmostTransactions.
        TopmostTransactions_.clear();
        for (const auto& pair : TransactionMap_) {
            auto* transaction = pair.second;
            if (IsObjectAlive(transaction) && !transaction->GetParent()) {
                YCHECK(TopmostTransactions_.insert(transaction).second);
            }
        }
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        TransactionMap_.Clear();
        TopmostTransactions_.clear();
    }


    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        for (const auto& pair : TransactionMap_) {
            auto* transaction = pair.second;
            if (transaction->GetState() == ETransactionState::Active ||
                transaction->GetState() == ETransactionState::PersistentCommitPrepared)
            {
                CreateLease(transaction);
            }
        }
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        // Reset all transiently prepared transactions back into active state.
        for (const auto& pair : TransactionMap_) {
            auto* transaction = pair.second;
            transaction->SetState(transaction->GetPersistentState());
            CloseLease(transaction);
        }
    }


    void CreateLease(TTransaction* transaction)
    {
        if (!transaction->GetTimeout())
            return;

        auto hydraFacade = Bootstrap_->GetHydraFacade();
        auto lease = TLeaseManager::CreateLease(
            *transaction->GetTimeout(),
            BIND(&TImpl::OnTransactionExpired, MakeStrong(this), transaction->GetId())
                .Via(hydraFacade->GetEpochAutomatonInvoker()));
        transaction->SetLease(lease);
    }

    void CloseLease(TTransaction* transaction)
    {
        TLeaseManager::CloseLease(transaction->GetLease());
        transaction->SetLease(NullLease);
    }

    void OnTransactionExpired(const TTransactionId& transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = FindTransaction(transactionId);
        if (!IsObjectAlive(transaction))
            return;
        if (transaction->GetState() != ETransactionState::Active)
            return;

        LOG_DEBUG("Transaction lease expired (TransactionId: %v)", transactionId);

        auto transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
        transactionSupervisor->AbortTransaction(transactionId).Subscribe(BIND([=] (const TError& error) {
            if (!error.IsOK()) {
                LOG_DEBUG(error, "Error aborting expired transaction (TransactionId: %v)",
                    transactionId);
            }
        }));
    }


    void OnSecondaryMasterRegistered(TCellTag cellTag)
    {
        // Run stable BFS to figure out the order in which transactions are to be replicated.
        auto transactions = SortTransactions(TopmostTransactions_);
        for (auto* transaction : transactions) {
            auto nestedTransactions = SortTransactions(transaction->NestedTransactions());
            transactions.insert(transactions.end(), nestedTransactions.begin(), nestedTransactions.end());
        }

        // Replicate transactions to the secondary master.
        auto objectManager = Bootstrap_->GetObjectManager();
        for (auto* transaction : transactions) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(transaction, cellTag);
        }

        // TODO(babenko): do we need to replicate prepare requests?
    }

    static std::vector<TTransaction*> SortTransactions(const yhash_set<TTransaction*>& set)
    {
        std::vector<TTransaction*> vector;
        for (auto* transaction : set) {
            if (IsObjectAlive(transaction)) {
                vector.push_back(transaction);
            }
        }
        std::sort(
            vector.begin(),
            vector.end(),
            [] (TTransaction* lhs, TTransaction* rhs) { return lhs->GetId() < rhs->GetId(); });
        return vector;
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTransactionManager::TImpl, Transaction, TTransaction, TTransactionId, TransactionMap_)

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionTypeHandler::TTransactionTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->TransactionMap_)
    , Owner_(owner)
{ }

TNonversionedObjectBase* TTransactionManager::TTransactionTypeHandler::CreateObject(
    const TObjectId& hintId,
    TTransaction* parent,
    TAccount* /*account*/,
    IAttributeDictionary* /*attributes*/,
    const TObjectCreationExtensions& extensions)
{
    const auto& requestExt = extensions.GetExtension(TTransactionCreationExt::transaction_creation_ext);
    auto timeout = TDuration::MilliSeconds(requestExt.timeout());
    auto* transaction = Owner_->StartTransaction(parent, timeout, hintId);
    transaction->SetUncommittedAccountingEnabled(requestExt.enable_uncommitted_accounting());
    transaction->SetStagedAccountingEnabled(requestExt.enable_staged_accounting());
    return transaction;
}

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TTransactionManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

void TTransactionManager::Initialize()
{
    Impl_->Initialize();
}

TTransaction* TTransactionManager::StartTransaction(
    TTransaction* parent,
    TNullable<TDuration> timeout)
{
    return Impl_->StartTransaction(parent, timeout, NullObjectId);
}

void TTransactionManager::CommitTransaction(
    TTransaction* transaction,
    TTimestamp commitTimestamp)
{
    Impl_->CommitTransaction(transaction, commitTimestamp);
}

void TTransactionManager::AbortTransaction(
    TTransaction* transaction,
    bool force)
{
    Impl_->AbortTransaction(transaction, force);
}

void TTransactionManager::PingTransaction(
    TTransaction* transaction,
    bool pingAncestors)
{
    Impl_->PingTransaction(transaction, pingAncestors);
}

TTransaction* TTransactionManager::GetTransactionOrThrow(const TTransactionId& transactionId)
{
    return Impl_->GetTransactionOrThrow(transactionId);
}

void TTransactionManager::StageObject(
    TTransaction* transaction,
    TObjectBase* object)
{
    Impl_->StageObject(transaction, object);
}

void TTransactionManager::UnstageObject(
    TObjectBase* object,
    bool recursive)
{
    Impl_->UnstageObject(object, recursive);
}

void TTransactionManager::StageNode(
    TTransaction* transaction,
    TCypressNodeBase* trunkNode)
{
    Impl_->StageNode(transaction, trunkNode);
}

void TTransactionManager::PrepareTransactionCommit(
    const TTransactionId& transactionId,
    bool persistent,
    TTimestamp prepareTimestamp)
{
    Impl_->PrepareTransactionCommit(transactionId, persistent, prepareTimestamp);
}

void TTransactionManager::PrepareTransactionAbort(const TTransactionId& transactionId, bool force)
{
    Impl_->PrepareTransactionAbort(transactionId, force);
}

void TTransactionManager::CommitTransaction(
    const TTransactionId& transactionId,
    TTimestamp commitTimestamp)
{
    Impl_->CommitTransaction(transactionId, commitTimestamp);
}

void TTransactionManager::AbortTransaction(
    const TTransactionId& transactionId,
    bool force)
{
    Impl_->AbortTransaction(transactionId, force);
}

void TTransactionManager::PingTransaction(
    const TTransactionId& transactionId,
    const NHive::NProto::TReqPingTransaction& request)
{
    Impl_->PingTransaction(transactionId, request);
}

DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionStarted, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionCommitted, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionAborted, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TTransactionManager, yhash_set<TTransaction*>, TopmostTransactions, *Impl_);
DELEGATE_ENTITY_MAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
