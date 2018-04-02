#include "transaction_manager.h"
#include "private.h"
#include "config.h"
#include "transaction.h"
#include "transaction_proxy.h"

#include <yt/server/cell_master/automaton.h>
#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/cypress_server/cypress_manager.h>
#include <yt/server/cypress_server/node.h>

#include <yt/server/hive/transaction_supervisor.h>
#include <yt/server/hive/transaction_lease_tracker.h>
#include <yt/server/hive/transaction_manager_detail.h>

#include <yt/server/hydra/composite_automaton.h>
#include <yt/server/hydra/mutation.h>

#include <yt/server/object_server/attribute_set.h>
#include <yt/server/object_server/object.h>
#include <yt/server/object_server/type_handler_detail.h>

#include <yt/server/security_server/account.h>
#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/user.h>

#include <yt/server/transaction_server/transaction_manager.pb.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/transaction_client/transaction_service.pb.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/id_generator.h>
#include <yt/core/misc/string.h>

#include <yt/core/ytree/attributes.h>
#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NObjectServer;
using namespace NCypressServer;
using namespace NHydra;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NCypressServer;
using namespace NTransactionClient;
using namespace NTransactionClient::NProto;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransactionTypeHandler
    : public TObjectTypeHandlerWithMapBase<TTransaction>
{
public:
    TTransactionTypeHandler(
        TImpl* owner,
        EObjectType objectType);

    virtual ETypeFlags GetFlags() const override
    {
        return ETypeFlags::ReplicateAttributes;
    }

    virtual EObjectType GetType() const override
    {
        return ObjectType_;
    }

private:
    TImpl* const Owner_;
    const EObjectType ObjectType_;


    virtual TCellTagList DoGetReplicationCellTags(const TTransaction* transaction) override
    {
        return transaction->SecondaryCellTags();
    }

    virtual TString DoGetName(const TTransaction* transaction) override
    {
        return Format("transaction %v", transaction->GetId());
    }

    virtual IObjectProxyPtr DoGetProxy(TTransaction* transaction, TTransaction* /*dummyTransaction*/) override
    {
        return CreateTransactionProxy(Bootstrap_, &Metadata_, transaction);
    }

    virtual TAccessControlDescriptor* DoFindAcd(TTransaction* transaction) override
    {
        return &transaction->Acd();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TMasterAutomatonPart
    , public TTransactionManagerBase<TTransaction>
{
public:
    //! Raised when a new transaction is started.
    DEFINE_SIGNAL(void(TTransaction*), TransactionStarted);

    //! Raised when a transaction is committed.
    DEFINE_SIGNAL(void(TTransaction*), TransactionCommitted);

    //! Raised when a transaction is aborted.
    DEFINE_SIGNAL(void(TTransaction*), TransactionAborted);

    DEFINE_BYREF_RO_PROPERTY(THashSet<TTransaction*>, TopmostTransactions);

    DECLARE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction);

public:
    TImpl(
        TTransactionManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::TransactionManager)
        , Config_(config)
        , LeaseTracker_(New<TTransactionLeaseTracker>(
            Bootstrap_->GetHydraFacade()->GetTransactionTrackerInvoker(),
            TransactionServerLogger))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetTransactionTrackerInvoker(), TrackerThread);

        Logger = TransactionServerLogger;

        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraStartTransaction, Unretained(this)));
        TCompositeAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterTransactionActions, Unretained(this)));
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
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(this, EObjectType::Transaction));
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(this, EObjectType::NestedTransaction));

        if (Bootstrap_->IsPrimaryMaster()) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->SubscribeValidateSecondaryMasterRegistration(
                BIND(&TImpl::OnValidateSecondaryMasterRegistration, MakeWeak(this)));
        }
    }

    TTransaction* StartTransaction(
        TTransaction* parent,
        std::vector<TTransaction*> prerequisiteTransactions,
        const TCellTagList& secondaryCellTags,
        const TCellTagList& replicateToCellTags,
        TNullable<TDuration> timeout,
        const TNullable<TString>& title,
        const IAttributeDictionary& attributes,
        const TTransactionId& hintId,
        bool system)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (parent && parent->GetPersistentState() != ETransactionState::Active) {
            parent->ThrowInvalidState();
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto transactionId = objectManager->GenerateId(
            parent ? EObjectType::NestedTransaction : EObjectType::Transaction,
            hintId);

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
        transaction->SecondaryCellTags() = secondaryCellTags;
        transaction->SetSystem(system || parent && parent->GetSystem());

        transaction->PrerequisiteTransactions() = std::move(prerequisiteTransactions);
        for (auto* prerequisiteTransaction : transaction->PrerequisiteTransactions()) {
            // NB: Duplicates are fine; prerequisite transactions may be duplicated.
            prerequisiteTransaction->DependentTransactions().insert(transaction);
        }

        bool foreign = (CellTagFromId(transactionId) != Bootstrap_->GetCellTag());
        if (foreign) {
            transaction->SetForeign();
        }

        if (!foreign && timeout) {
            transaction->SetTimeout(std::min(*timeout, Config_->MaxTransactionTimeout));
        }

        if (IsLeader()) {
            CreateLease(transaction);
        }

        transaction->SetTitle(title);

        // NB: This is not quite correct for replicated transactions but we don't care.
        const auto* mutationContext = GetCurrentMutationContext();
        transaction->SetStartTime(mutationContext->GetTimestamp());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        transaction->Acd().SetOwner(user);

        objectManager->FillAttributes(transaction, attributes);

        TransactionStarted_.Fire(transaction);

        if (!replicateToCellTags.empty()) {
            NTransactionServer::NProto::TReqStartTransaction startRequest;
            ToProto(startRequest.mutable_attributes(), attributes);
            ToProto(startRequest.mutable_hint_id(), transactionId);
            if (parent) {
                ToProto(startRequest.mutable_parent_id(), parent->GetId());
            }
            if (timeout) {
                startRequest.set_timeout(ToProto<i64>(*timeout));
            }
            startRequest.set_user_name(user->GetName());
            if (title) {
                startRequest.set_title(*title);
            }
            startRequest.set_system(transaction->GetSystem());

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMasters(startRequest, replicateToCellTags);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction started (TransactionId: %v, ParentId: %v, PrerequisiteTransactionIds: %v, "
            "SecondaryCellTags: %v, Timeout: %v, Title: %v, System: %v)",
            transactionId,
            GetObjectId(parent),
            MakeFormattableRange(transaction->PrerequisiteTransactions(), [] (auto* builder, const auto* prerequisiteTransaction) {
                FormatValue(builder, prerequisiteTransaction->GetId(), TStringBuf());
            }),
            transaction->SecondaryCellTags(),
            transaction->GetTimeout(),
            title,
            transaction->GetSystem());

        return transaction;
    }

    void CommitTransaction(
        TTransaction* transaction,
        TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto transactionId = transaction->GetId();

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::Committed) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction is already committed (TransactionId: %v)",
                transactionId);
            return;
        }

        if (state != ETransactionState::Active &&
            state != ETransactionState::PersistentCommitPrepared)
        {
            transaction->ThrowInvalidState();
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            NProto::TReqCommitTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_commit_timestamp(commitTimestamp);

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMasters(request, transaction->SecondaryCellTags());
        }

        SmallVector<TTransaction*, 16> nestedTransactions(
            transaction->NestedTransactions().begin(),
            transaction->NestedTransactions().end());
        std::sort(nestedTransactions.begin(), nestedTransactions.end(), TObjectRefComparer::Compare);
        for (auto* nestedTransaction : nestedTransactions) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Aborting nested transaction on parent commit (TransactionId: %v, ParentId: %v)",
                nestedTransaction->GetId(),
                transactionId);
            AbortTransaction(nestedTransaction, true);
        }
        YCHECK(transaction->NestedTransactions().empty());

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetState(ETransactionState::Committed);

        TransactionCommitted_.Fire(transaction);

        RunCommitTransactionActions(transaction);

        auto* parent = transaction->GetParent();
        if (parent) {
            parent->ExportedObjects().insert(
                parent->ExportedObjects().end(),
                transaction->ExportedObjects().begin(),
                transaction->ExportedObjects().end());
            parent->ImportedObjects().insert(
                parent->ImportedObjects().end(),
                transaction->ImportedObjects().begin(),
                transaction->ImportedObjects().end());

            parent->RecomputeResourceUsage();
        } else {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            for (auto* object : transaction->ImportedObjects()) {
                objectManager->UnrefObject(object);
            }
        }
        transaction->ExportedObjects().clear();
        transaction->ImportedObjects().clear();

        FinishTransaction(transaction);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction committed (TransactionId: %v, CommitTimestamp: %llx)",
            transactionId,
            commitTimestamp);
    }

    void AbortTransaction(
        TTransaction* transaction,
        bool force,
        bool validatePermissions = true)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::Aborted) {
            return;
        }

        if (state == ETransactionState::PersistentCommitPrepared && !force ||
            state == ETransactionState::Committed)
        {
            transaction->ThrowInvalidState();
        }

        if (validatePermissions) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ValidatePermission(transaction, EPermission::Write);
        }

        auto transactionId = transaction->GetId();

        if (Bootstrap_->IsPrimaryMaster()) {
            NProto::TReqAbortTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_force(force);

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMasters(request, transaction->SecondaryCellTags());
        }

        SmallVector<TTransaction*, 16> nestedTransactions(
            transaction->NestedTransactions().begin(),
            transaction->NestedTransactions().end());
        std::sort(nestedTransactions.begin(), nestedTransactions.end(), TObjectRefComparer::Compare);
        for (auto* nestedTransaction : nestedTransactions) {
            AbortTransaction(nestedTransaction, force, false);
        }
        YCHECK(transaction->NestedTransactions().empty());

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetState(ETransactionState::Aborted);

        TransactionAborted_.Fire(transaction);

        RunAbortTransactionActions(transaction);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (const auto& entry : transaction->ExportedObjects()) {
            auto* object = entry.Object;
            objectManager->UnrefObject(object);
            const auto& handler = objectManager->GetHandler(object);
            handler->UnexportObject(object, entry.DestinationCellTag, 1);
        }
        for (auto* object : transaction->ImportedObjects()) {
            objectManager->UnrefObject(object);
            object->ImportUnrefObject();
        }
        transaction->ExportedObjects().clear();
        transaction->ImportedObjects().clear();

        FinishTransaction(transaction);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %v, Force: %v)",
            transactionId,
            force);
    }

    TTransaction* GetTransactionOrThrow(const TTransactionId& transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::NoSuchTransaction,
                "No such transaction %v",
                transactionId);
        }
        return transaction;
    }

    TFuture<TInstant> GetLastPingTime(const TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return LeaseTracker_->GetLastPingTime(transaction->GetId());
    }

    void StageObject(TTransaction* transaction, TObjectBase* object)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YCHECK(transaction->StagedObjects().insert(object).second);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(object);
    }

    void UnstageObject(TTransaction* transaction, TObjectBase* object, bool recursive)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& handler = objectManager->GetHandler(object);
        handler->UnstageObject(object, recursive);

        if (transaction) {
            YCHECK(transaction->StagedObjects().erase(object) == 1);
            objectManager->UnrefObject(object);
        }
    }

    void StageNode(TTransaction* transaction, TCypressNodeBase* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        Y_ASSERT(trunkNode->IsTrunk());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        transaction->StagedNodes().push_back(trunkNode);
        objectManager->RefObject(trunkNode);
    }

    void ImportObject(TTransaction* transaction, TObjectBase* object)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        transaction->ImportedObjects().push_back(object);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(object);
        object->ImportRefObject();
    }

    void ExportObject(TTransaction* transaction, TObjectBase* object, TCellTag destinationCellTag)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        transaction->ExportedObjects().push_back({object, destinationCellTag});

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(object);

        const auto& handler = objectManager->GetHandler(object);
        handler->ExportObject(object, destinationCellTag);
    }


    std::unique_ptr<TMutation> CreateStartTransactionMutation(
        TCtxStartTransactionPtr context,
        const NTransactionServer::NProto::TReqStartTransaction& request)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            request,
            &TImpl::HydraStartTransaction,
            this);
    }

    std::unique_ptr<TMutation> CreateRegisterTransactionActionsMutation(TCtxRegisterTransactionActionsPtr context)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TImpl::HydraRegisterTransactionActions,
            this);
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
        // This check applies not only to #transaction itself but also to all of its ancestors.
        {
            auto* currentTransaction = transaction;
            while (currentTransaction) {
                auto state = persistent ? currentTransaction->GetPersistentState() : currentTransaction->GetState();
                if (state != ETransactionState::Active) {
                    currentTransaction->ThrowInvalidState();
                }
                currentTransaction = currentTransaction->GetParent();
            }
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(transaction, EPermission::Write);

        auto oldState = persistent ? transaction->GetPersistentState() : transaction->GetState();
        if (oldState == ETransactionState::Active) {
            transaction->SetState(persistent
                ? ETransactionState::PersistentCommitPrepared
                : ETransactionState::TransientCommitPrepared);

            RunPrepareTransactionActions(transaction, persistent);

            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction commit prepared (TransactionId: %v, Persistent: %v, PrepareTimestamp: %llx)",
                transactionId,
                persistent,
                prepareTimestamp);
        }

        if (persistent && Bootstrap_->IsPrimaryMaster()) {
            NProto::TReqPrepareTransactionCommit request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_prepare_timestamp(prepareTimestamp);

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMasters(request, transaction->SecondaryCellTags());
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

        auto* transaction = GetTransactionOrThrow(transactionId);
        CommitTransaction(transaction, commitTimestamp);
    }

    void AbortTransaction(
        const TTransactionId& transactionId,
        bool force)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        AbortTransaction(transaction, force);
    }

    void PingTransaction(
        const TTransactionId& transactionId,
        bool pingAncestors)
    {
        VERIFY_THREAD_AFFINITY(TrackerThread);

        LeaseTracker_->PingTransaction(transactionId, pingAncestors);
    }

private:
    friend class TTransactionTypeHandler;

    const TTransactionManagerConfigPtr Config_;
    const TTransactionLeaseTrackerPtr LeaseTracker_;

    NHydra::TEntityMap<TTransaction> TransactionMap_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(TrackerThread);


    void HydraStartTransaction(
        const TCtxStartTransactionPtr& context,
        NTransactionServer::NProto::TReqStartTransaction* request,
        NTransactionServer::NProto::TRspStartTransaction* response)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetUserByNameOrThrow(request->user_name());
        TAuthenticatedUserGuard userGuard(securityManager, user);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* schema = objectManager->GetSchema(EObjectType::Transaction);
        securityManager->ValidatePermission(schema, user, EPermission::Create);

        auto hintId = FromProto<TTransactionId>(request->hint_id());

        auto parentId = FromProto<TTransactionId>(request->parent_id());
        auto* parent = parentId ? GetTransactionOrThrow(parentId) : nullptr;

        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
        std::vector<TTransaction*> prerequisiteTransactions;
        for (const auto& id : prerequisiteTransactionIds) {
            auto* prerequisiteTransaction = FindTransaction(id);
            if (!prerequisiteTransaction) {
                THROW_ERROR_EXCEPTION(NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                    "Prerequisite check failed: transaction %v is missing",
                    id);
            }
            if (prerequisiteTransaction->GetState() != ETransactionState::Active) {
                THROW_ERROR_EXCEPTION(NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                    "Prerequisite check failed: transaction %v is in %Qlv state",
                    id,
                    prerequisiteTransaction->GetState());
            }
            prerequisiteTransactions.push_back(prerequisiteTransaction);
        }

        auto attributes = request->has_attributes()
            ? FromProto(request->attributes())
            : CreateEphemeralAttributes();

        auto title = request->has_title() ? MakeNullable(request->title()) : Null;

        auto system = request->has_system() && request->system();

        auto timeout = FromProto<TDuration>(request->timeout());

        TCellTagList secondaryCellTags;
        if (Bootstrap_->IsPrimaryMaster()) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            secondaryCellTags = multicellManager->GetRegisteredMasterCellTags();
        }

        auto* transaction = StartTransaction(
            parent,
            prerequisiteTransactions,
            secondaryCellTags,
            secondaryCellTags,
            timeout,
            title,
            *attributes,
            hintId,
            system);
        const auto& id = transaction->GetId();

        if (response) {
            ToProto(response->mutable_id(), id);
        }

        if (context) {
            context->SetResponseInfo("TransactionId: %v", id);
        }
    }

    void HydraRegisterTransactionActions(
        const TCtxRegisterTransactionActionsPtr& /*context*/,
        TReqRegisterTransactionActions* request,
        TRspRegisterTransactionActions* /*response*/)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        auto* transaction = GetTransactionOrThrow(transactionId);

        auto state = transaction->GetPersistentState();
        if (state != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        for (const auto& protoData : request->actions()) {
            auto data = FromProto<TTransactionActionData>(protoData);
            transaction->Actions().push_back(data);

            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction action registered (TransactionId: %v, ActionType: %v)",
                transactionId,
                data.Type);
        }
    }


    // Primary-secondary replication only.
    void HydraPrepareTransactionCommit(NProto::TReqPrepareTransactionCommit* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto prepareTimestamp = request->prepare_timestamp();
        PrepareTransactionCommit(transactionId, true, prepareTimestamp);
    }

    void HydraCommitTransaction(NProto::TReqCommitTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto commitTimestamp = request->commit_timestamp();
        CommitTransaction(transactionId, commitTimestamp);
    }

    void HydraAbortTransaction(NProto::TReqAbortTransaction* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        bool force = request->force();
        AbortTransaction(transactionId, force);
    }


    void FinishTransaction(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& objectManager = Bootstrap_->GetObjectManager();

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

        for (auto* prerequisiteTransaction : transaction->PrerequisiteTransactions()) {
            // NB: Duplicates are fine; prerequisite transactions may be duplicated.
            prerequisiteTransaction->DependentTransactions().erase(transaction);
        }
        transaction->PrerequisiteTransactions().clear();

        SmallVector<TTransaction*, 16> dependentTransactions(
            transaction->DependentTransactions().begin(),
            transaction->DependentTransactions().end());
        std::sort(dependentTransactions.begin(), dependentTransactions.end(), TObjectRefComparer::Compare);
        for (auto* dependentTransaction : dependentTransactions) {
            LOG_DEBUG("Aborting dependent transaction (DependentTransactionId: %v, PrerequisiteTransactionId: %v)",
                dependentTransaction->GetId(),
                transaction->GetId());
            AbortTransaction(dependentTransaction, true, false);
        }
        transaction->DependentTransactions().clear();

        // Kill the fake reference thus destroying the object.
        objectManager->UnrefObject(transaction);
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

        // COMPAT(babenko)
        YCHECK(context.GetVersion() >= 400 || TransactionMap_.GetSize() == 0);
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

        LeaseTracker_->Start();
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        LeaseTracker_->Stop();

        // Reset all transiently prepared transactions back into active state.
        for (const auto& pair : TransactionMap_) {
            auto* transaction = pair.second;
            transaction->SetState(transaction->GetPersistentState());
        }
    }


    void CreateLease(TTransaction* transaction)
    {
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        LeaseTracker_->RegisterTransaction(
            transaction->GetId(),
            GetObjectId(transaction->GetParent()),
            transaction->GetTimeout(),
            BIND(&TImpl::OnTransactionExpired, MakeStrong(this))
                .Via(hydraFacade->GetEpochAutomatonInvoker(EAutomatonThreadQueue::TransactionSupervisor)));
    }

    void CloseLease(TTransaction* transaction)
    {
        LeaseTracker_->UnregisterTransaction(transaction->GetId());
    }

    void OnTransactionExpired(const TTransactionId& transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = FindTransaction(transactionId);
        if (!IsObjectAlive(transaction))
            return;
        if (transaction->GetState() != ETransactionState::Active)
            return;

        const auto& transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
        transactionSupervisor->AbortTransaction(transactionId).Subscribe(BIND([=] (const TError& error) {
            if (!error.IsOK()) {
                LOG_DEBUG(error, "Error aborting expired transaction (TransactionId: %v)",
                    transactionId);
            }
        }));
    }


    void OnValidateSecondaryMasterRegistration(TCellTag cellTag)
    {
        if (TransactionMap_.GetSize() > 0) {
            THROW_ERROR_EXCEPTION("Cannot register a new secondary master %v while %d transaction(s) are present",
                cellTag,
                TransactionMap_.GetSize());
        }
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTransactionManager::TImpl, Transaction, TTransaction, TransactionMap_)

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionTypeHandler::TTransactionTypeHandler(
    TImpl* owner,
    EObjectType objectType)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->TransactionMap_)
    , Owner_(owner)
    , ObjectType_(objectType)
{ }

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
    std::vector<TTransaction*> prerequisiteTransactions,
    const TCellTagList& secondaryCellTags,
    const TCellTagList& replicateToCellTags,
    TNullable<TDuration> timeout,
    const TNullable<TString>& title,
    const IAttributeDictionary& attributes,
    const TTransactionId& hintId,
    bool isSystem)
{
    return Impl_->StartTransaction(
        parent,
        std::move(prerequisiteTransactions),
        secondaryCellTags,
        replicateToCellTags,
        timeout,
        title,
        attributes,
        hintId,
        isSystem);
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

TTransaction* TTransactionManager::GetTransactionOrThrow(const TTransactionId& transactionId)
{
    return Impl_->GetTransactionOrThrow(transactionId);
}

TFuture<TInstant> TTransactionManager::GetLastPingTime(const TTransaction* transaction)
{
    return Impl_->GetLastPingTime(transaction);
}

void TTransactionManager::StageObject(
    TTransaction* transaction,
    TObjectBase* object)
{
    Impl_->StageObject(transaction, object);
}

void TTransactionManager::UnstageObject(
    TTransaction* transaction,
    TObjectBase* object,
    bool recursive)
{
    Impl_->UnstageObject(transaction, object, recursive);
}

void TTransactionManager::StageNode(
    TTransaction* transaction,
    TCypressNodeBase* trunkNode)
{
    Impl_->StageNode(transaction, trunkNode);
}

void TTransactionManager::ExportObject(
    TTransaction* transaction,
    TObjectBase* object,
    TCellTag destinationCellTag)
{
    Impl_->ExportObject(transaction, object, destinationCellTag);
}

void TTransactionManager::ImportObject(
    TTransaction* transaction,
    TObjectBase* object)
{
    Impl_->ImportObject(transaction, object);
}

void TTransactionManager::RegisterPrepareActionHandler(const TTransactionPrepareActionHandlerDescriptor<TTransaction>& descriptor)
{
    Impl_->RegisterPrepareActionHandler(descriptor);
}

void TTransactionManager::RegisterCommitActionHandler(const TTransactionCommitActionHandlerDescriptor<TTransaction>& descriptor)
{
    Impl_->RegisterCommitActionHandler(descriptor);
}

void TTransactionManager::RegisterAbortActionHandler(const TTransactionAbortActionHandlerDescriptor<TTransaction>& descriptor)
{
    Impl_->RegisterAbortActionHandler(descriptor);
}

std::unique_ptr<TMutation> TTransactionManager::CreateStartTransactionMutation(
    TCtxStartTransactionPtr context,
    const NTransactionServer::NProto::TReqStartTransaction& request)
{
    return Impl_->CreateStartTransactionMutation(std::move(context), request);
}

std::unique_ptr<TMutation> TTransactionManager::CreateRegisterTransactionActionsMutation(TCtxRegisterTransactionActionsPtr context)
{
    return Impl_->CreateRegisterTransactionActionsMutation(std::move(context));
}

void TTransactionManager::PrepareTransactionCommit(
    const TTransactionId& transactionId,
    bool persistent,
    TTimestamp prepareTimestamp)
{
    Impl_->PrepareTransactionCommit(transactionId, persistent, prepareTimestamp);
}

void TTransactionManager::PrepareTransactionAbort(
    const TTransactionId& transactionId,
    bool force)
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
    bool pingAncestors)
{
    Impl_->PingTransaction(transactionId, pingAncestors);
}

DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionStarted, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionCommitted, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionAborted, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TTransactionManager, THashSet<TTransaction*>, TopmostTransactions, *Impl_);
DELEGATE_ENTITY_MAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
