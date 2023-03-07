#include "transaction_manager.h"
#include "private.h"
#include "config.h"
#include "transaction.h"
#include "transaction_proxy.h"

#include <yt/server/master/cell_master/automaton.h>
#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/multicell_manager.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/cypress_server/cypress_manager.h>
#include <yt/server/master/cypress_server/node.h>

#include <yt/server/lib/hive/transaction_supervisor.h>
#include <yt/server/lib/hive/transaction_lease_tracker.h>
#include <yt/server/lib/hive/transaction_manager_detail.h>

#include <yt/server/lib/hydra/composite_automaton.h>
#include <yt/server/lib/hydra/mutation.h>

#include <yt/server/master/object_server/attribute_set.h>
#include <yt/server/master/object_server/object.h>
#include <yt/server/master/object_server/type_handler_detail.h>

#include <yt/server/master/security_server/account.h>
#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/user.h>

#include <yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/transaction_client/proto/transaction_service.pb.h>
#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/id_generator.h>
#include <yt/core/misc/string.h>

#include <yt/core/ytree/attributes.h>
#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/fluent.h>

namespace NYT::NTransactionServer {

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

static const TString NullUserId("<null>");

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
    const EObjectType ObjectType_;


    virtual TCellTagList DoGetReplicationCellTags(const TTransaction* transaction) override
    {
        return transaction->ReplicatedToCellTags();
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
    explicit TImpl(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::TransactionManager)
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
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(this, EObjectType::ExternalizedTransaction));
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(this, EObjectType::ExternalizedNestedTransaction));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeValidateSecondaryMasterRegistration(
                BIND(&TImpl::OnValidateSecondaryMasterRegistration, MakeWeak(this)));
        }
    }

    TTransaction* StartTransaction(
        TTransaction* parent,
        std::vector<TTransaction*> prerequisiteTransactions,
        const TCellTagList& replicatedToCellTags,
        bool replicateStart,
        std::optional<TDuration> timeout,
        std::optional<TInstant> deadline,
        const std::optional<TString>& title,
        const IAttributeDictionary& attributes,
        TTransactionId hintId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NProfiling::TWallTimer timer;

        const auto& dynamicConfig = GetDynamicConfig();

        if (parent) {
            if (parent->GetPersistentState() != ETransactionState::Active) {
                parent->ThrowInvalidState();
            }

            if (parent->GetDepth() >= dynamicConfig->MaxTransactionDepth) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::TransactionDepthLimitReached,
                    "Transaction depth limit reached")
                    << TErrorAttribute("limit", dynamicConfig->MaxTransactionDepth);
            }
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto transactionId = objectManager->GenerateId(
            parent ? EObjectType::NestedTransaction : EObjectType::Transaction,
            hintId);

        auto transactionHolder = std::make_unique<TTransaction>(transactionId);
        auto* transaction = TransactionMap_.Insert(transactionId, std::move(transactionHolder));

        // Every active transaction has a fake reference to itself.
        YT_VERIFY(transaction->RefObject() == 1);

        if (parent) {
            transaction->SetParent(parent);
            transaction->SetDepth(parent->GetDepth() + 1);
            YT_VERIFY(parent->NestedTransactions().insert(transaction).second);
            objectManager->RefObject(transaction);
        } else {
            YT_VERIFY(TopmostTransactions_.insert(transaction).second);
        }

        transaction->SetState(ETransactionState::Active);
        transaction->ReplicatedToCellTags() = replicatedToCellTags;

        transaction->PrerequisiteTransactions() = std::move(prerequisiteTransactions);
        for (auto* prerequisiteTransaction : transaction->PrerequisiteTransactions()) {
            // NB: Duplicates are fine; prerequisite transactions may be duplicated.
            prerequisiteTransaction->DependentTransactions().insert(transaction);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        bool foreign = (CellTagFromId(transactionId) != multicellManager->GetCellTag());

        if (foreign) {
            transaction->SetForeign();
        }

        if (!foreign && timeout) {
            transaction->SetTimeout(std::min(*timeout, dynamicConfig->MaxTransactionTimeout));
        }

        if (!foreign) {
            transaction->SetDeadline(deadline);
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

        if (replicateStart && !replicatedToCellTags.empty()) {
            NTransactionServer::NProto::TReqStartTransaction startRequest;
            startRequest.set_dont_replicate(true);
            ToProto(startRequest.mutable_attributes(), attributes);
            ToProto(startRequest.mutable_hint_id(), transactionId);
            if (parent) {
                ToProto(startRequest.mutable_parent_id(), parent->GetId());
            }
            if (title) {
                startRequest.set_title(*title);
            }
            multicellManager->PostToMasters(startRequest, replicatedToCellTags);
        }

        auto time = timer.GetElapsedTime();

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Transaction started (TransactionId: %v, ParentId: %v, PrerequisiteTransactionIds: %v, "
            "ReplicatedToCellTags: %v, Timeout: %v, Deadline: %v, User: %v, Title: %v, WallTime: %v)",
            transactionId,
            GetObjectId(parent),
            MakeFormattableView(transaction->PrerequisiteTransactions(), [] (auto* builder, const auto* prerequisiteTransaction) {
                FormatValue(builder, prerequisiteTransaction->GetId(), TStringBuf());
            }),
            replicatedToCellTags,
            transaction->GetTimeout(),
            transaction->GetDeadline(),
            user->GetName(),
            title,
            time);

        securityManager->ChargeUser(user, {EUserWorkloadType::Write, 1, time});

        return transaction;
    }

    void CommitTransaction(
        TTransaction* transaction,
        TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NProfiling::TWallTimer timer;

        auto transactionId = transaction->GetId();

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::Committed) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Transaction is already committed (TransactionId: %v)",
                transactionId);
            return;
        }

        if (state != ETransactionState::Active &&
            state != ETransactionState::PersistentCommitPrepared)
        {
            transaction->ThrowInvalidState();
        }

        bool temporaryRefTimestampHolder = false;
        if (!transaction->LockedDynamicTables().empty()) {
            // Usually ref is held by chunk views in branched tables. However, if
            // all tables are empty no natural ref exist, so we have to take it here.
            temporaryRefTimestampHolder = true;
            CreateOrRefTimestampHolder(transactionId);

            SetTimestampHolderTimestamp(transactionId, commitTimestamp);
        }

        SmallVector<TTransaction*, 16> nestedTransactions(
            transaction->NestedTransactions().begin(),
            transaction->NestedTransactions().end());
        std::sort(nestedTransactions.begin(), nestedTransactions.end(), TObjectRefComparer::Compare);
        for (auto* nestedTransaction : nestedTransactions) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Aborting nested transaction on parent commit (TransactionId: %v, ParentId: %v)",
                nestedTransaction->GetId(),
                transactionId);
            AbortTransaction(nestedTransaction, true);
        }
        YT_VERIFY(transaction->NestedTransactions().empty());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        if (!transaction->ReplicatedToCellTags().empty()) {
            NProto::TReqCommitTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_commit_timestamp(commitTimestamp);
            multicellManager->PostToMasters(request, transaction->ReplicatedToCellTags());
        }

        if (!transaction->ExternalizedToCellTags().empty()) {
            NProto::TReqCommitTransaction request;
            ToProto(request.mutable_transaction_id(), MakeExternalizedTransactionId(transactionId, multicellManager->GetCellTag()));
            request.set_commit_timestamp(commitTimestamp);
            multicellManager->PostToMasters(request, transaction->ExternalizedToCellTags());
        }

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetState(ETransactionState::Committed);

        TransactionCommitted_.Fire(transaction);

        if (temporaryRefTimestampHolder) {
            UnrefTimestampHolder(transactionId);
        }

        RunCommitTransactionActions(transaction);

        if (auto* parent = transaction->GetParent()) {
            parent->ExportedObjects().insert(
                parent->ExportedObjects().end(),
                transaction->ExportedObjects().begin(),
                transaction->ExportedObjects().end());
            parent->ImportedObjects().insert(
                parent->ImportedObjects().end(),
                transaction->ImportedObjects().begin(),
                transaction->ImportedObjects().end());

            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->RecomputeTransactionAccountResourceUsage(parent);
        } else {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            for (auto* object : transaction->ImportedObjects()) {
                objectManager->UnrefObject(object);
            }
        }
        transaction->ExportedObjects().clear();
        transaction->ImportedObjects().clear();

        auto* user = transaction->Acd().GetOwner()->AsUser();

        FinishTransaction(transaction);

        auto time = timer.GetElapsedTime();

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Transaction committed (TransactionId: %v, User: %v, CommitTimestamp: %llx, WallTime: %v)",
            transactionId,
            user ? user->GetName() : NullUserId,
            commitTimestamp,
            time);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ChargeUser(user, {EUserWorkloadType::Write, 1, time});
    }

    void AbortTransaction(
        TTransaction* transaction,
        bool force,
        bool validatePermissions = true)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NProfiling::TWallTimer timer;

        auto transactionId = transaction->GetId();

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

        SmallVector<TTransaction*, 16> nestedTransactions(
            transaction->NestedTransactions().begin(),
            transaction->NestedTransactions().end());
        std::sort(nestedTransactions.begin(), nestedTransactions.end(), TObjectRefComparer::Compare);
        for (auto* nestedTransaction : nestedTransactions) {
            AbortTransaction(nestedTransaction, true, false);
        }
        YT_VERIFY(transaction->NestedTransactions().empty());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        if (!transaction->ReplicatedToCellTags().empty()) {
            NProto::TReqAbortTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_force(true);
            multicellManager->PostToMasters(request, transaction->ReplicatedToCellTags());
        }

        if (!transaction->ExternalizedToCellTags().empty()) {
            NProto::TReqAbortTransaction request;
            ToProto(request.mutable_transaction_id(), MakeExternalizedTransactionId(transactionId, multicellManager->GetCellTag()));
            request.set_force(true);
            multicellManager->PostToMasters(request, transaction->ExternalizedToCellTags());
        }

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

        auto* user = transaction->Acd().GetOwner()->AsUser();

        FinishTransaction(transaction);

        auto time = timer.GetElapsedTime();

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Transaction aborted (TransactionId: %v, User: %v, Force: %v, WallTime: %v)",
            transactionId,
            user ? user->GetName() : NullUserId,
            force,
            time);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ChargeUser(user, {EUserWorkloadType::Write, 1, time});
    }

    TTransactionId ExternalizeTransaction(TTransaction* transaction, TCellTag dstCellTag)
    {
        if (!transaction) {
            return {};
        }
        if (!transaction->IsForeign()) {
            return transaction->GetId();
        }

        SmallVector<TTransaction*, 32> transactionsToExternalize;
        {
            auto* currentTransaction = transaction;
            while (currentTransaction) {
                auto& externalizedToCellTags = currentTransaction->ExternalizedToCellTags();
                if (std::find(externalizedToCellTags.begin(), externalizedToCellTags.end(), dstCellTag) != externalizedToCellTags.end()) {
                    break;
                }
                externalizedToCellTags.push_back(dstCellTag);
                transactionsToExternalize.push_back(currentTransaction);
                currentTransaction = currentTransaction->GetParent();
            }
        }

        std::reverse(transactionsToExternalize.begin(), transactionsToExternalize.end());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        for (auto* currentTransaction : transactionsToExternalize) {
            auto transactionId = currentTransaction->GetId();
            auto externalizedTransactionId = MakeExternalizedTransactionId(transactionId, multicellManager->GetCellTag());
            auto externalizedParentTransactionId = MakeExternalizedTransactionId(GetObjectId(currentTransaction->GetParent()), multicellManager->GetCellTag());
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Externalizing transaction (TransactionId: %v, DstCellTag: %v, ExternalizedTransactionId: %v)",
                transactionId,
                dstCellTag,
                externalizedTransactionId);

            NTransactionServer::NProto::TReqStartTransaction startRequest;
            startRequest.set_dont_replicate(true);
            ToProto(startRequest.mutable_hint_id(), externalizedTransactionId);
            if (externalizedParentTransactionId) {
                ToProto(startRequest.mutable_parent_id(), externalizedParentTransactionId);
            }
            if (currentTransaction->GetTitle()) {
                startRequest.set_title(*currentTransaction->GetTitle());
            }
            multicellManager->PostToMaster(startRequest, dstCellTag);
        }

        return MakeExternalizedTransactionId(transaction->GetId(), multicellManager->GetCellTag());
    }

    TTransactionId GetNearestExternalizedTransactionAncestor(
        TTransaction* transaction,
        TCellTag dstCellTag)
    {
        if (!transaction) {
            return {};
        }
        if (!transaction->IsForeign()) {
            return transaction->GetId();
        }

        auto* currentTransaction = transaction;
        while (currentTransaction) {
            const auto& externalizedToCellTags = currentTransaction->ExternalizedToCellTags();
            if (std::find(externalizedToCellTags.begin(), externalizedToCellTags.end(), dstCellTag) != externalizedToCellTags.end()) {
                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                return MakeExternalizedTransactionId(currentTransaction->GetId(), multicellManager->GetCellTag());
            }
            currentTransaction = currentTransaction->GetParent();
        }
        return {};
    }

    TTransaction* GetTransactionOrThrow(TTransactionId transactionId)
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

    void SetTransactionTimeout(
        TTransaction* transaction,
        TDuration timeout)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        transaction->SetTimeout(timeout);

        if (IsLeader()) {
            LeaseTracker_->SetTimeout(transaction->GetId(), timeout);
        }
    }

    void StageObject(TTransaction* transaction, TObject* object)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(transaction->StagedObjects().insert(object).second);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(object);
    }

    void UnstageObject(TTransaction* transaction, TObject* object, bool recursive)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& handler = objectManager->GetHandler(object);
        handler->UnstageObject(object, recursive);

        if (transaction) {
            YT_VERIFY(transaction->StagedObjects().erase(object) == 1);
            objectManager->UnrefObject(object);
        }
    }

    void StageNode(TTransaction* transaction, TCypressNode* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(trunkNode->IsTrunk());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        transaction->StagedNodes().push_back(trunkNode);
        objectManager->RefObject(trunkNode);
    }

    void ImportObject(TTransaction* transaction, TObject* object)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        transaction->ImportedObjects().push_back(object);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(object);
        object->ImportRefObject();
    }

    void ExportObject(TTransaction* transaction, TObject* object, TCellTag destinationCellTag)
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
        TTransactionId transactionId,
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

        auto state = persistent ? transaction->GetPersistentState() : transaction->GetState();
        if (state != ETransactionState::Active) {
            return;
        }

        RunPrepareTransactionActions(transaction, persistent);

        transaction->SetState(persistent
            ? ETransactionState::PersistentCommitPrepared
            : ETransactionState::TransientCommitPrepared);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Transaction commit prepared (TransactionId: %v, Persistent: %v, PrepareTimestamp: %llx)",
            transactionId,
            persistent,
            prepareTimestamp);
    }

    void PrepareTransactionAbort(TTransactionId transactionId, bool force)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        auto state = transaction->GetState();
        if (state != ETransactionState::Active && !force) {
            transaction->ThrowInvalidState();
        }

        if (state != ETransactionState::Active) {
            return;
        }

        transaction->SetState(ETransactionState::TransientAbortPrepared);

        YT_LOG_DEBUG("Transaction abort prepared (TransactionId: %v)",
            transactionId);
    }

    void CommitTransaction(
        TTransactionId transactionId,
        TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        CommitTransaction(transaction, commitTimestamp);
    }

    void AbortTransaction(
        TTransactionId transactionId,
        bool force)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* transaction = GetTransactionOrThrow(transactionId);
        AbortTransaction(transaction, force);
    }

    void PingTransaction(
        TTransactionId transactionId,
        bool pingAncestors)
    {
        VERIFY_THREAD_AFFINITY(TrackerThread);

        LeaseTracker_->PingTransaction(transactionId, pingAncestors);
    }

    void CreateOrRefTimestampHolder(TTransactionId transactionId)
    {
        if (auto it = TimestampHolderMap_.find(transactionId)) {
            ++it->second.RefCount;
        }
        TimestampHolderMap_.emplace(transactionId, TTimestampHolder{});
    }

    void SetTimestampHolderTimestamp(TTransactionId transactionId, TTimestamp timestamp)
    {
        if (auto it = TimestampHolderMap_.find(transactionId)) {
            it->second.Timestamp = timestamp;
        }
    }

    TTimestamp GetTimestampHolderTimestamp(TTransactionId transactionId)
    {
        if (auto it = TimestampHolderMap_.find(transactionId)) {
            return it->second.Timestamp;
        }
        return NullTimestamp;
    }

    void UnrefTimestampHolder(TTransactionId transactionId)
    {
        if (auto it = TimestampHolderMap_.find(transactionId)) {
            --it->second.RefCount;
            if (it->second.RefCount == 0) {
                TimestampHolderMap_.erase(it);
            }
        }
    }

private:
    struct TTimestampHolder
    {
        TTimestamp Timestamp = NullTimestamp;
        i64 RefCount = 1;

        void Persist(NCellMaster::TPersistenceContext& context)
        {
            using ::NYT::Persist;
            Persist(context, Timestamp);
            Persist(context, RefCount);
        }
    };

    friend class TTransactionTypeHandler;

    const TTransactionLeaseTrackerPtr LeaseTracker_;

    NHydra::TEntityMap<TTransaction> TransactionMap_;

    THashMap<TTransactionId, TTimestampHolder> TimestampHolderMap_;

    // COMPAT(babenko)
    bool AddRefsFromTransactionToUsageAccounts_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
    DECLARE_THREAD_AFFINITY_SLOT(TrackerThread);


    void HydraStartTransaction(
        const TCtxStartTransactionPtr& context,
        NTransactionServer::NProto::TReqStartTransaction* request,
        NTransactionServer::NProto::TRspStartTransaction* response)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        auto* user = request->has_user_name()
            ? securityManager->GetUserByNameOrThrow(request->user_name())
            : nullptr;

        TAuthenticatedUserGuard userGuard(securityManager, user);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* schema = objectManager->GetSchema(EObjectType::Transaction);
        securityManager->ValidatePermission(schema, user, EPermission::Create);

        auto hintId = FromProto<TTransactionId>(request->hint_id());

        auto parentId = FromProto<TTransactionId>(request->parent_id());
        auto* parent = parentId ? GetTransactionOrThrow(parentId) : nullptr;

        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
        std::vector<TTransaction*> prerequisiteTransactions;
        for (auto id : prerequisiteTransactionIds) {
            auto* prerequisiteTransaction = FindTransaction(id);
            if (!IsObjectAlive(prerequisiteTransaction)) {
                THROW_ERROR_EXCEPTION(NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                    "Prerequisite check failed: transaction %v is missing",
                    id);
            }
            if (prerequisiteTransaction->GetPersistentState() != ETransactionState::Active) {
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

        auto title = request->has_title() ? std::make_optional(request->title()) : std::nullopt;

        auto timeout = FromProto<TDuration>(request->timeout());

        std::optional<TInstant> deadline;
        if (request->has_deadline()) {
            deadline = FromProto<TInstant>(request->deadline());
        }

        TCellTagList replicateToCellTags;
        if (!request->dont_replicate())  {
            replicateToCellTags = FromProto<TCellTagList>(request->replicate_to_cell_tags());
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (replicateToCellTags.empty() && multicellManager->IsPrimaryMaster()) {
                replicateToCellTags = multicellManager->GetRegisteredMasterCellTags();
            }
        }

        auto* transaction = StartTransaction(
            parent,
            prerequisiteTransactions,
            replicateToCellTags,
            /* replicateStart */ true,
            timeout,
            deadline,
            title,
            *attributes,
            hintId);

        auto id = transaction->GetId();

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

            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Transaction action registered (TransactionId: %v, ActionType: %v)",
                transactionId,
                data.Type);
        }
    }

    void HydraPrepareTransactionCommit(NProto::TReqPrepareTransactionCommit* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto prepareTimestamp = request->prepare_timestamp();

        const auto& securityManager = Bootstrap_->GetSecurityManager();

        auto* user = request->has_user_name()
            ? securityManager->GetUserByNameOrThrow(request->user_name())
            : nullptr;

        TAuthenticatedUserGuard(securityManager, user);

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


// COMPAT(shakurov)
public:
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
            YT_VERIFY(parent->NestedTransactions().erase(transaction) == 1);
            objectManager->UnrefObject(transaction);
            transaction->SetParent(nullptr);
        } else {
            YT_VERIFY(TopmostTransactions_.erase(transaction) == 1);
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
            if (!IsObjectAlive(dependentTransaction)) {
                continue;
            }
            if (dependentTransaction->GetPersistentState() != ETransactionState::Active) {
                continue;
            }
            YT_LOG_DEBUG("Aborting dependent transaction (DependentTransactionId: %v, PrerequisiteTransactionId: %v)",
                dependentTransaction->GetId(),
                transaction->GetId());
            AbortTransaction(dependentTransaction, true, false);
        }
        transaction->DependentTransactions().clear();

        transaction->SetDeadline(std::nullopt);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ResetTransactionAccountResourceUsage(transaction);

        // Kill the fake reference thus destroying the object.
        objectManager->UnrefObject(transaction);
    }
private:

    void SaveKeys(NCellMaster::TSaveContext& context)
    {
        TransactionMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context)
    {
        TransactionMap_.SaveValues(context);
        Save(context, TimestampHolderMap_);
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

        // COMPAT(savrus)
        if (context.GetVersion() >= EMasterReign::BulkInsert) {
            Load(context, TimestampHolderMap_);
        }

        // COMPAT(babenko)
        if (context.GetVersion() < EMasterReign::AddRefsFromTransactionToUsageAccounts) {
            AddRefsFromTransactionToUsageAccounts_ = true;
        }
    }


    void OnAfterSnapshotLoaded()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Reconstruct TopmostTransactions.
        TopmostTransactions_.clear();
        for (auto [id, transaction] : TransactionMap_) {
            if (IsObjectAlive(transaction) && !transaction->GetParent()) {
                YT_VERIFY(TopmostTransactions_.insert(transaction).second);
            }
        }

        // COMPAT(babenko)
        if (AddRefsFromTransactionToUsageAccounts_) {
            for (auto [id, transaction] : TransactionMap_) {
                if (transaction->GetState() == ETransactionState::Committed ||
                    transaction->GetState() == ETransactionState::Aborted)
                {
                    continue;
                }
                for (const auto& [account, usage] : transaction->AccountResourceUsage()) {
                    account->RefObject();
                }
            }
        }
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        TransactionMap_.Clear();
        TopmostTransactions_.clear();
        AddRefsFromTransactionToUsageAccounts_ = false;
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
            transaction->GetDeadline(),
            BIND(&TImpl::OnTransactionExpired, MakeStrong(this))
                .Via(hydraFacade->GetEpochAutomatonInvoker(EAutomatonThreadQueue::TransactionSupervisor)));
    }

    void CloseLease(TTransaction* transaction)
    {
        LeaseTracker_->UnregisterTransaction(transaction->GetId());
    }

    void OnTransactionExpired(TTransactionId transactionId)
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
                YT_LOG_DEBUG(error, "Error aborting expired transaction (TransactionId: %v)",
                    transactionId);
            }
        }));
    }


    void OnValidateSecondaryMasterRegistration(TCellTag cellTag)
    {
        if (TransactionMap_.GetSize() > 0) {
            THROW_ERROR_EXCEPTION("Cannot register a new secondary master %v while %v transaction(s) are present",
                cellTag,
                TransactionMap_.GetSize());
        }
    }


    const TDynamicTransactionManagerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->TransactionManager;
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTransactionManager::TImpl, Transaction, TTransaction, TransactionMap_)

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionTypeHandler::TTransactionTypeHandler(
    TImpl* owner,
    EObjectType objectType)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->TransactionMap_)
    , ObjectType_(objectType)
{ }

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TTransactionManager::~TTransactionManager()
{ }

void TTransactionManager::Initialize()
{
    Impl_->Initialize();
}

TTransaction* TTransactionManager::StartTransaction(
    TTransaction* parent,
    std::vector<TTransaction*> prerequisiteTransactions,
    const TCellTagList& replicatedToCellTags,
    bool replicateStart,
    std::optional<TDuration> timeout,
    std::optional<TInstant> deadline,
    const std::optional<TString>& title,
    const IAttributeDictionary& attributes,
    TTransactionId hintId)
{
    return Impl_->StartTransaction(
        parent,
        std::move(prerequisiteTransactions),
        replicatedToCellTags,
        replicateStart,
        timeout,
        deadline,
        title,
        attributes,
        hintId);
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

TTransactionId TTransactionManager::ExternalizeTransaction(TTransaction* transaction, TCellTag dstCellTag)
{
    return Impl_->ExternalizeTransaction(transaction, dstCellTag);
}

TTransactionId TTransactionManager::GetNearestExternalizedTransactionAncestor(
    TTransaction* transaction,
    TCellTag dstCellTag)
{
    return Impl_->GetNearestExternalizedTransactionAncestor(transaction, dstCellTag);
}

// COMPAT(shakurov)
void TTransactionManager::FinishTransaction(TTransaction* transaction)
{
    Impl_->FinishTransaction(transaction);
}

TTransaction* TTransactionManager::GetTransactionOrThrow(TTransactionId transactionId)
{
    return Impl_->GetTransactionOrThrow(transactionId);
}

TFuture<TInstant> TTransactionManager::GetLastPingTime(const TTransaction* transaction)
{
    return Impl_->GetLastPingTime(transaction);
}

void TTransactionManager::SetTransactionTimeout(
    TTransaction* transaction,
    TDuration timeout)
{
    Impl_->SetTransactionTimeout(transaction, timeout);
}

void TTransactionManager::StageObject(
    TTransaction* transaction,
    TObject* object)
{
    Impl_->StageObject(transaction, object);
}

void TTransactionManager::UnstageObject(
    TTransaction* transaction,
    TObject* object,
    bool recursive)
{
    Impl_->UnstageObject(transaction, object, recursive);
}

void TTransactionManager::StageNode(
    TTransaction* transaction,
    TCypressNode* trunkNode)
{
    Impl_->StageNode(transaction, trunkNode);
}

void TTransactionManager::ExportObject(
    TTransaction* transaction,
    TObject* object,
    TCellTag destinationCellTag)
{
    Impl_->ExportObject(transaction, object, destinationCellTag);
}

void TTransactionManager::ImportObject(
    TTransaction* transaction,
    TObject* object)
{
    Impl_->ImportObject(transaction, object);
}

void TTransactionManager::RegisterTransactionActionHandlers(
    const TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
    const TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
    const TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor)
{
    Impl_->RegisterTransactionActionHandlers(
        prepareActionDescriptor,
        commitActionDescriptor,
        abortActionDescriptor);
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
    TTransactionId transactionId,
    bool persistent,
    TTimestamp prepareTimestamp)
{
    Impl_->PrepareTransactionCommit(transactionId, persistent, prepareTimestamp);
}

void TTransactionManager::PrepareTransactionAbort(
    TTransactionId transactionId,
    bool force)
{
    Impl_->PrepareTransactionAbort(transactionId, force);
}

void TTransactionManager::CommitTransaction(
    TTransactionId transactionId,
    TTimestamp commitTimestamp)
{
    Impl_->CommitTransaction(transactionId, commitTimestamp);
}

void TTransactionManager::AbortTransaction(
    TTransactionId transactionId,
    bool force)
{
    Impl_->AbortTransaction(transactionId, force);
}

void TTransactionManager::PingTransaction(
    TTransactionId transactionId,
    bool pingAncestors)
{
    Impl_->PingTransaction(transactionId, pingAncestors);
}

void TTransactionManager::CreateOrRefTimestampHolder(TTransactionId transactionId)
{
    Impl_->CreateOrRefTimestampHolder(transactionId);
}

void TTransactionManager::SetTimestampHolderTimestamp(TTransactionId transactionId, TTimestamp timestamp)
{
    Impl_->SetTimestampHolderTimestamp(transactionId, timestamp);
}

TTimestamp TTransactionManager::GetTimestampHolderTimestamp(TTransactionId transactionId)
{
    return Impl_->GetTimestampHolderTimestamp(transactionId);
}

void TTransactionManager::UnrefTimestampHolder(TTransactionId transactionId)
{
    Impl_->UnrefTimestampHolder(transactionId);
}

DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionStarted, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionCommitted, *Impl_);
DELEGATE_SIGNAL(TTransactionManager, void(TTransaction*), TransactionAborted, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TTransactionManager, THashSet<TTransaction*>, TopmostTransactions, *Impl_);
DELEGATE_ENTITY_MAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
