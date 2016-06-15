#include "transaction_manager.h"
#include "private.h"
#include "config.h"
#include "transaction.h"

#include <yt/server/cell_master/automaton.h>
#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/cypress_server/cypress_manager.h>
#include <yt/server/cypress_server/node.h>

#include <yt/server/hive/transaction_supervisor.h>
#include <yt/server/hive/transaction_lease_tracker.h>

#include <yt/server/hydra/composite_automaton.h>
#include <yt/server/hydra/mutation.h>

#include <yt/server/object_server/attribute_set.h>
#include <yt/server/object_server/object.h>
#include <yt/server/object_server/type_handler_detail.h>

#include <yt/server/security_server/account.h>
#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/user.h>

#include <yt/server/transaction_server/transaction_manager.pb.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/transaction_client/transaction_ypath.pb.h>

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
using namespace NSecurityServer;
using namespace NTransactionClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TTransactionProxy
    : public TNonversionedObjectProxyBase<TTransaction>
{
public:
    TTransactionProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction)
        : TBase(bootstrap, metadata, transaction)
    { }

private:
    typedef TNonversionedObjectProxyBase<TTransaction> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* transaction = GetThisTypedImpl();

        descriptors->push_back("state");
        descriptors->push_back("secondary_cell_tags");
        descriptors->push_back(TAttributeDescriptor("timeout")
            .SetPresent(transaction->GetTimeout().HasValue()));
        descriptors->push_back(TAttributeDescriptor("last_ping_time")
            .SetPresent(transaction->GetTimeout().HasValue()));
        descriptors->push_back(TAttributeDescriptor("title")
            .SetPresent(transaction->GetTitle().HasValue()));
        descriptors->push_back("accounting_enabled");
        descriptors->push_back("parent_id");
        descriptors->push_back("start_time");
        descriptors->push_back("nested_transaction_ids");
        descriptors->push_back("staged_object_ids");
        descriptors->push_back("exported_objects");
        descriptors->push_back("exported_object_count");
        descriptors->push_back("imported_object_ids");
        descriptors->push_back("imported_object_count");
        descriptors->push_back("staged_node_ids");
        descriptors->push_back("branched_node_ids");
        descriptors->push_back("locked_node_ids");
        descriptors->push_back("lock_ids");
        descriptors->push_back("resource_usage");
        descriptors->push_back("multicell_resource_usage");
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* transaction = GetThisTypedImpl();

        if (key == "state") {
            BuildYsonFluently(consumer)
                .Value(transaction->GetState());
            return true;
        }

        if (key == "secondary_cell_tags") {
            BuildYsonFluently(consumer)
                .Value(transaction->SecondaryCellTags());
            return true;
        }

        if (key == "timeout" && transaction->GetTimeout()) {
            BuildYsonFluently(consumer)
                .Value(*transaction->GetTimeout());
            return true;
        }

        if (key == "title" && transaction->GetTitle()) {
            BuildYsonFluently(consumer)
                .Value(*transaction->GetTitle());
            return true;
        }

        if (key == "accounting_enabled") {
            BuildYsonFluently(consumer)
                .Value(transaction->GetAccountingEnabled());
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

        if (key == "exported_objects") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->ExportedObjects(), [=] (TFluentList fluent, const TTransaction::TExportEntry& entry) {
                    fluent
                        .Item().BeginMap()
                            .Item("id").Value(entry.Object->GetId())
                            .Item("destination_cell_tag").Value(entry.DestinationCellTag)
                        .EndMap();
                });
            return true;
        }

        if (key == "exported_object_count") {
            BuildYsonFluently(consumer)
                .Value(transaction->ExportedObjects().size());
            return true;
        }

        if (key == "imported_object_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(transaction->ImportedObjects(), [=] (TFluentList fluent, const TObjectBase* object) {
                    fluent.Item().Value(object->GetId());
                });
            return true;
        }

        if (key == "imported_object_count") {
            BuildYsonFluently(consumer)
                .Value(transaction->ImportedObjects().size());
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

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(const Stroka& key) override
    {
        const auto* transaction = GetThisTypedImpl();

        if (key == "last_ping_time") {
            RequireLeader();
            return Bootstrap_
                ->GetTransactionManager()
                ->GetLastPingTime(transaction)
                 .Apply(BIND([] (TInstant value) {
                     return ConvertToYsonString(value);
                 }));
        }

        if (key == "resource_usage") {
            return GetAggregatedResourceUsageMap().Apply(BIND([] (const TAccountResourcesMap& usageMap) {
                return BuildYsonStringFluently()
                    .DoMapFor(usageMap, [=] (TFluentMap fluent, const TAccountResourcesMap::value_type& nameAndUsage) {
                        fluent
                            .Item(nameAndUsage.first)
                            .Value(nameAndUsage.second);
                    });
            }));
        }
        
        if (key == "multicell_resource_usage") {
            return GetMulticellResourceUsageMap().Apply(BIND([] (const TMulticellAccountResourcesMap& multicellUsageMap) {
                return BuildYsonStringFluently()
                    .DoMapFor(multicellUsageMap, [=] (TFluentMap fluent, const TMulticellAccountResourcesMap::value_type& cellTagAndUsageMap) {
                        fluent
                            .Item(ToString(cellTagAndUsageMap.first))
                            .DoMapFor(cellTagAndUsageMap.second, [=] (TFluentMap fluent, const TAccountResourcesMap::value_type& nameAndUsage) {
                                fluent
                                    .Item(nameAndUsage.first)
                                    .Value(nameAndUsage.second);
                            });
                    });
            }));
        }
        
        return Null;
    }

    // account name -> cluster resources
    using TAccountResourcesMap = yhash<Stroka, NSecurityServer::TClusterResources>;
    // cell tag -> account name -> cluster resources
    using TMulticellAccountResourcesMap = yhash_map<TCellTag, TAccountResourcesMap>;

    TFuture<TMulticellAccountResourcesMap> GetMulticellResourceUsageMap()
    {
        std::vector<TFuture<std::pair<TCellTag, TAccountResourcesMap>>> asyncResults;
        asyncResults.push_back(GetLocalResourcesMap(Bootstrap_->GetCellTag()));
        if (Bootstrap_->IsPrimaryMaster()) {
            for (auto cellTag : Bootstrap_->GetSecondaryCellTags()) {
                asyncResults.push_back(GetRemoteResourcesMap(cellTag));
            }
        }

        return Combine(asyncResults).Apply(BIND([] (const std::vector<std::pair<TCellTag, TAccountResourcesMap>>& results) {
            TMulticellAccountResourcesMap multicellMap;
            for (const auto& pair : results) {
                YCHECK(multicellMap.insert(pair).second);
            }
            return multicellMap;
        }));
    }

    TFuture<TAccountResourcesMap> GetAggregatedResourceUsageMap()
    {
        return GetMulticellResourceUsageMap().Apply(BIND([] (const TMulticellAccountResourcesMap& multicellMap) {
            TAccountResourcesMap aggregatedMap;
            for (const auto& cellTagAndUsageMap : multicellMap) {
                for (const auto& nameAndUsage : cellTagAndUsageMap.second) {
                    aggregatedMap[nameAndUsage.first] += nameAndUsage.second;
                }
            }
            return aggregatedMap;
        }));
    }

    TFuture<std::pair<TCellTag, TAccountResourcesMap>> GetLocalResourcesMap(TCellTag cellTag)
    {
        const auto* transaction = GetThisTypedImpl();
        TAccountResourcesMap result;
        for (const auto& pair : transaction->AccountResourceUsage()) {
            YCHECK(result.insert(std::make_pair(pair.first->GetName(), pair.second)).second);
        }
        return MakeFuture(std::make_pair(cellTag, result));
    }

    TFuture<std::pair<TCellTag, TAccountResourcesMap>> GetRemoteResourcesMap(TCellTag cellTag)
    {
        auto multicellManager = Bootstrap_->GetMulticellManager();
        auto channel = multicellManager->GetMasterChannelOrThrow(
            cellTag,
            EPeerKind::LeaderOrFollower);

        auto id = GetId();
        auto req = TYPathProxy::Get(FromObjectId(id) + "/@resource_usage");

        TObjectServiceProxy proxy(channel);
        return proxy.Execute(req).Apply(BIND([=] (const TYPathProxy::TErrorOrRspGetPtr& rspOrError) {
            if (rspOrError.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction) {
                // Transaction is missing.
                return std::make_pair(cellTag, TAccountResourcesMap());
            }
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching resource usage of transaction %v from cell %v",
                id,
                cellTag);
            const auto& rsp = rspOrError.Value();
            return std::make_pair(
                cellTag,
                ConvertTo<TAccountResourcesMap>(TYsonString(rsp->value())));
        }));
    }
};

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
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable;
    }

    virtual EObjectType GetType() const override
    {
        return ObjectType_;
    }

    virtual TNonversionedObjectBase* CreateObject(
        const TObjectId& hintId,
        IAttributeDictionary* attributes,
        const TObjectCreationExtensions& extensions) override;

private:
    TImpl* const Owner_;
    const EObjectType ObjectType_;


    virtual TCellTagList DoGetReplicationCellTags(const TTransaction* transaction) override
    {
        return transaction->SecondaryCellTags();
    }

    virtual Stroka DoGetName(const TTransaction* transaction) override
    {
        return Format("transaction %v", transaction->GetId());
    }

    virtual IObjectProxyPtr DoGetProxy(TTransaction* transaction, TTransaction* /*dummyTransaction*/) override
    {
        return New<TTransactionProxy>(Bootstrap_, &Metadata_, transaction);
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
            auto* reqExt = request->mutable_extensions()->MutableExtension(NTransactionClient::NProto::TTransactionCreationExt::transaction_creation_ext);
            ToProto(reqExt->mutable_parent_id(), transaction->GetParent()->GetId());
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

    DECLARE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction);

public:
    TImpl(
        TTransactionManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap)
        , Config_(config)
        , LeaseTracker_(New<TTransactionLeaseTracker>(
            Bootstrap_->GetHydraFacade()->GetTransactionTrackerInvoker(),
            TransactionServerLogger))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(), AutomatonThread);
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetTransactionTrackerInvoker(), TrackerThread);

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
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(this, EObjectType::Transaction));
        objectManager->RegisterHandler(New<TTransactionTypeHandler>(this, EObjectType::NestedTransaction));

        if (Bootstrap_->IsPrimaryMaster()) {
            auto multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->SubscribeValidateSecondaryMasterRegistration(
                BIND(&TImpl::OnValidateSecondaryMasterRegistration, MakeWeak(this)));
        }
    }

    TTransaction* StartTransaction(
        TTransaction* parent,
        const TCellTagList& secondaryCellTags,
        TNullable<TDuration> timeout,
        const TNullable<Stroka>& title,
        const TTransactionId& hintId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (parent && parent->GetPersistentState() != ETransactionState::Active) {
            parent->ThrowInvalidState();
        }

        auto objectManager = Bootstrap_->GetObjectManager();
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

        // NB: For transactions replicated from the primary cell the timeout is null.
        if (CellTagFromId(transactionId) == Bootstrap_->GetCellTag() && timeout) {
            transaction->SetTimeout(std::min(*timeout, Config_->MaxTransactionTimeout));
        }

        if (IsLeader()) {
            CreateLease(transaction);
        }

        transaction->SetTitle(title);

        // NB: This is not quite correct for replicated transactions but we don't care.
        const auto* mutationContext = GetCurrentMutationContext();
        transaction->SetStartTime(mutationContext->GetTimestamp());

        auto securityManager = Bootstrap_->GetSecurityManager();
        transaction->Acd().SetOwner(securityManager->GetRootUser());

        TransactionStarted_.Fire(transaction);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction started (TransactionId: %v, ParentId: %v, "
            "SecondaryCellTags: %v, Timeout: %v, Title: %v)",
            transactionId,
            GetObjectId(parent),
            transaction->SecondaryCellTags(),
            transaction->GetTimeout(),
            title);

        return transaction;
    }

    void CommitTransaction(
        TTransaction* transaction,
        TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto state = transaction->GetPersistentState();
        if (state == ETransactionState::Committed) {
            return;
        }

        if (state != ETransactionState::Active &&
            state != ETransactionState::PersistentCommitPrepared)
        {
            transaction->ThrowInvalidState();
        }

        auto transactionId = transaction->GetId();

        if (Bootstrap_->IsPrimaryMaster()) {
            NProto::TReqCommitTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_commit_timestamp(commitTimestamp);

            auto multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMasters(request, transaction->SecondaryCellTags());
        }

        SmallVector<TTransaction*, 16> nestedTransactions(transaction->NestedTransactions().begin(), transaction->NestedTransactions().end());
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
        } else {
            auto objectManager = Bootstrap_->GetObjectManager();
            for (auto* object : transaction->ImportedObjects()) {
                objectManager->UnrefObject(object);
            }
        }
        transaction->ExportedObjects().clear();
        transaction->ImportedObjects().clear();

        FinishTransaction(transaction);

        LOG_DEBUG_UNLESS(IsRecovery(), "Transaction committed (TransactionId: %v, CommitTimestamp: %v)",
            transactionId,
            commitTimestamp);
    }

    void AbortTransaction(
        TTransaction* transaction,
        bool force)
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

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(transaction, EPermission::Write);

        auto transactionId = transaction->GetId();

        if (Bootstrap_->IsPrimaryMaster()) {
            NProto::TReqAbortTransaction request;
            ToProto(request.mutable_transaction_id(), transactionId);
            request.set_force(force);

            auto multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMasters(request, transaction->SecondaryCellTags());
        }

        SmallVector<TTransaction*, 16> nestedTransactions(transaction->NestedTransactions().begin(), transaction->NestedTransactions().end());
        std::sort(nestedTransactions.begin(), nestedTransactions.end(), TObjectRefComparer::Compare);
        for (auto* nestedTransaction : nestedTransactions) {
            AbortTransaction(nestedTransaction, force);
        }
        YCHECK(transaction->NestedTransactions().empty());

        if (IsLeader()) {
            CloseLease(transaction);
        }

        transaction->SetState(ETransactionState::Aborted);

        TransactionAborted_.Fire(transaction);

        auto objectManager = Bootstrap_->GetObjectManager();
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
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(object);
    }

    void UnstageObject(TTransaction* transaction, TObjectBase* object, bool recursive)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap_->GetObjectManager();
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

        auto objectManager = Bootstrap_->GetObjectManager();
        transaction->StagedNodes().push_back(trunkNode);
        objectManager->RefObject(trunkNode);
    }

    void ImportObject(TTransaction* transaction, TObjectBase* object)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        transaction->ImportedObjects().push_back(object);
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(object);
        object->ImportRefObject();
    }

    void ExportObject(TTransaction* transaction, TObjectBase* object, TCellTag destinationCellTag)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        transaction->ExportedObjects().push_back({object, destinationCellTag});

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(object);

        const auto& handler = objectManager->GetHandler(object);
        handler->ExportObject(object, destinationCellTag);
    }


    // ITransactionManager implementation.
    void PrepareTransactionCommit(
        const TTransactionId& transactionId,
        bool persistent)
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

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(transaction, EPermission::Write);

        auto oldState = persistent ? transaction->GetPersistentState() : transaction->GetState();

        transaction->SetState(persistent
            ? ETransactionState::PersistentCommitPrepared
            : ETransactionState::TransientCommitPrepared);

        if (oldState == ETransactionState::Active) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Transaction commit prepared (TransactionId: %v, Persistent: %v)",
                transactionId,
                persistent);
        }

        if (persistent && Bootstrap_->IsPrimaryMaster()) {
            NProto::TReqPrepareTransactionCommit request;
            ToProto(request.mutable_transaction_id(), transactionId);

            auto multicellManager = Bootstrap_->GetMulticellManager();
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

    // Primary-secondary replication only.
    void HydraPrepareTransactionCommit(NProto::TReqPrepareTransactionCommit* request)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        PrepareTransactionCommit(transactionId, true);
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
        auto hydraFacade = Bootstrap_->GetHydraFacade();
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

        auto transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
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

TNonversionedObjectBase* TTransactionManager::TTransactionTypeHandler::CreateObject(
    const TObjectId& hintId,
    IAttributeDictionary* attributes,
    const TObjectCreationExtensions& extensions)
{
    TCellTagList secondaryCellTags;
    if (Bootstrap_->IsPrimaryMaster()) {
        auto multicellManager = Bootstrap_->GetMulticellManager();
        secondaryCellTags = multicellManager->GetRegisteredMasterCellTags();
    }

    const auto& requestExt = extensions.GetExtension(TTransactionCreationExt::transaction_creation_ext);
    auto timeout = FromProto<TDuration>(requestExt.timeout());

    TTransaction* parent = nullptr;
    if (requestExt.has_parent_id()) {
        auto parentId = FromProto<TTransactionId>(requestExt.parent_id());
        parent = Owner_->GetTransactionOrThrow(parentId);
    }

    auto title = attributes->Find<Stroka>("title");
    attributes->Remove("title");

    return Owner_->StartTransaction(
        parent,
        secondaryCellTags,
        timeout,
        title,
        hintId);
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
    const TCellTagList& secondaryCellTags,
    TNullable<TDuration> timeout,
    const TNullable<Stroka>& title,
    const TTransactionId& hintId)
{
    return Impl_->StartTransaction(parent, secondaryCellTags, timeout, title, hintId);
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

void TTransactionManager::PrepareTransactionCommit(
    const TTransactionId& transactionId,
    bool persistent)
{
    Impl_->PrepareTransactionCommit(transactionId, persistent);
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
DELEGATE_BYREF_RO_PROPERTY(TTransactionManager, yhash_set<TTransaction*>, TopmostTransactions, *Impl_);
DELEGATE_ENTITY_MAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
