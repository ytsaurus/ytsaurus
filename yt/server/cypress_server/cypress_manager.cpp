#include "stdafx.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "node_proxy_detail.h"
#include "config.h"
#include "access_tracker.h"
#include "lock_proxy.h"
#include "private.h"
#include "public.h"
#include "node.h"
#include "type_handler.h"
#include "node_proxy.h"
#include "lock.h"

#include <core/misc/id_generator.h>

#include <core/concurrency/thread_affinity.h>

#include <core/ytree/ypath_service.h>
#include <core/ytree/tree_builder.h>

#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/ypath_detail.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/cypress_ypath.pb.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/journal_client/journal_ypath_proxy.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>

#include <server/object_server/object_manager.h>
#include <server/object_server/type_handler_detail.h>
#include <server/object_server/object_detail.h>

#include <server/security_server/account.h>
#include <server/security_server/group.h>
#include <server/security_server/user.h>
#include <server/security_server/security_manager.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/entity_map.h>
#include <server/hydra/mutation.h>

#include <server/cell_master/automaton.h>
#include <server/cell_master/multicell_manager.h>

#include <server/transaction_server/transaction.h>
#include <server/transaction_server/transaction_manager.h>

#include <server/cypress_server/cypress_manager.pb.h>

#include <server/journal_server/journal_node.h>

// COMPAT(babenko)
#include <server/chunk_server/chunk_owner_base.h>
#include <server/chunk_server/chunk_list.h>

namespace NYT {
namespace NCypressServer {

using namespace NCellMaster;
using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NTransactionServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NObjectServer;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NCypressClient::NProto;
// COMPAT(babenko)
using namespace NChunkServer;
using namespace NJournalClient;
using namespace NJournalServer;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TNodeFactory
    : public ICypressNodeFactory
{
public:
    TNodeFactory(
        NCellMaster::TBootstrap* bootstrap,
        TCypressManagerConfigPtr config,
        TTransaction* transaction,
        TAccount* account,
        bool preserveAccount)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , Transaction_(transaction)
        , Account_(account)
        , PreserveAccount_(preserveAccount)
    {
        YCHECK(bootstrap);
        YCHECK(account);
    }

    ~TNodeFactory()
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        for (auto* node : CreatedNodes_) {
            objectManager->UnrefObject(node);
        }
    }

    virtual IStringNodePtr CreateString() override
    {
        return CreateNode(EObjectType::StringNode)->AsString();
    }

    virtual IInt64NodePtr CreateInt64() override
    {
        return CreateNode(EObjectType::Int64Node)->AsInt64();
    }

    virtual IUint64NodePtr CreateUint64() override
    {
        return CreateNode(EObjectType::Uint64Node)->AsUint64();
    }

    virtual IDoubleNodePtr CreateDouble() override
    {
        return CreateNode(EObjectType::DoubleNode)->AsDouble();
    }

    virtual IBooleanNodePtr CreateBoolean() override
    {
        return CreateNode(EObjectType::BooleanNode)->AsBoolean();
    }

    virtual IMapNodePtr CreateMap() override
    {
        return CreateNode(EObjectType::MapNode)->AsMap();
    }

    virtual IListNodePtr CreateList() override
    {
        return CreateNode(EObjectType::ListNode)->AsList();
    }

    virtual IEntityNodePtr CreateEntity() override
    {
        THROW_ERROR_EXCEPTION("Entity nodes cannot be created inside Cypress");
    }

    virtual NTransactionServer::TTransaction* GetTransaction() override
    {
        return Transaction_;
    }

    virtual TAccount* GetNewNodeAccount() override
    {
        return Account_;
    }

    virtual TAccount* GetClonedNodeAccount(
        TCypressNodeBase* sourceNode) override
    {
        return PreserveAccount_ ? sourceNode->GetAccount() : Account_;
    }

    virtual ICypressNodeProxyPtr CreateNode(
        EObjectType type,
        IAttributeDictionary* attributes = nullptr) override
    {
        ValidateCreatedNodeType(type);

        auto* account = GetNewNodeAccount();
        account->ValidateResourceUsageIncrease(TClusterResources(0, 1, 0));

        auto cypressManager = Bootstrap_->GetCypressManager();
        auto handler = cypressManager->FindHandler(type);
        if (!handler) {
            THROW_ERROR_EXCEPTION("Unknown object type %Qlv",
                type);
        }

        std::unique_ptr<IAttributeDictionary> attributeHolder;
        if (!attributes) {
            attributeHolder = CreateEphemeralAttributes();
            attributes = attributeHolder.get();
        }

        auto externalizationMode = Config_->ExternalizationMode;
        bool isExternal = false;
        auto multicellManager = Bootstrap_->GetMulticellManager();
        if (attributes->Contains("external")) {
            isExternal = attributes->Get<bool>("external");
            attributes->Remove("external");
        } else {
            isExternal =
                externalizationMode == EExternalizationMode::Automatic &&
                Bootstrap_->IsPrimaryMaster() &&
                !multicellManager->GetRegisteredSecondaryMasterCellTags().empty() &&
                handler->IsExternalizable();
        }

        auto externalCellTag = NotReplicatedCellTag;
        if (isExternal) {
            if (!Bootstrap_->IsPrimaryMaster()) {
                THROW_ERROR_EXCEPTION("External nodes are only created at primary masters");
            }

            if (externalizationMode == EExternalizationMode::Disabled) {
                THROW_ERROR_EXCEPTION("External nodes are disabled");
            }

            if (!handler->IsExternalizable()) {
                THROW_ERROR_EXCEPTION("Type %Qlv is not externalizable",
                    handler->GetObjectType());
            }

            auto maybeExternalCellTag = attributes->Find<TCellTag>("external_cell_tag");
            if (maybeExternalCellTag) {
                externalCellTag = *maybeExternalCellTag;
                if (!multicellManager->IsRegisteredSecondaryMaster(externalCellTag)) {
                    THROW_ERROR_EXCEPTION("Unknown cell tag %v", externalCellTag);
                }
                attributes->Remove("external_cell_tag");
            } else {
                externalCellTag = multicellManager->GetLeastLoadedSecondaryMaster();
                if (externalCellTag == InvalidCellTag) {
                    THROW_ERROR_EXCEPTION("No secondary masters registered");
                }
            }
        }

        // INodeTypeHandler::Create may modify the attributes.
        std::unique_ptr<IAttributeDictionary> replicationAttributes;
        if (isExternal) {
            replicationAttributes = attributes->Clone();
        }

        auto* trunkNode = cypressManager->CreateNode(
            NullObjectId,
            externalCellTag,
            handler,
            account,
            Transaction_,
            attributes);

        RegisterCreatedNode(trunkNode);

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->FillAttributes(trunkNode, *attributes);

        cypressManager->LockNode(trunkNode, Transaction_, ELockMode::Exclusive);

        if (isExternal) {
            NProto::TReqCreateForeignNode replicationRequest;
            ToProto(replicationRequest.mutable_node_id(), trunkNode->GetId());
            if (Transaction_) {
                ToProto(replicationRequest.mutable_transaction_id(), Transaction_->GetId());
            }
            replicationRequest.set_type(static_cast<int>(type));
            ToProto(replicationRequest.mutable_node_attributes(), *replicationAttributes);
            ToProto(replicationRequest.mutable_account_id(), Account_->GetId());
            multicellManager->PostToMaster(replicationRequest, externalCellTag);
        }

        return cypressManager->GetNodeProxy(trunkNode, Transaction_);
    }

    virtual TCypressNodeBase* InstantiateNode(
        const TNodeId& id,
        TCellTag externalCellTag) override
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->InstantiateNode(id, externalCellTag);

        RegisterCreatedNode(node);

        return node;
    }

    virtual TCypressNodeBase* CloneNode(
        TCypressNodeBase* sourceNode,
        ENodeCloneMode mode) override
    {
        ValidateCreatedNodeType(sourceNode->GetType());

        auto account = GetClonedNodeAccount(sourceNode);
        account->ValidateResourceUsageIncrease(TClusterResources(0, 1, 0));

        auto cypressManager = Bootstrap_->GetCypressManager();
        auto* clonedTrunkNode = cypressManager->CloneNode(sourceNode, this, mode);
        auto* clonedNode = cypressManager->LockNode(clonedTrunkNode, Transaction_, ELockMode::Exclusive);

        // NB: No need to call RegisterCreatedNode since
        // cloning a node involves calling ICypressNodeFactory::InstantiateNode,
        // which calls RegisterCreatedNode.
        if (sourceNode->IsExternal()) {
            NProto::TReqCloneForeignNode protoRequest;
            ToProto(protoRequest.mutable_source_node_id(), sourceNode->GetId());
            if (sourceNode->GetTransaction()) {
                ToProto(protoRequest.mutable_source_transaction_id(), sourceNode->GetTransaction()->GetId());
            }
            ToProto(protoRequest.mutable_cloned_node_id(), clonedNode->GetId());
            if (clonedNode->GetTransaction()) {
                ToProto(protoRequest.mutable_cloned_transaction_id(), clonedNode->GetTransaction()->GetId());
            }
            protoRequest.set_mode(static_cast<int>(mode));
            ToProto(protoRequest.mutable_account_id(), clonedNode->GetAccount()->GetId());

            auto multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMaster(protoRequest, sourceNode->GetExternalCellTag());
        }

        return clonedTrunkNode;
    }

    virtual void Commit() override
    {
        if (Transaction_) {
            auto transactionManager = Bootstrap_->GetTransactionManager();
            for (auto* node : CreatedNodes_) {
                transactionManager->StageNode(Transaction_, node);
            }
        }
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TCypressManagerConfigPtr Config_;
    TTransaction* const Transaction_;
    TAccount* const Account_;
    const bool PreserveAccount_;

    std::vector<TCypressNodeBase*> CreatedNodes_;


    void ValidateCreatedNodeType(EObjectType type)
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        auto* schema = objectManager->GetSchema(type);

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(schema, EPermission::Create);
    }

    void RegisterCreatedNode(TCypressNodeBase* trunkNode)
    {
        YASSERT(trunkNode->IsTrunk());
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(trunkNode);
        CreatedNodes_.push_back(trunkNode);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TNodeTypeHandler
    : public TObjectTypeHandlerBase<TCypressNodeBase>
{
public:
    TNodeTypeHandler(TImpl* owner, EObjectType type);

    virtual EObjectReplicationFlags GetReplicationFlags() const override
    {
        return
            EObjectReplicationFlags::ReplicateAttributes |
            EObjectReplicationFlags::ReplicateDestroy;
    }

    virtual TCellTag GetReplicationCellTag(const TObjectBase* object) override
    {
        return static_cast<const TCypressNodeBase*>(object)->GetExternalCellTag();
    }

    virtual EObjectType GetType() const override
    {
        return Type_;
    }

    virtual TObjectBase* FindObject(const TObjectId& id) override
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->FindNode(TVersionedNodeId(id));
    }

    virtual void DestroyObject(TObjectBase* object) throw();

    virtual TNullable<TTypeCreationOptions> GetCreationOptions() const override
    {
        return TTypeCreationOptions(
            EObjectTransactionMode::Optional,
            EObjectAccountMode::Required);
    }

    virtual void ResetAllObjects() override
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        for (const auto& pair : cypressManager->Nodes()) {
            DoResetObject(pair.second);
        }
    }

private:
    TImpl* const Owner_;
    const EObjectType Type_;


    virtual Stroka DoGetName(TCypressNodeBase* node);

    virtual IObjectProxyPtr DoGetProxy(
        TCypressNodeBase* node,
        TTransaction* transaction) override
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->GetNodeProxy(node, transaction);
    }

    virtual TAccessControlDescriptor* DoFindAcd(TCypressNodeBase* node) override
    {
        return &node->GetTrunkNode()->Acd();
    }

    virtual TObjectBase* DoGetParent(TCypressNodeBase* node) override
    {
        return node->GetParent();
    }

    void DoResetObject(TCypressNodeBase* node)
    {
        node->ResetWeakRefCounter();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TLockTypeHandler
    : public TObjectTypeHandlerWithMapBase<TLock>
{
public:
    explicit TLockTypeHandler(TImpl* owner);

    virtual EObjectType GetType() const override
    {
        return EObjectType::Lock;
    }

private:
    virtual Stroka DoGetName(TLock* lock) override
    {
        return Format("lock %v", lock->GetId());
    }

    virtual IObjectProxyPtr DoGetProxy(
        TLock* lock,
        TTransaction* /*transaction*/) override
    {
        return CreateLockProxy(Bootstrap_, lock);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TYPathResolver
    : public INodeResolver
{
public:
    TYPathResolver(
        TBootstrap* bootstrap,
        TTransaction* transaction)
        : Bootstrap(bootstrap)
        , Transaction_(transaction)
    { }

    virtual INodePtr ResolvePath(const TYPath& path) override
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto* resolver = objectManager->GetObjectResolver();
        auto objectProxy = resolver->ResolvePath(path, Transaction_);
        auto* nodeProxy = dynamic_cast<ICypressNodeProxy*>(objectProxy.Get());
        if (!nodeProxy) {
            THROW_ERROR_EXCEPTION("Path %v points to a nonversioned %Qlv object instead of a node",
                path,
                TypeFromId(objectProxy->GetId()));
        }
        return nodeProxy;
    }

    virtual TYPath GetPath(INodePtr node) override
    {
        INodePtr root;
        auto path = GetNodeYPath(node, &root);

        auto* rootProxy = dynamic_cast<ICypressNodeProxy*>(root.Get());
        YCHECK(rootProxy);

        auto cypressManager = Bootstrap->GetCypressManager();
        auto rootId = cypressManager->GetRootNode()->GetId();
        return rootProxy->GetId() == rootId
            ? "/" + path
            : "?" + path;
    }

private:
    TBootstrap* const Bootstrap;
    TTransaction* const Transaction_;

};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TImpl
    : public NCellMaster::TMasterAutomatonPart
{
public:
    TImpl(
        TCypressManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap)
        , Config_(config)
        , AccessTracker_(New<TAccessTracker>(config, bootstrap))
        , NodeMap_(TNodeMapTraits(this))
    {
        auto hydraFacade = Bootstrap_->GetHydraFacade();
        VERIFY_INVOKER_THREAD_AFFINITY(hydraFacade->GetAutomatonInvoker(), AutomatonThread);

        RootNodeId_ = MakeWellKnownId(EObjectType::MapNode, Bootstrap_->GetCellTag());

        RegisterHandler(New<TStringNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TInt64NodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TUint64NodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TDoubleNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TBooleanNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TMapNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TListNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TLinkNodeTypeHandler>(Bootstrap_));
        RegisterHandler(New<TDocumentNodeTypeHandler>(Bootstrap_));

        RegisterLoader(
            "CypressManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "CypressManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "CypressManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "CypressManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        RegisterMethod(BIND(&TImpl::HydraUpdateAccessStatistics, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraCreateForeignNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraCloneForeignNode, Unretained(this)));
    }

    void Initialize()
    {
        auto transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND(
            &TImpl::OnTransactionCommitted,
            MakeStrong(this)));
        transactionManager->SubscribeTransactionAborted(BIND(
            &TImpl::OnTransactionAborted,
            MakeStrong(this)));

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TLockTypeHandler>(this));
    }


    void RegisterHandler(INodeTypeHandlerPtr handler)
    {
        // No thread affinity is given here.
        // This will be called during init-time only.
        YCHECK(handler);

        auto type = handler->GetObjectType();
        YCHECK(!TypeToHandler_[type]);
        TypeToHandler_[type] = handler;

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TNodeTypeHandler>(this, type));
    }

    INodeTypeHandlerPtr FindHandler(EObjectType type)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (type < TEnumTraits<EObjectType>::GetMinValue() || type > TEnumTraits<EObjectType>::GetMaxValue()) {
            return nullptr;
        }

        return TypeToHandler_[type];
    }

    INodeTypeHandlerPtr GetHandler(EObjectType type)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto handler = FindHandler(type);
        YCHECK(handler);
        return handler;
    }

    INodeTypeHandlerPtr GetHandler(const TCypressNodeBase* node)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetHandler(node->GetType());
    }


    ICypressNodeFactoryPtr CreateNodeFactory(
        TTransaction* transaction,
        TAccount* account,
        bool preserveAccount)
    {
        return New<TNodeFactory>(
            Bootstrap_,
            Config_,
            transaction,
            account,
            preserveAccount);
    }

    TCypressNodeBase* CreateNode(
        const TNodeId& hintId,
        TCellTag externalCellTag,
        INodeTypeHandlerPtr handler,
        TAccount* account,
        TTransaction* transaction,
        IAttributeDictionary* attributes)
    {
        YCHECK(handler);
        YCHECK(account);
        YCHECK(attributes);

        auto nodeHolder = handler->Create(
            hintId,
            externalCellTag,
            transaction,
            attributes);
        auto* node = RegisterNode(std::move(nodeHolder));

        // Set account.
        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->SetAccount(node, account);

        // Set owner.
        auto* user = securityManager->GetAuthenticatedUser();
        auto* acd = securityManager->GetAcd(node);
        acd->SetOwner(user);

        return node;
    }


    TCypressNodeBase* InstantiateNode(
        const TNodeId& id,
        TCellTag externalCellTag)
    {
        auto type = TypeFromId(id);
        auto handler = GetHandler(type);
        auto nodeHolder = handler->Instantiate(TVersionedNodeId(id), externalCellTag);
        return RegisterNode(std::move(nodeHolder));
    }

    TCypressNodeBase* CloneNode(
        TCypressNodeBase* sourceNode,
        ICypressNodeFactoryPtr factory,
        ENodeCloneMode mode)
    {
        YCHECK(sourceNode);
        YCHECK(factory);

        // Validate account access _before_ creating the actual copy.
        auto securityManager = Bootstrap_->GetSecurityManager();
        auto* account = factory->GetClonedNodeAccount(sourceNode);
        securityManager->ValidatePermission(account, EPermission::Use);

        return DoCloneNode(
            sourceNode,
            factory,
            NullObjectId,
            mode);
    }


    TCypressNodeBase* GetRootNode() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RootNode_;
    }

    TCypressNodeBase* GetNodeOrThrow(const TVersionedNodeId& id)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* node = FindNode(id);
        if (!IsObjectAlive(node)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such node %v",
                id);
        }

        return node;
    }

    INodeResolverPtr CreateResolver(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return New<TYPathResolver>(Bootstrap_, transaction);
    }

    TCypressNodeBase* FindNode(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(trunkNode->IsTrunk());

        // Fast path -- no transaction.
        if (!transaction) {
            return trunkNode;
        }

        TVersionedNodeId versionedId(trunkNode->GetId(), GetObjectId(transaction));
        return FindNode(versionedId);
    }

    TCypressNodeBase* GetVersionedNode(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(trunkNode->IsTrunk());

        auto* currentTransaction = transaction;
        while (true) {
            auto* currentNode = FindNode(trunkNode, currentTransaction);
            if (currentNode) {
                return currentNode;
            }
            currentTransaction = currentTransaction->GetParent();
        }
    }

    ICypressNodeProxyPtr GetNodeProxy(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(trunkNode->IsTrunk());

        auto handler = GetHandler(trunkNode);
        return handler->GetProxy(trunkNode, transaction);
    }


    TCypressNodeBase* LockNode(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request,
        bool recursive = false)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(trunkNode->IsTrunk());
        YCHECK(request.Mode != ELockMode::None);

        TSubtreeNodes childrenToLock;
        if (recursive) {
            YCHECK(!request.ChildKey);
            YCHECK(!request.AttributeKey);
            ListSubtreeNodes(trunkNode, transaction, true, &childrenToLock);
        } else {
            childrenToLock.push_back(trunkNode);
        }

        // Validate all potentials lock to see if we need to take at least one of them.
        // This throws an exception in case the validation fails.
        bool isMandatory = false;
        for (auto* child : childrenToLock) {
            auto* trunkChild = child->GetTrunkNode();

            bool isChildMandatory;
            auto error = CheckLock(
                trunkChild,
                transaction,
                request,
                true,
                &isChildMandatory);

            if (!error.IsOK()) {
                THROW_ERROR error;
            }

            isMandatory |= isChildMandatory;
        }

        if (!isMandatory) {
            return GetVersionedNode(trunkNode, transaction);
        }

        // Ensure deterministic order of children.
        std::sort(
            childrenToLock.begin(),
            childrenToLock.end(),
            [] (const TCypressNodeBase* lhs, const TCypressNodeBase* rhs) {
                return lhs->GetVersionedId() < rhs->GetVersionedId();
            });

        TCypressNodeBase* lockedNode = nullptr;
        for (auto* child : childrenToLock) {
            auto* lock = DoCreateLock(child, transaction, request);
            auto* lockedChild = DoAcquireLock(lock);
            if (child == trunkNode) {
                lockedNode = lockedChild;
            }
        }

        YCHECK(lockedNode);
        return lockedNode;
    }

    TLock* CreateLock(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request,
        bool waitable)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(trunkNode->IsTrunk());
        YCHECK(transaction);
        YCHECK(request.Mode != ELockMode::None);

        if (waitable && !transaction) {
            THROW_ERROR_EXCEPTION("Waitable lock requires a transaction");
        }

        // Try to lock without waiting in the queue.
        bool isMandatory;
        auto error = CheckLock(
            trunkNode,
            transaction,
            request,
            true,
            &isMandatory);

        // Is it OK?
        if (error.IsOK()) {
            if (!isMandatory) {
                return nullptr;
            }

            auto* lock = DoCreateLock(trunkNode, transaction, request);
            DoAcquireLock(lock);
            return lock;
        }

        // Should we wait?
        if (!waitable) {
            THROW_ERROR error;
        }

        // Will wait.
        YCHECK(isMandatory);
        return DoCreateLock(trunkNode, transaction, request);
    }


    void SetModified(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AccessTracker_->SetModified(trunkNode, transaction);
    }

    void SetAccessed(TCypressNodeBase* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (HydraManager_->IsLeader() || HydraManager_->IsFollower() && !HasMutationContext()) {
            AccessTracker_->SetAccessed(trunkNode);
        }
    }


    TSubtreeNodes ListSubtreeNodes(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        bool includeRoot)
    {
        TSubtreeNodes result;
        ListSubtreeNodes(trunkNode, transaction, includeRoot, &result);
        return result;
    }

    void AbortSubtreeTransactions(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction)
    {
        auto nodes = ListSubtreeNodes(trunkNode, transaction, true);

        // NB: std::set ensures stable order.
        std::set<TTransaction*> transactions;
        for (const auto* node : nodes) {
            for (auto* lock : node->AcquiredLocks()) {
                transactions.insert(lock->GetTransaction());
            }
            for (auto* lock : node->PendingLocks()) {
                transactions.insert(lock->GetTransaction());
            }
        }

        auto transactionManager = Bootstrap_->GetTransactionManager();
        for (auto* transaction : transactions) {
            transactionManager->AbortTransaction(transaction, true);
        }
    }

    void AbortSubtreeTransactions(INodePtr node)
    {
        auto cypressNode = dynamic_cast<ICypressNodeProxy*>(node.Get());
        AbortSubtreeTransactions(cypressNode->GetTrunkNode(), cypressNode->GetTransaction());
    }


    bool IsOrphaned(TCypressNodeBase* trunkNode)
    {
        auto* currentNode = trunkNode;
        while (true) {
            if (!IsObjectAlive(currentNode)) {
                return true;
            }
            if (currentNode == RootNode_) {
                return false;
            }
            currentNode = currentNode->GetParent();
        }
    }

    bool IsAlive(TCypressNodeBase* trunkNode, TTransaction* transaction)
    {
        auto hasChild = [&] (TCypressNodeBase* parentTrunkNode, TCypressNodeBase* childTrunkNode) {
            // Compute child key or index.
            auto parentOriginators = GetNodeOriginators(transaction, parentTrunkNode);
            TNullable<Stroka> key;
            for (const auto* parentNode : parentOriginators) {
                if (IsMapLikeType(parentNode->GetType())) {
                    const auto* parentMapNode = static_cast<const TMapNode*>(parentNode);
                    auto it = parentMapNode->ChildToKey().find(childTrunkNode);
                    if (it != parentMapNode->ChildToKey().end()) {
                        key = it->second;
                    }
                } else if (IsListLikeType(parentNode->GetType())) {
                    const auto* parentListNode = static_cast<const TListNode*>(parentNode);
                    auto it = parentListNode->ChildToIndex().find(childTrunkNode);
                    return it != parentListNode->ChildToIndex().end();
                } else {
                    YUNREACHABLE();
                }

                if (key) {
                    break;
                }
            }

            if (!key) {
                return false;
            }

            // Look for tombstones.
            for (const auto* parentNode : parentOriginators) {
                if (IsMapLikeType(parentNode->GetType())) {
                    const auto* parentMapNode = static_cast<const TMapNode*>(parentNode);
                    auto it = parentMapNode->KeyToChild().find(*key);
                    if (it != parentMapNode->KeyToChild().end() && it->second != childTrunkNode) {
                        return false;
                    }
                } else if (IsListLikeType(parentNode->GetType())) {
                    // Do nothing.
                } else {
                    YUNREACHABLE();
                }
            }

            return true;
        };


        auto* currentNode = trunkNode;
        while (true) {
            if (!IsObjectAlive(currentNode)) {
                return false;
            }
            if (currentNode == RootNode_) {
                return true;
            }
            auto* parentNode = currentNode->GetParent();
            if (!parentNode) {
                return false;
            }
            if (!hasChild(parentNode, currentNode)) {
                return false;
            }
            currentNode = parentNode;
        }
    }


    TCypressNodeList GetNodeOriginators(
        TTransaction* transaction,
        TCypressNodeBase* trunkNode)
    {
        YCHECK(trunkNode->IsTrunk());

        // Fast path.
        if (!transaction) {
            return TCypressNodeList(1, trunkNode);
        }

        // Slow path.
        TCypressNodeList result;
        auto* currentNode = GetVersionedNode(trunkNode, transaction);
        while (currentNode) {
            result.push_back(currentNode);
            currentNode = currentNode->GetOriginator();
        }

        return result;
    }

    TCypressNodeList GetNodeReverseOriginators(
        TTransaction* transaction,
        TCypressNodeBase* trunkNode)
    {
        auto result = GetNodeOriginators(transaction, trunkNode);
        std::reverse(result.begin(), result.end());
        return result;
    }


    void SealJournal(
        TJournalNode* trunkNode,
        const TDataStatistics* statistics)
    {
        YCHECK(trunkNode->IsTrunk());

        trunkNode->SnapshotStatistics() = statistics
            ? *statistics
            :  trunkNode->SnapshotStatistics() = trunkNode->GetChunkList()->Statistics().ToDataStatistics();

        trunkNode->SetSealed(true);

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateAccountNodeUsage(trunkNode);

        LOG_DEBUG_UNLESS(IsRecovery(), "Journal node sealed (NodeId: %v)",
            trunkNode->GetId());

        auto objectManager = Bootstrap_->GetObjectManager();
        if (objectManager->IsForeign(trunkNode)) {
            auto req = TJournalYPathProxy::Seal(FromObjectId(trunkNode->GetId()));
            *req->mutable_statistics() = trunkNode->SnapshotStatistics();

            auto multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToPrimaryMaster(req);
        }
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Node, TCypressNodeBase, TVersionedNodeId);
    DECLARE_ENTITY_MAP_ACCESSORS(Lock, TLock, TLockId);

private:
    friend class TNodeTypeHandler;
    friend class TLockTypeHandler;

    class TNodeMapTraits
    {
    public:
        explicit TNodeMapTraits(TImpl* owner);

        std::unique_ptr<TCypressNodeBase> Create(const TVersionedNodeId& id) const;

    private:
        TImpl* const Owner_;

    };

    const TCypressManagerConfigPtr Config_;

    const TAccessTrackerPtr AccessTracker_;

    NHydra::TEntityMap<TVersionedNodeId, TCypressNodeBase, TNodeMapTraits> NodeMap_;
    NHydra::TEntityMap<TLockId, TLock> LockMap_;

    TEnumIndexedVector<INodeTypeHandlerPtr, NObjectClient::EObjectType> TypeToHandler_;

    TNodeId RootNodeId_;
    TCypressNodeBase* RootNode_ = nullptr;

    // COMPAT(babenko)
    bool RecomputeChunkOwnerStatistics_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveKeys(context);
        LockMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        NodeMap_.SaveValues(context);
        LockMap_.SaveValues(context);
    }


    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NodeMap_.LoadKeys(context);
        LockMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        NodeMap_.LoadValues(context);
        LockMap_.LoadValues(context);
        // COMPAT(babenko)
        RecomputeChunkOwnerStatistics_ = (context.GetVersion() < 200);
    }


    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        NodeMap_.Clear();
        LockMap_.Clear();

        InitBuiltin();
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        auto transactionManager = Bootstrap_->GetTransactionManager();

        for (const auto& pair : NodeMap_) {
            auto* node = pair.second;

            // Reconstruct immediate ancestor sets.
            auto* parent = node->GetParent();
            if (parent) {
                YCHECK(parent->ImmediateDescendants().insert(node).second);
            }

            // Compute originators.
            if (!node->IsTrunk()) {
                auto* parentTransaction = node->GetTransaction()->GetParent();
                auto* originator = GetVersionedNode(node->GetTrunkNode(), parentTransaction);
                node->SetOriginator(originator);
            }


            // Reconstruct TrunkNode and Transaction.
            auto transactionId = node->GetVersionedId().TransactionId;
            if (transactionId) {
                node->SetTrunkNode(GetNode(TVersionedNodeId(node->GetId())));
                node->SetTransaction(transactionManager->GetTransaction(transactionId));
            }

            // Reconstruct iterators from locks to their positions in the lock list.
            for (auto it = node->AcquiredLocks().begin(); it != node->AcquiredLocks().end(); ++it) {
                auto* lock = *it;
                lock->SetLockListIterator(it);
            }
            for (auto it = node->PendingLocks().begin(); it != node->PendingLocks().end(); ++it) {
                auto* lock = *it;
                lock->SetLockListIterator(it);
            }

            // COMPAT(babenko)
            if (RecomputeChunkOwnerStatistics_ && (node->GetType() == EObjectType::Table || node->GetType() == EObjectType::Table)) {
                auto* chunkOwnerNode = static_cast<TChunkOwnerBase*>(node);
                const auto* chunkList = chunkOwnerNode->GetChunkList();
                if (chunkList) {
                    chunkOwnerNode->SnapshotStatistics() = chunkList->Statistics().ToDataStatistics();
                }
            }
        }

        InitBuiltin();
    }


    void InitBuiltin()
    {
        RootNode_ = FindNode(TVersionedNodeId(RootNodeId_));
        if (!RootNode_) {
            // Create the root.
            auto securityManager = Bootstrap_->GetSecurityManager();
            auto rootNodeHolder = std::make_unique<TMapNode>(TVersionedNodeId(RootNodeId_));
            rootNodeHolder->SetTrunkNode(rootNodeHolder.get());
            rootNodeHolder->SetAccount(securityManager->GetSysAccount());
            rootNodeHolder->Acd().SetInherit(false);
            rootNodeHolder->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetEveryoneGroup(),
                EPermission::Read));
            rootNodeHolder->Acd().SetOwner(securityManager->GetRootUser());

            RootNode_ = NodeMap_.Insert(TVersionedNodeId(RootNodeId_), std::move(rootNodeHolder));
            YCHECK(RootNode_->RefObject() == 1);
        }
    }


    virtual void OnRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnRecoveryComplete();

        AccessTracker_->Start();
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        AccessTracker_->Stop();
    }

    virtual void OnStopFollowing() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopFollowing();

        AccessTracker_->Stop();
    }


    TCypressNodeBase* RegisterNode(std::unique_ptr<TCypressNodeBase> trunkNodeHolder)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(trunkNodeHolder->IsTrunk());

        const auto& nodeId = trunkNodeHolder->GetId();
        auto* node = NodeMap_.Insert(TVersionedNodeId(nodeId), std::move(trunkNodeHolder));

        const auto* mutationContext = GetCurrentMutationContext();
        node->SetCreationTime(mutationContext->GetTimestamp());
        node->SetModificationTime(mutationContext->GetTimestamp());
        node->SetAccessTime(mutationContext->GetTimestamp());
        node->SetRevision(mutationContext->GetVersion().ToRevision());

        if (node->IsExternal()) {
            LOG_DEBUG_UNLESS(IsRecovery(), "External node registered (NodeId: %v, Type: %v, ExternalCellTag: %v)",
                node->GetId(),
                node->GetType(),
                node->GetExternalCellTag());
        } else {
            LOG_DEBUG_UNLESS(IsRecovery(), "Local node registered (NodeId: %v, Type: %v)",
                node->GetId(),
                node->GetType());
        }

        return node;
    }

    void DestroyNode(TCypressNodeBase* trunkNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(trunkNode->IsTrunk());

        NodeMap_.Release(trunkNode->GetVersionedId()).release();

        TCypressNodeBase::TLockList acquiredLocks;
        trunkNode->AcquiredLocks().swap(acquiredLocks);

        TCypressNodeBase::TLockList pendingLocks;
        trunkNode->PendingLocks().swap(pendingLocks);

        TCypressNodeBase::TLockStateMap lockStateMap;
        trunkNode->LockStateMap().swap(lockStateMap);

        auto objectManager = Bootstrap_->GetObjectManager();

        for (auto* lock : acquiredLocks) {
            lock->SetTrunkNode(nullptr);
        }

        for (auto* lock : pendingLocks) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Lock orphaned (LockId: %v)",
                lock->GetId());
            lock->SetTrunkNode(nullptr);
            auto* transaction = lock->GetTransaction();
            YCHECK(transaction->Locks().erase(lock) == 1);
            lock->SetTransaction(nullptr);
            objectManager->UnrefObject(lock);
        }

        for (const auto& pair : lockStateMap) {
            auto* transaction = pair.first;
            YCHECK(transaction->LockedNodes().erase(trunkNode) == 1);
        }

        auto handler = GetHandler(trunkNode);
        handler->Destroy(trunkNode);
    }


    void OnTransactionCommitted(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        MergeNodes(transaction);
        ReleaseLocks(transaction);
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        RemoveBranchedNodes(transaction);
        ReleaseLocks(transaction);
    }


    TError CheckLock(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request,
        bool checkPending,
        bool* isMandatory)
    {
        YCHECK(trunkNode->IsTrunk());

        *isMandatory = true;

        // Snapshot locks can only be taken inside a transaction.
        if (request.Mode == ELockMode::Snapshot && !transaction) {
            return TError("%Qlv lock requires a transaction",
                request.Mode);
        }

        // Check for conflicts with other transactions.
        for (const auto& pair : trunkNode->LockStateMap()) {
            auto* existingTransaction = pair.first;
            const auto& existingState = pair.second;

            // Skip same transaction.
            if (existingTransaction == transaction)
                continue;

            // Ignore other Snapshot locks.
            if (existingState.Mode == ELockMode::Snapshot)
                continue;

            if (!transaction || IsConcurrentTransaction(transaction, existingTransaction)) {
                // For Exclusive locks we check locks held by concurrent transactions.
                if ((request.Mode == ELockMode::Exclusive && existingState.Mode != ELockMode::Snapshot) ||
                    (existingState.Mode == ELockMode::Exclusive && request.Mode != ELockMode::Snapshot))
                {
                    return TError(
                        NCypressClient::EErrorCode::ConcurrentTransactionLockConflict,
                        "Cannot take %Qlv lock for node %v since %Qlv lock is taken by concurrent transaction %v",
                        request.Mode,
                        GetNodePath(trunkNode, transaction),
                        existingState.Mode,
                        existingTransaction->GetId());
                }

                // For Shared locks we check child and attribute keys.
                if (request.Mode == ELockMode::Shared && existingState.Mode == ELockMode::Shared) {
                    if (request.ChildKey &&
                        existingState.ChildKeys.find(request.ChildKey.Get()) != existingState.ChildKeys.end())
                    {
                        return TError(
                            NCypressClient::EErrorCode::ConcurrentTransactionLockConflict,
                            "Cannot take %Qlv lock for child %Qv of node %v since %Qlv lock is taken by concurrent transaction %v",
                            request.Mode,
                            request.ChildKey.Get(),
                            GetNodePath(trunkNode, transaction),
                            existingState.Mode,
                            existingTransaction->GetId());
                    }
                    if (request.AttributeKey &&
                        existingState.AttributeKeys.find(request.AttributeKey.Get()) != existingState.AttributeKeys.end())
                    {
                        return TError(
                            NCypressClient::EErrorCode::ConcurrentTransactionLockConflict,
                            "Cannot take %Qlv lock for attribute %Qv of node %v since %Qlv lock is taken by concurrent transaction %v",
                            request.Mode,
                            request.AttributeKey.Get(),
                            GetNodePath(trunkNode, transaction),
                            existingState.Mode,
                            existingTransaction->GetId());
                    }
                }
            }
        }

        // Examine existing locks.
        // A quick check: same transaction, same or weaker lock mode (beware of Snapshot!).
        {
            auto it = trunkNode->LockStateMap().find(transaction);
            if (it != trunkNode->LockStateMap().end()) {
                const auto& existingState = it->second;
                if (IsRedundantLockRequest(existingState, request)) {
                    *isMandatory = false;
                    return TError();
                }
                if (existingState.Mode == ELockMode::Snapshot) {
                    return TError(
                        NCypressClient::EErrorCode::SameTransactionLockConflict,
                        "Cannot take %Qlv lock for node %v since %Qlv lock is already taken by the same transaction",
                        request.Mode,
                        GetNodePath(trunkNode, transaction),
                        existingState.Mode);
                }
            }
        }

        // If we're outside of a transaction then the lock is not needed.
        if (!transaction) {
            *isMandatory = false;
        }

        // Check pending locks.
        if (request.Mode != ELockMode::Snapshot && checkPending && !trunkNode->PendingLocks().empty()) {
            return TError(
                NCypressClient::EErrorCode::PendingLockConflict,
                "Cannot take %Qlv lock for node %v since there are %v pending lock(s) for this node",
                request.Mode,
                GetNodePath(trunkNode, transaction),
                trunkNode->PendingLocks().size());
        }

        return TError();
    }

    bool IsRedundantLockRequest(
        const TTransactionLockState& state,
        const TLockRequest& request)
    {
        if (state.Mode == ELockMode::Snapshot && request.Mode == ELockMode::Snapshot) {
            return true;
        }

        if (state.Mode > request.Mode && request.Mode != ELockMode::Snapshot) {
            return true;
        }

        if (state.Mode == request.Mode) {
            if (request.Mode == ELockMode::Shared) {
                if (request.ChildKey &&
                    state.ChildKeys.find(request.ChildKey.Get()) == state.ChildKeys.end())
                {
                    return false;
                }
                if (request.AttributeKey &&
                    state.AttributeKeys.find(request.AttributeKey.Get()) == state.AttributeKeys.end())
                {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    bool IsParentTransaction(
        TTransaction* transaction,
        TTransaction* parent)
    {
        auto currentTransaction = transaction;
        while (currentTransaction) {
            if (currentTransaction == parent) {
                return true;
            }
            currentTransaction = currentTransaction->GetParent();
        }
        return false;
    }

    bool IsConcurrentTransaction(
        TTransaction* requestingTransaction,
        TTransaction* existingTransaction)
    {
        return !IsParentTransaction(requestingTransaction, existingTransaction);
    }

    TCypressNodeBase* DoAcquireLock(TLock* lock)
    {
        auto* trunkNode = lock->GetTrunkNode();
        auto* transaction = lock->GetTransaction();
        const auto& request = lock->Request();

        LOG_DEBUG_UNLESS(IsRecovery(), "Lock acquired (LockId: %v)",
            lock->GetId());

        YCHECK(lock->GetState() == ELockState::Pending);
        lock->SetState(ELockState::Acquired);

        trunkNode->PendingLocks().erase(lock->GetLockListIterator());
        trunkNode->AcquiredLocks().push_back(lock);
        lock->SetLockListIterator(--trunkNode->AcquiredLocks().end());

        UpdateNodeLockState(trunkNode, transaction, request);

        // Upgrade locks held by parent transactions, if needed.
        if (request.Mode != ELockMode::Snapshot) {
            auto* currentTransaction = transaction->GetParent();
            while (currentTransaction) {
                UpdateNodeLockState(trunkNode, currentTransaction, request);
                currentTransaction = currentTransaction->GetParent();
            }
        }

        // Branch node, if needed.
        auto* branchedNode = FindNode(trunkNode, transaction);
        if (branchedNode) {
            if (branchedNode->GetLockMode() < request.Mode) {
                branchedNode->SetLockMode(request.Mode);
            }
            return branchedNode;
        }

        TCypressNodeBase* originatingNode;
        std::vector<TTransaction*> intermediateTransactions;
        // Walk up to the root, find originatingNode, construct the list of
        // intermediate transactions.
        auto* currentTransaction = transaction;
        while (true) {
            originatingNode = FindNode(trunkNode, currentTransaction);
            if (originatingNode) {
                break;
            }
            if (!currentTransaction) {
                break;
            }
            intermediateTransactions.push_back(currentTransaction);
            currentTransaction = currentTransaction->GetParent();
        }

        YCHECK(originatingNode);
        YCHECK(!intermediateTransactions.empty());

        if (request.Mode == ELockMode::Snapshot) {
            // Branch at requested transaction only.
            return BranchNode(originatingNode, transaction, request.Mode);
        } else {
            // Branch at all intermediate transactions.
            std::reverse(intermediateTransactions.begin(), intermediateTransactions.end());
            auto* currentNode = originatingNode;
            for (auto* transactionToBranch : intermediateTransactions) {
                currentNode = BranchNode(currentNode, transactionToBranch, request.Mode);
            }
            return currentNode;
        }
    }

    void UpdateNodeLockState(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request)
    {
        YCHECK(trunkNode->IsTrunk());

        TVersionedNodeId versionedId(trunkNode->GetId(), transaction->GetId());
        TTransactionLockState* lockState;
        auto it = trunkNode->LockStateMap().find(transaction);
        if (it == trunkNode->LockStateMap().end()) {
            lockState = &trunkNode->LockStateMap()[transaction];
            lockState->Mode = request.Mode;
            YCHECK(transaction->LockedNodes().insert(trunkNode).second);

            LOG_DEBUG_UNLESS(IsRecovery(), "Node locked (NodeId: %v, Mode: %v)",
                versionedId,
                request.Mode);
        } else {
            lockState = &it->second;
            if (lockState->Mode < request.Mode) {
                lockState->Mode = request.Mode;

                LOG_DEBUG_UNLESS(IsRecovery(), "Node lock upgraded (NodeId: %v, Mode: %v)",
                    versionedId,
                    lockState->Mode);
            }
        }

        if (request.ChildKey &&
            lockState->ChildKeys.find(request.ChildKey.Get()) == lockState->ChildKeys.end())
        {
            YCHECK(lockState->ChildKeys.insert(request.ChildKey.Get()).second);
            LOG_DEBUG_UNLESS(IsRecovery(), "Node child locked (NodeId: %v, Key: %v)",
                versionedId,
                request.ChildKey.Get());
        }

        if (request.AttributeKey &&
            lockState->AttributeKeys.find(request.AttributeKey.Get()) == lockState->AttributeKeys.end())
        {
            YCHECK(lockState->AttributeKeys.insert(request.AttributeKey.Get()).second);
            LOG_DEBUG_UNLESS(IsRecovery(), "Node attribute locked (NodeId: %v, Key: %v)",
                versionedId,
                request.AttributeKey.Get());
        }
    }

    TLock* DoCreateLock(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        const TLockRequest& request)
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Lock, NullObjectId);
        auto lockHolder = std::make_unique<TLock>(id);
        lockHolder->SetState(ELockState::Pending);
        lockHolder->SetTrunkNode(trunkNode);
        lockHolder->SetTransaction(transaction);
        lockHolder->Request() = request;
        trunkNode->PendingLocks().push_back(lockHolder.get());
        lockHolder->SetLockListIterator(--trunkNode->PendingLocks().end());
        auto* lock = LockMap_.Insert(id, std::move(lockHolder));

        YCHECK(transaction->Locks().insert(lock).second);
        objectManager->RefObject(lock);

        LOG_DEBUG_UNLESS(IsRecovery(), "Lock created (LockId: %v, Mode: %v, NodeId: %v)",
            id,
            request.Mode,
            TVersionedNodeId(trunkNode->GetId(), transaction->GetId()));

        return lock;
    }

    void ReleaseLocks(TTransaction* transaction)
    {
        auto* parentTransaction = transaction->GetParent();
        auto objectManager = Bootstrap_->GetObjectManager();

        TTransaction::TLockSet locks;
        transaction->Locks().swap(locks);

        TTransaction::TLockedNodeSet lockedNodes;
        transaction->LockedNodes().swap(lockedNodes);

        for (auto* lock : locks) {
            auto* trunkNode = lock->GetTrunkNode();
            // Decide if the lock must be promoted.
            if (parentTransaction && lock->Request().Mode != ELockMode::Snapshot) {
                lock->SetTransaction(parentTransaction);
                YCHECK(parentTransaction->Locks().insert(lock).second);
                LOG_DEBUG_UNLESS(IsRecovery(), "Lock promoted (LockId: %v, NewTransactionId: %v)",
                    lock->GetId(),
                    parentTransaction->GetId());
            } else {
                if (trunkNode) {
                    switch (lock->GetState()) {
                        case ELockState::Acquired:
                            trunkNode->AcquiredLocks().erase(lock->GetLockListIterator());
                            break;
                        case ELockState::Pending:
                            trunkNode->PendingLocks().erase(lock->GetLockListIterator());
                            break;
                        default:
                            YUNREACHABLE();
                    }
                    lock->SetTrunkNode(nullptr);
                }
                lock->SetTransaction(nullptr);
                objectManager->UnrefObject(lock);
            }
        }

        for (auto* trunkNode : lockedNodes) {
            YCHECK(trunkNode->LockStateMap().erase(transaction) == 1);

            TVersionedNodeId versionedId(trunkNode->GetId(), transaction->GetId());
            LOG_DEBUG_UNLESS(IsRecovery(), "Node unlocked (NodeId: %v)",
                versionedId);
        }

        for (auto* trunkNode : lockedNodes) {
            CheckPendingLocks(trunkNode);
        }
    }

    void CheckPendingLocks(TCypressNodeBase* trunkNode)
    {
        // Ignore orphaned nodes.
        // Eventually the node will get destroyed and the lock will become
        // orphaned.
        if (IsOrphaned(trunkNode))
            return;

        // Make acquisitions while possible.
        auto it = trunkNode->PendingLocks().begin();
        while (it != trunkNode->PendingLocks().end()) {
            // Be prepared to possible iterator invalidation.
            auto jt = it++;
            auto* lock = *jt;

            bool isMandatory;
            auto error = CheckLock(
                trunkNode,
                lock->GetTransaction(),
                lock->Request(),
                false,
                &isMandatory);

            // Is it OK?
            if (!error.IsOK())
                return;

            DoAcquireLock(lock);
        }
    }


    void ListSubtreeNodes(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction,
        bool includeRoot,
        TSubtreeNodes* subtreeNodes)
    {
        YCHECK(trunkNode->IsTrunk());

        if (includeRoot) {
            subtreeNodes->push_back(trunkNode);
        }

        if (IsMapLikeType(trunkNode->GetType())) {
            auto originators = GetNodeReverseOriginators(transaction, trunkNode);
            yhash_map<Stroka, TCypressNodeBase*> children;
            for (const auto* node : originators) {
                const auto* mapNode = static_cast<const TMapNode*>(node);
                for (const auto& pair : mapNode->KeyToChild()) {
                    if (pair.second) {
                        children[pair.first] = pair.second;
                    } else {
                        // NB: erase may fail.
                        children.erase(pair.first);
                    }
                }
            }

            for (const auto& pair : children) {
                ListSubtreeNodes(pair.second, transaction, true, subtreeNodes);
            }
        } else if (IsListLikeType(trunkNode->GetType())) {
            auto* node = GetVersionedNode(trunkNode, transaction);
            auto* listRoot = static_cast<TListNode*>(node);
            for (auto* trunkChild : listRoot->IndexToChild()) {
                ListSubtreeNodes(trunkChild, transaction, true, subtreeNodes);
            }
        }
    }


    TCypressNodeBase* BranchNode(
        TCypressNodeBase* originatingNode,
        TTransaction* transaction,
        ELockMode mode)
    {
        YCHECK(originatingNode);
        YCHECK(transaction);
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap_->GetObjectManager();
        auto securityManager = Bootstrap_->GetSecurityManager();

        const auto& id = originatingNode->GetId();

        // Create a branched node and initialize its state.
        auto handler = GetHandler(originatingNode);
        auto branchedNodeHolder = handler->Branch(originatingNode, transaction, mode);

        TVersionedNodeId versionedId(id, transaction->GetId());
        auto* branchedNode = NodeMap_.Insert(versionedId, std::move(branchedNodeHolder));

        YCHECK(branchedNode->GetLockMode() == mode);

        // Register the branched node with the transaction.
        transaction->BranchedNodes().push_back(branchedNode);

        // The branched node holds an implicit reference to its originator.
        objectManager->RefObject(originatingNode->GetTrunkNode());

        // Update resource usage.
        auto* account = originatingNode->GetAccount();
        securityManager->SetAccount(branchedNode, account);

        LOG_DEBUG_UNLESS(IsRecovery(), "Node branched (NodeId: %v, Mode: %v)",
            TVersionedNodeId(id, transaction->GetId()),
            mode);

        return branchedNode;
    }

    void MergeNode(
        TTransaction* transaction,
        TCypressNodeBase* branchedNode)
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        auto securityManager = Bootstrap_->GetSecurityManager();

        auto handler = GetHandler(branchedNode);

        auto* trunkNode = branchedNode->GetTrunkNode();
        auto branchedId = branchedNode->GetVersionedId();
        auto* parentTransaction = transaction->GetParent();
        auto originatingId = TVersionedNodeId(branchedId.ObjectId, GetObjectId(parentTransaction));

        if (branchedNode->GetLockMode() != ELockMode::Snapshot) {
            auto* originatingNode = NodeMap_.Get(originatingId);

            // Merge changes back.
            handler->Merge(originatingNode, branchedNode);

            // The root needs a special handling.
            // When Cypress gets cleared, the root is created and is assigned zero creation time.
            // (We don't have any mutation context at hand to provide a synchronized timestamp.)
            // Later on, Cypress is initialized and filled with nodes.
            // At this point we set the root's creation time.
            if (trunkNode == RootNode_ && !parentTransaction) {
                originatingNode->SetCreationTime(originatingNode->GetModificationTime());
            }

            // Update resource usage.
            securityManager->UpdateAccountNodeUsage(originatingNode);

            LOG_DEBUG_UNLESS(IsRecovery(), "Node merged (NodeId: %v)", branchedId);
        } else {
            // Destroy the branched copy.
            handler->Destroy(branchedNode);

            LOG_DEBUG_UNLESS(IsRecovery(), "Node snapshot destroyed (NodeId: %v)", branchedId);
        }

        // Drop the implicit reference to the originator.
        objectManager->UnrefObject(trunkNode);

        // Remove the branched copy.
        NodeMap_.Remove(branchedId);

        LOG_DEBUG_UNLESS(IsRecovery(), "Branched node removed (NodeId: %v)", branchedId);
    }

    void MergeNodes(TTransaction* transaction)
    {
        for (auto* node : transaction->BranchedNodes()) {
            MergeNode(transaction, node);
        }
        transaction->BranchedNodes().clear();
    }

    void RemoveBranchedNode(
        TTransaction* transaction,
        TCypressNodeBase* branchedNode)
    {
        auto objectManager = Bootstrap_->GetObjectManager();

        auto handler = GetHandler(branchedNode);

        auto* trunkNode = branchedNode->GetTrunkNode();
        auto branchedNodeId = branchedNode->GetVersionedId();

        // Drop the implicit reference to the originator.
        objectManager->UnrefObject(trunkNode);

        if (branchedNode->GetLockMode() != ELockMode::Snapshot) {
            // Cleanup the branched node.
            auto branchedId = branchedNode->GetVersionedId();
            auto* parentTransaction = transaction->GetParent();
            auto originatingId = TVersionedNodeId(branchedId.ObjectId, GetObjectId(parentTransaction));
            auto* originatingNode = NodeMap_.Get(originatingId);
            handler->Unbranch(originatingNode, branchedNode);
        }

        // Remove the node.
        handler->Destroy(branchedNode);
        NodeMap_.Remove(branchedNodeId);

        LOG_DEBUG_UNLESS(IsRecovery(), "Branched node removed (NodeId: %v)", branchedNodeId);
    }

    void RemoveBranchedNodes(TTransaction* transaction)
    {
        for (auto* branchedNode : transaction->BranchedNodes()) {
            RemoveBranchedNode(transaction, branchedNode);
        }
        transaction->BranchedNodes().clear();
    }


    TYPath GetNodePath(
        TCypressNodeBase* trunkNode,
        TTransaction* transaction)
    {
        YCHECK(trunkNode->IsTrunk());

        auto proxy = GetNodeProxy(trunkNode, transaction);
        return proxy->GetResolver()->GetPath(proxy);
    }


    TCypressNodeBase* DoCloneNode(
        TCypressNodeBase* sourceNode,
        ICypressNodeFactoryPtr factory,
        const TNodeId& hintId,
        ENodeCloneMode mode)
    {
        auto handler = GetHandler(sourceNode);
        auto* clonedNode = handler->Clone(
            sourceNode,
            factory,
            hintId,
            mode);

        // Set account.
        auto securityManager = Bootstrap_->GetSecurityManager();
        auto* account = factory->GetClonedNodeAccount(sourceNode);
        securityManager->SetAccount(clonedNode, account);

        // Set owner.
        auto* user = securityManager->GetAuthenticatedUser();
        auto* acd = securityManager->GetAcd(clonedNode);
        acd->SetOwner(user);

        return clonedNode;
    }


    void HydraUpdateAccessStatistics(const NProto::TReqUpdateAccessStatistics& request) throw()
    {
        for (const auto& update : request.updates()) {
            auto nodeId = FromProto<TNodeId>(update.node_id());
            auto* node = FindNode(TVersionedNodeId(nodeId));
            if (!IsObjectAlive(node))
                continue;

            // Update access time.
            auto accessTime = TInstant(update.access_time());
            if (accessTime > node->GetAccessTime()) {
                node->SetAccessTime(accessTime);
            }

            // Update access counter.
            i64 accessCounter = node->GetAccessCounter() + update.access_counter_delta();
            node->SetAccessCounter(accessCounter);
        }
    }

    void HydraCreateForeignNode(const NProto::TReqCreateForeignNode& request) throw()
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        auto nodeId = FromProto<TObjectId>(request.node_id());
        auto transactionId = request.has_transaction_id()
            ? FromProto<TTransactionId>(request.transaction_id())
            : NullTransactionId;
        auto accountId = request.has_account_id()
            ? FromProto<TAccountId>(request.account_id())
            : NullObjectId;
        auto type = EObjectType(request.type());

        auto transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionId
            ? transactionManager->GetTransaction(transactionId)
            : nullptr;

        auto securityManager = Bootstrap_->GetSecurityManager();
        auto* account = accountId
            ? securityManager->GetAccount(accountId)
            : nullptr;

        auto attributes = request.has_node_attributes()
            ? FromProto(request.node_attributes())
            : std::unique_ptr<IAttributeDictionary>();

        auto versionedNodeId = TVersionedNodeId(nodeId, transactionId);

        LOG_DEBUG_UNLESS(IsRecovery(), "Creating foreign node (NodeId: %v, Type: %v, Account: %v)",
            versionedNodeId,
            transactionId,
            type,
            account ? MakeNullable(account->GetName()) : Null);

        auto handler = GetHandler(type);

        auto* trunkNode = CreateNode(
            nodeId,
            NotReplicatedCellTag,
            handler,
            account,
            transaction,
            attributes.get());

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(trunkNode);
        objectManager->FillAttributes(trunkNode, *attributes);

        LockNode(trunkNode, transaction, ELockMode::Exclusive);
    }

    void HydraCloneForeignNode(const NProto::TReqCloneForeignNode& request) throw()
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        auto sourceNodeId = FromProto<TNodeId>(request.source_node_id());
        auto sourceTransactionId = request.has_source_transaction_id()
            ? FromProto<TTransactionId>(request.source_transaction_id())
            : NullTransactionId;
        auto clonedNodeId = FromProto<TNodeId>(request.cloned_node_id());
        auto clonedTransactionId = request.has_cloned_transaction_id()
            ? FromProto<TNodeId>(request.cloned_transaction_id())
            : NullTransactionId;
        auto mode = ENodeCloneMode(request.mode());
        auto accountId = FromProto<TAccountId>(request.account_id());

        auto* sourceNode = GetNode(TVersionedObjectId(sourceNodeId, sourceTransactionId));

        auto securityManager = Bootstrap_->GetSecurityManager();
        auto* account = securityManager->GetAccount(accountId);

        auto transactionManager = Bootstrap_->GetTransactionManager();
        auto* clonedTransaction = clonedTransactionId
            ? transactionManager->GetTransaction(clonedTransactionId)
            : nullptr;

        auto factory = CreateNodeFactory(clonedTransaction, account, false);

        LOG_DEBUG_UNLESS(IsRecovery(), "Cloning foreign node (SourceNodeId: %v, ClonedNodeId: %v, Account: %v)",
            TVersionedNodeId(sourceNodeId, sourceTransactionId),
            TVersionedNodeId(clonedNodeId, clonedTransactionId),
            account->GetName());

        auto* clonedTrunkNode = DoCloneNode(
            sourceNode,
            factory,
            clonedNodeId,
            mode);

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(clonedTrunkNode);

        LockNode(clonedTrunkNode, clonedTransaction, ELockMode::Exclusive);

        factory->Commit();
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager::TImpl, Node, TCypressNodeBase, TVersionedNodeId, NodeMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TCypressManager::TImpl, Lock, TLock, TLockId, LockMap_);

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TImpl::TNodeMapTraits::TNodeMapTraits(TImpl* owner)
    : Owner_(owner)
{ }

auto TCypressManager::TImpl::TNodeMapTraits::Create(const TVersionedNodeId& id) const -> std::unique_ptr<TCypressNodeBase>
{
    auto type = TypeFromId(id.ObjectId);
    auto handler = Owner_->GetHandler(type);
    // This cell tag is fake and will be overwritten on load.
    return handler->Instantiate(id, InvalidCellTag);
}

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TNodeTypeHandler::TNodeTypeHandler(TImpl* owner, EObjectType type)
    : TObjectTypeHandlerBase(owner->Bootstrap_)
    , Owner_(owner)
    , Type_(type)
{ }

void TCypressManager::TNodeTypeHandler::DestroyObject(TObjectBase* object) throw()
{
    Owner_->DestroyNode(static_cast<TCypressNodeBase*>(object));
}

Stroka TCypressManager::TNodeTypeHandler::DoGetName(TCypressNodeBase* node)
{
    auto path = Owner_->GetNodePath(node->GetTrunkNode(), node->GetTransaction());
    return Format("node %v", path);
}

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TLockTypeHandler::TLockTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->LockMap_)
{ }

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TCypressManager(
    TCypressManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TCypressManager::~TCypressManager()
{ }

void TCypressManager::Initialize()
{
    Impl_->Initialize();
}

void TCypressManager::RegisterHandler(INodeTypeHandlerPtr handler)
{
    Impl_->RegisterHandler(std::move(handler));
}

INodeTypeHandlerPtr TCypressManager::FindHandler(EObjectType type)
{
    return Impl_->FindHandler(type);
}

INodeTypeHandlerPtr TCypressManager::GetHandler(EObjectType type)
{
    return Impl_->GetHandler(type);
}

INodeTypeHandlerPtr TCypressManager::GetHandler(const TCypressNodeBase* node)
{
    return Impl_->GetHandler(node);
}

ICypressNodeFactoryPtr TCypressManager::CreateNodeFactory(
    TTransaction* transaction,
    TAccount* account,
    bool preserveAccount)
{
    return Impl_->CreateNodeFactory(transaction, account, preserveAccount);
}

TCypressNodeBase* TCypressManager::CreateNode(
    const TNodeId& hintId,
    TCellTag externalCellTag,
    INodeTypeHandlerPtr handler,
    TAccount* account,
    TTransaction* transaction,
    IAttributeDictionary* attributes)
{
    return Impl_->CreateNode(
        hintId,
        externalCellTag,
        std::move(handler),
        account,
        transaction,
        attributes);
}

TCypressNodeBase* TCypressManager::InstantiateNode(
    const TNodeId& id,
    TCellTag externalCellTag)
{
    return Impl_->InstantiateNode(id, externalCellTag);
}

TCypressNodeBase* TCypressManager::CloneNode(
    TCypressNodeBase* sourceNode,
    ICypressNodeFactoryPtr factory,
    ENodeCloneMode mode)
{
    return Impl_->CloneNode(sourceNode, factory, mode);
}

TCypressNodeBase* TCypressManager::GetRootNode() const
{
    return Impl_->GetRootNode();
}

TCypressNodeBase* TCypressManager::GetNodeOrThrow(const TVersionedNodeId& id)
{
    return Impl_->GetNodeOrThrow(id);
}

INodeResolverPtr TCypressManager::CreateResolver(TTransaction* transaction)
{
    return Impl_->CreateResolver(transaction);
}

TCypressNodeBase* TCypressManager::FindNode(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    return Impl_->FindNode(trunkNode, transaction);
}

TCypressNodeBase* TCypressManager::GetVersionedNode(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    return Impl_->GetVersionedNode(trunkNode, transaction);
}

ICypressNodeProxyPtr TCypressManager::GetNodeProxy(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    return Impl_->GetNodeProxy(trunkNode, transaction);
}

TCypressNodeBase* TCypressManager::LockNode(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request,
    bool recursive)
{
    return Impl_->LockNode(trunkNode, transaction, request, recursive);
}

TLock* TCypressManager::CreateLock(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request,
    bool waitable)
{
    return Impl_->CreateLock(trunkNode, transaction, request, waitable);
}

void TCypressManager::SetModified(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    Impl_->SetModified(trunkNode, transaction);
}

void TCypressManager::SetAccessed(TCypressNodeBase* trunkNode)
{
    Impl_->SetAccessed(trunkNode);
}

TCypressManager::TSubtreeNodes TCypressManager::ListSubtreeNodes(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    bool includeRoot)
{
    return Impl_->ListSubtreeNodes(trunkNode, transaction, includeRoot);
}

void TCypressManager::AbortSubtreeTransactions(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    Impl_->AbortSubtreeTransactions(trunkNode, transaction);
}

void TCypressManager::AbortSubtreeTransactions(INodePtr node)
{
    Impl_->AbortSubtreeTransactions(std::move(node));
}

bool TCypressManager::IsOrphaned(TCypressNodeBase* trunkNode)
{
    return Impl_->IsOrphaned(trunkNode);
}

bool TCypressManager::IsAlive(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    return Impl_->IsAlive(trunkNode, transaction);
}

TCypressNodeList TCypressManager::GetNodeOriginators(
    TTransaction* transaction,
    TCypressNodeBase* trunkNode)
{
    return Impl_->GetNodeOriginators(transaction, trunkNode);
}

TCypressNodeList TCypressManager::GetNodeReverseOriginators(
    TTransaction* transaction,
    TCypressNodeBase* trunkNode)
{
    return Impl_->GetNodeReverseOriginators(transaction, trunkNode);
}

void TCypressManager::SealJournal(
    TJournalNode* trunkNode,
    const TDataStatistics* statistics)
{
    Impl_->SealJournal(trunkNode, statistics);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TCypressManager, Node, TCypressNodeBase, TVersionedNodeId, *Impl_);
DELEGATE_ENTITY_MAP_ACCESSORS(TCypressManager, Lock, TLock, TLockId, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
