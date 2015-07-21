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

#include <server/transaction_server/transaction.h>
#include <server/transaction_server/transaction_manager.h>

#include <server/cypress_server/cypress_manager.pb.h>

namespace NYT {
namespace NCypressServer {

using namespace NCellMaster;
using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NTransactionServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NCypressClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TNodeFactory
    : public ICypressNodeFactory
{
public:
    TNodeFactory(
        NCellMaster::TBootstrap* bootstrap,
        TTransaction* transaction,
        TAccount* account,
        bool preserveAccount)
        : Bootstrap_(bootstrap)
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
        IAttributeDictionary* attributes = nullptr,
        TReqCreate* request = nullptr,
        TRspCreate* response = nullptr) override
    {
        ValidateCreatedNodeType(type);

        GetNewNodeAccount()->ValidateResourceUsageIncrease(TClusterResources(0, 1, 0));

        auto cypressManager = Bootstrap_->GetCypressManager();
        auto handler = cypressManager->FindHandler(type);
        if (!handler) {
            THROW_ERROR_EXCEPTION("Unknown object type %Qlv",
                type);
        }

        auto* node = cypressManager->CreateNode(
            handler,
            this,
            request,
            response);
        auto* trunkNode = node->GetTrunkNode();

        RegisterCreatedNode(trunkNode);

        if (attributes) {
            handler->SetDefaultAttributes(attributes, Transaction_);
            auto keys = attributes->List();
            std::sort(keys.begin(), keys.end());
            if (!keys.empty()) {
                auto trunkProxy = cypressManager->GetNodeProxy(trunkNode, nullptr);

                std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
                trunkProxy->ListBuiltinAttributes(&systemAttributes);

                yhash_set<Stroka> systemAttributeKeys;
                for (const auto& attribute : systemAttributes) {
                    YCHECK(systemAttributeKeys.insert(attribute.Key).second);
                }

                for (const auto& key : keys) {
                    auto value = attributes->GetYson(key);
                    if (systemAttributeKeys.find(key) == systemAttributeKeys.end()) {
                        trunkProxy->MutableAttributes()->SetYson(key, value);
                    } else {
                        if (!trunkProxy->SetBuiltinAttribute(key, value)) {
                            ThrowCannotSetBuiltinAttribute(key);
                        }
                    }
                }
            }
        }

        handler->ValidateCreated(trunkNode);

        cypressManager->LockNode(trunkNode, Transaction_, ELockMode::Exclusive);

        return cypressManager->GetNodeProxy(trunkNode, Transaction_);
    }

    virtual TCypressNodeBase* CreateNode(const TNodeId& id) override
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->CreateNode(id);

        RegisterCreatedNode(node);

        return node;
    }

    virtual TCypressNodeBase* CloneNode(
        TCypressNodeBase* sourceNode,
        ENodeCloneMode mode) override
    {
        ValidateCreatedNodeType(sourceNode->GetType());

        GetClonedNodeAccount(sourceNode)->ValidateResourceUsageIncrease(sourceNode->GetResourceUsage());

        auto cypressManager = Bootstrap_->GetCypressManager();
        auto* clonedTrunkNode = cypressManager->CloneNode(sourceNode, this, mode);

        RegisterCreatedNode(clonedTrunkNode);

        cypressManager->LockNode(clonedTrunkNode, Transaction_, ELockMode::Exclusive);

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

    void RegisterCreatedNode(TCypressNodeBase* node)
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(node);
        CreatedNodes_.push_back(node);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TNodeTypeHandler
    : public TObjectTypeHandlerBase<TCypressNodeBase>
{
public:
    TNodeTypeHandler(TImpl* owner, EObjectType type);

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
            EObjectAccountMode::Forbidden);
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
            transaction,
            account,
            preserveAccount);
    }

    TCypressNodeBase* CreateNode(
        INodeTypeHandlerPtr handler,
        ICypressNodeFactoryPtr factory,
        TReqCreate* request,
        TRspCreate* response)
    {
        YCHECK(handler);
        YCHECK(factory);

        auto nodeHolder = handler->Create(request, response);
        auto* node = RegisterNode(std::move(nodeHolder));

        // Set account.
        auto securityManager = Bootstrap_->GetSecurityManager();
        auto* account = factory->GetNewNodeAccount();
        securityManager->SetAccount(node, account);

        // Set owner.
        auto* user = securityManager->GetAuthenticatedUser();
        auto* acd = securityManager->GetAcd(node);
        acd->SetOwner(user);

        if (response) {
            ToProto(response->mutable_node_id(), node->GetId());
        }

        return node;
    }

    TCypressNodeBase* CreateNode(const TNodeId& id)
    {
        auto type = TypeFromId(id);
        auto handler = GetHandler(type);
        auto nodeHolder = handler->Instantiate(TVersionedNodeId(id));
        nodeHolder->SetTrunkNode(nodeHolder.get());
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

        auto handler = GetHandler(sourceNode);
        auto* clonedNode = handler->Clone(sourceNode, factory, mode);

        // Set account.
        securityManager->SetAccount(clonedNode, account);

        // Set owner.
        auto* user = securityManager->GetAuthenticatedUser();
        auto* acd = securityManager->GetAcd(clonedNode);
        acd->SetOwner(user);

        return clonedNode;
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
        bool recursive)
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

            // Look for thombstones.
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
        NTransactionServer::TTransaction* transaction,
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
        NTransactionServer::TTransaction* transaction,
        TCypressNodeBase* trunkNode)
    {
        auto result = GetNodeOriginators(transaction, trunkNode);
        std::reverse(result.begin(), result.end());
        return result;
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

    bool InitializeCellTags_ = false;

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
        InitializeCellTags_ = (context.GetVersion() < 200);
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

        // COMPAT(babenko)
        auto cellTag = Bootstrap_->GetCellTag();

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
            if (transactionId != NullTransactionId) {
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
            if (InitializeCellTags_) {
                if (node->GetCellTag() == InvalidCellTag) {
                    node->SetCellTag(cellTag);
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


    TCypressNodeBase* RegisterNode(std::unique_ptr<TCypressNodeBase> nodeHolder)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(nodeHolder->IsTrunk());

        const auto* mutationContext = GetCurrentMutationContext();
        nodeHolder->SetCreationTime(mutationContext->GetTimestamp());
        nodeHolder->SetModificationTime(mutationContext->GetTimestamp());
        nodeHolder->SetAccessTime(mutationContext->GetTimestamp());
        nodeHolder->SetRevision(mutationContext->GetVersion().ToRevision());

        const auto& nodeId = nodeHolder->GetId();
        auto* node = NodeMap_.Insert(TVersionedNodeId(nodeId), std::move(nodeHolder));

        LOG_DEBUG_UNLESS(IsRecovery(), "Node registered (NodeId: %v, Type: %v)",
            nodeId,
            TypeFromId(nodeId));

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

    void RemoveBranchedNode(TCypressNodeBase* branchedNode)
    {
        auto objectManager = Bootstrap_->GetObjectManager();

        auto handler = GetHandler(branchedNode);

        auto* trunkNode = branchedNode->GetTrunkNode();
        auto branchedNodeId = branchedNode->GetVersionedId();

        // Drop the implicit reference to the originator.
        objectManager->UnrefObject(trunkNode);

        // Remove the node.
        handler->Destroy(branchedNode);
        NodeMap_.Remove(branchedNodeId);

        LOG_DEBUG_UNLESS(IsRecovery(), "Branched node removed (NodeId: %v)", branchedNodeId);
    }

    void RemoveBranchedNodes(TTransaction* transaction)
    {
        for (auto* branchedNode : transaction->BranchedNodes()) {
            RemoveBranchedNode(branchedNode);
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


    void HydraUpdateAccessStatistics(const NProto::TReqUpdateAccessStatistics& request)
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
    return handler->Instantiate(id);
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
    Impl_->RegisterHandler(handler);
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
    INodeTypeHandlerPtr handler,
    ICypressNodeFactoryPtr factory,
    TReqCreate* request,
    TRspCreate* response)
{
    return Impl_->CreateNode(handler, factory, request, response);
}

TCypressNodeBase* TCypressManager::CreateNode(const TNodeId& id)
{
    return Impl_->CreateNode(id);
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
    Impl_->AbortSubtreeTransactions(node);
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

DELEGATE_ENTITY_MAP_ACCESSORS(TCypressManager, Node, TCypressNodeBase, TVersionedNodeId, *Impl_);
DELEGATE_ENTITY_MAP_ACCESSORS(TCypressManager, Lock, TLock, TLockId, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
