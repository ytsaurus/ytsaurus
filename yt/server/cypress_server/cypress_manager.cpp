#include "stdafx.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "node_proxy_detail.h"

#include <ytlib/misc/singleton.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/cypress_ypath.pb.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/ypath_detail.h>

#include <server/cell_master/serialization_context.h>
#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

#include <server/object_server/type_handler_detail.h>

#include <server/security_server/account.h>
#include <server/security_server/security_manager.h>

namespace NYT {
namespace NCypressServer {

using namespace NCellMaster;
using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NTransactionServer;
using namespace NMetaState;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NCypressClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Cypress");

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TNodeTypeHandler
    : public IObjectTypeHandler
{
public:
    TNodeTypeHandler(
        TCypressManager* cypressManager,
        EObjectType type)
        : CypressManager(cypressManager)
        , Type(type)
    { }

    virtual EObjectType GetType() const override
    {
        return Type;
    }

    virtual TObjectBase* FindObject(const TObjectId& id) override
    {
        return CypressManager->FindNode(TVersionedNodeId(id));
    }

    virtual void Destroy(TObjectBase* object) override
    {
        auto* node = static_cast<TCypressNodeBase*>(object);
        CypressManager->DestroyNode(node);
    }

    virtual void Unstage(
        TObjectBase* object,
        TTransaction* transaction,
        bool recursive) override
    {
        UNUSED(object);
        UNUSED(transaction);
        UNUSED(recursive);
        YUNREACHABLE();
    }

    virtual IObjectProxyPtr GetProxy(
        TObjectBase* object,
        TTransaction* transaction) override
    {
        return CypressManager->GetVersionedNodeProxy(
            static_cast<TCypressNodeBase*>(object),
            transaction);
    }

    virtual TUnversionedObjectBase* Create(
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObject* request,
        TRspCreateObject* response) override
    {
        UNUSED(transaction);
        UNUSED(account);
        UNUSED(request);
        UNUSED(response);

        THROW_ERROR_EXCEPTION("Cannot create an instance of %s outside Cypress",
            ~FormatEnum(GetType()));
    }

    virtual EObjectTransactionMode GetTransactionMode() const override
    {
        return EObjectTransactionMode::Optional;
    }

    virtual EObjectAccountMode GetAccountMode() const override
    {
        return EObjectAccountMode::Forbidden;
    }

private:
    TCypressManager* CypressManager;
    EObjectType Type;

};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TYPathResolver
    : public IYPathResolver
{
public:
    explicit TYPathResolver(
        TBootstrap* bootstrap,
        TTransaction* transaction)
        : Bootstrap(bootstrap)
        , Transaction(transaction)
    { }

    virtual INodePtr ResolvePath(const TYPath& path) override
    {
        if (path.empty()) {
            THROW_ERROR_EXCEPTION("YPath cannot be empty");
        }

        if (path[0] != '/') {
            THROW_ERROR_EXCEPTION("YPath must start with \"/\"");
        }

        auto cypressManager = Bootstrap->GetCypressManager();
        auto rootProxy = cypressManager->GetVersionedNodeProxy(
            cypressManager->GetRootNode(),
            Transaction);

        return GetNodeByYPath(rootProxy, path.substr(1));
    }

    virtual TYPath GetPath(INodePtr node) override
    {
        return Stroka::Join("/", GetNodeYPath(node));
    }

private:
    TBootstrap* Bootstrap;
    TTransaction* Transaction;

};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TRootService
    : public TYPathServiceBase
{
public:
    explicit TRootService(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr context) override
    {
        UNUSED(context);

        auto cypressManager = Bootstrap->GetCypressManager();
        auto rootProxy = cypressManager->GetVersionedNodeProxy(cypressManager->GetRootNode());
        return TResolveResult::There(rootProxy, path);
    }

private:
    TBootstrap* Bootstrap;

};

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TNodeMapTraits::TNodeMapTraits(TCypressManager* cypressManager)
    : CypressManager(cypressManager)
{ }

TAutoPtr<TCypressNodeBase> TCypressManager::TNodeMapTraits::Create(const TVersionedNodeId& id) const
{
    auto type = TypeFromId(id.ObjectId);
    auto handler = CypressManager->GetHandler(type);
    return handler->Instantiate(id);
}

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TCypressManager(TBootstrap* bootstrap)
    : TMetaStatePart(
        bootstrap->GetMetaStateFacade()->GetManager(),
        bootstrap->GetMetaStateFacade()->GetState())
    , Bootstrap(bootstrap)
    , NodeMap(TNodeMapTraits(this))
    , TypeToHandler(MaxObjectType)
    , RootNode(nullptr)
{
    YCHECK(bootstrap);
    VERIFY_INVOKER_AFFINITY(bootstrap->GetMetaStateFacade()->GetInvoker(), StateThread);

    {
        auto cellId = Bootstrap->GetObjectManager()->GetCellId();
        RootNodeId = MakeWellKnownId(EObjectType::MapNode, cellId);
    }

    RootService = New<TRootService>(Bootstrap)->Via(
        Bootstrap->GetMetaStateFacade()->GetGuardedInvoker());

    RegisterHandler(New<TStringNodeTypeHandler>(Bootstrap));
    RegisterHandler(New<TIntegerNodeTypeHandler>(Bootstrap));
    RegisterHandler(New<TDoubleNodeTypeHandler>(Bootstrap));
    RegisterHandler(New<TMapNodeTypeHandler>(Bootstrap));
    RegisterHandler(New<TListNodeTypeHandler>(Bootstrap));

    {
        NCellMaster::TLoadContext context;
        context.SetBootstrap(Bootstrap);

        RegisterLoader(
            "Cypress.Keys",
            SnapshotVersionValidator(),
            BIND(&TCypressManager::LoadKeys, MakeStrong(this)),
            context);
        RegisterLoader(
            "Cypress.Values",
            SnapshotVersionValidator(),
            BIND(&TCypressManager::LoadValues, MakeStrong(this)),
            context);
    }

    {
        NCellMaster::TSaveContext context;

        RegisterSaver(
            ESavePriority::Keys,
            "Cypress.Keys",
            CurrentSnapshotVersion,
            BIND(&TCypressManager::SaveKeys, MakeStrong(this)),
            context);
        RegisterSaver(
            ESavePriority::Values,
            "Cypress.Values",
            CurrentSnapshotVersion,
            BIND(&TCypressManager::SaveValues, MakeStrong(this)),
            context);
    }
}

void TCypressManager::Initialize()
{
    auto transactionManager = Bootstrap->GetTransactionManager();
    transactionManager->SubscribeTransactionCommitted(BIND(
        &TThis::OnTransactionCommitted,
        MakeStrong(this)));
    transactionManager->SubscribeTransactionAborted(BIND(
        &TThis::OnTransactionAborted,
        MakeStrong(this)));
}

void TCypressManager::RegisterHandler(INodeTypeHandlerPtr handler)
{
    // No thread affinity is given here.
    // This will be called during init-time only.
    YCHECK(handler);

    auto type = handler->GetObjectType();
    int typeValue = type.ToValue();
    YCHECK(typeValue >= 0 && typeValue < MaxObjectType);
    YCHECK(!TypeToHandler[typeValue]);
    TypeToHandler[typeValue] = handler;

    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->RegisterHandler(New<TNodeTypeHandler>(this, type));
}

INodeTypeHandlerPtr TCypressManager::FindHandler(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    int typeValue = type.ToValue();
    if (typeValue < 0 || typeValue >= MaxObjectType) {
        return nullptr;
    }

    return TypeToHandler[typeValue];
}

INodeTypeHandlerPtr TCypressManager::GetHandler(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto handler = FindHandler(type);
    YCHECK(handler);
    return handler;
}

INodeTypeHandlerPtr TCypressManager::GetHandler(const TCypressNodeBase* node)
{
    return GetHandler(TypeFromId(node->GetId()));
}

TCypressNodeBase* TCypressManager::CreateNode(
    INodeTypeHandlerPtr handler,
    TTransaction* transaction,
    TAccount* account,
    IAttributeDictionary* attributes,
    TReqCreate* request,
    TRspCreate* response)
{
    YCHECK(handler);
    YCHECK(account);
    YCHECK(request);
    YCHECK(response);

    handler->SetDefaultAttributes(attributes);

    auto node = handler->Create(transaction, request, response);

    // Make a rawptr copy and transfer the ownership.
    auto node_ = ~node;
    RegisterNode(node, transaction, attributes);

    // Set account (if not given in attributes).
    if (!node_->GetAccount()) {
        auto securityManager = Bootstrap->GetSecurityManager();
        securityManager->SetAccount(node_, account);
    }

    *response->mutable_node_id() = node_->GetId().ToProto();

    return LockVersionedNode(node_, transaction, ELockMode::Exclusive);
}

TCypressNodeBase* TCypressManager::CloneNode(
    TCypressNodeBase* sourceNode,
    TTransaction* transaction)
{
    YCHECK(sourceNode);

    auto securityManager = Bootstrap->GetSecurityManager();

    auto handler = GetHandler(sourceNode);
    auto clonedNode = handler->Clone(sourceNode, transaction);

    // Make a rawptr copy and transfer the ownership.
    auto clonedNode_ = ~clonedNode;
    RegisterNode(clonedNode, transaction);

    auto* account = sourceNode->GetAccount();
    // COMPAT(babenko)
    if (!account) {
        account = securityManager->GetSysAccount();
    }
    securityManager->SetAccount(clonedNode_, account);

    return LockVersionedNode(clonedNode_, transaction, ELockMode::Exclusive);
}

void TCypressManager::CreateNodeBehavior(TCypressNodeBase* trunkNode)
{
    YCHECK(trunkNode->IsTrunk());

    auto handler = GetHandler(trunkNode);
    auto behavior = handler->CreateBehavior(trunkNode);
    if (!behavior)
        return;

    YCHECK(NodeBehaviors.insert(std::make_pair(trunkNode, behavior)).second);

    LOG_DEBUG("Node behavior created (NodeId: %s)",  ~trunkNode->GetId().ToString());
}

void TCypressManager::DestroyNodeBehavior(TCypressNodeBase* trunkNode)
{
    YCHECK(trunkNode->IsTrunk());

    auto it = NodeBehaviors.find(trunkNode);
    if (it == NodeBehaviors.end())
        return;

    it->second->Destroy();
    NodeBehaviors.erase(it);

    LOG_DEBUG("Node behavior destroyed (NodeId: %s)", ~trunkNode->GetId().ToString());
}

TCypressNodeBase* TCypressManager::GetRootNode() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RootNode;
}

IYPathServicePtr TCypressManager::GetRootService() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RootService;
}

IYPathResolverPtr TCypressManager::CreateResolver(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return New<TYPathResolver>(Bootstrap, transaction);
}

TCypressNodeBase* TCypressManager::FindNode(
    TCypressNodeBase* trunkNode,
    NTransactionServer::TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(trunkNode->IsTrunk());

    // Fast path -- no transaction.
    if (!transaction) {
        return trunkNode;
    }

    TVersionedNodeId versionedId(trunkNode->GetId(), GetObjectId(transaction));
    return FindNode(versionedId);
}

TCypressNodeBase* TCypressManager::GetVersionedNode(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);
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

ICypressNodeProxyPtr TCypressManager::GetVersionedNodeProxy(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(trunkNode->IsTrunk());

    auto handler = GetHandler(trunkNode);
    return handler->GetProxy(trunkNode, transaction);
}

void TCypressManager::ValidateLock(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request,
    bool* isMandatory)
{
    YCHECK(trunkNode->IsTrunk());

    // Snapshot locks can only be taken inside a transaction.
    if (request.Mode == ELockMode::Snapshot && !transaction) {
        THROW_ERROR_EXCEPTION("Cannot take %s lock outside of a transaction",
            ~FormatEnum(request.Mode).Quote());
    }

    // Examine existing locks.
    // A quick check: same transaction, same or weaker lock mode (beware of Snapshot!).
    {
        auto it = trunkNode->Locks().find(transaction);
        if (it != trunkNode->Locks().end()) {
            const auto& existingLock = it->second;
            if (IsRedundantLock(existingLock, request)) {
                *isMandatory = false;
                return;
            }
            if (existingLock.Mode == ELockMode::Snapshot) {
                THROW_ERROR_EXCEPTION("Cannot take %s lock for node %s since %s lock is already taken by the same transaction",
                    ~FormatEnum(request.Mode).Quote(),
                    ~GetNodePath(trunkNode, transaction),
                    ~FormatEnum(existingLock.Mode).Quote());
            }
        }
    }

    FOREACH (const auto& pair, trunkNode->Locks()) {
        auto* existingTransaction = pair.first;
        const auto& existingLock = pair.second;

        // Ignore other Snapshot locks.
        if (existingLock.Mode == ELockMode::Snapshot) {
            continue;
        }

        // When a Snapshot is requested no descendant transaction (including |transaction| itself)
        // may hold a lock other than Snapshot.
        if (request.Mode == ELockMode::Snapshot &&
            IsParentTransaction(existingTransaction, transaction))
        {
            THROW_ERROR_EXCEPTION("Cannot take %s lock for node %s since %s lock is taken by descendant transaction %s",
                ~FormatEnum(request.Mode).Quote(),
                ~GetNodePath(trunkNode, transaction),
                ~FormatEnum(existingLock.Mode).Quote(),
                ~existingTransaction->GetId().ToString());
        }

        if (!transaction || IsConcurrentTransaction(transaction, existingTransaction)) {
            // For Exclusive locks we check locks held by concurrent transactions.
            if (request.Mode == ELockMode::Exclusive && existingLock.Mode != ELockMode::Snapshot ||
                existingLock.Mode == ELockMode::Exclusive && request.Mode != ELockMode::Snapshot)
            {
                THROW_ERROR_EXCEPTION("Cannot take %s lock for node %s since %s lock is taken by concurrent transaction %s",
                    ~FormatEnum(request.Mode).Quote(),
                    ~GetNodePath(trunkNode, transaction),
                    ~FormatEnum(existingLock.Mode).Quote(),
                    ~existingTransaction->GetId().ToString());
            }

            // For Shared locks we check child and attribute keys.
            if (request.Mode == ELockMode::Shared && existingLock.Mode == ELockMode::Shared) {
                if (request.ChildKey &&
                    existingLock.ChildKeys.find(request.ChildKey.Get()) != existingLock.ChildKeys.end())
                {
                    THROW_ERROR_EXCEPTION("Cannot take %s lock for child %s of node %s since %s lock is taken by concurrent transaction %s",
                        ~FormatEnum(request.Mode).Quote(),
                        ~request.ChildKey.Get().Quote(),
                        ~GetNodePath(trunkNode, transaction),
                        ~FormatEnum(existingLock.Mode).Quote(),
                        ~existingTransaction->GetId().ToString());
                }
                if (request.AttributeKey &&
                    existingLock.AttributeKeys.find(request.AttributeKey.Get()) != existingLock.AttributeKeys.end())
                {
                    THROW_ERROR_EXCEPTION("Cannot take %s lock for attribute %s of node %s since %s lock is taken by concurrent transaction %s",
                        ~FormatEnum(request.Mode).Quote(),
                        ~request.AttributeKey.Get().Quote(),
                        ~GetNodePath(trunkNode, transaction),
                        ~FormatEnum(existingLock.Mode).Quote(),
                        ~existingTransaction->GetId().ToString());
                }
            }
        }
    }

    // If we're outside of a transaction then the lock is not needed.
    *isMandatory = (transaction != nullptr);
}

void TCypressManager::ValidateLock(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request)
{
    bool dummy;
    ValidateLock(trunkNode, transaction, request, &dummy);
}

bool TCypressManager::IsRedundantLock(
    const TLock& existingLock,
    const TLockRequest& request)
{
    if (existingLock.Mode > request.Mode && request.Mode != ELockMode::Snapshot) {
        return true;
    }

    if (existingLock.Mode == request.Mode) {
        if (request.Mode == ELockMode::Shared) {
            if (request.ChildKey &&
                existingLock.ChildKeys.find(request.ChildKey.Get()) == existingLock.ChildKeys.end())
            {
                return false;
            }
            if (request.AttributeKey &&
                existingLock.AttributeKeys.find(request.AttributeKey.Get()) == existingLock.AttributeKeys.end())
            {
                return false;
            }
        }
        return true;
    }

    return false;
}

bool TCypressManager::IsParentTransaction(TTransaction* transaction, TTransaction* parent)
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

bool TCypressManager::IsConcurrentTransaction(TTransaction* transaction1, TTransaction* transaction2)
{
    return
        !IsParentTransaction(transaction1, transaction2) &&
        !IsParentTransaction(transaction2, transaction1);
}

TCypressNodeBase* TCypressManager::AcquireLock(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request)
{
    YCHECK(trunkNode->IsTrunk());
    YCHECK(transaction);

    DoAcquireLock(trunkNode, transaction, request);

    // Upgrade locks held by parent transactions, if needed.
    if (request.Mode != ELockMode::Snapshot) {
        auto* currentTransaction = transaction->GetParent();
        while (currentTransaction) {
            DoAcquireLock(trunkNode, currentTransaction, request);
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
        FOREACH (auto* transactionToBranch, intermediateTransactions) {
            currentNode = BranchNode(currentNode, transactionToBranch, request.Mode);
        }
        return currentNode;
    }
}

TLock* TCypressManager::DoAcquireLock(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request)
{
    YCHECK(trunkNode->IsTrunk());

    TVersionedNodeId versionedId(trunkNode->GetId(), transaction->GetId());
    TLock* lock;
    auto it = trunkNode->Locks().find(transaction);
    if (it == trunkNode->Locks().end()) {
        lock = &trunkNode->Locks()[transaction];
        lock->Mode = request.Mode;
        transaction->LockedNodes().push_back(trunkNode);

        LOG_INFO_UNLESS(IsRecovery(), "Node locked (NodeId: %s, Mode: %s)",
            ~versionedId.ToString(),
            ~request.Mode.ToString());
    } else {
        lock = &it->second;
        if (lock->Mode < request.Mode) {
            lock->Mode = request.Mode;

            LOG_INFO_UNLESS(IsRecovery(), "Node lock upgraded (NodeId: %s, Mode: %s)",
                ~versionedId.ToString(),
                ~lock->Mode.ToString());
        }
    }

    if (request.ChildKey &&
        lock->ChildKeys.find(request.ChildKey.Get()) == lock->ChildKeys.end())
    {
        YCHECK(lock->ChildKeys.insert(request.ChildKey.Get()).second);
        LOG_INFO_UNLESS(IsRecovery(), "Node child locked (NodeId: %s, Key: %s)",
            ~versionedId.ToString(),
            ~request.ChildKey.Get());
    }

    if (request.AttributeKey &&
        lock->AttributeKeys.find(request.AttributeKey.Get()) == lock->AttributeKeys.end())
    {
        YCHECK(lock->AttributeKeys.insert(request.AttributeKey.Get()).second);
        LOG_INFO_UNLESS(IsRecovery(), "Node attribute locked (NodeId: %s, Key: %s)",
            ~versionedId.ToString(),
            ~request.AttributeKey.Get());
    }

    return lock;
}

void TCypressManager::ReleaseLock(TCypressNodeBase* trunkNode, TTransaction* transaction)
{
    YCHECK(trunkNode->IsTrunk());

    YCHECK(trunkNode->Locks().erase(transaction) == 1);

    TVersionedNodeId versionedId(trunkNode->GetId(), transaction->GetId());
    LOG_INFO_UNLESS(IsRecovery(), "Node unlocked (NodeId: %s)",
        ~versionedId.ToString());
}

TCypressNodeBase* TCypressManager::LockVersionedNode(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request,
    bool recursive)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(trunkNode->IsTrunk());
    YCHECK(request.Mode != ELockMode::None);

    TSubtreeNodes nodesToLock;
    if (recursive) {
        YCHECK(!request.ChildKey);
        YCHECK(!request.AttributeKey);
        ListSubtreeNodes(trunkNode, transaction, &nodesToLock);
    } else {
        nodesToLock.push_back(trunkNode);
    }

    // Validate all potentials lock to see if we need to take at least one of them.
    // This throws an exception in case the validation fails.
    bool isMandatory = false;
    FOREACH (auto* child, nodesToLock) {
        bool isChildMandatory;
        ValidateLock(child->GetTrunkNode(), transaction, request, &isChildMandatory);
        isMandatory |= isChildMandatory;
    }

    if (!isMandatory) {
        return GetVersionedNode(trunkNode, transaction);
    }

    if (!transaction) {
        THROW_ERROR_EXCEPTION("The requested operation requires %s lock but no current transaction is given",
            ~FormatEnum(request.Mode).Quote());
    }

    TCypressNodeBase* lockedNode = nullptr;
    FOREACH (auto* child, nodesToLock) {
        auto* lockedChild = AcquireLock(child->GetTrunkNode(), transaction, request);
        if (child == trunkNode) {
            lockedNode = lockedChild;
        }
    }

    YCHECK(lockedNode);
    return lockedNode;
}

void TCypressManager::SetModified(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(trunkNode->IsTrunk());

    // Failure here means that the node wasn't indeed locked,
    // which is strange given that we're about to mark it as modified.
    TVersionedNodeId versionedId(trunkNode->GetId(), GetObjectId(transaction));
    auto* node = GetNode(versionedId);

    auto objectManager = Bootstrap->GetObjectManager();
    auto* mutationContext = Bootstrap
        ->GetMetaStateFacade()
        ->GetManager()
        ->GetMutationContext();

    node->SetModificationTime(mutationContext->GetTimestamp());
}

void TCypressManager::RegisterNode(
    TAutoPtr<TCypressNodeBase> node,
    TTransaction* transaction,
    IAttributeDictionary* attributes)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(node->IsTrunk());

    const auto& nodeId = node->GetId();

    auto objectManager = Bootstrap->GetObjectManager();

    auto* mutationContext = Bootstrap
        ->GetMetaStateFacade()
        ->GetManager()
        ->GetMutationContext();

    node->SetCreationTime(mutationContext->GetTimestamp());
    node->SetModificationTime(mutationContext->GetTimestamp());

    auto node_ = node.Get();
    NodeMap.Insert(TVersionedNodeId(nodeId), node.Release());

    // TODO(babenko): setting attributes here, in RegisterNode
    // is somewhat weird. Moving this logic to some other place, however,
    // complicates the code since we need to worry about possible
    // exceptions thrown from custom attribute validators.
    if (attributes) {
        auto proxy = GetVersionedNodeProxy(node_, nullptr);
        try {
            auto keys = attributes->List();
            FOREACH (const auto& key, keys) {
                auto value = attributes->GetYson(key);
                // Try to set as a system attribute. If fails then set as a user attribute.
                if (!proxy->SetSystemAttribute(key, value)) {
                    proxy->Attributes().SetYson(key, value);
                }
            }
        } catch (...) {
            auto handler = GetHandler(node_);
            handler->Destroy(node_);
            NodeMap.Remove(TVersionedNodeId(nodeId));
            throw;
        }
    }

    if (transaction) {
        transaction->StagedNodes().push_back(node_);
        objectManager->RefObject(node_);
    }

    LOG_INFO_UNLESS(IsRecovery(), "Node registered (NodeId: %s, Type: %s)",
        ~nodeId.ToString(),
        ~TypeFromId(nodeId).ToString());

    if (IsLeader()) {
        CreateNodeBehavior(node_);
    }
}

TCypressNodeBase* TCypressManager::BranchNode(
    TCypressNodeBase* originatingNode,
    TTransaction* transaction,
    ELockMode mode)
{
    YCHECK(originatingNode);
    YCHECK(transaction);
    VERIFY_THREAD_AFFINITY(StateThread);

    auto objectManager = Bootstrap->GetObjectManager();
    auto securityManager = Bootstrap->GetSecurityManager();

    const auto& id = originatingNode->GetId();

    // Create a branched node and initialize its state.
    auto handler = GetHandler(originatingNode);
    auto branchedNode = handler->Branch(originatingNode, transaction, mode);
    YCHECK(branchedNode->GetLockMode() == mode);
    auto* branchedNode_ = branchedNode.Release();

    TVersionedNodeId versionedId(id, transaction->GetId());
    NodeMap.Insert(versionedId, branchedNode_);

    // Register the branched node with the transaction.
    transaction->BranchedNodes().push_back(branchedNode_);

    // The branched node holds an implicit reference to its originator.
    objectManager->RefObject(originatingNode->GetTrunkNode());

    // Update resource usage.
    auto* account = originatingNode->GetAccount();
    // COMPAT(babenko)
    if (!account) {
        account = securityManager->GetSysAccount();
    }
    securityManager->SetAccount(branchedNode_, account);

    LOG_INFO_UNLESS(IsRecovery(), "Node branched (NodeId: %s, TransactionId: %s, Mode: %s)",
        ~id.ToString(),
        ~transaction->GetId().ToString(),
        ~mode.ToString());

    return branchedNode_;
}

void TCypressManager::SaveKeys(const NCellMaster::TSaveContext& context) const
{
    NodeMap.SaveKeys(context);
}

void TCypressManager::SaveValues(const NCellMaster::TSaveContext& context) const
{
    NodeMap.SaveValues(context);
}

void TCypressManager::LoadKeys(const NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.LoadKeys(context);

    RootNode = GetNode(TVersionedNodeId(RootNodeId));
}

void TCypressManager::LoadValues(const NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.LoadValues(context);

    // Reconstruct immediate ancestor sets.
    FOREACH (const auto& pair, NodeMap) {
        auto* node = pair.second;
        auto* parent = node->GetParent();
        if (parent) {
            YCHECK(parent->ImmediateAncestors().insert(node).second);
        }
    }
}

void TCypressManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto securityManager = Bootstrap->GetSecurityManager();

    NodeMap.Clear();

    // Create the root.
    RootNode = new TMapNode(TVersionedNodeId(RootNodeId));
    RootNode->SetTrunkNode(RootNode);
    RootNode->SetAccount(securityManager->GetSysAccount());

    NodeMap.Insert(TVersionedNodeId(RootNodeId), RootNode);
    YCHECK(RootNode->RefObject() == 1);
}

void TCypressManager::OnLeaderRecoveryComplete()
{
    LOG_INFO("Started creating node behaviors");
    YCHECK(NodeBehaviors.empty());
    FOREACH (const auto& pair, NodeMap) {
        if (!pair.first.IsBranched()) {
            CreateNodeBehavior(pair.second);
        }
    }
    LOG_INFO("Finished creating node behaviors");
}

void TCypressManager::OnStopLeading()
{
    FOREACH (const auto& pair, NodeBehaviors) {
        auto behavior = pair.second;
        behavior->Destroy();
    }
    NodeBehaviors.clear();
}

void TCypressManager::OnRecoveryComplete()
{
    FOREACH (const auto& pair, NodeMap) {
        auto* node = pair.second;
        node->ResetObjectLocks();
    }
}

void TCypressManager::DestroyNode(TCypressNodeBase* trunkNode)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(trunkNode->IsTrunk());

    auto securityManager = Bootstrap->GetSecurityManager();

    DestroyNodeBehavior(trunkNode);

    auto nodeHolder = NodeMap.Release(trunkNode->GetVersionedId());

    securityManager->ResetAccount(trunkNode);

    auto handler = GetHandler(trunkNode);
    handler->Destroy(trunkNode);
}

void TCypressManager::OnTransactionCommitted(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ReleaseLocks(transaction);
    MergeNodes(transaction);
    ReleaseCreatedNodes(transaction);
}

void TCypressManager::OnTransactionAborted(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ReleaseLocks(transaction);
    RemoveBranchedNodes(transaction);
    ReleaseCreatedNodes(transaction);
}

void TCypressManager::ReleaseLocks(TTransaction* transaction)
{
    FOREACH (auto* trunkNode, transaction->LockedNodes()) {
        ReleaseLock(trunkNode, transaction);
    }
    transaction->LockedNodes().clear();
}

void TCypressManager::ListSubtreeNodes(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    TSubtreeNodes* subtreeNodes)
{
    YCHECK(trunkNode->IsTrunk());

    auto transactionManager = Bootstrap->GetTransactionManager();

    const auto& rootId = trunkNode->GetId();
    subtreeNodes->push_back(trunkNode);
    switch (TypeFromId(rootId)) {
        case EObjectType::MapNode: {
            auto transactions = transactionManager->GetTransactionPath(transaction);
            std::reverse(transactions.begin(), transactions.end());

            yhash_map<Stroka, TCypressNodeBase*> children;
            FOREACH (auto* currentTransaction, transactions) {
                TVersionedObjectId versionedId(rootId, GetObjectId(currentTransaction));
                const auto* node = FindNode(versionedId);
                if (node) {
                    const auto* mapNode = static_cast<const TMapNode*>(node);
                    FOREACH (const auto& pair, mapNode->KeyToChild()) {
                        if (!pair.second) {
                            YCHECK(children.erase(pair.first) == 1);
                        } else {
                            auto* child = GetVersionedNode(pair.second, currentTransaction);
                            children[pair.first] = child;
                        }
                    }
                }
            }

            FOREACH (const auto& pair, children) {
                ListSubtreeNodes(pair.second, transaction, subtreeNodes);
            }
            break;
        }

        case EObjectType::ListNode: {
            auto* listRoot = static_cast<TListNode*>(trunkNode);
            FOREACH (auto* trunkChild, listRoot->IndexToChild()) {
                ListSubtreeNodes(trunkChild, transaction, subtreeNodes);
            }
            break;
        }

        default:
            break;
    }
}

void TCypressManager::MergeNode(
    TTransaction* transaction,
    TCypressNodeBase* branchedNode)
{
    auto objectManager = Bootstrap->GetObjectManager();
    auto securityManager = Bootstrap->GetSecurityManager();

    auto handler = GetHandler(branchedNode);

    auto* trunkNode = branchedNode->GetTrunkNode();
    auto branchedId = branchedNode->GetVersionedId();
    auto* parentTransaction = transaction->GetParent();
    auto originatingId = TVersionedNodeId(branchedId.ObjectId, GetObjectId(parentTransaction));

    if (branchedNode->GetLockMode() != ELockMode::Snapshot) {
        auto* originatingNode = NodeMap.Get(originatingId);

        // Merge changes back.
        handler->Merge(originatingNode, branchedNode);

        // The root needs a special handling.
        // When Cypress gets cleared, the root is created and is assigned zero creation time.
        // (We don't have any mutation context at hand to provide a synchronized timestamp.)
        // Later on, Cypress is initialized and filled with nodes.
        // At this point we set the root's creation time.
        if (trunkNode == RootNode && !parentTransaction) {
            originatingNode->SetCreationTime(originatingNode->GetModificationTime());
        }

        // Update resource usage.
        securityManager->ResetAccount(branchedNode);
        securityManager->UpdateAccountNodeUsage(originatingNode);

        LOG_INFO_UNLESS(IsRecovery(), "Node merged (NodeId: %s)", ~branchedId.ToString());
    } else {
        // Destroy the branched copy.
        handler->Destroy(branchedNode);

        // Update resource usage.
        securityManager->ResetAccount(branchedNode);

        LOG_INFO_UNLESS(IsRecovery(), "Node snapshot destroyed (NodeId: %s)", ~branchedId.ToString());
    }

    // Drop the implicit reference to the originator.
    objectManager->UnrefObject(trunkNode);

    // Remove the branched copy.
    NodeMap.Remove(branchedId);

    LOG_INFO_UNLESS(IsRecovery(), "Branched node removed (NodeId: %s)", ~branchedId.ToString());
}

void TCypressManager::MergeNodes(TTransaction* transaction)
{
    FOREACH (auto* node, transaction->BranchedNodes()) {
        MergeNode(transaction, node);
    }
    transaction->BranchedNodes().clear();
}

void TCypressManager::ReleaseCreatedNodes(TTransaction* transaction)
{
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (auto* node, transaction->StagedNodes()) {
        objectManager->UnrefObject(node);
    }
    transaction->StagedNodes().clear();
}

void TCypressManager::RemoveBranchedNode(TCypressNodeBase* branchedNode)
{
    auto objectManager = Bootstrap->GetObjectManager();
    auto securityManager = Bootstrap->GetSecurityManager();

    auto handler = GetHandler(branchedNode);

    auto* trunkNode = branchedNode->GetTrunkNode();
    auto branchedNodeId = branchedNode->GetVersionedId();

    // Update resource usage.
    securityManager->ResetAccount(branchedNode);

    // Drop the implicit reference to the originator.
    objectManager->UnrefObject(trunkNode);

    // Remove the node.
    handler->Destroy(branchedNode);
    NodeMap.Remove(branchedNodeId);

    LOG_INFO_UNLESS(IsRecovery(), "Branched node removed (NodeId: %s)", ~branchedNodeId.ToString());
}

void TCypressManager::RemoveBranchedNodes(TTransaction* transaction)
{
    FOREACH (auto* branchedNode, transaction->BranchedNodes()) {
        RemoveBranchedNode(branchedNode);
    }
    transaction->BranchedNodes().clear();
}

TYPath TCypressManager::GetNodePath(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    YCHECK(trunkNode->IsTrunk());

    auto proxy = GetVersionedNodeProxy(trunkNode, transaction);
    return proxy->GetResolver()->GetPath(proxy);
}

DEFINE_METAMAP_ACCESSORS(TCypressManager, Node, TCypressNodeBase, TVersionedNodeId, NodeMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
