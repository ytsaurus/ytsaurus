#include "stdafx.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "node_proxy_detail.h"

#include <ytlib/misc/singleton.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/cypress_ypath.pb.h>

#include <server/cell_master/serialization_context.h>
#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/ypath_detail.h>

#include <server/object_server/type_handler_detail.h>
#include <ytlib/object_client/object_service_proxy.h>

namespace NYT {
namespace NCypressServer {

using namespace NCellMaster;
using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NTransactionServer;
using namespace NMetaState;
using namespace NCypressClient::NProto;
using namespace NObjectClient;
using namespace NObjectServer;

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

    virtual EObjectType GetType() override
    {
        return Type;
    }

    virtual bool Exists(const TObjectId& id) override
    {
        auto* node = CypressManager->FindNode(id);
        return node && node->IsAlive();
    }

    virtual i32 RefObject(const TObjectId& id) override
    {
        return CypressManager->RefNode(id);
    }

    virtual i32 UnrefObject(const TObjectId& id) override
    {
        return CypressManager->UnrefNode(id);
    }

    virtual i32 GetObjectRefCounter(const TObjectId& id) override
    {
        return CypressManager->GetNodeRefCounter(id);
    }

    virtual void Destroy(const TObjectId& id) override
    {
        CypressManager->DestroyNode(id);
    }

    virtual IObjectProxyPtr GetProxy(
        const TObjectId& id,
        TTransaction* transaction) override
    {
        return CypressManager->GetVersionedNodeProxy(id, transaction);
    }

    virtual TObjectId Create(
        TTransaction* transaction,
        TReqCreateObject* request,
        TRspCreateObject* response) override
    {
        UNUSED(transaction);
        UNUSED(request);
        UNUSED(response);

        THROW_ERROR_EXCEPTION("Cannot create an instance of %s outside Cypress",
            ~FormatEnum(GetType()));
    }

    virtual bool IsTransactionRequired() const override
    {
        return false;
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
        auto root = cypressManager->FindVersionedNodeProxy(
            cypressManager->GetRootNodeId(),
            Transaction);

        return GetNodeByYPath(root, path.substr(1));
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
        auto service = cypressManager->GetVersionedNodeProxy(
            cypressManager->GetRootNodeId());
        return TResolveResult::There(service, path);
    }

private:
    TBootstrap* Bootstrap;

};

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TNodeMapTraits::TNodeMapTraits(TCypressManager* cypressManager)
    : CypressManager(cypressManager)
{ }

TAutoPtr<ICypressNode> TCypressManager::TNodeMapTraits::Create(const TVersionedNodeId& id) const
{
    auto type = TypeFromId(id.ObjectId);
    return CypressManager->GetHandler(type)->Instantiate(id);
}

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TCypressManager(TBootstrap* bootstrap)
    : TMetaStatePart(
        bootstrap->GetMetaStateFacade()->GetManager(),
        bootstrap->GetMetaStateFacade()->GetState())
    , Bootstrap(bootstrap)
    , NodeMap(TNodeMapTraits(this))
    , TypeToHandler(MaxObjectType)
{
    YCHECK(bootstrap);
    VERIFY_INVOKER_AFFINITY(bootstrap->GetMetaStateFacade()->GetInvoker(), StateThread);

    auto cellId = Bootstrap->GetObjectManager()->GetCellId();
    RootNodeId = MakeId(
        EObjectType::MapNode,
        cellId,
        0xffffffffffffffff,
        static_cast<ui32>(cellId * 901517) ^ 0x140a8383);

    RootService = New<TRootService>(Bootstrap)->Via(
        Bootstrap->GetMetaStateFacade()->GetGuardedInvoker());

    auto transactionManager = bootstrap->GetTransactionManager();
    transactionManager->SubscribeTransactionCommitted(BIND(
        &TThis::OnTransactionCommitted,
        MakeStrong(this)));
    transactionManager->SubscribeTransactionAborted(BIND(
        &TThis::OnTransactionAborted,
        MakeStrong(this)));

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
            BIND(&TCypressManager::LoadKeys, MakeStrong(this)),
            context);
        RegisterLoader(
            "Cypress.Values",
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

    Bootstrap->GetObjectManager()->RegisterHandler(New<TNodeTypeHandler>(this, type));
}

INodeTypeHandlerPtr TCypressManager::FindHandler(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    int typeValue = type.ToValue();
    if (typeValue < 0 || typeValue >= MaxObjectType) {
        return NULL;
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

INodeTypeHandlerPtr TCypressManager::GetHandler(const ICypressNode* node)
{
    return GetHandler(node->GetObjectType());
}

ICypressNode* TCypressManager::CreateNode(
    INodeTypeHandlerPtr handler,
    NTransactionServer::TTransaction* transaction,
    TReqCreate* request,
    TRspCreate* response,
    IAttributeDictionary* attributes)
{
    YASSERT(handler);
    YASSERT(request);
    YASSERT(response);

    auto node = handler->Create(
        transaction,
        request,
        response);
    
    // Make a rawptr copy, the next call will transfer the ownership.
    auto node_ = ~node;
    RegisterNode(transaction, node, attributes);

    auto nodeId = node_->GetId().ObjectId;
    *response->mutable_object_id() = nodeId.ToProto();

    return LockVersionedNode(node_, transaction, ELockMode::Exclusive);
}

ICypressNode* TCypressManager::CloneNode(
    ICypressNode* sourceNode,
    TTransaction* transaction)
{
    YASSERT(sourceNode);

    auto handler = GetHandler(sourceNode);
    auto clonedNode = handler->Clone(sourceNode, transaction);

    // Make a rawptr copy, the next call will transfer the ownership.
    auto clonedNode_ = ~clonedNode;
    RegisterNode(transaction, clonedNode, NULL);

    return LockVersionedNode(clonedNode_, transaction, ELockMode::Exclusive);
}

void TCypressManager::CreateNodeBehavior(const TNodeId& id)
{
    auto handler = GetHandler(TypeFromId(id));
    auto behavior = handler->CreateBehavior(id);
    if (!behavior)
        return;

    YCHECK(NodeBehaviors.insert(MakePair(id, behavior)).second);

    LOG_DEBUG("Node behavior created (NodeId: %s)",  ~id.ToString());
}

void TCypressManager::DestroyNodeBehavior(const TNodeId& id)
{
    auto it = NodeBehaviors.find(id);
    if (it == NodeBehaviors.end())
        return;

    it->second->Destroy();
    NodeBehaviors.erase(it);

    LOG_DEBUG("Node behavior destroyed (NodeId: %s)", ~id.ToString());
}

const TNodeId& TCypressManager::GetRootNodeId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RootNodeId;
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

ICypressNode* TCypressManager::FindVersionedNode(
    const TNodeId& nodeId,
    const TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* currentTransaction = transaction;
    while (true) {
        auto* currentNode = FindNode(TVersionedNodeId(nodeId, GetObjectId(currentTransaction)));
        if (currentNode) {
            return currentNode;
        }

        if (!currentTransaction) {
            // Looks like there's no such node at all.
            return NULL;
        }

        // Move to the parent transaction.
        currentTransaction = currentTransaction->GetParent();
    }
}

ICypressNode* TCypressManager::GetVersionedNode(
    const TNodeId& nodeId,
    const TTransaction* transaction)
{
    auto* node = FindVersionedNode(nodeId, transaction);
    YCHECK(node);
    return node;
}

ICypressNodeProxyPtr TCypressManager::FindVersionedNodeProxy(
    const TNodeId& id,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* node = FindVersionedNode(id, transaction);
    if (!node) {
        return NULL;
    }

    return GetHandler(node)->GetProxy(
        node->GetTrunkNode(),
        transaction);
}

ICypressNodeProxyPtr TCypressManager::GetVersionedNodeProxy(
    const TNodeId& nodeId,
    TTransaction* transaction)
{
    auto proxy = FindVersionedNodeProxy(nodeId, transaction);
    YCHECK(proxy);
    return proxy;
}

ICypressNodeProxyPtr TCypressManager::GetVersionedNodeProxy(
    const TVersionedNodeId& versionedId)
{
    auto transactionManager = Bootstrap->GetTransactionManager();
    auto* transaction =
        versionedId.TransactionId == NullTransactionId
        ?  NULL
        : transactionManager->GetTransaction(versionedId.TransactionId);
    return GetVersionedNodeProxy(versionedId.ObjectId, transaction);
}

void TCypressManager::ValidateLock(
    ICypressNode* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request,
    bool* isMandatory)
{
    YASSERT(isMandatory);

    auto nodeId = trunkNode->GetId().ObjectId;

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
                    ~GetNodePath(nodeId, transaction),
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
                ~GetNodePath(nodeId, transaction),
                ~FormatEnum(existingLock.Mode).Quote(),
                ~existingTransaction->GetId().ToString());
        }

        if (!transaction || IsConcurrentTransaction(transaction, existingTransaction)) {
            // For Exclusive locks we check locks held by concurrent transactions.
            if (request.Mode == ELockMode::Exclusive || existingLock.Mode == ELockMode::Exclusive) {
                THROW_ERROR_EXCEPTION("Cannot take %s lock for node %s since %s lock is taken by concurrent transaction %s",
                    ~FormatEnum(request.Mode).Quote(),
                    ~GetNodePath(nodeId, transaction),
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
                        ~GetNodePath(nodeId, transaction),
                        ~FormatEnum(existingLock.Mode).Quote(),
                        ~existingTransaction->GetId().ToString());
                }
                if (request.AttributeKey &&
                    existingLock.AttributeKeys.find(request.AttributeKey.Get()) != existingLock.AttributeKeys.end())
                {
                    THROW_ERROR_EXCEPTION("Cannot take %s lock for attribute %s of node %s since %s lock is taken by concurrent transaction %s",
                        ~FormatEnum(request.Mode).Quote(),
                        ~request.AttributeKey.Get().Quote(),
                        ~GetNodePath(nodeId, transaction),
                        ~FormatEnum(existingLock.Mode).Quote(),
                        ~existingTransaction->GetId().ToString());
                }
            }
        }
    }

    // If we're outside of a transaction then the lock is not needed.
    *isMandatory = (transaction != NULL);
}

void TCypressManager::ValidateLock(
    ICypressNode* trunkNode,
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

ICypressNode* TCypressManager::AcquireLock(
    ICypressNode* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request)
{
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
    auto nodeId = trunkNode->GetId().ObjectId;
    auto* branchedNode = FindNode(TVersionedNodeId(nodeId, transaction->GetId()));
    if (branchedNode) {
        if (branchedNode->GetLockMode() < request.Mode) {
            branchedNode->SetLockMode(request.Mode);
        }
        return branchedNode;
    }

    ICypressNode* originatingNode;
    std::vector<TTransaction*> intermediateTransactions;
    // Walk up to the root, find originatingNode, construct the list of
    // intermediate transactions.
    auto* currentTransaction = transaction;
    while (true) {
        originatingNode = FindNode(TVersionedNodeId(nodeId, GetObjectId(currentTransaction)));
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
    ICypressNode* trunkNode,
    TTransaction* transaction,
    const TLockRequest& request)
{
    TVersionedNodeId versionedId(trunkNode->GetId().ObjectId, transaction->GetId());
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

void TCypressManager::ReleaseLock(ICypressNode* trunkNode, TTransaction* transaction)
{
    YCHECK(trunkNode->Locks().erase(transaction) == 1);

    LOG_INFO_UNLESS(IsRecovery(), "Node unlocked (NodeId: %s, TransactionId: %s)",
        ~trunkNode->GetId().ToString(),
        ~transaction->GetId().ToString());
}

ICypressNode* TCypressManager::LockVersionedNode(
    const TNodeId& nodeId,
    TTransaction* transaction,
    const TLockRequest& request,
    bool recursive)
{
    auto* trunkNode = GetNode(nodeId);
    return LockVersionedNode(trunkNode, transaction, request, recursive);
}

ICypressNode* TCypressManager::LockVersionedNode(
    ICypressNode* node,
    TTransaction* transaction,
    const TLockRequest& request,
    bool recursive)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(request.Mode != ELockMode::None);

    TSubtreeNodes nodesToLock;
    if (recursive) {
        YCHECK(!request.ChildKey);
        YCHECK(!request.AttributeKey);
        ListSubtreeNodeIds(node, transaction, &nodesToLock);
    } else {
        nodesToLock.push_back(node);
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
        return GetVersionedNode(node->GetId().ObjectId, transaction);
    }

    if (!transaction) {
        THROW_ERROR_EXCEPTION("The requested operation requires %s lock but no current transaction is given",
            ~FormatEnum(request.Mode).Quote());
    }

    ICypressNode* lockedNode = NULL;
    FOREACH (auto* child, nodesToLock) {
        auto* lockedChild = AcquireLock(child->GetTrunkNode(), transaction, request);
        if (child == node) {
            lockedNode = lockedChild;
        }
    }

    YCHECK(lockedNode);
    return lockedNode;
}

void TCypressManager::SetModified(
    const TNodeId& nodeId,
    TTransaction* transaction)
{
    // Failure here means that the node wasn't indeed locked,
    // which is strange given that we're about to mark it as modified.
    auto* node = GetNode(TVersionedNodeId(
        nodeId,
        GetObjectId(transaction)));

    auto objectManager = Bootstrap->GetObjectManager();
    auto* mutationContext = Bootstrap
        ->GetMetaStateFacade()
        ->GetManager()
        ->GetMutationContext();

    node->SetModificationTime(mutationContext->GetTimestamp());
}

void TCypressManager::RegisterNode(
    TTransaction* transaction,
    TAutoPtr<ICypressNode> node,
    IAttributeDictionary* attributes)
{
    auto nodeId = node->GetId().ObjectId;
    YASSERT(node->GetId().TransactionId == NullTransactionId);
    
    auto objectManager = Bootstrap->GetObjectManager();
    auto* mutationContext = Bootstrap
        ->GetMetaStateFacade()
        ->GetManager()
        ->GetMutationContext();

    node->SetCreationTime(mutationContext->GetTimestamp());
    node->SetModificationTime(mutationContext->GetTimestamp());

    auto node_ = node.Get();
    NodeMap.Insert(nodeId, node.Release());

    // TODO(babenko): setting attributes here, in RegisterNode
    // is somewhat weird. Moving this logic to some other place, however,
    // complicates the code since we need to worry about possible
    // exceptions thrown from custom attribute validators.
    if (attributes) {
        auto proxy = GetVersionedNodeProxy(nodeId, transaction);
        try {
            proxy->Attributes().MergeFrom(*attributes);
        } catch (...) {
            GetHandler(node_)->Destroy(node_);
            NodeMap.Remove(nodeId);
            throw;
        }
    }

    if (transaction) {
        transaction->CreatedNodes().push_back(node_);
        objectManager->RefObject(node_);
    }

    LOG_INFO_UNLESS(IsRecovery(), "Node registered (NodeId: %s, Type: %s)",
        ~node_->GetId().ToString(),
        ~TypeFromId(nodeId).ToString());

    if (IsLeader()) {
        CreateNodeBehavior(nodeId);
    }
}

ICypressNode* TCypressManager::BranchNode(
    ICypressNode* node,
    TTransaction* transaction,
    ELockMode mode)
{
    YASSERT(node);
    YASSERT(transaction);
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = node->GetId();

    // Create a branched node and initialize its state.
    auto branchedNode = GetHandler(node)->Branch(node, transaction, mode);
    YASSERT(branchedNode->GetLockMode() == mode);
    auto* branchedNode_ = branchedNode.Release();
    NodeMap.Insert(TVersionedNodeId(id.ObjectId, transaction->GetId()), branchedNode_);

    // Register the branched node with the transaction.
    transaction->BranchedNodes().push_back(branchedNode_);

    // The branched node holds an implicit reference to its originator.
    Bootstrap->GetObjectManager()->RefObject(branchedNode_);
    
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
}

void TCypressManager::LoadValues(const NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.LoadValues(context);
}

void TCypressManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.Clear();

    // Create the root.
    auto* root = new TMapNode(GetRootNodeId());
    root->SetTrunkNode(root);

    NodeMap.Insert(root->GetId(), root);
    YCHECK(root->RefObject() == 1);
}

void TCypressManager::OnLeaderRecoveryComplete()
{
    YCHECK(NodeBehaviors.empty());
    FOREACH (const auto& pair, NodeMap) {
        if (!pair.first.IsBranched()) {
            CreateNodeBehavior(pair.first.ObjectId);
        }
    }
}

void TCypressManager::OnStopLeading()
{
    FOREACH (const auto& pair, NodeBehaviors) {
        pair.second->Destroy();
    }
    NodeBehaviors.clear();
}

i32 TCypressManager::RefNode(const TNodeId& nodeId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* node = NodeMap.Get(nodeId);
    return node->RefObject();
}

i32 TCypressManager::UnrefNode(const TNodeId& nodeId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* node = NodeMap.Get(nodeId);
    return node->UnrefObject();
}

void TCypressManager::DestroyNode(const TNodeId& nodeId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    DestroyNodeBehavior(nodeId);
    
    TAutoPtr<ICypressNode> node(NodeMap.Release(nodeId));
    GetHandler(~node)->Destroy(~node);
}

i32 TCypressManager::GetNodeRefCounter(const TNodeId& nodeId)
{
    const auto* node = NodeMap.Get(nodeId);
    return node->GetObjectRefCounter();
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

void TCypressManager::ListSubtreeNodeIds(
    ICypressNode* root,
    NTransactionServer::TTransaction* transaction,
    TSubtreeNodes* subtreeNodes)
{
    auto transactionManager = Bootstrap->GetTransactionManager();
    
    auto rootId = root->GetId().ObjectId;
    subtreeNodes->push_back(root);  
    switch (TypeFromId(rootId)) {
        case EObjectType::MapNode: {
            auto transactions = transactionManager->GetTransactionPath(transaction);
            std::reverse(transactions.begin(), transactions.end());

            yhash_map<Stroka, ICypressNode*> children;
            FOREACH (const auto* currentTransaction, transactions) {
                TVersionedObjectId versionedId(rootId, GetObjectId(currentTransaction));
                const auto* node = FindNode(versionedId);
                if (node) {
                    const auto* mapNode = static_cast<const TMapNode*>(node);
                    FOREACH (const auto& pair, mapNode->KeyToChild()) {
                        if (pair.second == NullObjectId) {
                            YCHECK(children.erase(pair.first) == 1);
                        } else {
                            auto* child = GetVersionedNode(pair.second, currentTransaction);
                            children[pair.first] = child;
                        }
                    }
                }
            }

            FOREACH (const auto& pair, children) {
                ListSubtreeNodeIds(pair.second, transaction, subtreeNodes);
            }
            break;
        }

        case EObjectType::ListNode: {
            auto* listRoot = static_cast<TListNode*>(root);
            FOREACH (const auto& childId, listRoot->IndexToChild()) {
                auto* child = GetVersionedNode(childId, transaction);
                ListSubtreeNodeIds(child, transaction, subtreeNodes);
            }
            break;
        }

        default:
            break;
    }
}

void TCypressManager::MergeNode(TTransaction* transaction, ICypressNode* branchedNode)
{
    auto objectManager = Bootstrap->GetObjectManager();
    auto handler = GetHandler(branchedNode);
    
    auto branchedId = branchedNode->GetId();
    auto* parentTransaction = transaction->GetParent();
    auto originatingId = TVersionedNodeId(branchedId.ObjectId, GetObjectId(parentTransaction));

    if (branchedNode->GetLockMode() != ELockMode::Snapshot) {
        // Merge changes back.
        auto* originatingNode = NodeMap.Get(originatingId);
        handler->Merge(originatingNode, branchedNode);

        // The root needs a special handling.
        // When Cypress gets cleared, the root is created and is assigned zero creation time.
        // (We don't have any mutation context at hand to provide a synchronized timestamp.)
        // Later on, Cypress is initialized and filled with nodes.
        // At this point we set the root's creation time.
        if (originatingId == GetRootNodeId() && !parentTransaction) {
            originatingNode->SetCreationTime(originatingNode->GetModificationTime());
        }

        LOG_INFO_UNLESS(IsRecovery(), "Node merged (NodeId: %s)", ~branchedId.ToString());
    } else {
        handler->Destroy(branchedNode);
        LOG_INFO_UNLESS(IsRecovery(), "Node snapshot destroyed (NodeId: %s)", ~branchedId.ToString());
    }

    // Remove the branched copy.
    NodeMap.Remove(branchedId);

    // Drop the implicit reference to the originator.
    objectManager->UnrefObject(originatingId);

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
    FOREACH (auto* node, transaction->CreatedNodes()) {
        objectManager->UnrefObject(node);
    }
    transaction->CreatedNodes().clear();
}

void TCypressManager::RemoveBranchedNodes(TTransaction* transaction)
{
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (auto* branchedNode, transaction->BranchedNodes()) {
        // Remove the node.
        auto branchedNodeId = branchedNode->GetId();
        GetHandler(branchedNode)->Destroy(branchedNode);
        NodeMap.Remove(branchedNodeId);

        // Drop the implicit reference to the originator.
        objectManager->UnrefObject(branchedNodeId);

        LOG_INFO_UNLESS(IsRecovery(), "Branched node removed (NodeId: %s)", ~branchedNodeId.ToString());
    }
    transaction->BranchedNodes().clear();
}

TYPath TCypressManager::GetNodePath(
    const TNodeId& nodeId,
    TTransaction* transaction)
{
    auto proxy = GetVersionedNodeProxy(nodeId, transaction);
    return proxy->GetResolver()->GetPath(proxy);
}

DEFINE_METAMAP_ACCESSORS(TCypressManager, Node, ICypressNode, TVersionedNodeId, NodeMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
