#include "stdafx.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "node_proxy_detail.h"
#include "cypress_service_proxy.h"
#include "cypress_ypath_proxy.h"
#include <ytlib/cypress/cypress_ypath.pb.h>

#include <ytlib/actions/bind.h>
#include <ytlib/cell_master/load_context.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/misc/singleton.h>
#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/object_server/type_handler_detail.h>

namespace NYT {
namespace NCypress {

using namespace NCellMaster;
using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NTransactionServer;
using namespace NMetaState;
using namespace NProto;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Cypress");

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TLockTypeHandler
    : public TObjectTypeHandlerBase<TLock>
{
public:
    TLockTypeHandler(TCypressManager* owner)
        : TObjectTypeHandlerBase(owner->Bootstrap, &owner->LockMap)
    { }

    virtual EObjectType GetType()
    {
        return EObjectType::Lock;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TNodeTypeHandler
    : public IObjectTypeHandler
{
public:
    TNodeTypeHandler(
        TCypressManager* owner,
        EObjectType type)
        : Owner(owner)
        , Type(type)
    { }

    virtual EObjectType GetType()
    {
        return Type;
    }

    virtual bool Exists(const TObjectId& id)
    {
        return Owner->FindNode(id) != NULL;
    }

    virtual i32 RefObject(const TObjectId& id)
    {
        return Owner->RefNode(id);
    }

    virtual i32 UnrefObject(const TObjectId& id)
    {
        return Owner->UnrefNode(id);
    }

    virtual i32 GetObjectRefCounter(const TObjectId& id)
    {
        return Owner->GetNodeRefCounter(id);
    }

    virtual IObjectProxy::TPtr GetProxy(const TVersionedObjectId& id)
    {
        return Owner->GetVersionedNodeProxy(id.ObjectId, id.TransactionId);
    }

    virtual TObjectId Create(
        TTransaction* transaction,
        TReqCreateObject* request,
        TRspCreateObject* response)
    {
        UNUSED(transaction);
        UNUSED(request);
        UNUSED(response);

        ythrow yexception() << Sprintf("Cannot create an instance of %s outside Cypress",
            ~FormatEnum(GetType()));
    }

    virtual bool IsTransactionRequired() const
    {
        return false;
    }

private:
    TCypressManager* Owner;
    EObjectType Type;

};

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TCypressManager(TBootstrap* bootstrap)
    : TMetaStatePart(
        ~bootstrap->GetMetaStateManager(),
        ~bootstrap->GetMetaState())
    , Bootstrap(bootstrap)
    , NodeMap(TNodeMapTraits(this))
    , TypeToHandler(MaxObjectType)
{
    YASSERT(bootstrap);

    VERIFY_INVOKER_AFFINITY(bootstrap->GetStateInvoker(), StateThread);

    auto transactionManager = bootstrap->GetTransactionManager();
    transactionManager->SubscribeTransactionCommitted(BIND(
        &TThis::OnTransactionCommitted,
        MakeStrong(this)));
    transactionManager->SubscribeTransactionAborted(BIND(
        &TThis::OnTransactionAborted,
        MakeStrong(this)));

    auto objectManager = bootstrap->GetObjectManager();
    objectManager->RegisterHandler(~New<TLockTypeHandler>(this));

    RegisterHandler(~New<TStringNodeTypeHandler>(Bootstrap));
    RegisterHandler(~New<TIntegerNodeTypeHandler>(Bootstrap));
    RegisterHandler(~New<TDoubleNodeTypeHandler>(Bootstrap));
    RegisterHandler(~New<TMapNodeTypeHandler>(Bootstrap));
    RegisterHandler(~New<TListNodeTypeHandler>(Bootstrap));

    TLoadContext context(bootstrap);

    auto metaState = bootstrap->GetMetaState();
    metaState->RegisterLoader(
        "Cypress.Keys.1",
        BIND(&TCypressManager::LoadKeys, MakeStrong(this)));
    metaState->RegisterLoader(
        "Cypress.Values.1",
        BIND(&TCypressManager::LoadValues, MakeStrong(this), context));
    metaState->RegisterSaver(
        "Cypress.Keys.1",
        BIND(&TCypressManager::SaveKeys, MakeStrong(this)),
        ESavePhase::Keys);
    metaState->RegisterSaver(
        "Cypress.Values.1",
        BIND(&TCypressManager::SaveValues, MakeStrong(this)),
        ESavePhase::Values);

    metaState->RegisterPart(this);
}

void TCypressManager::RegisterHandler(INodeTypeHandler::TPtr handler)
{
    // No thread affinity is given here.
    // This will be called during init-time only.

    YASSERT(handler);
    auto type = handler->GetObjectType();
    int typeValue = type.ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);
    YASSERT(!TypeToHandler[typeValue]);
    TypeToHandler[typeValue] = handler;

    Bootstrap->GetObjectManager()->RegisterHandler(~New<TNodeTypeHandler>(this, type));
}

INodeTypeHandler::TPtr TCypressManager::FindHandler(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    int typeValue = type.ToValue();
    if (typeValue < 0 || typeValue >= MaxObjectType) {
        return NULL;
    }

    return TypeToHandler[typeValue];
}

INodeTypeHandler::TPtr TCypressManager::GetHandler(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto handler = FindHandler(type);
    YASSERT(handler);
    return handler;
}

INodeTypeHandler::TPtr TCypressManager::GetHandler(const ICypressNode& node)
{
    return GetHandler(node.GetObjectType());
}

void TCypressManager::CreateNodeBehavior(const TNodeId& id)
{
    auto handler = GetHandler(TypeFromId(id));
    auto behavior = handler->CreateBehavior(id);
    if (!behavior)
        return;

    YVERIFY(NodeBehaviors.insert(MakePair(id, behavior)).second);

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

TNodeId TCypressManager::GetRootNodeId()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CreateId(
        EObjectType::MapNode,
        Bootstrap->GetObjectManager()->GetCellId(),
        0xffffffffffffffff);
}

namespace {

class TNotALeaderRootService
    : public TYPathServiceBase
{
public:
    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        UNUSED(path);
        UNUSED(verb);
        ythrow NRpc::TServiceException(TError(NRpc::EErrorCode::Unavailable, "Not an active leader"));
    }
};

class TLeaderRootService
    : public TYPathServiceBase
{
public:
    TLeaderRootService(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        UNUSED(verb);

        // Make a rigorous coarse check at the right thread.
        if (Bootstrap->GetMetaStateManager()->GetStateStatus() != EPeerStatus::Leading) {
            ythrow yexception() << "Not a leader";
        }

        auto cypressManager = Bootstrap->GetCypressManager();
        auto service = cypressManager->GetVersionedNodeProxy(
            cypressManager->GetRootNodeId(),
            NObjectServer::NullTransactionId);
        return TResolveResult::There(~service, path);
    }

private:
    TBootstrap* Bootstrap;

};

} // namespace <anonymous>

TYPathServiceProducer TCypressManager::GetRootServiceProducer()
{
    auto stateInvoker = MetaStateManager->GetStateInvoker();
    auto this_ = MakeStrong(this);
    return BIND([=] () -> IYPathServicePtr
        {
            // Make a coarse check at this (wrong) thread first.
            auto status = this_->MetaStateManager->GetStateStatusAsync();
            if (status == EPeerStatus::Leading) {
                return New<TLeaderRootService>(Bootstrap)->Via(~stateInvoker);
            } else {
                return RefCountedSingleton<TNotALeaderRootService>();
            }
        });

}

const ICypressNode* TCypressManager::FindVersionedNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId) const
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto transactionManager = Bootstrap->GetTransactionManager();
    auto currentTransactionId = transactionId;
    auto currentTransaction = transactionManager->FindTransaction(transactionId);

    // Check transactionId for validness. NullTransaction is OK here.
    if (transactionId != NullTransactionId && !currentTransaction) {
        return NULL;
    }

    while (true) {
        auto* currentNode = FindNode(TVersionedNodeId(nodeId, currentTransactionId));
        if (currentNode) {
            return currentNode;
        }

        if (!currentTransaction) {
            // Looks like there's no such node at all.
            return NULL;
        }

        // Move to the parent transaction.
        currentTransactionId = currentTransaction->GetParentId();
        currentTransaction = currentTransaction->GetParent();
    }
}

const ICypressNode& TCypressManager::GetVersionedNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId) const
{
    auto* node = FindVersionedNode(nodeId, transactionId);
    YASSERT(node);
    return *node;
}

ICypressNode* TCypressManager::FindVersionedNodeForUpdate(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    ELockMode requestedMode)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(requestedMode == ELockMode::Shared || requestedMode == ELockMode::Exclusive);

    // Validate a potential lock to see if we need to take it.
    // This throws an exception in case the validation fails.
    bool isMandatory;
    ValidateLock(nodeId, transactionId, requestedMode, &isMandatory);
    if (isMandatory) {
        if (transactionId == NullTransactionId) {
            ythrow yexception() << Sprintf("The requested operation requires %s lock but no current transaction is given",
                ~FormatEnum(requestedMode).Quote());
        }
        AcquireLock(nodeId, transactionId, requestedMode);
    }

    auto transactionManager = Bootstrap->GetTransactionManager();
    auto currentTransactionId = transactionId;
    auto currentTransaction = transactionManager->FindTransaction(transactionId);

    // Check transactionId for validness.
    if (transactionId != NullTransactionId && !currentTransaction) {
        return NULL;
    }

    // Walk up from the current transaction to the root.
    while (true) {
        auto* currentNode = FindNode(TVersionedNodeId(nodeId, currentTransactionId));
        if (currentNode) {
            // Check if we have found a node for the requested transaction or we need to branch it.
            if (currentTransactionId == transactionId) {
                // Update the lock mode if a higher one was requested (unless this is the null transaction).
                if (currentTransactionId != NullTransactionId && currentNode->GetLockMode() < requestedMode) {
                    LOG_INFO_IF(!IsRecovery(), "Node lock mode upgraded (NodeId: %s, TransactionId: %s, OldMode: %s, NewMode: %s)",
                        ~nodeId.ToString(),
                        ~transactionId.ToString(),
                        ~currentNode->GetLockMode().ToString(),
                        ~requestedMode.ToString());
                    currentNode->SetLockMode(requestedMode);
                }
                return currentNode;
            } else {
                return &BranchNode(*currentNode, transactionId, requestedMode);
            }
        }

        if (!currentTransaction) {
            // Looks like there's no such node at all.
            return NULL;
        }

        // Move to the parent transaction.
        currentTransactionId = currentTransaction->GetParentId();
        currentTransaction = currentTransaction->GetParent();
    }
}

ICypressNode& TCypressManager::GetVersionedNodeForUpdate(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    ELockMode requestedMode)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* node = FindVersionedNodeForUpdate(nodeId, transactionId, requestedMode);
    YASSERT(node);
    return *node;
}

ICypressNodeProxy::TPtr TCypressManager::FindVersionedNodeProxy(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YASSERT(nodeId != NullObjectId);
    const auto* node = FindVersionedNode(nodeId, transactionId);
    if (!node) {
        return NULL;
    }

    return GetHandler(*node)->GetProxy(TVersionedNodeId(nodeId, transactionId));
}

ICypressNodeProxy::TPtr TCypressManager::GetVersionedNodeProxy(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    auto proxy = FindVersionedNodeProxy(nodeId, transactionId);
    YASSERT(proxy);
    return proxy;
}

void TCypressManager::ValidateLock(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    ELockMode requestedMode,
    bool* isMandatory)
{
    auto handler = GetHandler(TypeFromId(nodeId));
    if (!handler->IsLockModeSupported(requestedMode)) {
        ythrow yexception() << Sprintf("Cannot take %s lock for node %s: the mode is not supported",
            ~FormatEnum(requestedMode).Quote(),
            ~nodeId.ToString());
    }

    // Check if we already have branched this node within the current or parent transaction.
    auto transactionManager = Bootstrap->GetTransactionManager();
    auto currentTransactionId = transactionId;
    while (currentTransactionId != NullTransactionId) {
        const auto* node = FindNode(TVersionedNodeId(nodeId, currentTransactionId));
        if (node) {
            if (!AreConcurrentLocksCompatible(node->GetLockMode(), requestedMode)) {
                ythrow yexception() << Sprintf("Cannot take %s lock for node %s: the node is already locked in %s mode",
                    ~FormatEnum(requestedMode).Quote(),
                    ~nodeId.ToString(),
                    ~FormatEnum(node->GetLockMode()).Quote());
            }
            if (node->GetLockMode() >= requestedMode) {
                // This node already has a lock that is at least as strong the requested one.
                if (isMandatory) {
                    *isMandatory = false;
                }
                return;
            }
        }

        // Move to the parent transaction.
        const auto& transaction = transactionManager->GetTransaction(currentTransactionId);
        currentTransactionId = transaction.GetParentId();
    }

    if (requestedMode != ELockMode::Snapshot) {
        // Examine existing locks in the subtree.
        const auto& lockedNode = NodeMap.Get(nodeId);
        FOREACH (const auto& lock, lockedNode.SubtreeLocks()) {
            // Check for download conflict.
            if (!AreCompetingLocksCompatible(lock->GetMode(), requestedMode)) {
                ythrow yexception() << Sprintf("Cannot take %s lock for node %s: conflict with %s downward lock at node %s taken by transaction %s",
                    ~FormatEnum(requestedMode).Quote(),
                    ~nodeId.ToString(),
                    ~FormatEnum(lock->GetMode()).Quote(),
                    ~lock->GetNodeId().ToString(),
                    ~lock->GetTransactionId().ToString());
            }
        }

        // Check existing locks on the upward path to the root.
        auto currentNodeId = nodeId;
        while (currentNodeId != NullObjectId) {
            const auto& currentNode = NodeMap.Get(currentNodeId);
            FOREACH (const auto& lock, currentNode.Locks()) {
                // Check if this is lock was taken by the same transaction,
                // is at least as strong as the requested one,
                // and has a proper recursive behavior.
                if (lock->GetTransactionId() == transactionId &&
                    lock->GetMode() >= requestedMode &&
                    (IsLockRecursive(lock->GetMode()) || currentNodeId == nodeId))
                {
                    if (isMandatory) {
                        *isMandatory = false;
                    }
                    return;
                }
                // Check for upward conflict.
                if (!AreCompetingLocksCompatible(lock->GetMode(), requestedMode)) {
                    ythrow yexception() << Sprintf("Cannot take %s lock for node %s: conflict with %s upward lock at node %s taken by transaction %s",
                        ~FormatEnum(requestedMode).Quote(),
                        ~nodeId.ToString(),
                        ~FormatEnum(lock->GetMode()).Quote(),
                        ~lock->GetNodeId().ToString(),
                        ~lock->GetTransactionId().ToString());
                }
            }
            currentNodeId = currentNode.GetParentId();
        }
    }

    // If we're outside of a transaction then the lock is not needed.
    if (transactionId == NullTransactionId) {
        if (requestedMode == ELockMode::Snapshot) {
            ythrow yexception() << "Cannot take Snapshot lock outside of a transaction";
        }
        if (isMandatory) {
            *isMandatory = false;
        }
    } else {
        if (isMandatory) {
            *isMandatory = true;
        }
    }
}

bool TCypressManager::AreCompetingLocksCompatible(ELockMode existingMode, ELockMode requestedMode)
{
    // For competing transactions snapshot locks are safe.
    if (existingMode == ELockMode::Snapshot || requestedMode == ELockMode::Snapshot) {
        return true;
    }
    // For competing exclusive locks are not compatible with others.
    if (existingMode == ELockMode::Exclusive || requestedMode == ELockMode::Exclusive) {
        return false;
    }
    return true;
}

bool TCypressManager::AreConcurrentLocksCompatible(ELockMode existingMode, ELockMode requestedMode)
{
    // For concurrent transactions snapshot lock is only compatible with another snapshot lock.
    if (existingMode == ELockMode::Snapshot && requestedMode != ELockMode::Snapshot) {
        return false;
    }
    if (requestedMode == ELockMode::Snapshot && existingMode != ELockMode::Snapshot) {
        return false;
    }
    return true;
}

bool TCypressManager::IsLockRecursive(ELockMode mode)
{
    return
        mode == ELockMode::Shared ||
        mode == ELockMode::Exclusive;
}

TLockId TCypressManager::AcquireLock(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    ELockMode mode)
{
    // Create a lock and register it within the transaction.
    auto objectManager = Bootstrap->GetObjectManager();
    auto lockId = objectManager ->GenerateId(EObjectType::Lock);
    auto* lock = new TLock(lockId, nodeId, transactionId, mode);
    LockMap.Insert(lockId, lock);

    auto transactionManager = Bootstrap->GetTransactionManager();
    auto& transaction = transactionManager->GetTransaction(transactionId);
    transaction.Locks().push_back(lock);
    objectManager->RefObject(lockId);

    LOG_INFO_IF(!IsRecovery(), "Node locked (LockId: %s, NodeId: %s, TransactionId: %s, Mode: %s)",
        ~lockId.ToString(),
        ~nodeId.ToString(),
        ~transactionId.ToString(),
        ~mode.ToString());

    // Assign the node to the node itself.
    auto& lockedNode = NodeMap.Get(nodeId);
    YVERIFY(lockedNode.Locks().insert(lock).second);

    // For recursive locks, also assign this lock to every node on the upward path.
    if (IsLockRecursive(mode)) {
        auto currentNodeId = lockedNode.GetParentId();
        while (currentNodeId != NullObjectId) {
            auto& currentNode = NodeMap.Get(currentNodeId);
            YVERIFY(currentNode.SubtreeLocks().insert(lock).second);
            currentNodeId = currentNode.GetParentId();
        }
    }

    // Snapshot locks always involve branching (unless the node is already branched by another Snapshot lock).
    if (mode == ELockMode::Snapshot) {
        const auto& originatingNode = GetVersionedNode(nodeId, transactionId);
        if (originatingNode.GetId().TransactionId == transactionId) {
            YASSERT(originatingNode.GetLockMode() == ELockMode::Snapshot);
        } else {
            BranchNode(GetNode(originatingNode.GetId()), transactionId, mode);
        }
    }

    return lockId;
}

void TCypressManager::ReleaseLock(TLock* lock)
{
    // Remove the lock from the node itself.
    auto& lockedNode = NodeMap.Get(lock->GetNodeId());
    YVERIFY(lockedNode.Locks().erase(lock) == 1);

    // For recursive locks, also remove the lock from the nodes on the upward path.
    if (IsLockRecursive(lock->GetMode())) {
        auto currentNodeId = lockedNode.GetParentId();
        while (currentNodeId != NullObjectId) {
            auto& node = NodeMap.Get(currentNodeId);
            YVERIFY(node.SubtreeLocks().erase(lock) == 1);
            currentNodeId = node.GetParentId();
        }
    }

    Bootstrap->GetObjectManager()->UnrefObject(lock->GetId());
}

TLockId TCypressManager::LockVersionedNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    ELockMode requestedMode)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(requestedMode != ELockMode::None);

    if (transactionId == NullTransactionId) {
        ythrow yexception() << "Cannot take a lock outside of a transaction";
    }

    ValidateLock(nodeId, transactionId, requestedMode);
    return AcquireLock(nodeId, transactionId, requestedMode);
}

void TCypressManager::RegisterNode(
    NTransactionServer::TTransaction* transaction,
    TAutoPtr<ICypressNode> node)
{
    auto nodeId = node->GetId().ObjectId;
    YASSERT(node->GetId().TransactionId == NullTransactionId);

    // If there's a transaction then append this node's id to the list.
    if (transaction) {
        transaction->CreatedNodeIds().push_back(nodeId);
    }

    NodeMap.Insert(nodeId, node.Release());

    LOG_INFO_IF(!IsRecovery(), "Node registered (Type: %s, NodeId: %s, TransactionId: %s)",
        ~TypeFromId(nodeId).ToString(),
        ~nodeId.ToString(),
        transaction ? ~transaction->GetId().ToString() : "None");

    if (IsLeader()) {
        CreateNodeBehavior(nodeId);
    }
}

ICypressNode& TCypressManager::BranchNode(
    ICypressNode& node,
    const TTransactionId& transactionId,
    ELockMode mode)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto nodeId = node.GetId().ObjectId;

    // Create a branched node and initialize its state.
    auto branchedNode = GetHandler(node)->Branch(node, transactionId, mode);
    auto* branchedNode_ = branchedNode.Release();
    NodeMap.Insert(TVersionedNodeId(nodeId, transactionId), branchedNode_);

    // Register the branched node with the transaction.
    auto& transaction = Bootstrap->GetTransactionManager()->GetTransaction(transactionId);
    transaction.BranchedNodeIds().push_back(nodeId);

    // The branched node holds an implicit reference to its originator.
    Bootstrap->GetObjectManager()->RefObject(nodeId);
    
    LOG_INFO_IF(!IsRecovery(), "Node branched (NodeId: %s, TransactionId: %s, Mode: %s)",
        ~nodeId.ToString(),
        ~transactionId.ToString(),
        ~mode.ToString());

    return *branchedNode_;
}

void TCypressManager::SaveKeys(TOutputStream* output) const
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.SaveKeys(output);
    LockMap.SaveKeys(output);
}

void TCypressManager::SaveValues(TOutputStream* output) const
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.SaveValues(output);
    LockMap.SaveValues(output);
}

void TCypressManager::LoadKeys(TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.LoadKeys(input);
    LockMap.LoadKeys(input);
}

void TCypressManager::LoadValues(const TLoadContext& context, TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.LoadValues(context, input);
    LockMap.LoadValues(context, input);
}

void TCypressManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.Clear();
    LockMap.Clear();

    // Create the root.
    auto* root = new TMapNode(GetRootNodeId());
    NodeMap.Insert(root->GetId(), root);
    Bootstrap->GetObjectManager()->RefObject(root->GetId().ObjectId);
}

void TCypressManager::OnLeaderRecoveryComplete()
{
    YASSERT(NodeBehaviors.empty());
    FOREACH(const auto& pair, NodeMap) {
        if (!pair.first.IsBranched()) {
            CreateNodeBehavior(pair.first.ObjectId);
        }
    }
}

void TCypressManager::OnStopLeading()
{
    FOREACH(const auto& pair, NodeBehaviors) {
        pair.second->Destroy();
    }
    NodeBehaviors.clear();
}

i32 TCypressManager::RefNode(const TNodeId& nodeId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto& node = NodeMap.Get(nodeId);
    return node.RefObject();
}

i32 TCypressManager::UnrefNode(const TNodeId& nodeId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto& node = NodeMap.Get(nodeId);

    i32 refCounter = node.UnrefObject();
    if (refCounter == 0) {
        DestroyNodeBehavior(nodeId);

        GetHandler(node)->Destroy(node);
        NodeMap.Remove(nodeId);
    }

    return refCounter;
}

i32 TCypressManager::GetNodeRefCounter(const TNodeId& nodeId)
{
    const auto& node = NodeMap.Get(nodeId);
    return node.GetObjectRefCounter();
}

void TCypressManager::OnTransactionCommitted(TTransaction& transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ReleaseLocks(transaction);
    MergeBranchedNodes(transaction);
    UnrefOriginatingNodes(transaction);
}

void TCypressManager::OnTransactionAborted(TTransaction& transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ReleaseLocks(transaction);
    RemoveBranchedNodes(transaction);
    UnrefOriginatingNodes(transaction);
}

void TCypressManager::ReleaseLocks(const TTransaction& transaction)
{
    auto transactionId = transaction.GetId();

    // Iterate over all locks created by the transaction.
    FOREACH (const auto& lockId, transaction.Locks()) {
        ReleaseLock(lockId);
    }
}

void TCypressManager::MergeBranchedNode(
    const TTransaction& transaction,
    const TNodeId& nodeId)
{
    TVersionedNodeId branchedId(nodeId, transaction.GetId());
    auto& branchedNode = NodeMap.Get(branchedId);

    // Find the appropriate originating node.
    ICypressNode* originatingNode;
    const auto* currentTransaction = &transaction;
    while (true) {
        YASSERT(currentTransaction);
        TVersionedNodeId currentOriginatingId(nodeId, currentTransaction->GetParentId());
        originatingNode = FindNode(currentOriginatingId);
        if (originatingNode)
            break;
        currentTransaction = currentTransaction->GetParent();
    }

    GetHandler(branchedNode)->Merge(*originatingNode, branchedNode);

    NodeMap.Remove(branchedId);

    LOG_INFO_IF(!IsRecovery(), "Node merged (NodeId: %s, TransactionId: %s)",
        ~nodeId.ToString(),
        ~transaction.GetId().ToString());
}

void TCypressManager::MergeBranchedNodes(const TTransaction& transaction)
{
    // Merge all branched nodes and remove them.
    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        MergeBranchedNode(transaction, nodeId);
    }
}

void TCypressManager::UnrefOriginatingNodes(const TTransaction& transaction)
{
    // Drop implicit references from branched nodes to their originators.
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        objectManager->UnrefObject(nodeId);
    }
}

void TCypressManager::RemoveBranchedNodes(const TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        TVersionedNodeId versionedId(nodeId, transactionId);
        auto& node = NodeMap.Get(versionedId);
        GetHandler(node)->Destroy(node);
        NodeMap.Remove(versionedId);

        LOG_INFO_IF(!IsRecovery(), "Branched node removed (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }
}

DEFINE_METAMAP_ACCESSORS(TCypressManager, Lock, TLock, TLockId, LockMap);
DEFINE_METAMAP_ACCESSORS(TCypressManager, Node, ICypressNode, TVersionedNodeId, NodeMap);

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TNodeMapTraits::TNodeMapTraits(TCypressManager* cypressManager)
    : CypressManager(cypressManager)
{ }

TAutoPtr<ICypressNode> TCypressManager::TNodeMapTraits::Create(const TVersionedNodeId& id) const
{
    auto type = TypeFromId(id.ObjectId);
    return CypressManager->GetHandler(type)->Create(id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
