#include "stdafx.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "node_proxy_detail.h"
#include "cypress_ypath_proxy.h"
#include <ytlib/cypress/cypress_ypath.pb.h>

#include <ytlib/actions/bind.h>
#include <ytlib/cell_master/load_context.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/misc/singleton.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/object_server/type_handler_detail.h>
#include <ytlib/object_server/object_service_proxy.h>

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
    explicit TLockTypeHandler(TCypressManagerPtr owner)
        : TObjectTypeHandlerBase(owner->Bootstrap, &owner->LockMap)
        , Owner(owner)
    { }

    virtual EObjectType GetType()
    {
        return EObjectType::Lock;
    }

    virtual IObjectProxy::TPtr GetProxy(
        const TObjectId& id,
        TTransaction* transaction);

private:
    TCypressManagerPtr Owner;

};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TLockProxy
    : public NObjectServer::TUnversionedObjectProxyBase<TLock>
{
public:
    TLockProxy(TCypressManagerPtr owner, const TLockId& id)
        : TBase(owner->Bootstrap, id, &owner->LockMap)
        , Owner(owner)
    {
        Logger = NCypress::Logger;
    }

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Confirm);
        return TBase::IsWriteRequest(context);
    }

private:
    typedef TUnversionedObjectProxyBase<TLock> TBase;

    TCypressManagerPtr Owner;

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
    {
        const auto& chunk = GetTypedImpl();
        attributes->push_back("mode");
        attributes->push_back("node_id");
        attributes->push_back("transaction_id");
        TBase::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer)
    {
        const auto& lock = GetTypedImpl();

        if (name == "mode") {
            BuildYsonFluently(consumer)
                .Scalar(FormatEnum(lock.GetMode()));
            return true;
        }

        if (name == "node_id") {
            BuildYsonFluently(consumer)
                .Scalar(lock.GetNodeId().ToString());
            return true;
        }

        if (name == "transaction_id") {
            BuildYsonFluently(consumer)
                .Scalar(lock.GetTransaction()->GetId().ToString());
            return true;
        }

        return TBase::GetSystemAttribute(name, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxy::TPtr TCypressManager::TLockTypeHandler::GetProxy(
    const TObjectId& id,
    NTransactionServer::TTransaction* transaction)
{
    UNUSED(transaction);
    return New<TLockProxy>(Owner, id);
}

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

    virtual IObjectProxy::TPtr GetProxy(
        const TObjectId& id,
        TTransaction* transaction)
    {
        return Owner->GetVersionedNodeProxy(id, transaction);
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

    auto metaState = bootstrap->GetMetaState();
    TLoadContext context(bootstrap);
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

    LOG_DEBUG("Created behavior for node %s",  ~id.ToString());
}

void TCypressManager::DestroyNodeBehavior(const TNodeId& id)
{
    auto it = NodeBehaviors.find(id);
    if (it == NodeBehaviors.end())
        return;

    it->second->Destroy();
    NodeBehaviors.erase(it);

    LOG_DEBUG("Destroyed behavior for node %s", ~id.ToString());
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
            NULL);
        return TResolveResult::There(~service, path);
    }

private:
    TBootstrap* Bootstrap;

};

} // namespace

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

ICypressNode* TCypressManager::FindVersionedNode(
    const TNodeId& nodeId,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto currentTransaction = transaction;
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

ICypressNode& TCypressManager::GetVersionedNode(
    const TNodeId& nodeId,
    TTransaction* transaction)
{
    auto* node = FindVersionedNode(nodeId, transaction);
    YASSERT(node);
    return *node;
}

ICypressNode* TCypressManager::FindVersionedNodeForUpdate(
    const TNodeId& nodeId,
    TTransaction* transaction,
    ELockMode requestedMode)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(requestedMode == ELockMode::Shared || requestedMode == ELockMode::Exclusive);

    // Validate a potential lock to see if we need to take it.
    // This throws an exception in case the validation fails.
    bool isMandatory;
    ValidateLock(nodeId, transaction, requestedMode, &isMandatory);
    if (isMandatory) {
        if (transaction == NULL) {
            ythrow yexception() << Sprintf("The requested operation requires %s lock but no current transaction is given",
                ~FormatEnum(requestedMode).Quote());
        }
        AcquireLock(nodeId, transaction, requestedMode);
    }

    auto transactionManager = Bootstrap->GetTransactionManager();
    auto currentTransaction = transaction;

    // Walk up from the current transaction to the root.
    while (true) {
        auto* currentNode = FindNode(TVersionedNodeId(nodeId, GetObjectId(currentTransaction)));
        if (currentNode) {
            // Check if we have found a node for the requested transaction or we need to branch it.
            if (currentTransaction == transaction) {
                // Update the lock mode if a higher one was requested (unless this is the null transaction).
                if (currentTransaction != NULL && currentNode->GetLockMode() < requestedMode) {
                    LOG_INFO_IF(!IsRecovery(), "Upgraded node %s lock from %s to %s at transaction %s",
                        ~nodeId.ToString(),
                        ~FormatEnum(currentNode->GetLockMode()).Quote(),
                        ~FormatEnum(requestedMode).Quote(),
                        ~GetObjectId(transaction).ToString());
                    currentNode->SetLockMode(requestedMode);
                }
                return currentNode;
            } else {
                return &BranchNode(*currentNode, transaction, requestedMode);
            }
        }

        if (!currentTransaction) {
            // Looks like there's no such node at all.
            return NULL;
        }

        // Move to the parent transaction.
        currentTransaction = currentTransaction->GetParent();
    }
}

ICypressNode& TCypressManager::GetVersionedNodeForUpdate(
    const TNodeId& nodeId,
    TTransaction* transaction,
    ELockMode requestedMode)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* node = FindVersionedNodeForUpdate(nodeId, transaction, requestedMode);
    YASSERT(node);
    return *node;
}

ICypressNodeProxy::TPtr TCypressManager::FindVersionedNodeProxy(
    const TNodeId& id,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    const auto* node = FindVersionedNode(id, transaction);
    if (!node) {
        return NULL;
    }

    return GetHandler(*node)->GetProxy(id, transaction);
}

ICypressNodeProxy::TPtr TCypressManager::GetVersionedNodeProxy(
    const TNodeId& nodeId,
    TTransaction* transaction)
{
    auto proxy = FindVersionedNodeProxy(nodeId, transaction);
    YASSERT(proxy);
    return proxy;
}

void TCypressManager::ValidateLock(
    const TNodeId& nodeId,
    TTransaction* transaction,
    ELockMode requestedMode,
    bool* isMandatory)
{
    YASSERT(requestedMode != ELockMode::None);
    YASSERT(isMandatory);

    // Check if the node supports this particular mode at all.
    auto handler = GetHandler(TypeFromId(nodeId));
    if (!handler->IsLockModeSupported(requestedMode)) {
        ythrow yexception() << Sprintf("Node %s does not support %s locks",
            ~GetNodePath(nodeId, transaction).Quote(),
            ~FormatEnum(requestedMode).Quote());
    }

    // Snapshot locks can only be taken inside a transaction.
    if (requestedMode == ELockMode::Snapshot && !transaction) {
        ythrow yexception() << Sprintf("Cannot take %s lock outside of a transaction",
            ~FormatEnum(requestedMode).Quote());
    }

    // Examine existing locks.
    const auto& node = NodeMap.Get(nodeId);
    FOREACH (auto* lock, node.Locks()) {
        // If the requested mode is Snapshot then no descendant transaction (including |transaction| itself)
        // may hold a lock other than Snapshot.
        // Allowing otherwise would cause a trouble when this nested transaction commits.
        if (requestedMode == ELockMode::Snapshot &&
            IsParentTransaction(lock->GetTransaction(), transaction) &&
            lock->GetMode() != ELockMode::Snapshot)
        {
            ythrow yexception() << Sprintf("Cannot take %s lock for node %s since %s lock is taken by descendant transaction %s",
                ~FormatEnum(requestedMode).Quote(),
                ~GetNodePath(nodeId, transaction).Quote(),
                ~FormatEnum(lock->GetMode()).Quote(),
                ~lock->GetTransaction()->GetId().ToString());
        }

        if (lock->GetTransaction() == transaction) {
            // Check for locks taken by the same transaction.

            // Same mode -- no lock is needed.
            if (requestedMode == lock->GetMode()) {
                *isMandatory = false;
                return;
            }

            // Stricter mode -- no lock is need (beware of Snapshot).
            if (requestedMode != ELockMode::Snapshot && requestedMode < lock->GetMode()) {
                *isMandatory = false;
                return;
            }

            // Snapshot lock is not compatible with any other lock.
            // NB: requestedMode == lock->GetMode() == ELockMode::Snapshot is already handled.
            if (lock->GetMode() == ELockMode::Snapshot) {
                ythrow yexception() << Sprintf("Cannot take %s lock for node %s since %s lock is already taken",
                    ~FormatEnum(requestedMode).Quote(),
                    ~GetNodePath(nodeId, transaction).Quote(),
                    ~FormatEnum(lock->GetMode()).Quote());
            }
        } else {
            // Check for locks taken by the other transactions.

            // Exclusive locks cannot be taken simultaneously with other locks (except for Snapshot).
            if (requestedMode == ELockMode::Exclusive && lock->GetMode() != ELockMode::Snapshot ||
                requestedMode != ELockMode::Snapshot  && lock->GetMode() == ELockMode::Exclusive)
            {
                ythrow yexception() << Sprintf("Cannot take %s lock for node %s since %s lock is taken by transaction %s",
                    ~FormatEnum(requestedMode).Quote(),
                    ~GetNodePath(nodeId, transaction).Quote(),
                    ~FormatEnum(lock->GetMode()).Quote(),
                    ~lock->GetTransaction()->GetId().ToString());
            }
        }
    }

    // If we're outside of a transaction then the lock is not needed.
    *isMandatory = transaction;
}

void TCypressManager::ValidateLock(
    const TNodeId& nodeId,
    TTransaction* transaction,
    ELockMode requestedMode)
{
    bool dummy;
    ValidateLock(nodeId, transaction, requestedMode, &dummy);
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

TLockId TCypressManager::AcquireLock(
    const TNodeId& nodeId,
    TTransaction* transaction,
    ELockMode mode)
{
    YASSERT(transaction);

    // Create a lock and register it within the transaction.
    auto objectManager = Bootstrap->GetObjectManager();
    auto lockId = objectManager->GenerateId(EObjectType::Lock);
    auto* lock = new TLock(lockId, nodeId, transaction, mode);
    LockMap.Insert(lockId, lock);

    transaction->Locks().push_back(lock);
    objectManager->RefObject(lockId);

    LOG_INFO_IF(!IsRecovery(), "Locked node %s with mode %s at transaction %s (LockId: %s)",
        ~nodeId.ToString(),
        ~FormatEnum(mode).Quote(),
        ~transaction->GetId().ToString(),
        ~lockId.ToString());

    // Assign the node to the node itself.
    auto& lockedNode = NodeMap.Get(nodeId);
    YVERIFY(lockedNode.Locks().insert(lock).second);

    // Snapshot locks always involve branching (unless the node is already branched by another Snapshot lock).
    if (mode == ELockMode::Snapshot) {
        auto& originatingNode = GetVersionedNode(nodeId, transaction);
        if (originatingNode.GetId().TransactionId == transaction->GetId()) {
            YASSERT(originatingNode.GetLockMode() == ELockMode::Snapshot);
        } else {
            BranchNode(originatingNode, transaction, mode);
        }
    }

    return lockId;
}

void TCypressManager::ReleaseLock(TLock* lock)
{
    // Remove the lock from the node itself.
    auto& lockedNode = NodeMap.Get(lock->GetNodeId());
    YVERIFY(lockedNode.Locks().erase(lock) == 1);

    Bootstrap->GetObjectManager()->UnrefObject(lock->GetId());
}

TLockId TCypressManager::LockVersionedNode(
    const TNodeId& nodeId,
    TTransaction* transaction,
    ELockMode requestedMode)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(requestedMode != ELockMode::None);

    if (transaction == NULL) {
        ythrow yexception() << "Cannot take a lock outside of a transaction";
    }

    ValidateLock(nodeId, transaction, requestedMode);
    return AcquireLock(nodeId, transaction, requestedMode);
}

void TCypressManager::RegisterNode(
    TTransaction* transaction,
    TAutoPtr<ICypressNode> node)
{
    auto nodeId = node->GetId().ObjectId;
    YASSERT(node->GetId().TransactionId == NullTransactionId);
    node->SetCreationTime(TInstant::Zero()); // TODO(roizner): fill in correctly

    auto node_ = node.Get();
    NodeMap.Insert(nodeId, node.Release());

    if (transaction) {
        transaction->CreatedNodes().push_back(node_);
        Bootstrap->GetObjectManager()->RefObject(nodeId);
    }

    LOG_INFO_IF(!IsRecovery(), "Registered node %s of type %s at transaction %s",
        ~nodeId.ToString(),
        ~TypeFromId(nodeId).ToString(),
        ~GetObjectId(transaction).ToString());

    if (IsLeader()) {
        CreateNodeBehavior(nodeId);
    }
}

ICypressNode& TCypressManager::BranchNode(
    ICypressNode& node,
    TTransaction* transaction,
    ELockMode mode)
{
    YASSERT(transaction);

    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = node.GetId();

    // Create a branched node and initialize its state.
    auto branchedNode = GetHandler(node)->Branch(node, transaction, mode);
    auto* branchedNode_ = branchedNode.Release();
    NodeMap.Insert(TVersionedNodeId(id.ObjectId, transaction->GetId()), branchedNode_);

    // Register the branched node with the transaction.
    transaction->BranchedNodes().push_back(branchedNode_);

    // The branched node holds an implicit reference to its originator.
    Bootstrap->GetObjectManager()->RefObject(id.ObjectId);
    
    LOG_INFO_IF(!IsRecovery(), "Branched node %s with %s mode",
        ~id.ToString(),
        ~FormatEnum(mode).Quote());

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

    MergeBranchedNodes(transaction);
    if (transaction.GetParent()) {
        PromoteLocks(transaction);
        PromoteCreatedNodes(transaction);
    } else {
        ReleaseLocks(transaction);
        ReleaseCreatedNodes(transaction);
    }
}

void TCypressManager::OnTransactionAborted(TTransaction& transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ReleaseLocks(transaction);
    RemoveBranchedNodes(transaction);
    ReleaseCreatedNodes(transaction);
}

void TCypressManager::ReleaseLocks(const TTransaction& transaction)
{
    FOREACH (auto* lock, transaction.Locks()) {
        ReleaseLock(lock);
    }
}

void TCypressManager::MergeBranchedNode(TTransaction& transaction, ICypressNode* branchedNode)
{
    auto handler = GetHandler(*branchedNode);
    auto parentTransaction = transaction.GetParent();
    auto branchedId = branchedNode->GetId();
    auto originatingId = TVersionedNodeId(branchedId.ObjectId, GetObjectId(parentTransaction));
    auto* originatingNode = NodeMap.Find(originatingId);
    if (originatingNode) {
        // Merge the changes back (unless the node is locked in Snapshot mode).
        if (branchedNode->GetLockMode() == ELockMode::Snapshot) {
            handler->Destroy(*branchedNode);
            LOG_INFO_IF(!IsRecovery(), "Removed branched node %s", ~branchedId.ToString());
        } else {
            handler->Merge(*originatingNode, *branchedNode);
            LOG_INFO_IF(!IsRecovery(), "Merged branched node %s", ~branchedId.ToString());
        }

        // Remove the branched copy.
        NodeMap.Remove(branchedId);

        // Upgrade lock mode if needed.
        if (parentTransaction && originatingNode->GetLockMode() < branchedNode->GetLockMode()) {
            YASSERT(originatingNode->GetLockMode() != ELockMode::Snapshot);
            YASSERT(branchedNode->GetLockMode() != ELockMode::Snapshot);
            originatingNode->SetLockMode(branchedNode->GetLockMode());
            LOG_INFO_IF(!IsRecovery(), "Upgraded lock mode of node %s to %s",
                ~originatingId.ToString(),
                ~FormatEnum(originatingNode->GetLockMode()).Quote());
        }

        // Drop the implicit reference to the originator.
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->UnrefObject(originatingId.ObjectId);
    } else {
        // Promote branched node to the parent transaction.
        YASSERT(parentTransaction);
        originatingNode = branchedNode;
        NodeMap.Release(branchedId);
        NodeMap.Insert(originatingId, originatingNode);
        parentTransaction->BranchedNodes().push_back(branchedNode);
        branchedNode->PromoteToTransaction(parentTransaction);

        LOG_DEBUG_IF(!IsRecovery(), "Promoted branched node %s to transaction %s",
            ~branchedId.ToString(),
            ~parentTransaction->GetId().ToString());
    }
}

void TCypressManager::MergeBranchedNodes(TTransaction& transaction)
{
    FOREACH (auto* node, transaction.BranchedNodes()) {
        MergeBranchedNode(transaction, node);
    }
}

void TCypressManager::PromoteCreatedNodes(NTransactionServer::TTransaction& transaction)
{
    auto* parentTransaction = transaction.GetParent();
    FOREACH (auto* node, transaction.CreatedNodes()) {
        parentTransaction->CreatedNodes().push_back(node);
        LOG_DEBUG_IF(!IsRecovery(), "Promoted node %s to transaction %s",
            ~node->GetId().ObjectId.ToString(),
            ~parentTransaction->GetId().ToString());
    }
}

void TCypressManager::ReleaseCreatedNodes(NTransactionServer::TTransaction& transaction)
{
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (auto* node, transaction.CreatedNodes()) {
        objectManager->UnrefObject(node->GetId().ObjectId);
    }
}

void TCypressManager::PromoteLocks(TTransaction& transaction)
{
    auto* parentTransaction = transaction.GetParent();
    FOREACH (auto* lock, transaction.Locks()) {
        lock->PromoteToTransaction(parentTransaction);
        parentTransaction->Locks().push_back(lock);
        LOG_DEBUG_IF(!IsRecovery(), "Promoted lock %s to transaction %s",
            ~lock->GetId().ToString(),
            ~parentTransaction->GetId().ToString());
    }
}

void TCypressManager::RemoveBranchedNodes(const TTransaction& transaction)
{
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (auto* branchedNode, transaction.BranchedNodes()) {
        // Remove the node.
        auto id = branchedNode->GetId();
        GetHandler(*branchedNode)->Destroy(*branchedNode);
        NodeMap.Remove(branchedNode->GetId());

        // Drop the implicit reference to the originator.
        objectManager->UnrefObject(id.ObjectId);

        LOG_INFO_IF(!IsRecovery(), "Removed branched node %s", ~id.ToString());
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

TYPath TCypressManager::GetNodePath(ICypressNodeProxy::TPtr proxy)
{
    INodePtr root;
    auto path = GetYPath(proxy, &root);
    auto rootId = dynamic_cast<ICypressNodeProxy*>(~root)->GetId();
    YASSERT(rootId == GetRootNodeId());
    return Stroka(TokenTypeToChar(RootToken)) + path;
}

TYPath TCypressManager::GetNodePath(
    const TNodeId& nodeId,
    NTransactionServer::TTransaction* transaction)
{
    return GetNodePath(GetVersionedNodeProxy(nodeId, transaction));
}

TYPath TCypressManager::GetNodePath(const TVersionedNodeId& id)
{
    auto transactionManager = Bootstrap->GetTransactionManager();
    auto* transaction = id.TransactionId == NullTransactionId
        ? NULL
        : &transactionManager->GetTransaction(id.TransactionId);
    return GetNodePath(id.ObjectId, transaction);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
