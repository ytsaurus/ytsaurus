#include "stdafx.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "node_proxy_detail.h"
#include "cypress_service_proxy.h"
#include "cypress_ypath_proxy.h"
#include "cypress_ypath.pb.h"

#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/rpc/message.h>
#include <ytlib/object_server/type_handler_detail.h>

namespace NYT {
namespace NCypress {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NTransactionServer;
using namespace NMetaState;
using namespace NProto;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TLockTypeHandler
    : public TObjectTypeHandlerBase<TLock>
{
public:
    TLockTypeHandler(TCypressManager* owner)
        : TObjectTypeHandlerBase(~owner->ObjectManager, &owner->LockMap)
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

    virtual IObjectProxy::TPtr GetProxy(const TObjectId& id)
    {
        return Owner->GetVersionedNodeProxy(id, NullTransactionId);
    }

    virtual TObjectId CreateFromManifest(
        const TTransactionId& transactionId,
        IMapNode* manifest)
    {
        UNUSED(transactionId);
        UNUSED(manifest);
        ythrow yexception() << "Cannot create a node outside Cypress";
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

//! A wrapper that is used to postpone a reply until the change is committed by quorum.
class TCypressManager::TServiceContextWrapper
    : public IServiceContext
{
public:
    TServiceContextWrapper(IServiceContext* underlyingContext)
        : UnderlyingContext(underlyingContext)
        , Replied(false)
    { }

    virtual NBus::IMessage::TPtr GetRequestMessage() const
    {
        return UnderlyingContext->GetRequestMessage();
    }

    virtual const NRpc::TRequestId& GetRequestId() const
    {
        return UnderlyingContext->GetRequestId();
    }

    virtual const Stroka& GetPath() const
    {
        return UnderlyingContext->GetPath();
    }

    virtual const Stroka& GetVerb() const
    {
        return UnderlyingContext->GetVerb();
    }

    virtual bool IsOneWay() const
    {
        return UnderlyingContext->IsOneWay();
    }

    virtual bool IsReplied() const
    {
        return Replied;
    }

    virtual void Reply(const TError& error)
    {
        YASSERT(!Replied);
        Replied = true;
        Error = error;
    }

    void Flush()
    {
        YASSERT(Replied);
        UnderlyingContext->Reply(Error);
    }

    virtual TError GetError() const
    {
        return Error;
    }

    virtual TSharedRef GetRequestBody() const
    {
        return UnderlyingContext->GetRequestBody();
    }

    virtual void SetResponseBody(const TSharedRef& responseBody)
    {
        UnderlyingContext->SetResponseBody(responseBody);
    }

    virtual const yvector<TSharedRef>& RequestAttachments() const
    {
        return UnderlyingContext->RequestAttachments();
    }

    virtual yvector<TSharedRef>& ResponseAttachments()
    {
        return UnderlyingContext->ResponseAttachments();
    }

    virtual void SetRequestInfo(const Stroka& info)
    {
       UnderlyingContext->SetRequestInfo(info);
    }

    virtual Stroka GetRequestInfo() const
    {
        return UnderlyingContext->GetRequestInfo();
    }

    virtual void SetResponseInfo(const Stroka& info)
    {
        UnderlyingContext->SetRequestInfo(info);
    }

    virtual Stroka GetResponseInfo()
    {
        return UnderlyingContext->GetRequestInfo();
    }

    virtual IAction::TPtr Wrap(IAction* action) 
    {
        return UnderlyingContext->Wrap(action);
    }

private:
    IServiceContext::TPtr UnderlyingContext;
    TError Error;
    bool Replied;

};

class TCypressManager::TYPathProcessor
    : public IYPathProcessor
{
public:
    TYPathProcessor(TCypressManager* owner)
        : Owner(owner)
    { }

    virtual void Resolve(
        const TYPath& path,
        const Stroka& verb,
        IYPathService::TPtr* suffixService,
        TYPath* suffixPath) 
    {
        if (path.empty()) {
            ythrow yexception() << "YPath cannot be empty";
        }

        auto currentPath = path;

        if (!currentPath.empty() && currentPath.has_prefix(TransactionIdMarker)) {
            Stroka token;
            ChopTransactionIdToken(currentPath, &token, &currentPath);
            if (!TObjectId::FromString(token.substr(TransactionIdMarker.length()), &TransactionId)) {
                ythrow yexception() << Sprintf("Error parsing transaction id (Value: %s)", ~token);
            }
            if (TransactionId != NullTransactionId && !Owner->TransactionManager->FindTransaction(TransactionId)) {
                ythrow yexception() <<  Sprintf("No such transaction (TransactionId: %s)", ~TransactionId.ToString());
            }
        }

        if (currentPath.has_prefix(RootMarker)) {
            currentPath = currentPath.substr(RootMarker.length());
            RootId = Owner->GetRootNodeId();
        } else if (currentPath.has_prefix(ObjectIdMarker)) {
            Stroka token;
            ChopYPathToken(currentPath, &token, &currentPath);
            if (!TObjectId::FromString(token.substr(ObjectIdMarker.length()), &RootId)) {
                ythrow yexception() << Sprintf("Error parsing object id (Value: %s)", ~token);
            }
        } else {
            ythrow yexception() << Sprintf("Invalid YPath syntax (Path: %s)", ~path);
        }

        auto rootProxy = Owner->FindObjectProxy(RootId, TransactionId);
        if (!rootProxy) {
            ythrow yexception() << Sprintf("No such object (ObjectId: %s)", ~RootId.ToString());
        }

        ResolveYPath(
            ~rootProxy,
            currentPath,
            verb,
            suffixService,
            suffixPath);
    }

    virtual void Execute(
        IYPathService* service,
        NRpc::IServiceContext* context)
    {
        VERIFY_THREAD_AFFINITY(Owner->StateThread);

        auto proxy = dynamic_cast<IObjectProxy*>(service);
        if (!proxy || !proxy->IsWriteRequest(context)) {
            LOG_INFO("Executing a read-only request (Path: %s, Verb: %s, ObjectId: %s, TransactionId: %s)",
                ~context->GetPath(),
                ~context->GetVerb(),
                proxy ? ~proxy->GetId().ToString() : "N/A",
                ~TransactionId.ToString());
            service->Invoke(context);
            return;
        }

        TMsgExecuteVerb message;
        message.set_object_id(proxy->GetId().ToProto());
        message.set_transaction_id(TransactionId.ToProto());

        auto requestMessage = context->GetRequestMessage();
        FOREACH (const auto& part, requestMessage->GetParts()) {
            message.add_request_parts(part.Begin(), part.Size());
        }

        auto context_ = IServiceContext::TPtr(context);
        auto wrappedContext = New<TServiceContextWrapper>(context);

        auto change = CreateMetaChange(
            ~Owner->MetaStateManager,
            message,
            ~FromMethod(
                &TCypressManager::DoExecuteVerb,
                Owner,
                TransactionId,
                proxy,
                ~wrappedContext));

        change
            ->OnSuccess(~FromFunctor([=] (TVoid)
                {
                    wrappedContext->Flush();
                }))
            ->OnError(~FromFunctor([=] ()
                {
                    context_->Reply(TError(
                        NRpc::EErrorCode::Unavailable,
                        "Error committing meta state changes"));
                }))
            ->Commit();
    }

private:
    TCypressManager::TPtr Owner;
    TObjectId RootId;
    TTransactionId TransactionId;

    static void ChopTransactionIdToken(
        const TYPath& path,
        Stroka* token,
        TYPath* suffixPath)
    {
        size_t index = path.find_first_of("/#");
        if (index == TYPath::npos) {
            ythrow yexception() << Sprintf("YPath does not refer to any object (Path: %s)", ~path);
        }

        *token = path.substr(0, index);
        *suffixPath = path.substr(index);
    }

};

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TCypressManager(
    IMetaStateManager* metaStateManager,
    TCompositeMetaState* metaState,
    TTransactionManager* transactionManager,
    NObjectServer::TObjectManager* objectManager)
    : TMetaStatePart(metaStateManager, metaState)
    , TransactionManager(transactionManager)
    , ObjectManager(objectManager)
    , NodeMap(TNodeMapTraits(this))
    , TypeToHandler(MaxObjectType)
{
    YASSERT(transactionManager);
    YASSERT(objectManager);

    VERIFY_INVOKER_AFFINITY(metaStateManager->GetStateInvoker(), StateThread);

    transactionManager->OnTransactionCommitted().Subscribe(FromMethod(
        &TThis::OnTransactionCommitted,
        TPtr(this)));
    transactionManager->OnTransactionAborted().Subscribe(FromMethod(
        &TThis::OnTransactionAborted,
        TPtr(this)));

    objectManager->RegisterHandler(~New<TLockTypeHandler>(this));

    RegisterHandler(~New<TStringNodeTypeHandler>(this));
    RegisterHandler(~New<TInt64NodeTypeHandler>(this));
    RegisterHandler(~New<TDoubleNodeTypeHandler>(this));
    RegisterHandler(~New<TMapNodeTypeHandler>(this));
    RegisterHandler(~New<TListNodeTypeHandler>(this));

    RegisterMethod(this, &TThis::DoReplayVerb);

    metaState->RegisterLoader(
        "Cypress.1",
        FromMethod(&TCypressManager::Load, TPtr(this)));
    metaState->RegisterSaver(
        "Cypress.1",
        FromMethod(&TCypressManager::Save, TPtr(this)));

    metaState->RegisterPart(this);
}

void TCypressManager::RegisterHandler(INodeTypeHandler* handler)
{
    // No thread affinity is given here.
    // This will be called during init-time only.

    YASSERT(handler);
    auto type = handler->GetObjectType();
    int typeValue = type.ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);
    YASSERT(!TypeToHandler[typeValue]);
    TypeToHandler[typeValue] = handler;

    ObjectManager->RegisterHandler(~New<TNodeTypeHandler>(this, type));
}

INodeTypeHandler* TCypressManager::GetHandler(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto handler = TypeToHandler[static_cast<int>(type)];
    YASSERT(handler);
    return ~handler;
}

INodeTypeHandler* TCypressManager::GetHandler(const ICypressNode& node)
{
    return GetHandler(node.GetObjectType());
}

void TCypressManager::CreateNodeBehavior(const ICypressNode& node)
{
    auto nodeId = node.GetId();
    YASSERT(!nodeId.IsBranched());

    auto handler = GetHandler(node);
    auto behavior = handler->CreateBehavior(node);
    if (!behavior)
        return;

    YVERIFY(NodeBehaviors.insert(MakePair(nodeId.ObjectId, behavior)).second);

    LOG_DEBUG_IF(!IsRecovery(), "Node behavior created (NodeId: %s)",  ~nodeId.ObjectId.ToString());
}

void TCypressManager::DestroyNodeBehavior(const ICypressNode& node)
{
    auto nodeId = node.GetId();
    YASSERT(!nodeId.IsBranched());

    auto it = NodeBehaviors.find(nodeId.ObjectId);
    if (it == NodeBehaviors.end())
        return;

    it->second->Destroy();
    NodeBehaviors.erase(it);

    LOG_DEBUG_IF(!IsRecovery(), "Node behavior destroyed (NodeId: %s)", ~nodeId.ObjectId.ToString());
}

TNodeId TCypressManager::GetRootNodeId()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CreateId(
        EObjectType::MapNode,
        ObjectManager->GetCellId(),
        0xffffffffffffffff);
}

TObjectManager* TCypressManager::GetObjectManager() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ~ObjectManager;
}

TTransactionManager* TCypressManager::GetTransactionManager() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ~TransactionManager;
}


const ICypressNode* TCypressManager::FindVersionedNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    // Walk up from the current transaction to the root.
    auto currentTransactionId = transactionId;
    while (true) {
        auto* currentNode = FindNodeForUpdate(TVersionedNodeId(nodeId, currentTransactionId));
        if (currentNode) {
            return currentNode;
        }

        if (currentTransactionId == NullTransactionId) {
            // Looks like there's no such node at all.
            return NULL;
        }

        // Move to the parent transaction.
        const auto& transaction = TransactionManager->GetTransaction(transactionId);
        currentTransactionId = transaction.GetParentId();
    }
}

const ICypressNode& TCypressManager::GetVersionedNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
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

    // Walk up from the current transaction to the root.
    auto currentTransactionId = transactionId;
    while (true) {
        auto* currentNode = FindNodeForUpdate(TVersionedNodeId(nodeId, currentTransactionId));
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

        if (currentTransactionId == NullTransactionId) {
            // Looks like there's no such node at all.
            return NULL;
        }

        // Move to the parent transaction.
        const auto& transaction = TransactionManager->GetTransaction(transactionId);
        currentTransactionId = transaction.GetParentId();
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

IObjectProxy::TPtr TCypressManager::FindObjectProxy(
    const TObjectId& objectId,
    const TTransactionId& transactionId)
{
    // NullObjectId means the root transaction.
    if (objectId == NullObjectId) {
        return TransactionManager->GetRootTransactionProxy();
    }

    // First try to fetch a proxy of a Cypress node.
    // This way we don't lose information about the current transaction.
    auto nodeProxy = FindVersionedNodeProxy(objectId, transactionId);
    if (nodeProxy) {
        return nodeProxy;
    }

    // Next try fetching a proxy for a generic object.
    auto objectProxy = ObjectManager->FindProxy(objectId);
    if (objectProxy) {
        return objectProxy;
    }

    // Nothing found.
    return NULL;
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

    return GetHandler(*node)->GetProxy(*node, transactionId);
}

IObjectProxy::TPtr TCypressManager::GetVersionedObjectProxy(
    const TObjectId& objectId,
    const TTransactionId& transactionId)
{
    auto proxy = FindObjectProxy(objectId, transactionId);
    YASSERT(proxy);
    return proxy;
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
        const auto& transaction = TransactionManager->GetTransaction(currentTransactionId);
        currentTransactionId = transaction.GetParentId();
    }

    if (requestedMode != ELockMode::Snapshot) {
        // Examine existing locks in the subtree.
        const auto& lockedNode = NodeMap.Get(nodeId);
        FOREACH (const auto& lockId, lockedNode.SubtreeLockIds()) {
            const auto& lock = GetLock(lockId);
            // Check for download conflict.
            if (!AreCompetingLocksCompatible(lock.GetMode(), requestedMode)) {
                ythrow yexception() << Sprintf("Cannot take %s lock for node %s: conflict with %s a downward lock at node %s taken by transaction %s",
                    ~FormatEnum(requestedMode).Quote(),
                    ~nodeId.ToString(),
                    ~FormatEnum(lock.GetMode()).Quote(),
                    ~lock.GetNodeId().ToString(),
                    ~lock.GetTransactionId().ToString());
            }
        }

        // Check existing locks on the upward path to the root.
        auto currentNodeId = nodeId;
        while (currentNodeId != NullObjectId) {
            const auto& currentNode = NodeMap.Get(currentNodeId);
            FOREACH (const auto& lockId, currentNode.LockIds()) {
                const auto& lock = GetLock(lockId);
                // Check if this is lock was taken by the same transaction,
                // is at least as strong as the requested one,
                // and has a proper recursive behavior.
                if (lock.GetTransactionId() == transactionId &&
                    lock.GetMode() >= requestedMode &&
                    (IsLockRecursive(lock.GetMode()) || currentNodeId == nodeId))
                {
                    if (isMandatory) {
                        *isMandatory = false;
                    }
                    return;
                }
                // Check for upward conflict.
                if (!AreCompetingLocksCompatible(lock.GetMode(), requestedMode)) {
                    ythrow yexception() << Sprintf("Cannot take %s lock for node %s: conflict with %s an upward lock at node %s taken by transaction %s",
                        ~FormatEnum(requestedMode).Quote(),
                        ~nodeId.ToString(),
                        ~FormatEnum(lock.GetMode()).Quote(),
                        ~lock.GetNodeId().ToString(),
                        ~lock.GetTransactionId().ToString());
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
    auto lockId = ObjectManager->GenerateId(EObjectType::Lock);
    auto* lock = new TLock(lockId, nodeId, transactionId, mode);
    LockMap.Insert(lockId, lock);
    ObjectManager->RefObject(lockId);

    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.LockIds().push_back(lock->GetId());

    LOG_INFO_IF(!IsRecovery(), "Node locked (LockId: %s, NodeId: %s, TransactionId: %s, Mode: %s)",
        ~lockId.ToString(),
        ~nodeId.ToString(),
        ~transactionId.ToString(),
        ~mode.ToString());

    // Assign the node to the node itself.
    auto& lockedNode = NodeMap.GetForUpdate(nodeId);
    YVERIFY(lockedNode.LockIds().insert(lockId).second);

    // For recursive locks, also assign this lock to every node on the upward path.
    if (IsLockRecursive(mode)) {
        auto currentNodeId = lockedNode.GetParentId();
        while (currentNodeId != NullObjectId) {
            auto& currentNode = NodeMap.GetForUpdate(currentNodeId);
            YVERIFY(currentNode.SubtreeLockIds().insert(lockId).second);
            currentNodeId = currentNode.GetParentId();
        }
    }

    // Snapshot locks always involve branching (unless the node is already branched by another Snapshot lock).
    if (mode == ELockMode::Snapshot) {
        const auto& originatingNode = GetVersionedNode(nodeId, transactionId);
        if (originatingNode.GetId().TransactionId == transactionId) {
            YASSERT(originatingNode.GetLockMode() == ELockMode::Snapshot);
        } else {
            BranchNode(GetNodeForUpdate(originatingNode.GetId()), transactionId, mode);
        }
    }

    return lockId;
}

void TCypressManager::ReleaseLock(const TLockId& lockId)
{
    const auto& lock = LockMap.Get(lockId);

    // Remove the lock from the node itself.
    auto& lockedNode = NodeMap.GetForUpdate(lock.GetNodeId());
    YVERIFY(lockedNode.LockIds().erase(lockId) == 1);

    // For recursive locks, also remove the lock from the nodes on the upward path.
    if (IsLockRecursive(lock.GetMode())) {
        auto currentNodeId = lockedNode.GetParentId();
        while (currentNodeId != NullObjectId) {
            auto& node = NodeMap.GetForUpdate(currentNodeId);
            YVERIFY(node.SubtreeLockIds().erase(lockId) == 1);
            currentNodeId = node.GetParentId();
        }
    }

    ObjectManager->UnrefObject(lockId);
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

ICypressNodeProxy::TPtr TCypressManager::CreateNode(
    EObjectType type,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto handler = GetHandler(type);

    auto nodeId = ObjectManager->GenerateId(type);

    TAutoPtr<ICypressNode> node = handler->Create(nodeId);

    return RegisterNode(nodeId, transactionId, handler, node);
}

ICypressNodeProxy::TPtr TCypressManager::CreateDynamicNode(
    const TTransactionId& transactionId,
    EObjectType type,
    IMapNode* manifest)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto handler = GetHandler(type);
    auto nodeId = ObjectManager->GenerateId(type);
    auto node = handler->CreateFromManifest(
        nodeId,
        transactionId,
        manifest);
    auto * node_ = ~node;
    auto proxy = RegisterNode(nodeId, transactionId, handler, node);

    if (IsLeader()) {
        CreateNodeBehavior(*node_);
    }

    return proxy;
}

ICypressNodeProxy::TPtr TCypressManager::RegisterNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    INodeTypeHandler* handler,
    TAutoPtr<ICypressNode> node)
{
    // If there's a transaction then append this node's id to the list.
    if (transactionId != NullTransactionId) {
        auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
        transaction.CreatedNodeIds().push_back(nodeId);
    }

    // Keep a pointer to the node (the ownership will be transferred to NodeMap).
    auto* node_ = node.Get();
    NodeMap.Insert(nodeId, node.Release());

    LOG_INFO_IF(!IsRecovery(), "Node created (Type: %s, NodeId: %s, TransactionId: %s)",
        ~handler->GetObjectType().ToString(),
        ~nodeId.ToString(),
        ~transactionId.ToString());

    return handler->GetProxy(*node_, transactionId);
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
    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.BranchedNodeIds().push_back(nodeId);

    // The branched node holds an implicit reference to its originator.
    ObjectManager->RefObject(nodeId);
    
    LOG_INFO_IF(!IsRecovery(), "Node branched (NodeId: %s, TransactionId: %s, Mode: %s)",
        ~nodeId.ToString(),
        ~transactionId.ToString(),
        ~mode.ToString());

    return *branchedNode_;
}

IYPathProcessor::TPtr TCypressManager::CreateProcessor()
{
    return New<TYPathProcessor>(this);
}

TFuture<TVoid>::TPtr TCypressManager::Save(const TCompositeMetaState::TSaveContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.Save(context.Invoker, context.Output);
    return LockMap.Save(context.Invoker, context.Output);
}

void TCypressManager::Load(TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.Load(input);
    LockMap.Load(input);
}

void TCypressManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeMap.Clear();
    LockMap.Clear();

    // Create the root.
    auto* root = new TMapNode(GetRootNodeId());
    NodeMap.Insert(root->GetId(), root);
    ObjectManager->RefObject(root->GetId().ObjectId);
}

void TCypressManager::OnLeaderRecoveryComplete()
{
    YASSERT(NodeBehaviors.empty());
    FOREACH(const auto& pair, NodeMap) {
        if (!pair.first.IsBranched()) {
            CreateNodeBehavior(*pair.second);
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

    auto& node = NodeMap.GetForUpdate(nodeId);
    return node.RefObject();
}

i32 TCypressManager::UnrefNode(const TNodeId& nodeId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto& node = NodeMap.GetForUpdate(nodeId);

    i32 refCounter = node.UnrefObject();
    if (refCounter == 0) {
        DestroyNodeBehavior(node);

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
    FOREACH (const auto& lockId, transaction.LockIds()) {
        ReleaseLock(lockId);
    }
}

void TCypressManager::MergeBranchedNode(
    const TTransaction& transaction,
    const TNodeId& nodeId)
{
    TVersionedNodeId branchedId(nodeId, transaction.GetId());
    auto& branchedNode = NodeMap.GetForUpdate(branchedId);

    // Find the appropriate originating node.
    ICypressNode* originatingNode;
    auto currentTransactionId = transaction.GetParentId();
    while (true) {
        TVersionedNodeId currentOriginatingId(nodeId, currentTransactionId);
        originatingNode = FindNodeForUpdate(currentOriginatingId);
        if (originatingNode)
            break;

        const auto& transaction = TransactionManager->GetTransaction(currentTransactionId);
        currentTransactionId = transaction.GetParentId();
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
    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        ObjectManager->UnrefObject(nodeId);
    }
}

void TCypressManager::RemoveBranchedNodes(const TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        TVersionedNodeId versionedId(nodeId, transactionId);
        auto& node = NodeMap.GetForUpdate(versionedId);
        GetHandler(node)->Destroy(node);
        NodeMap.Remove(versionedId);

        LOG_INFO_IF(!IsRecovery(), "Branched node removed (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }
}

TVoid TCypressManager::DoReplayVerb(const TMsgExecuteVerb& message)
{
    auto objectId = TNodeId::FromProto(message.object_id());
    auto transactionId = TTransactionId::FromProto(message.transaction_id());

    yvector<TSharedRef> parts(message.request_parts_size());
    for (int partIndex = 0; partIndex < static_cast<int>(message.request_parts_size()); ++partIndex) {
        // Construct a non-owning TSharedRef to avoid copying.
        // This is feasible since the message will outlive the request.
        const auto& part = message.request_parts(partIndex);
        parts[partIndex] = TSharedRef::FromRefNonOwning(TRef(const_cast<char*>(part.begin()), part.size()));
    }

    auto requestMessage = CreateMessageFromParts(MoveRV(parts));
    auto header = GetRequestHeader(~requestMessage);
    TYPath path = header.path();
    Stroka verb = header.verb();

    auto context = CreateYPathContext(
        ~requestMessage,
        path,
        verb,
        Logger.GetCategory(),
        NULL);

    auto proxy = GetVersionedObjectProxy(objectId, transactionId);

    DoExecuteVerb(transactionId, proxy, context);

    return TVoid();
}

TVoid TCypressManager::DoExecuteVerb(
    const TTransactionId& transactionId,
    IObjectProxy::TPtr proxy,
    IServiceContext::TPtr context)
{
    LOG_INFO_IF(!IsRecovery(), "Executing a read-write request (Path: %s, Verb: %s, ObjectId: %s, TransactionId: %s)",
        ~context->GetPath(),
        ~context->GetVerb(),
        ~proxy->GetId().ToString(),
        ~transactionId.ToString());

    proxy->Invoke(~context);

    LOG_FATAL_IF(!context->IsReplied(), "Request did not complete synchronously");

    return TVoid();
}

DEFINE_METAMAP_ACCESSORS(TCypressManager, Lock, TLock, TLockId, LockMap);
DEFINE_METAMAP_ACCESSORS(TCypressManager, Node, ICypressNode, TVersionedNodeId, NodeMap);

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TNodeMapTraits::TNodeMapTraits(TCypressManager* cypressManager)
    : CypressManager(cypressManager)
{ }

TAutoPtr<ICypressNode> TCypressManager::TNodeMapTraits::Clone(ICypressNode* value) const
{
    return value->Clone();
}

void TCypressManager::TNodeMapTraits::Save(ICypressNode* value, TOutputStream* output) const
{
    ::Save(output, value->GetObjectType());
    value->Save(output);
}

TAutoPtr<ICypressNode> TCypressManager::TNodeMapTraits::Load(const TVersionedNodeId& id, TInputStream* input) const
{
    EObjectType type;
    ::Load(input, type);
    
    auto value = CypressManager->GetHandler(type)->Create(id);
    value->Load(input);

    return value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
