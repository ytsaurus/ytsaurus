#include "stdafx.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "node_proxy_detail.h"
#include "cypress_service_proxy.h"
#include "cypress_ypath_proxy.h"
#include "cypress_ypath.pb.h"

#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/ephemeral.h>
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
        : TObjectTypeHandlerBase(&owner->LockMap)
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

    virtual IObjectProxy::TPtr FindProxy(const TObjectId& id)
    {
        return Owner->FindNodeProxy(id, NullTransactionId);
    }

    virtual TObjectId CreateFromManifest(IMapNode* manifest)
    {
        UNUSED(manifest);
        ythrow yexception() << "Cypress objects cannot be created unbounded";
    }

private:
    TCypressManager* Owner;
    EObjectType Type;

};

////////////////////////////////////////////////////////////////////////////////

class TCypressManager::TSystemProxy
    : public TYPathServiceBase
    , public virtual IObjectProxy
{
public:
    TSystemProxy(TCypressManager* owner)
        : Owner(owner)
    { }

    virtual bool IsLogged(IServiceContext* context) const
    {
        Stroka verb = context->GetVerb();
        if (verb == "Create") {
            return true;
        } else {
            return false;
        }
    }

    virtual TObjectId GetId() const
    {
        return NullObjectId;
    }

private:
    TCypressManager::TPtr Owner;

    virtual void DoInvoke(NRpc::IServiceContext* context)
    {
        Stroka verb = context->GetVerb();
        if (verb == "Create") {
            CreateThunk(context);
        } else {
            TYPathServiceBase::DoInvoke(context);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Create)
    {
        // TODO(babenko): validate type
        auto type = EObjectType(request->type());

        context->SetRequestInfo("Type: %s", ~type.ToString());

        auto handler = Owner->ObjectManager->GetHandler(type);

        NYTree::INode::TPtr manifestNode =
            request->has_manifest()
            ? NYTree::DeserializeFromYson(request->manifest())
            : NYTree::GetEphemeralNodeFactory()->CreateMap();

        if (manifestNode->GetType() != NYTree::ENodeType::Map) {
            ythrow yexception() << "Manifest must be a map";
        }

        auto id = handler->CreateFromManifest(~manifestNode->AsMap());

        response->set_id(id.ToProto());

        context->SetResponseInfo("ObjectId: %s", ~id.ToString());

        context->Reply();
    }

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

    virtual Stroka GetPath() const
    {
        return UnderlyingContext->GetPath();
    }

    virtual Stroka GetVerb() const
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
        auto currentPath = path;

        if (!currentPath.empty() && currentPath.has_prefix(TransactionIdMarker)) {
            currentPath = currentPath.substr(1);
            TransactionId = ParseId(currentPath);
            if (TransactionId != NullTransactionId && !Owner->TransactionManager->FindTransaction(TransactionId)) {
                ythrow yexception() <<  Sprintf("No such transaction (TransactionId: %s)", ~TransactionId.ToString());
            }
        }

        if (currentPath.empty()) {
            ythrow yexception() << "YPath cannot be empty";
        }

        if (currentPath.has_prefix(RootMarker)) {
            currentPath = currentPath.substr(RootMarker.length());
            RootId = Owner->GetRootNodeId();
        } else if (currentPath.has_prefix(ObjectIdMarker)) {
            currentPath = currentPath.substr(1);
            RootId = ParseId(currentPath);
        } else {
            ythrow yexception() << Sprintf("Invalid YPath syntax (Path: %s)", ~currentPath);
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
        if (!proxy || !proxy->IsLogged(context)) {
            LOG_INFO("Executing a non-logged operation (Path: %s, Verb: %s, ObjectId: %s, TransactionId: %s)",
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

    static bool IsIdChar(char ch)
    {
        return
            ch >= '0' && ch <= '9' ||
            ch >= 'a' && ch <= 'f' ||
            ch == '-';
    }

    TObjectId ParseId(TYPath& path)
    {
        int index = 0;
        while (index < path.length() && IsIdChar(path[index])) {
            ++index;
        }

        TObjectId id;
        if (!TObjectId::FromString(path.substr(0, index), &id)) {
            ythrow yexception() << "Error parsing id in YPath";
        }

        path = path.substr(index);
        return id;
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
    if (nodeId.IsBranched())
        return;

    auto handler = GetHandler(node);
    auto behavior = handler->CreateBehavior(node);
    if (!behavior)
        return;

    YVERIFY(NodeBehaviors.insert(MakePair(nodeId.NodeId, behavior)).Second());

    LOG_DEBUG_IF(!IsRecovery(), "Node behavior created (NodeId: %s)",
        ~nodeId.NodeId.ToString());
}

void TCypressManager::DestroyNodeBehavior(const ICypressNode& node)
{
    auto nodeId = node.GetId();
    if (nodeId.IsBranched())
        return;

    auto it = NodeBehaviors.find(nodeId.NodeId);
    if (it == NodeBehaviors.end())
        return;

    it->Second()->Destroy();
    NodeBehaviors.erase(it);

    LOG_DEBUG_IF(!IsRecovery(), "Node behavior destroyed (NodeId: %s)",
        ~nodeId.NodeId.ToString());
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

const ICypressNode* TCypressManager::FindVersionedNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    // First try to fetch a branched copy.
    auto* impl = FindNode(TVersionedNodeId(nodeId, transactionId));
    if (!impl) {
        // Then try a non-branched one.
        impl = FindNode(nodeId);
    }

    return impl;
}

const ICypressNode& TCypressManager::GetVersionedNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    auto* impl = FindVersionedNode(nodeId, transactionId);
    YASSERT(impl);
    return *impl;
}

ICypressNode* TCypressManager::FindVersionedNodeForUpdate(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    // Check if we're inside a transaction.
    // If not, find and return the non-branched node.
    if (transactionId == NullTransactionId) {
        return FindNodeForUpdate(nodeId);
    }

    // Try to fetch a branched copy.
    auto* branchedImpl = FindNodeForUpdate(TVersionedNodeId(nodeId, transactionId));
    if (branchedImpl) {
        YASSERT(branchedImpl->GetState() == ENodeState::Branched);
        return branchedImpl;
    }

    // Then fetch an unbranched copy and check if we have a valid node at all.
    auto* nonbranchedImpl = FindNodeForUpdate(nodeId);
    if (!nonbranchedImpl) {
        return NULL;
    }

    // Branch the node if it is committed.
    if (nonbranchedImpl->GetState() == ENodeState::Committed) {
        return &BranchNode(*nonbranchedImpl, transactionId);
    } else {
        return nonbranchedImpl;
    }
}

ICypressNode& TCypressManager::GetVersionedNodeForUpdate(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* impl = FindVersionedNodeForUpdate(nodeId, transactionId);
    YASSERT(impl);
    return *impl;
}

IObjectProxy::TPtr TCypressManager::FindObjectProxy(
    const TObjectId& objectId,
    const TTransactionId& transactionId)
{
    // NullObjectId is a special case.
    if (objectId == NullObjectId) {
        return New<TSystemProxy>(this);
    }

    // First try to fetch a proxy of a Cypress node.
    // This way we don't lose information about the current transaction.
    auto nodeProxy = FindNodeProxy(objectId, transactionId);
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

ICypressNodeProxy::TPtr TCypressManager::FindNodeProxy(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YASSERT(nodeId != NullNodeId);
    const auto* impl = FindVersionedNode(nodeId, transactionId);
    if (!impl) {
        return NULL;
    }

    return GetHandler(*impl)->GetProxy(*impl, transactionId);
}

IObjectProxy::TPtr TCypressManager::GetObjectProxy(
    const TObjectId& objectId,
    const TTransactionId& transactionId)
{
    auto proxy = FindObjectProxy(objectId, transactionId);
    YASSERT(proxy);
    return proxy;
}

ICypressNodeProxy::TPtr TCypressManager::GetNodeProxy(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    auto proxy = FindNodeProxy(nodeId, transactionId);
    YASSERT(proxy);
    return proxy;
}

bool TCypressManager::IsLockNeeded(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    // Check if the node is still uncommitted.
    const auto* impl = FindNode(nodeId);
    if (impl && impl->GetState() == ENodeState::Uncommitted) {
        // No way to lock it anyway.
        return false;
    }

    // Walk up to the root and examine the locks.
    auto currentNodeId = nodeId;
    while (currentNodeId != NullNodeId) {
        const auto& currentImpl = NodeMap.Get(currentNodeId);
        FOREACH (const auto& lockId, currentImpl.LockIds()) {
            const auto& lock = GetLock(lockId);
            if (lock.GetTransactionId() == transactionId) {
                // This is our lock.
                return false;
            }
            // This is someone else's lock.
            // Let's report we need ours (we shall probably fail while taking it).
            if (lock.GetNodeId() == currentNodeId) {
                return true;
            }
        }
        currentNodeId = currentImpl.GetParentId();
    }

    // If we're outside of a transaction than the lock is not needed.
    return transactionId != NullTransactionId;
}

TLockId TCypressManager::LockTransactionNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(transactionId != NullTransactionId);

    // NB: Locks are assigned to non-branched nodes.
    const auto& impl = NodeMap.Get(nodeId);

    // Make sure that the node is committed.
    if (impl.GetState() != ENodeState::Committed) {
        ythrow yexception() << "Cannot lock an uncommitted node";
    }

    // Make sure that the node is not locked by another transaction.
    FOREACH (const auto& lockId, impl.LockIds()) {
        const auto& lock = GetLock(lockId);
        if (lock.GetTransactionId() != transactionId) {
            ythrow yexception() << Sprintf("Node is already locked by another transaction (TransactionId: %s)",
                ~lock.GetTransactionId().ToString());
        }
    }

    // Create a lock and register it within the transaction.
    auto& lock = CreateLock(nodeId, transactionId);

    // Walk up to the root and apply locks.
    auto currentNodeId = nodeId;
    while (currentNodeId != NullNodeId) {
        auto& impl = NodeMap.GetForUpdate(currentNodeId);
        impl.LockIds().insert(lock.GetId());
        currentNodeId = impl.GetParentId();
    }

    return lock.GetId();
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
    TAutoPtr<ICypressNode> node = handler->CreateFromManifest(
        nodeId,
        transactionId,
        manifest);
    ICypressNode* node_ = ~node;

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
    // Set an appropriate state and register the node with the transaction (if any).
    // When inside a transaction, all newly-created nodes are marked as Uncommitted.
    // If no transaction is active then the node is marked as Committed.
    if (transactionId == NullTransactionId) {
        node->SetState(ENodeState::Committed);
    } else {
        node->SetState(ENodeState::Uncommitted);

        // We shall traverse this list on transaction commit and set node status to Committed.
        auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
        transaction.CreatedNodeIds().push_back(nodeId);
    }

    // Keep a pointer to the node (the ownership will be transferred to NodeMap).
    auto* node_ = ~node;
    NodeMap.Insert(nodeId, node.Release());

    LOG_INFO_IF(!IsRecovery(), "Node created (NodeId: %s, Type: %s, TransactionId: %s)",
        ~nodeId.ToString(),
        ~handler->GetObjectType().ToString(),
        ~transactionId.ToString());

    return handler->GetProxy(*node_, transactionId);
}

TLock& TCypressManager::CreateLock(const TNodeId& nodeId, const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto lockId = ObjectManager->GenerateId(EObjectType::Lock);
    auto* lock = new TLock(lockId, nodeId, transactionId, ELockMode::ExclusiveWrite);
    LockMap.Insert(lockId, lock);
    ObjectManager->RefObject(lockId);

    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.LockIds().push_back(lock->GetId());

    LOG_INFO_IF(!IsRecovery(), "Lock created (LockId: %s, NodeId: %s, TransactionId: %s)",
        ~lockId.ToString(),
        ~nodeId.ToString(),
        ~transactionId.ToString());

    return *lock;
}

ICypressNode& TCypressManager::BranchNode(ICypressNode& node, const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YASSERT(!node.GetId().IsBranched());
    auto nodeId = node.GetId().NodeId;

    // Create a branched node and initialize its state.
    auto branchedNode = GetHandler(node)->Branch(node, transactionId);
    branchedNode->SetState(ENodeState::Branched);
    auto* branchedNodePtr = branchedNode.Release();
    NodeMap.Insert(TVersionedNodeId(nodeId, transactionId), branchedNodePtr);

    // Register the branched node with a transaction.
    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.BranchedNodeIds().push_back(nodeId);

    // The branched node holds an implicit reference to its originator.
    ObjectManager->RefObject(nodeId);
    
    LOG_INFO_IF(!IsRecovery(), "Node branched (NodeId: %s, TransactionId: %s)",
        ~nodeId.ToString(),
        ~transactionId.ToString());

    return *branchedNodePtr;
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
    auto* rootImpl = new TMapNode(
        TVersionedNodeId(GetRootNodeId(), NullTransactionId),
        EObjectType::MapNode);
    rootImpl->SetState(ENodeState::Committed);
    NodeMap.Insert(rootImpl->GetId(), rootImpl);
    ObjectManager->RefObject(rootImpl->GetId().NodeId);
}

void TCypressManager::OnLeaderRecoveryComplete()
{
    FOREACH(const auto& pair, NodeMap) {
        CreateNodeBehavior(*pair.Second());
    }
}

void TCypressManager::OnStopLeading()
{
    FOREACH(const auto& pair, NodeBehaviors) {
        pair.Second()->Destroy();
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
    CommitCreatedNodes(transaction);
    UnrefOriginatingNodes(transaction);
}

void TCypressManager::OnTransactionAborted(TTransaction& transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ReleaseLocks(transaction);
    RemoveBranchedNodes(transaction);
    UnrefOriginatingNodes(transaction);

    // TODO: check that all created nodes died
}

void TCypressManager::ReleaseLocks(TTransaction& transaction)
{
    auto transactionId = transaction.GetId();

    // Iterate over all locks created by the transaction.
    FOREACH (const auto& lockId, transaction.LockIds()) {
        const auto& lock = LockMap.Get(lockId);

        // Walk up to the root and remove the locks.
        auto currentNodeId = lock.GetNodeId();
        while (currentNodeId != NullNodeId) {
            auto& node = NodeMap.GetForUpdate(currentNodeId);
            YVERIFY(node.LockIds().erase(lockId) == 1);
            currentNodeId = node.GetParentId();
        }

        ObjectManager->UnrefObject(lockId);
    }

    transaction.LockIds().clear();
}

void TCypressManager::MergeBranchedNodes(TTransaction& transaction)
{
    auto transactionId = transaction.GetId();

    // Merge all branched nodes and remove them.
    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        auto& node = NodeMap.GetForUpdate(nodeId);
        YASSERT(node.GetState() != ENodeState::Branched);

        auto& branchedNode = NodeMap.GetForUpdate(TVersionedNodeId(nodeId, transactionId));
        YASSERT(branchedNode.GetState() == ENodeState::Branched);

        GetHandler(node)->Merge(node, branchedNode);

        NodeMap.Remove(TVersionedNodeId(nodeId, transactionId));

        LOG_INFO_IF(!IsRecovery(), "Node merged (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }
}

void TCypressManager::UnrefOriginatingNodes(TTransaction& transaction)
{
    // Drop implicit references from branched nodes to their originators.
    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        ObjectManager->UnrefObject(nodeId);
    }

    transaction.BranchedNodeIds().clear();
}

void TCypressManager::RemoveBranchedNodes(TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        auto& node = NodeMap.GetForUpdate(TVersionedNodeId(nodeId, transactionId));
        GetHandler(node)->Destroy(node);
        NodeMap.Remove(TVersionedNodeId(nodeId, transactionId));

        LOG_INFO_IF(!IsRecovery(), "Branched node removed (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }

    transaction.BranchedNodeIds().clear();
}

void TCypressManager::CommitCreatedNodes(TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.CreatedNodeIds()) {
        auto& node = NodeMap.GetForUpdate(nodeId);
        node.SetState(ENodeState::Committed);

        LOG_INFO_IF(!IsRecovery(), "Node committed (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }

    transaction.CreatedNodeIds().clear();
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

    auto proxy = GetObjectProxy(objectId, transactionId);

    DoExecuteVerb(transactionId, proxy, context);

    return TVoid();
}

TVoid TCypressManager::DoExecuteVerb(
    const TTransactionId& transactionId,
    IObjectProxy::TPtr proxy,
    IServiceContext::TPtr context)
{
    LOG_INFO_IF(!IsRecovery(), "Executing a logged operation (Path: %s, Verb: %s, ObjectId: %s, TransactionId: %s)",
        ~context->GetPath(),
        ~context->GetVerb(),
        ~proxy->GetId().ToString(),
        ~transactionId.ToString());

    proxy->Invoke(~context);

    LOG_FATAL_IF(!context->IsReplied(), "Logged operation did not complete synchronously");

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
