#include "stdafx.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "node_proxy_detail.h"

#include "../misc/config.h"
#include "../ytree/yson_reader.h"
#include "../ytree/yson_writer.h"
#include "../ytree/ephemeral.h"
#include "../ytree/forwarding_yson_events.h"

namespace NYT {
namespace NCypress {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NTransaction;
using namespace NMetaState;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TCypressManager(
    TMetaStateManager* metaStateManager,
    TCompositeMetaState* metaState,
    TTransactionManager* transactionManager)
    : TMetaStatePart(metaStateManager, metaState)
    , TransactionManager(transactionManager)
    // Some random number.
    , NodeIdGenerator(0x5f1b61936a3e1741)
    , NodeMap(TNodeMapTraits(this))
    // Another random number.
    , LockIdGenerator(0x465901ab71fe2671)
    , RuntimeTypeToHandler(static_cast<int>(ERuntimeNodeType::Last))
{
    YASSERT(transactionManager != NULL);
    VERIFY_INVOKER_AFFINITY(metaStateManager->GetStateInvoker(), StateThread);

    transactionManager->OnTransactionCommitted().Subscribe(FromMethod(
        &TThis::OnTransactionCommitted,
        TPtr(this)));
    transactionManager->OnTransactionAborted().Subscribe(FromMethod(
        &TThis::OnTransactionAborted,
        TPtr(this)));

    RegisterNodeType(~New<TStringNodeTypeHandler>(this));
    RegisterNodeType(~New<TInt64NodeTypeHandler>(this));
    RegisterNodeType(~New<TDoubleNodeTypeHandler>(this));
    RegisterNodeType(~New<TMapNodeTypeHandler>(this));
    RegisterNodeType(~New<TListNodeTypeHandler>(this));

    RegisterMethod(this, &TThis::DoExecuteVerb);

    metaState->RegisterLoader(
        "Cypress.1",
        FromMethod(&TCypressManager::Load, TPtr(this)));
    metaState->RegisterSaver(
        "Cypress.1",
        FromMethod(&TCypressManager::Save, TPtr(this)));

    metaState->RegisterPart(this);
}

void TCypressManager::RegisterNodeType(INodeTypeHandler* handler)
{
    RuntimeTypeToHandler.at(static_cast<int>(handler->GetRuntimeType())) = handler;
    YVERIFY(TypeNameToHandler.insert(MakePair(handler->GetTypeName(), handler)).Second());
}

INodeTypeHandler::TPtr TCypressManager::GetTypeHandler(const ICypressNode& node)
{
    return GetTypeHandler(node.GetRuntimeType());
}

INodeTypeHandler::TPtr TCypressManager::GetTypeHandler(ERuntimeNodeType type)
{
    auto handler = RuntimeTypeToHandler[static_cast<int>(type)];
    YASSERT(~handler != NULL);
    return handler;
}

const ICypressNode* TCypressManager::FindTransactionNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    // First try to fetch a branched copy.
    auto* impl = FindNode(TBranchedNodeId(nodeId, transactionId));
    if (impl == NULL) {
        // Then try a committed or an uncommitted one.
        impl = FindNode(TBranchedNodeId(nodeId, NullTransactionId));
    }
    return impl;
}

const ICypressNode& TCypressManager::GetTransactionNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    auto* impl = FindTransactionNode(nodeId, transactionId);
    YASSERT(impl != NULL);
    return *impl;
}

ICypressNode* TCypressManager::FindTransactionNodeForUpdate(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    // Try to fetch a branched copy.
    auto* branchedImpl = FindNodeForUpdate(TBranchedNodeId(nodeId, transactionId));
    if (branchedImpl != NULL) {
        YASSERT(branchedImpl->GetState() == ENodeState::Branched);
        return branchedImpl;
    }

    // Then fetch an unbranched copy and check if we have a valid node at all.
    auto* nonbranchedImpl = FindNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
    if (nonbranchedImpl == NULL) {
        return NULL;
    }

    // Branch it!
    return &BranchNode(*nonbranchedImpl, transactionId);
}

ICypressNode& TCypressManager::GetTransactionNodeForUpdate(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* impl = FindTransactionNodeForUpdate(nodeId, transactionId);
    YASSERT(impl != NULL);
    return *impl;
}

ICypressNodeProxy::TPtr TCypressManager::GetNodeProxy(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    YASSERT(nodeId != NullNodeId);
    const auto& impl = GetTransactionNode(nodeId, transactionId);
    return GetTypeHandler(impl)->GetProxy(impl, transactionId);
}

bool TCypressManager::IsTransactionNodeLocked(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    // Check if the node is created by the current transaction and is still uncommitted.
    const auto* impl = FindNode(TBranchedNodeId(nodeId, NullTransactionId));
    if (impl != NULL && impl->GetState() == ENodeState::Uncommitted) {
        return true;
    }

    // Walk up to the root.
    auto currentNodeId = nodeId;
    while (currentNodeId != NullNodeId) {
        const auto& currentImpl = GetNode(TBranchedNodeId(currentNodeId, NullTransactionId));
        // Check the locks assigned to the current node.
        FOREACH (const auto& lockId, currentImpl.LockIds()) {
            const auto& lock = GetLock(lockId);
            if (lock.GetTransactionId() == transactionId) {
                return true;
            }
        }
        currentNodeId = currentImpl.GetParentId();
    }

    return false;
}

TLockId TCypressManager::LockTransactionNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (transactionId == NullTransactionId) {
        ythrow yexception() << "Cannot lock a node outside of a transaction";
    }

    auto& impl = GetNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));

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
        auto& impl = GetNodeForUpdate(TBranchedNodeId(currentNodeId, NullTransactionId));
        impl.LockIds().insert(lock.GetId());
        currentNodeId = impl.GetParentId();
    }

    return lock.GetId();
}

template <class TImpl, class TProxy>
TIntrusivePtr<TProxy> TCypressManager::CreateNode(
    const TTransactionId& transactionId,
    ERuntimeNodeType type)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (transactionId == NullTransactionId) {
        ythrow yexception() << "Cannot create a node outside of a transaction";
    }

    // Create a new node.
    auto nodeId = NodeIdGenerator.Next();
    TBranchedNodeId branchedNodeId(nodeId, NullTransactionId);
    auto* nodeImpl = new TImpl(branchedNodeId);
    NodeMap.Insert(branchedNodeId, nodeImpl);

    // Register the node with the transaction.
    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.CreatedNodes().push_back(nodeId);

    // Create a proxy.
    auto proxy = New<TProxy>(
        ~RuntimeTypeToHandler[static_cast<int>(type)],
        this,
        transactionId,
        nodeId);

    LOG_INFO_IF(!IsRecovery(), "Node created (NodeId: %s, NodeType: %s, TransactionId: %s)",
        ~nodeId.ToString(),
        ~proxy->GetTypeHandler()->GetTypeName(),
        ~transactionId.ToString());

    return proxy;
}

IStringNode::TPtr TCypressManager::CreateStringNodeProxy(const TTransactionId& transactionId)
{
    return ~CreateNode<TStringNode, TStringNodeProxy>(transactionId, ERuntimeNodeType::String);
}

IInt64Node::TPtr TCypressManager::CreateInt64NodeProxy(const TTransactionId& transactionId)
{
    return ~CreateNode<TInt64Node, TInt64NodeProxy>(transactionId, ERuntimeNodeType::Int64);
}

IDoubleNode::TPtr TCypressManager::CreateDoubleNodeProxy(const TTransactionId& transactionId)
{
    return ~CreateNode<TDoubleNode, TDoubleNodeProxy>(transactionId, ERuntimeNodeType::Double);
}

IMapNode::TPtr TCypressManager::CreateMapNodeProxy(const TTransactionId& transactionId)
{
    return ~CreateNode<TMapNode, TMapNodeProxy>(transactionId, ERuntimeNodeType::Map);
}

IListNode::TPtr TCypressManager::CreateListNodeProxy(const TTransactionId& transactionId)
{
    return ~CreateNode<TListNode, TListNodeProxy>(transactionId, ERuntimeNodeType::List);
}

struct TManifest
    : public TConfigBase
{
    Stroka Type;

    TManifest()
    {
        Register("type", Type).NonEmpty();
    }
};

ICypressNodeProxy::TPtr TCypressManager::CreateDynamicNode(
    const TTransactionId& transactionId,
    INode* manifestNode)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (transactionId == NullTransactionId) {
        ythrow yexception() << "Cannot create a node outside of a transaction";
    }

    if (manifestNode->GetType() != ENodeType::Map) {
        ythrow yexception() << "Dynamic node manifest must be a map";
    }
    auto manifestMapNode = manifestNode->AsMap();

    TManifest manifest;
    manifest.Load(~manifestMapNode);

    Stroka type = manifest.Type;
    auto it = TypeNameToHandler.find(type);
    if (it == TypeNameToHandler.end()) {
        ythrow yexception() << Sprintf("Unknown dynamic node type %s", ~type.Quote());
    }

    auto handler = it->Second();

    auto nodeId = NodeIdGenerator.Next();
    TBranchedNodeId branchedNodeId(nodeId, NullTransactionId);
    TAutoPtr<ICypressNode> nodeImpl(handler->CreateFromManifest(
        nodeId,
        transactionId,
        manifestMapNode));
    auto* nodePtr = nodeImpl.Get();
    NodeMap.Insert(branchedNodeId, nodeImpl.Release());

    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.CreatedNodes().push_back(nodeId);

    auto proxy = GetTypeHandler(*nodePtr)->GetProxy(*nodePtr, transactionId);

    LOG_INFO_IF(!IsRecovery(), "Dynamic node created (NodeId: %s, TransactionId: %s, Type: %s)",
        ~nodeId.ToString(),
        ~transactionId.ToString(),
        ~type);

    return ~proxy;
}

TLock& TCypressManager::CreateLock(const TNodeId& nodeId, const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = LockIdGenerator.Next();
    auto* lock = new TLock(id, nodeId, transactionId, ELockMode::ExclusiveWrite);
    LockMap.Insert(id, lock);
    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.LockIds().push_back(lock->GetId());

    LOG_INFO_IF(!IsRecovery(), "Lock created (LockId: %s, NodeId: %s, TransactionId: %s)",
        ~id.ToString(),
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
    auto branchedNode = GetTypeHandler(node)->Branch(node, transactionId);
    branchedNode->SetState(ENodeState::Branched);
    auto* branchedNodePtr = branchedNode.Release();
    NodeMap.Insert(TBranchedNodeId(nodeId, transactionId), branchedNodePtr);

    // Register the branched node with a transaction.
    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.BranchedNodes().push_back(nodeId);

    // The branched node holds an implicit reference to its originator.
    RefNode(node);
    
    LOG_INFO_IF(!IsRecovery(), "Node branched (NodeId: %s, TransactionId: %s)",
        ~nodeId.ToString(),
        ~transactionId.ToString());

    return *branchedNodePtr;
}

void TCypressManager::ExecuteVerb(IYPathService* service, IServiceContext* context)
{
    auto proxy = dynamic_cast<ICypressNodeProxy*>(service);
    YASSERT(proxy != NULL);

    if (!proxy->IsOperationLogged(context->GetPath(), context->GetVerb())) {
        service->Invoke(context);
        return;
    }

    IYPathService::TPtr service_ = service;
    IServiceContext::TPtr context_ = context;

    TMsgExecuteVerb message;
    message.SetNodeId(proxy->GetNodeId().ToProto());
    message.SetTransactionId(proxy->GetTransactionId().ToProto());

    auto requestMessage = context->GetRequestMessage();
    FOREACH (const auto& part, requestMessage->GetParts()) {
        message.AddRequestParts(part.Begin(), part.Size());
    }

    auto change = CreateMetaChange(
        ~MetaStateManager,
        message,
        ~FromMethod(&TCypressManager::DoExecuteVerbFast, TPtr(this), service, context));

    change
        ->OnError(~FromFunctor([=] ()
            {
                context_->Reply(TError(EYPathErrorCode(EYPathErrorCode::GenericError)));
            }))
        ->Commit();
}

TVoid TCypressManager::DoExecuteVerb(const TMsgExecuteVerb& message)
{
    auto nodeId = TNodeId::FromProto(message.GetNodeId());
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());

    yvector<TSharedRef> parts(message.RequestPartsSize());
    for (int partIndex = 0; partIndex < static_cast<int>(message.RequestPartsSize()); ++partIndex) {
        // NB: This constructs a non-owning TSharedRef to avoid copying.
        // This is feasible since the message will outlive the request.
        const auto& part = message.GetRequestParts(partIndex);
        parts[partIndex] = TSharedRef::FromRefNonOwning(TRef(const_cast<char*>(part.begin()), part.size()));
    }

    YASSERT(parts.ysize() >= 2);

    TYPath path;
    Stroka verb;
    ParseYPathRequestHeader(
        parts[0],
        &path,
        &verb);

    auto requestMessage = CreateMessageFromParts(MoveRV(parts));

    auto context = CreateYPathContext(
        ~requestMessage,
        path,
        verb,
        Logger.GetCategory(),
        NULL);

    auto proxy = GetNodeProxy(nodeId, transactionId);
    auto service = IYPathService::FromNode(~proxy);
    service->Invoke(~context);

    LOG_FATAL_IF(!context->IsReplied(), "Logged operation did not complete synchronously");

    return TVoid();
}

TVoid TCypressManager::DoExecuteVerbFast(
    NYTree::IYPathService::TPtr service,
    NRpc::IServiceContext::TPtr context)
{
    service->Invoke(~context);

    LOG_FATAL_IF(!context->IsReplied(), "Logged operation did not complete synchronously");

    return TVoid();
}

TFuture<TVoid>::TPtr TCypressManager::Save(const TCompositeMetaState::TSaveContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* output = context.Output;
    auto invoker = context.Invoker;

    auto nodeIdGenerator = NodeIdGenerator;
    auto lockIdGenerator = LockIdGenerator;
    invoker->Invoke(FromFunctor([=] ()
        {
            ::Save(output, nodeIdGenerator);
            ::Save(output, lockIdGenerator);
        }));
        
    NodeMap.Save(invoker, output);
    return LockMap.Save(invoker, output);
}

void TCypressManager::Load(TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ::Load(input, NodeIdGenerator);
    ::Load(input, LockIdGenerator);
    
    NodeMap.Load(input);
    LockMap.Load(input);
}

void TCypressManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    NodeIdGenerator.Reset();
    NodeMap.Clear();

    LockIdGenerator.Reset();
    LockMap.Clear();

    // Create the root.
    auto* rootImpl = new TMapNode(TBranchedNodeId(RootNodeId, NullTransactionId));
    rootImpl->SetState(ENodeState::Committed);
    RefNode(*rootImpl);
    NodeMap.Insert(rootImpl->GetId(), rootImpl);
}

void TCypressManager::RefNode(ICypressNode& node)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto nodeId = node.GetId();

    int refCounter;
    if (nodeId.IsBranched()) {
        auto& nonbranchedNode = NodeMap.GetForUpdate(TBranchedNodeId(nodeId.NodeId, NullTransactionId));
        refCounter = nonbranchedNode.Ref();
    } else {
        refCounter = node.Ref();
    }

    LOG_DEBUG_IF(!IsRecovery(), "Node referenced (NodeId: %s, RefCounter: %d)",
        ~nodeId.NodeId.ToString(),
        refCounter);
}

void TCypressManager::RefNode(const TNodeId& nodeId)
{
    auto& node = GetNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
    RefNode(node);
}

void TCypressManager::UnrefNode(ICypressNode& node)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto nodeId = node.GetId();

    int refCounter;
    if (nodeId.IsBranched()) {
        auto& nonbranchedNode = GetNodeForUpdate(TBranchedNodeId(nodeId.NodeId, NullTransactionId));
        refCounter = nonbranchedNode.Unref();
        YVERIFY(refCounter > 0);
    } else {
        refCounter = node.Unref();
    }

    LOG_DEBUG_IF(!IsRecovery(), "Node unreferenced (NodeId: %s, RefCounter: %d)",
        ~nodeId.NodeId.ToString(),
        refCounter);

    if (refCounter == 0) {
        LOG_INFO_IF(!IsRecovery(), "Node removed (NodeId: %s)", ~nodeId.NodeId.ToString());

        GetTypeHandler(node)->Destroy(node);
        NodeMap.Remove(nodeId);
    }
}

void TCypressManager::UnrefNode(const TNodeId& nodeId)
{
    auto& node = GetNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
    UnrefNode(node);
}

void TCypressManager::OnTransactionCommitted(const TTransaction& transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ReleaseLocks(transaction);
    MergeBranchedNodes(transaction);
    CommitCreatedNodes(transaction);
    UnrefOriginatingNodes(transaction);
}

void TCypressManager::OnTransactionAborted(const TTransaction& transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ReleaseLocks(transaction);
    RemoveBranchedNodes(transaction);
    UnrefOriginatingNodes(transaction);

    // TODO: check that all created nodes died
}

void TCypressManager::ReleaseLocks(const TTransaction& transaction)
{
    // Iterate over all locks created by the transaction.
    FOREACH (const auto& lockId, transaction.LockIds()) {
        const auto& lock = LockMap.Get(lockId);

        // Walk up to the root and remove the locks.
        auto currentNodeId = lock.GetNodeId();
        while (currentNodeId != NullNodeId) {
            auto& node = NodeMap.GetForUpdate(TBranchedNodeId(currentNodeId, NullTransactionId));
            YVERIFY(node.LockIds().erase(lockId) == 1);
            currentNodeId = node.GetParentId();
        }

        LockMap.Remove(lockId);

        LOG_INFO_IF(!IsRecovery(), "Lock removed (LockId: %s", ~lockId.ToString());
    }
}

void TCypressManager::MergeBranchedNodes(const TTransaction& transaction)
{
    auto transactionId = transaction.GetId();

    // Merge all branched nodes and remove them.
    FOREACH (const auto& nodeId, transaction.BranchedNodes()) {
        auto& node = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        YASSERT(node.GetState() != ENodeState::Branched);

        auto& branchedNode = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, transactionId));
        YASSERT(branchedNode.GetState() == ENodeState::Branched);

        GetTypeHandler(node)->Merge(node, branchedNode);

        NodeMap.Remove(TBranchedNodeId(nodeId, transactionId));

        LOG_INFO_IF(!IsRecovery(), "Node merged (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }
}

void TCypressManager::UnrefOriginatingNodes(const TTransaction& transaction)
{
    // Drop implicit references from branched nodes to their originators.
    FOREACH (const auto& nodeId, transaction.BranchedNodes()) {
        UnrefNode(nodeId);
    }
}

void TCypressManager::RemoveBranchedNodes(const TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.BranchedNodes()) {
        auto& node = GetNodeForUpdate(TBranchedNodeId(nodeId, transactionId));
        GetTypeHandler(node)->Destroy(node);
        NodeMap.Remove(TBranchedNodeId(nodeId, transactionId));

        LOG_INFO_IF(!IsRecovery(), "Branched node removed (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }
}

void TCypressManager::CommitCreatedNodes(const TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.CreatedNodes()) {
        auto& node = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        node.SetState(ENodeState::Committed);

        LOG_INFO_IF(!IsRecovery(), "Node committed (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }
}

METAMAP_ACCESSORS_IMPL(TCypressManager, Lock, TLock, TLockId, LockMap);
METAMAP_ACCESSORS_IMPL(TCypressManager, Node, ICypressNode, TBranchedNodeId, NodeMap);

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
    ::Save(output, value->GetRuntimeType());
    //::Save(output, value->GetId());
    value->Save(output);
}

TAutoPtr<ICypressNode> TCypressManager::TNodeMapTraits::Load(const TBranchedNodeId& id, TInputStream* input) const
{
    ERuntimeNodeType type;
    ::Load(input, type);
    
    auto value = CypressManager->GetTypeHandler(type)->Create(id);
    value->Load(input);

    return value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
