#include "stdafx.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "node_proxy_detail.h"

#include "../ytree/yson_reader.h"
#include "../ytree/yson_writer.h"
#include "../ytree/ephemeral.h"
#include "../ytree/forwarding_yson_events.h"

namespace NYT {
namespace NCypress {

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

    RegisterMethod(this, &TThis::SetYPath);
    RegisterMethod(this, &TThis::RemoveYPath);
    RegisterMethod(this, &TThis::LockYPath);
    RegisterMethod(this, &TThis::CreateWorld);

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

bool TCypressManager::IsWorldInitialized()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return NodeMap.GetSize() > 1;
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

    // Handle sys transaction first.
    if (transactionId == SysTransactionId) {
        return FindNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
    }

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

    // Handle sys transaction first.
    if (transactionId == SysTransactionId) {
        return FindNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
    }

    // First fetch an unbranched copy and check if it is uncommitted.
    auto* nonbranchedImpl = FindNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
    if (nonbranchedImpl != NULL && nonbranchedImpl->GetState() == ENodeState::Uncommitted) {
        return nonbranchedImpl;
    }

    // Then try to fetch a branched copy.
    auto* branchedImpl = FindNodeForUpdate(TBranchedNodeId(nodeId, transactionId));
    if (branchedImpl != NULL) {
        return branchedImpl;
    }

    // Now check if we have any copy at all.
    if (nonbranchedImpl == NULL) {
        return NULL;;
    }

    // The non-branched copy must be committed.
    YASSERT(nonbranchedImpl->GetState() == ENodeState::Committed);

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

    // No locking is need for sys transaction.
    if (transactionId == SysTransactionId) {
        return true;
    }

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
        FOREACH (const auto& lockId, currentImpl.Locks()) {
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
    YASSERT(transactionId != SysTransactionId);

    if (transactionId == NullTransactionId) {
        throw TYTreeException() << "Cannot lock a node outside of a transaction";
    }

    auto& impl = GetNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));

    // Make sure that the node is committed.
    if (impl.GetState() != ENodeState::Committed) {
        throw TYTreeException() << "Cannot lock an uncommitted node";
    }

    // Make sure that the node is not locked by another transaction.
    FOREACH (const auto& lockId, impl.Locks()) {
        const auto& lock = GetLock(lockId);
        if (lock.GetTransactionId() != transactionId) {
            throw TYTreeException() << Sprintf("Node is already locked by another transaction (TransactionId: %s)",
                ~lock.GetTransactionId().ToString());
        }
    }

    // Create a lock and register it within the transaction.
    auto& lock = CreateLock(nodeId, transactionId);

    // Walk up to the root and apply locks.
    auto currentNodeId = nodeId;
    while (currentNodeId != NullNodeId) {
        auto& impl = GetNodeForUpdate(TBranchedNodeId(currentNodeId, NullTransactionId));
        impl.Locks().insert(lock.GetId());
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
        throw TYTreeException() << "Cannot create a node outside of a transaction";
    }

    // Create a new node.
    auto nodeId = NodeIdGenerator.Next();
    TBranchedNodeId branchedNodeId(nodeId, NullTransactionId);
    auto* nodeImpl = new TImpl(branchedNodeId);
    NodeMap.Insert(branchedNodeId, nodeImpl);

    // Register the node with the transaction (unless this is a sys transaction).
    if (transactionId != SysTransactionId) {
        auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
        transaction.CreatedNodes().push_back(nodeId);
    }

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

class TCypressManager::TDeserializationBuilder
    : public TForwardingYsonConsumer
    , public virtual ITreeBuilder
{
public:
    TDeserializationBuilder(
        TCypressManager* cypressManager,
        const TTransactionId& transactionId)
        : CypressManager(cypressManager)
        , TransactionId(transactionId)
        , Factory(cypressManager, transactionId)
        , StaticBuilder(CreateBuilderFromFactory(&Factory))
        , DynamicBuilder(CreateBuilderFromFactory(GetEphemeralNodeFactory()))
    { }

    virtual void BeginTree()
    {
        StaticBuilder->BeginTree();
    }

    virtual INode::TPtr EndTree()
    {
        return StaticBuilder->EndTree();
    }

private:
    typedef TDeserializationBuilder TThis;

    TCypressManager::TPtr CypressManager;
    TTransactionId TransactionId;
    TNodeFactory Factory;
    TAutoPtr<ITreeBuilder> StaticBuilder;
    TAutoPtr<ITreeBuilder> DynamicBuilder;

    virtual void OnNode(INode* node)
    {
        UNUSED(node);
        YUNREACHABLE();
    }


    virtual void OnMyStringScalar(const Stroka& value, bool hasAttributes)
    {
        StaticBuilder->OnStringScalar(value, hasAttributes);
    }

    virtual void OnMyInt64Scalar(i64 value, bool hasAttributes)
    {
        StaticBuilder->OnInt64Scalar(value, hasAttributes);
    }

    virtual void OnMyDoubleScalar(double value, bool hasAttributes)
    {
        StaticBuilder->OnDoubleScalar(value, hasAttributes);
    }


    virtual void OnMyBeginList()
    {
        StaticBuilder->OnBeginList();
    }

    virtual void OnMyListItem()
    {
        StaticBuilder->OnListItem();
    }

    virtual void OnMyEndList(bool hasAttributes)
    {
        StaticBuilder->OnEndList(hasAttributes);
    }


    virtual void OnMyBeginMap()
    {
        StaticBuilder->OnBeginMap();
    }

    virtual void OnMyMapItem(const Stroka& name)
    {
        StaticBuilder->OnMapItem(name);
    }

    virtual void OnMyEndMap(bool hasAttributes)
    {
        StaticBuilder->OnEndMap(hasAttributes);
    }

    virtual void OnMyBeginAttributes()
    {
        StaticBuilder->OnBeginAttributes();
    }

    virtual void OnMyAttributesItem(const Stroka& name)
    {
        StaticBuilder->OnAttributesItem(name);
    }

    virtual void OnMyEndAttributes()
    {
        StaticBuilder->OnEndAttributes();
    }


    virtual void OnMyEntity(bool hasAttributes)
    {
        if (!hasAttributes) {
            throw TYTreeException() << "Must specify a manifest in attributes";
        }

        DynamicBuilder->BeginTree();
        DynamicBuilder->OnEntity(true);
        ForwardAttributes(~DynamicBuilder, FromMethod(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        auto manifest = DynamicBuilder->EndTree()->GetAttributes();
        YASSERT(~manifest != NULL);
        auto node = CypressManager->CreateDynamicNode(TransactionId, ~manifest);
        StaticBuilder->OnNode(~node);
    }
};

TAutoPtr<ITreeBuilder> TCypressManager::GetDeserializationBuilder(const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    return new TDeserializationBuilder(this, transactionId);
}

INode::TPtr TCypressManager::CreateDynamicNode(
    const TTransactionId& transactionId,
    IMapNode* manifest)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (transactionId == NullTransactionId) {
        throw TYTreeException() << "Cannot create a node outside of a transaction";
    }

    // TODO: refactor using upcoming YSON configuration API
    auto typeNode = manifest->FindChild("type");
    if (~typeNode == NULL) {
        throw TYTreeException() << "Must specify a valid \"type\" attribute to create a dynamic node";
    }

    Stroka typeName = typeNode->GetValue<Stroka>();
    auto it = TypeNameToHandler.find(typeName);
    if (it == TypeNameToHandler.end()) {
        throw TYTreeException() << Sprintf("Unknown dynamic node type %s", ~typeName.Quote());
    }

    auto handler = it->Second();

    auto nodeId = NodeIdGenerator.Next();
    TBranchedNodeId branchedNodeId(nodeId, NullTransactionId);
    TAutoPtr<ICypressNode> nodeImpl(handler->CreateFromManifest(
        nodeId,
        transactionId,
        manifest));
    auto* nodePtr = nodeImpl.Get();
    NodeMap.Insert(branchedNodeId, nodeImpl.Release());

    if (transactionId != SysTransactionId) {
        auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
        transaction.CreatedNodes().push_back(nodeId);
    }

    auto proxy = GetTypeHandler(*nodePtr)->GetProxy(*nodePtr, transactionId);

    LOG_INFO_IF(!IsRecovery(), "Dynamic node created (NodeId: %s, TypeName: %s, TransactionId: %s)",
        ~nodeId.ToString(),
        ~typeName,
        ~transactionId.ToString());

    return ~proxy;
}

TLock& TCypressManager::CreateLock(const TNodeId& nodeId, const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto id = LockIdGenerator.Next();
    auto* lock = new TLock(id, nodeId, transactionId, ELockMode::ExclusiveWrite);
    LockMap.Insert(id, lock);
    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.Locks().push_back(lock->GetId());

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

void TCypressManager::GetYPath(
    const TTransactionId& transactionId,
    TYPath path,
    IYsonConsumer* consumer)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto root = GetNodeProxy(RootNodeId, transactionId);
    NYTree::GetYPath(IYPathService::FromNode(~root), path, consumer);
}

INode::TPtr TCypressManager::NavigateYPath(
    const TTransactionId& transactionId,
    TYPath path)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto root = GetNodeProxy(RootNodeId, transactionId);
    return NYTree::NavigateYPath(IYPathService::FromNode(~root), path);
}

TMetaChange<TNodeId>::TPtr TCypressManager::InitiateSetYPath(
    const TTransactionId& transactionId,
    TYPath path,
    const Stroka& value)
{
    TMsgSet message;
    message.SetTransactionId(transactionId.ToProto());
    message.SetPath(~path);
    message.SetValue(value);

    return CreateMetaChange(
        MetaStateManager,
        message,
        &TThis::SetYPath,
        TPtr(this),
        ECommitMode::MayFail);
}

TNodeId TCypressManager::SetYPath(const NProto::TMsgSet& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    TStringInput inputStream(message.GetValue());
    auto producer = TYsonReader::GetProducer(&inputStream);
    auto root = GetNodeProxy(RootNodeId, transactionId);
    auto node = NYTree::SetYPath(IYPathService::FromNode(~root), path, producer);
    auto* typedNode = dynamic_cast<ICypressNodeProxy*>(~node);
    return typedNode == NULL ? NullNodeId : typedNode->GetNodeId();
}

TMetaChange<TVoid>::TPtr TCypressManager::InitiateRemoveYPath(
    const TTransactionId& transactionId,
    TYPath path)
{
    TMsgRemove message;
    message.SetTransactionId(transactionId.ToProto());
    message.SetPath(~path);

    return CreateMetaChange(
        MetaStateManager,
        message,
        &TThis::RemoveYPath,
        TPtr(this),
        ECommitMode::MayFail);
}

TVoid TCypressManager::RemoveYPath(const NProto::TMsgRemove& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    auto root = GetNodeProxy(RootNodeId, transactionId);
    NYTree::RemoveYPath(IYPathService::FromNode(~root), path);
    return TVoid();
}

TMetaChange<TVoid>::TPtr TCypressManager::InitiateLockYPath(
    const TTransactionId& transactionId,
    TYPath path)
{
    TMsgLock message;
    message.SetTransactionId(transactionId.ToProto());
    message.SetPath(~path);

    return CreateMetaChange(
        MetaStateManager,
        message,
        &TThis::LockYPath,
        TPtr(this),
        ECommitMode::MayFail);
}

NYT::TVoid TCypressManager::LockYPath(const NProto::TMsgLock& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    auto root = GetNodeProxy(RootNodeId, transactionId);
    NYTree::LockYPath(IYPathService::FromNode(~root), path);
    return TVoid();
}

TMetaChange<TVoid>::TPtr TCypressManager::InitiateCreateWorld()
{
    return CreateMetaChange(
        MetaStateManager,
        TMsgCreateWorld(),
        &TThis::CreateWorld,
        TPtr(this),
        ECommitMode::MayFail);
}

TVoid TCypressManager::CreateWorld(const TMsgCreateWorld& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    UNUSED(message);

    // Create the root.
    auto* rootImpl = new TMapNode(TBranchedNodeId(RootNodeId, NullTransactionId));
    rootImpl->SetState(ENodeState::Committed);
    RefNode(*rootImpl);
    NodeMap.Insert(rootImpl->GetId(), rootImpl);

    // Create the other stuff around it.
    auto root = GetNodeProxy(RootNodeId, SysTransactionId);
    NYTree::SetYPath(
        IYPathService::FromNode(~root),
        "/",
        FromFunctor([] (IYsonConsumer* consumer)
        {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("sys").BeginMap()
                        // TODO: use named constants instead of literals
                        .Item("chunks").WithAttributes().Entity().BeginAttributes()
                            .Item("type").Scalar("chunk_map")
                        .EndAttributes()
                        .Item("monitoring").WithAttributes().Entity().BeginAttributes()
                            .Item("type").Scalar("monitoring")
                        .EndAttributes()
                    .EndMap()
                    .Item("home").BeginMap()
                    .EndMap()
                .EndMap();
        }));
    
    LOG_INFO_IF(!IsRecovery(), "World created");

    return TVoid();
}

TFuture<TVoid>::TPtr TCypressManager::Save(TSaveContext context)
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
    FOREACH (const auto& lockId, transaction.Locks()) {
        const auto& lock = LockMap.Get(lockId);

        // Walk up to the root and remove the locks.
        auto currentNodeId = lock.GetNodeId();
        while (currentNodeId != NullNodeId) {
            auto& node = NodeMap.GetForUpdate(TBranchedNodeId(currentNodeId, NullTransactionId));
            YVERIFY(node.Locks().erase(lockId) == 1);
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
        YASSERT(node.GetState() == ENodeState::Committed);

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
    // Drop the implicit references from branched nodes to their originators.
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
