#include "stdafx.h"
#include "cypress_manager.h"
#include "node_detail.h"
#include "node_proxy_detail.h"

#include "../ytree/yson_reader.h"
#include "../ytree/yson_writer.h"
// TODO: fix this once TForwardingYsonConsumer is moved
#include "../ytree/ypath_detail.h"
#include "../ytree/ephemeral.h"

namespace NYT {
namespace NCypress {

using namespace NMetaState;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger& Logger = CypressLogger;

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

    metaState->RegisterPart(this);
}

void TCypressManager::RegisterNodeType(INodeTypeHandler* handler)
{
    RuntimeTypeToHandler.at(static_cast<int>(handler->GetRuntimeType())) = handler;
    YVERIFY(TypeNameToHandler.insert(MakePair(handler->GetTypeName(), handler)).Second());
}

INodeTypeHandler::TPtr TCypressManager::GetNodeHandler(const ICypressNode& node)
{
    int type = static_cast<int>(node.GetRuntimeType());
    return RuntimeTypeToHandler[type];
}

const ICypressNode* TCypressManager::FindTransactionNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
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
    auto* impl = FindTransactionNodeForUpdate(nodeId, transactionId);
    YASSERT(impl != NULL);
    return *impl;
}

ICypressNodeProxy::TPtr TCypressManager::GetNodeProxy(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    YASSERT(nodeId != NullNodeId);
    const auto& impl = GetTransactionNode(nodeId, transactionId);
    return GetNodeHandler(impl)->GetProxy(impl, transactionId);
}

bool TCypressManager::IsTransactionNodeLocked(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
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
    if (transactionId == NullTransactionId) {
        throw TYTreeException() << "Cannot create a node outside of a transaction";
    }

    auto nodeId = NodeIdGenerator.Next();
    TBranchedNodeId branchedNodeId(nodeId, NullTransactionId);
    auto* nodeImpl = new TImpl(branchedNodeId);
    NodeMap.Insert(branchedNodeId, nodeImpl);
    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.CreatedNodes().push_back(nodeId);
    
    auto proxy = New<TProxy>(
        ~RuntimeTypeToHandler[static_cast<int>(type)],
        this,
        transactionId,
        nodeId);

    LOG_INFO_IF(!IsRecovery(), "Node created (NodeId: %s, NodeType: %s, TransactionId: %s)",
        ~nodeId.ToString(),
        ~proxy->GetType().ToString(),
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

class TCypressManager::TYsonDeserializationConsumer
    : public TForwardingYsonConsumer
{
public:
    TYsonDeserializationConsumer(
        TCypressManager* cypressManager,
        const TTransactionId& transactionId)
        : CypressManager(cypressManager)
        , TransactionId(transactionId)
        , Factory(cypressManager, transactionId)
        , StaticBuilder(&Factory)
        , DynamicBuilder(TEphemeralNodeFactory::Get())
    { }

    INode::TPtr GetResult()
    {
        return ~DynamicResult != NULL ? DynamicResult : StaticBuilder.GetRoot();
    }

private:
    typedef TYsonDeserializationConsumer TThis;

    TCypressManager::TPtr CypressManager;
    TTransactionId TransactionId;
    TNodeFactory Factory;
    TTreeBuilder StaticBuilder;
    TTreeBuilder DynamicBuilder;
    INode::TPtr DynamicResult;

    virtual void OnMyStringScalar(const Stroka& value, bool hasAttributes)
    {
        StaticBuilder.OnStringScalar(value, hasAttributes);
    }

    virtual void OnMyInt64Scalar(i64 value, bool hasAttributes)
    {
        StaticBuilder.OnInt64Scalar(value, hasAttributes);
    }

    virtual void OnMyDoubleScalar(double value, bool hasAttributes)
    {
        StaticBuilder.OnDoubleScalar(value, hasAttributes);
    }


    virtual void OnMyBeginList()
    {
        StaticBuilder.OnBeginList();
    }

    virtual void OnMyListItem()
    {
        StaticBuilder.OnListItem();
    }

    virtual void OnMyEndList(bool hasAttributes)
    {
        StaticBuilder.OnEndList(hasAttributes);
    }


    virtual void OnMyBeginMap()
    {
        StaticBuilder.OnBeginMap();
    }

    virtual void OnMyMapItem(const Stroka& name)
    {
        StaticBuilder.OnMapItem(name);
    }

    virtual void OnMyEndMap(bool hasAttributes)
    {
        StaticBuilder.OnEndMap(hasAttributes);
    }

    virtual void OnMyBeginAttributes()
    {
        StaticBuilder.OnBeginAttributes();
    }

    virtual void OnMyAttributesItem(const Stroka& name)
    {
        StaticBuilder.OnAttributesItem(name);
    }

    virtual void OnMyEndAttributes()
    {
        StaticBuilder.OnEndAttributes();
    }


    virtual void OnMyEntity(bool hasAttributes)
    {
        YASSERT(hasAttributes);
        DynamicBuilder.OnEntity(true);
        ForwardAttributes(&DynamicBuilder, FromMethod(&TThis::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        auto manifest = DynamicBuilder.GetRoot()->GetAttributes();
        YASSERT(~manifest != NULL);
        DynamicResult = CypressManager->CreateDynamicNode(TransactionId, ~manifest);
    }

};

TYsonBuilder::TPtr TCypressManager::GetYsonDeserializer(const TTransactionId& transactionId)
{
    TPtr thisPtr = this;
    return FromFunctor([=] (TYsonProducer::TPtr producer) -> INode::TPtr
        {
            TYsonDeserializationConsumer consumer(this, transactionId);
            producer->Do(&consumer);
            return consumer.GetResult();
        });

}

INode::TPtr TCypressManager::CreateDynamicNode(
    const TTransactionId& transactionId,
    IMapNode* manifest)
{
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
    TAutoPtr<ICypressNode> nodeImpl(handler->Create(
        nodeId,
        transactionId,
        manifest));
    auto* nodePtr = nodeImpl.Get();
    NodeMap.Insert(branchedNodeId, nodeImpl.Release());

    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.CreatedNodes().push_back(nodeId);

    auto proxy = GetNodeHandler(*nodePtr)->GetProxy(*nodePtr, transactionId);

    LOG_INFO_IF(!IsRecovery(), "Dynamic node created (NodeId: %s, TypeName: %s, TransactionId: %s)",
        ~nodeId.ToString(),
        ~typeName,
        ~transactionId.ToString());

    return ~proxy;
}

TLock& TCypressManager::CreateLock(const TNodeId& nodeId, const TTransactionId& transactionId)
{
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
    YASSERT(!node.GetId().IsBranched());
    auto nodeId = node.GetId().NodeId;

    // Create a branched node and initialize its state.
    auto branchedNode = GetNodeHandler(node)->Branch(node, transactionId);
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
    auto root = GetNodeProxy(RootNodeId, transactionId);
    NYTree::GetYPath(AsYPath(root), path, consumer);
}


INode::TPtr TCypressManager::NavigateYPath(
    const TTransactionId& transactionId,
    TYPath path)
{
    auto root = GetNodeProxy(RootNodeId, transactionId);
    return NYTree::NavigateYPath(AsYPath(root), path);
}

TMetaChange<TVoid>::TPtr TCypressManager::InitiateSetYPath(
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

TVoid TCypressManager::SetYPath(const NProto::TMsgSet& message)
{
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    TStringInput inputStream(message.GetValue());
    auto producer = TYsonReader::GetProducer(&inputStream);
    auto root = GetNodeProxy(RootNodeId, transactionId);
    NYTree::SetYPath(AsYPath(root), path, producer);
    return TVoid();
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
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    auto root = GetNodeProxy(RootNodeId, transactionId);
    NYTree::RemoveYPath(AsYPath(root), path);
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
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    auto root = GetNodeProxy(RootNodeId, transactionId);
    NYTree::LockYPath(AsYPath(root), path);
    return TVoid();
}

Stroka TCypressManager::GetPartName() const
{
    return "Cypress";
}

TFuture<TVoid>::TPtr TCypressManager::Save(TOutputStream* stream, IInvoker::TPtr invoker)
{
    UNUSED(stream);
    UNUSED(invoker);
    YUNIMPLEMENTED();
    //*stream << NodeIdGenerator
    //        << LockIdGenerator;
}

TFuture<TVoid>::TPtr TCypressManager::Load(TInputStream* stream, IInvoker::TPtr invoker)
{
    UNUSED(stream);
    UNUSED(invoker);
    YUNIMPLEMENTED();
    //*stream >> NodeIdGenerator
    //        >> LockIdGenerator;
}

void TCypressManager::Clear()
{
    NodeIdGenerator.Reset();
    LockIdGenerator.Reset();
    CreateWorld();
}

void TCypressManager::CreateWorld()
{
    // / (the root)
    auto* root = new TMapNode(TBranchedNodeId(RootNodeId, NullTransactionId));
    root->SetState(ENodeState::Committed);
    RefNode(*root);
    NodeMap.Insert(root->GetId(), root);

    LOG_INFO_IF(!IsRecovery(), "World initialized");
}

void TCypressManager::RefNode(ICypressNode& node)
{
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

        GetNodeHandler(node)->Destroy(node);
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
    ReleaseLocks(transaction);
    MergeBranchedNodes(transaction);
    CommitCreatedNodes(transaction);
    UnrefOriginatingNodes(transaction);
}

void TCypressManager::OnTransactionAborted(const TTransaction& transaction)
{
    ReleaseLocks(transaction);
    RemoveBranchedNodes(transaction);
    RemoveCreatedNodes(transaction);
    UnrefOriginatingNodes(transaction);
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

        GetNodeHandler(node)->Merge(node, branchedNode);

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
        GetNodeHandler(node)->Destroy(node);
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

void TCypressManager::RemoveCreatedNodes(const TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.CreatedNodes()) {
        auto& node = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        GetNodeHandler(node)->Destroy(node);
        NodeMap.Remove(TBranchedNodeId(nodeId, NullTransactionId));

        LOG_INFO_IF(!IsRecovery(), "Uncommitted node removed (NodeId: %s, TransactionId: %s)",
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
    ::Save(output, static_cast<i32>(value->GetRuntimeType()));
    ::Save(output, value->GetId());
    value->Save(output);
}

TAutoPtr<ICypressNode> TCypressManager::TNodeMapTraits::Load(TInputStream* input) const
{
    i32 type;
    TBranchedNodeId id;
    ::Load(input, type);
    ::Load(input, id);

    auto value = CypressManager->RuntimeTypeToHandler[type]->Create(id);
    value->Load(input);

    return value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
