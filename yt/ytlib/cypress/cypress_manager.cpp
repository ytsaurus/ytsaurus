#include "stdafx.h"
#include "cypress_manager.h"
#include "node_proxy.h"

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
    TMetaStateManager::TPtr metaStateManager,
    TCompositeMetaState::TPtr metaState,
    TTransactionManager::TPtr transactionManager)
    : TMetaStatePart(metaStateManager, metaState)
    , TransactionManager(transactionManager)
    // Some random number.
    , NodeIdGenerator(0x5f1b61936a3e1741)
    // Another random number.
    , LockIdGenerator(0x465901ab71fe2671)
{
    YASSERT(~transactionManager != NULL);

    transactionManager->OnTransactionCommitted().Subscribe(FromMethod(
        &TThis::OnTransactionCommitted,
        TPtr(this)));
    transactionManager->OnTransactionAborted().Subscribe(FromMethod(
        &TThis::OnTransactionAborted,
        TPtr(this)));

    RegisterMethod(this, &TThis::SetYPath);
    RegisterMethod(this, &TThis::RemoveYPath);
    RegisterMethod(this, &TThis::LockYPath);

    metaState->RegisterPart(this);
}

void TCypressManager::RegisterDynamicType(IDynamicTypeHandler::TPtr handler)
{
    YVERIFY(RuntimeTypeToHandler.insert(MakePair(handler->GetRuntimeType(), handler)).Second());
    YVERIFY(TypeNameToHandler.insert(MakePair(handler->GetTypeName(), handler)).Second());
}

INode::TPtr TCypressManager::FindNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    auto impl = FindNode(TBranchedNodeId(nodeId, transactionId));
    if (impl == NULL) {
        impl = FindNode(TBranchedNodeId(nodeId, NullTransactionId));
    }
    if (impl == NULL) {
        return NULL;
    }
    return ~impl->GetProxy(this, transactionId);
}

INode::TPtr TCypressManager::GetNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    auto node = FindNode(nodeId, transactionId);
    YASSERT(~node != NULL);
    return node;
}

template <class TImpl, class TProxy>
TIntrusivePtr<TProxy> TCypressManager::CreateNode(const TTransactionId& transactionId)
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
    auto proxy = New<TProxy>(this, transactionId, nodeId);

    LOG_INFO_IF(!IsRecovery(), "Node created (NodeId: %s, NodeType: %s, TransactionId: %s)",
        ~nodeId.ToString(),
        ~proxy->GetType().ToString(),
        ~transactionId.ToString());

    return proxy;
}

IStringNode::TPtr TCypressManager::CreateStringNodeProxy(const TTransactionId& transactionId)
{
    return ~CreateNode<TStringNode, TStringNodeProxy>(transactionId);
}

IInt64Node::TPtr TCypressManager::CreateInt64NodeProxy(const TTransactionId& transactionId)
{
    return ~CreateNode<TInt64Node, TInt64NodeProxy>(transactionId);
}

IDoubleNode::TPtr TCypressManager::CreateDoubleNodeProxy(const TTransactionId& transactionId)
{
    return ~CreateNode<TDoubleNode, TDoubleNodeProxy>(transactionId);
}

IMapNode::TPtr TCypressManager::CreateMapNodeProxy(const TTransactionId& transactionId)
{
    return ~CreateNode<TMapNode, TMapNodeProxy>(transactionId);
}

IListNode::TPtr TCypressManager::CreateListNodeProxy(const TTransactionId& transactionId)
{
    return ~CreateNode<TListNode, TListNodeProxy>(transactionId);
}

class TCypressManager::TYsonDeserializationConsumer
    : public TForwardingYsonConsumer
{
public:
    TYsonDeserializationConsumer(
        TCypressManager::TPtr cypressManager,
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
        auto description = DynamicBuilder.GetRoot()->GetAttributes();
        YASSERT(~description != NULL);
        DynamicResult = CypressManager->CreateDynamicNode(TransactionId, description);
    }

};

TYsonBuilder::TPtr TCypressManager::GetYsonDeserializer(const TTransactionId& transactionId)
{
    return FromMethod(&TThis::YsonDeserializerThunk, TPtr(this), transactionId);
}

INode::TPtr TCypressManager::YsonDeserializerThunk(
    TYsonProducer::TPtr producer,
    const TTransactionId& transactionId)
{
    TYsonDeserializationConsumer consumer(this, transactionId);
    producer->Do(&consumer);
    return consumer.GetResult();
}

INode::TPtr TCypressManager::CreateDynamicNode(
    const TTransactionId& transactionId,
    IMapNode::TPtr description)
{
    if (transactionId == NullTransactionId) {
        throw TYTreeException() << "Cannot create a node outside of a transaction";
    }

    // TODO: refactor using upcoming YSON configuration API
    auto typeNode = description->FindChild("type");
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
        description));
    auto* nodePtr = nodeImpl.Get();
    NodeMap.Insert(branchedNodeId, nodeImpl.Release());

    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.CreatedNodes().push_back(nodeId);

    auto proxy = nodePtr->GetProxy(this, transactionId);

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
    auto branchedNode = node.Branch(this, transactionId);
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
    auto root = GetNode(RootNodeId, transactionId);
    NYTree::GetYPath(AsYPath(root), path, consumer);
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
        TPtr(this));
}

TVoid TCypressManager::SetYPath(const NProto::TMsgSet& message)
{
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    TStringInput inputStream(message.GetValue());
    auto producer = TYsonReader::GetProducer(&inputStream);
    auto root = GetNode(RootNodeId, transactionId);
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
        TPtr(this));
}

TVoid TCypressManager::RemoveYPath(const NProto::TMsgRemove& message)
{
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    auto root = GetNode(RootNodeId, transactionId);
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
        TPtr(this));
}

NYT::TVoid TCypressManager::LockYPath(const NProto::TMsgLock& message)
{
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    auto root = GetNode(RootNodeId, transactionId);
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
    LOG_DEBUG_IF(!IsRecovery(), "Node referenced (NodeId: %s)", ~nodeId.NodeId.ToString());

    if (nodeId.IsBranched()) {
        auto& nonbranchedNode = GetNodeForUpdate(TBranchedNodeId(nodeId.NodeId, NullTransactionId));
        nonbranchedNode.Ref();
    } else {
        node.Ref();
    }
}

void TCypressManager::UnrefNode(ICypressNode& node)
{
    auto nodeId = node.GetId();
    LOG_DEBUG_IF(!IsRecovery(), "Node unreferenced (NodeId: %s)", ~nodeId.NodeId.ToString());

    if (nodeId.IsBranched()) {
        auto& nonbranchedNode = GetNodeForUpdate(TBranchedNodeId(nodeId.NodeId, NullTransactionId));
        YVERIFY(nonbranchedNode.Unref() > 0);
    } else {
        if (node.Unref() == 0) {
            LOG_INFO_IF(!IsRecovery(), "Node removed (NodeId: %s)", ~nodeId.NodeId.ToString());

            node.Destroy(this);
            NodeMap.Remove(nodeId);
        }
    }
}

void TCypressManager::OnTransactionCommitted(TTransaction& transaction)
{
    ReleaseLocks(transaction);
    MergeBranchedNodes(transaction);
    CommitCreatedNodes(transaction);
}

void TCypressManager::OnTransactionAborted(TTransaction& transaction)
{
    ReleaseLocks(transaction);
    RemoveBranchedNodes(transaction);
    RemoveCreatedNodes(transaction);
}

void TCypressManager::ReleaseLocks(TTransaction& transaction)
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

void TCypressManager::MergeBranchedNodes(TTransaction& transaction)
{
    auto transactionId = transaction.GetId();

    // Merge all branched nodes and remove them.
    FOREACH (const auto& nodeId, transaction.BranchedNodes()) {
        auto& node = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        YASSERT(node.GetState() == ENodeState::Committed);

        auto& branchedNode = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, transactionId));
        YASSERT(branchedNode.GetState() == ENodeState::Branched);

        node.Merge(this, branchedNode);

        NodeMap.Remove(TBranchedNodeId(nodeId, transactionId));

        LOG_INFO_IF(!IsRecovery(), "Node merged (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }

    // Drop the implicit references from branched nodes to their originators.
    FOREACH (const auto& nodeId, transaction.BranchedNodes()) {
        auto& node = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        UnrefNode(node);
    }
}

void TCypressManager::RemoveBranchedNodes(TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.BranchedNodes()) {
        NodeMap.Remove(TBranchedNodeId(nodeId, transactionId));

        LOG_INFO_IF(!IsRecovery(), "Branched node removed (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }
}

void TCypressManager::CommitCreatedNodes(TTransaction& transaction)
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

void TCypressManager::RemoveCreatedNodes(TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.CreatedNodes()) {
        NodeMap.Remove(TBranchedNodeId(nodeId, NullTransactionId));

        LOG_INFO_IF(!IsRecovery(), "Uncommitted node removed (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }
}

METAMAP_ACCESSORS_IMPL(TCypressManager, Lock, TLock, TLockId, LockMap);
METAMAP_ACCESSORS_IMPL(TCypressManager, Node, ICypressNode, TBranchedNodeId, NodeMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
