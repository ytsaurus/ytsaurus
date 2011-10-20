#include "stdafx.h"
#include "cypress_manager.h"
#include "node_proxy.h"

#include "../ytree/yson_reader.h"
#include "../ytree/yson_writer.h"

namespace NYT {
namespace NCypress {

using namespace NMetaState;

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

TLock* TCypressManager::CreateLock(const TNodeId& nodeId, const TTransactionId& transactionId)
{
    auto id = LockIdGenerator.Next();
    auto* lock = new TLock(id, nodeId, transactionId, ELockMode::ExclusiveWrite);
    LockMap.Insert(id, lock);
    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.LockIds().push_back(lock->GetId());

    LOG_INFO("Lock created (LockId: %s, NodeId: %s, TransactionId: %s)",
        ~id.ToString(),
        ~nodeId.ToString(),
        ~transactionId.ToString());

    return lock;
}

ICypressNode& TCypressManager::BranchNode(const ICypressNode& node, const TTransactionId& transactionId)
{
    YASSERT(!node.GetId().IsBranched());
    auto nodeId = node.GetId().NodeId;

    auto branchedNode = node.Branch(transactionId);
    branchedNode->SetState(ENodeState::Branched);

    auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
    transaction.BranchedNodeIds().push_back(nodeId);

    auto* branchedNodePtr = branchedNode.Release();
    NodeMap.Insert(TBranchedNodeId(nodeId, transactionId), branchedNodePtr);
    
    LOG_INFO("Node branched (NodeId: %s, TransactionId: %s)",
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

void TCypressManager::SetYPath(
    const TTransactionId& transactionId,
    TYPath path,
    TYsonProducer::TPtr producer )
{
    auto root = GetNode(RootNodeId, transactionId);
    NYTree::SetYPath(AsYPath(root), path, producer);
}

TVoid TCypressManager::SetYPath(const NProto::TMsgSet& message)
{
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    TStringInput inputStream(message.GetValue());
    auto producer = TYsonReader::GetProducer(&inputStream);
    SetYPath(transactionId, path, producer);
    return TVoid();
}

void TCypressManager::RemoveYPath(
    const TTransactionId& transactionId,
    TYPath path)
{
    auto root = GetNode(RootNodeId, transactionId);
    NYTree::RemoveYPath(AsYPath(root), path);
}

TVoid TCypressManager::RemoveYPath(const NProto::TMsgRemove& message)
{
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    RemoveYPath(transactionId, path);
    return TVoid();
}

void TCypressManager::LockYPath(const TTransactionId& transactionId, TYPath path)
{
    auto root = GetNode(RootNodeId, transactionId);
    NYTree::LockYPath(AsYPath(root), path);
}

NYT::TVoid TCypressManager::LockYPath(const NProto::TMsgLock& message)
{
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    LockYPath(transactionId, path);
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

    LOG_INFO("World initialized");
}

void TCypressManager::RefNode(ICypressNode& node)
{
    LOG_DEBUG("Node referenced (NodeId: %s)", ~node.GetId().ToString());

    node.Ref();
}

void TCypressManager::UnrefNode(ICypressNode& node)
{
    LOG_DEBUG("Node unreferenced (NodeId: %s)", ~node.GetId().ToString());

    if (node.Unref() == 0) {
        LOG_INFO("Node removed (NodeId: %s)", ~node.GetId().ToString());

        node.Destroy(this);
        NodeMap.Remove(node.GetId());
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

        LOG_INFO("Lock removed (LockId: %s", ~lockId.ToString());
    }
}

void TCypressManager::MergeBranchedNodes(TTransaction& transaction)
{
    auto transactionId = transaction.GetId();

    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        auto& node = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        RefNode(node);
    }

    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        auto& node = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        YASSERT(node.GetState() == ENodeState::Committed);

        auto& branchedNode = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, transactionId));
        YASSERT(branchedNode.GetState() == ENodeState::Branched);

        node.Merge(this, branchedNode);

        NodeMap.Remove(TBranchedNodeId(nodeId, transactionId));

        LOG_INFO("Node merged (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }

    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        auto& node = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        UnrefNode(node);
    }
}

void TCypressManager::RemoveBranchedNodes(TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.BranchedNodeIds()) {
        NodeMap.Remove(TBranchedNodeId(nodeId, transactionId));

        LOG_INFO("Branched node removed (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }
}

void TCypressManager::CommitCreatedNodes(TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.CreatedNodeIds()) {
        auto& node = NodeMap.GetForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        node.SetState(ENodeState::Committed);

        LOG_INFO("Node committed (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }
}

void TCypressManager::RemoveCreatedNodes(TTransaction& transaction)
{
    auto transactionId = transaction.GetId();
    FOREACH (const auto& nodeId, transaction.CreatedNodeIds()) {
        NodeMap.Remove(TBranchedNodeId(nodeId, NullTransactionId));

        LOG_INFO("Uncommitted node removed (NodeId: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~transactionId.ToString());
    }
}

METAMAP_ACCESSORS_IMPL(TCypressManager, Lock, TLock, TLockId, LockMap);
METAMAP_ACCESSORS_IMPL(TCypressManager, Node, ICypressNode, TBranchedNodeId, NodeMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
