#include "cypress_manager.h"
#include "node_proxy.h"

#include "../ytree/yson_reader.h"
#include "../ytree/yson_writer.h"

namespace NYT {
namespace NCypress {

using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

TCypressManager::TCypressManager(
    TMetaStateManager::TPtr metaStateManager,
    TCompositeMetaState::TPtr metaState,
    TTransactionManager::TPtr transactionManager)
    : TMetaStatePart(metaStateManager, metaState)
    , TransactionManager(transactionManager)
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

IStringNode::TPtr TCypressManager::CreateStringNode(const TTransactionId& transactionId)
{
    return ~CreateNode<TStringNode, TStringNodeProxy>(transactionId);
}

IInt64Node::TPtr TCypressManager::CreateInt64Node(const TTransactionId& transactionId)
{
    return ~CreateNode<TInt64Node, TInt64NodeProxy>(transactionId);
}

IDoubleNode::TPtr TCypressManager::CreateDoubleNode(const TTransactionId& transactionId)
{
    return ~CreateNode<TDoubleNode, TDoubleNodeProxy>(transactionId);
}

IMapNode::TPtr TCypressManager::CreateMapNode(const TTransactionId& transactionId)
{
    return ~CreateNode<TMapNode, TMapNodeProxy>(transactionId);
}

TLock* TCypressManager::CreateLock(const TNodeId& nodeId, const TTransactionId& transactionId)
{
    auto id = LockIdGenerator.Next();
    auto* lock = new TLock(id, nodeId, transactionId, ELockMode::ExclusiveWrite);
    YVERIFY(LockMap.Insert(id, lock));
    return lock;
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

TVoid TCypressManager::SetYPath(const NProto::TMsgSetPath& message)
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

TVoid TCypressManager::RemoveYPath(const NProto::TMsgRemovePath& message)
{
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    RemoveYPath(transactionId, path);
    return TVoid();
}

Stroka TCypressManager::GetPartName() const
{
    return "Cypress";
}

TFuture<TVoid>::TPtr TCypressManager::Save(TOutputStream* stream, IInvoker::TPtr invoker)
{
    YASSERT(false);
    *stream << NodeIdGenerator
            << LockIdGenerator;
    return NULL;
}

TFuture<TVoid>::TPtr TCypressManager::Load(TInputStream* stream, IInvoker::TPtr invoker)
{
    YASSERT(false);
    *stream >> NodeIdGenerator
            >> LockIdGenerator;
    return NULL;
}

void TCypressManager::Clear()
{
    TBranchedNodeId id(RootNodeId, NullTransactionId);
    auto* root = new TMapNode(id);
    YVERIFY(NodeMap.Insert(id, root));
}

void TCypressManager::OnTransactionCommitted(TTransaction& transaction)
{
    UNUSED(transaction);
}

void TCypressManager::OnTransactionAborted(TTransaction& transaction)
{
    UNUSED(transaction);
}

METAMAP_ACCESSORS_IMPL(TCypressManager, Lock, TLock, TLockId, LockMap);
METAMAP_ACCESSORS_IMPL(TCypressManager, Node, ICypressNode, TBranchedNodeId, NodeMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
