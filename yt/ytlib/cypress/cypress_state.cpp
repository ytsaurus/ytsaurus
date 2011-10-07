#include "cypress_state.h"
#include "node_proxy.h"

#include "../ytree/yson_reader.h"
#include "../ytree/yson_writer.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = CypressLogger;

////////////////////////////////////////////////////////////////////////////////

TCypressState::TCypressState(
    NMetaState::TMetaStateManager::TPtr metaStateManager,
    NMetaState::TCompositeMetaState::TPtr metaState,
    TTransactionManager::TPtr transactionManager)
    : TMetaStatePart(metaStateManager, metaState)
    , TransactionManager(transactionManager)
{
    YASSERT(~transactionManager != NULL);

    metaState->RegisterPart(this);
    transactionManager->RegisterHander(this);
    
    RegisterMethod(this, &TThis::SetYPath);
    RegisterMethod(this, &TThis::RemoveYPath);
}

INode::TPtr TCypressState::FindNode(
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

INode::TPtr TCypressState::GetNode(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    auto node = FindNode(nodeId, transactionId);
    YASSERT(~node != NULL);
    return node;
}

IStringNode::TPtr TCypressState::CreateStringNode(const TTransactionId& transactionId)
{
    return ~CreateNode<TStringNode, TStringNodeProxy>(transactionId);
}

IInt64Node::TPtr TCypressState::CreateInt64Node(const TTransactionId& transactionId)
{
    return ~CreateNode<TInt64Node, TInt64NodeProxy>(transactionId);
}

IDoubleNode::TPtr TCypressState::CreateDoubleNode(const TTransactionId& transactionId)
{
    return ~CreateNode<TDoubleNode, TDoubleNodeProxy>(transactionId);
}

IMapNode::TPtr TCypressState::CreateMapNode(const TTransactionId& transactionId)
{
    return ~CreateNode<TMapNode, TMapNodeProxy>(transactionId);
}

void TCypressState::GetYPath(
    const TTransactionId& transactionId,
    TYPath path,
    IYsonConsumer* consumer)
{
    auto root = GetNode(RootNodeId, transactionId);
    NYTree::GetYPath(AsYPath(root), path, consumer);
}

void TCypressState::SetYPath(
    const TTransactionId& transactionId,
    TYPath path,
    TYsonProducer::TPtr producer )
{
    auto root = GetNode(RootNodeId, transactionId);
    NYTree::SetYPath(AsYPath(root), path, producer);
}

TVoid TCypressState::SetYPath(const NProto::TMsgSetPath& message)
{
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    TStringInput inputStream(message.GetValue());
    auto producer = TYsonReader::GetProducer(&inputStream);
    SetYPath(transactionId, path, producer);
    return TVoid();
}

void TCypressState::RemoveYPath(
    const TTransactionId& transactionId,
    TYPath path)
{
    auto root = GetNode(RootNodeId, transactionId);
    NYTree::RemoveYPath(AsYPath(root), path);
}

TVoid TCypressState::RemoveYPath(const NProto::TMsgRemovePath& message)
{
    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto path = message.GetPath();
    RemoveYPath(transactionId, path);
    return TVoid();
}

Stroka TCypressState::GetPartName() const
{
    return "Cypress";
}

TFuture<TVoid>::TPtr TCypressState::Save(TOutputStream* stream)
{
    YASSERT(false);
    *stream << NodeIdGenerator;
    return NULL;
}

TFuture<TVoid>::TPtr TCypressState::Load(TInputStream* stream)
{
    YASSERT(false);
    *stream >> NodeIdGenerator;
    return NULL;
}

void TCypressState::Clear()
{
    TBranchedNodeId id(RootNodeId, NullTransactionId);
    auto* root = new TMapNode(id);
    YVERIFY(Nodes.Insert(id, root));
}

void TCypressState::OnTransactionStarted(TTransaction& transaction)
{
    UNUSED(transaction);
}

void TCypressState::OnTransactionCommitted(TTransaction& transaction)
{
    UNUSED(transaction);
}

void TCypressState::OnTransactionAborted(TTransaction& transaction)
{
    UNUSED(transaction);
}

METAMAP_ACCESSORS_IMPL(TCypressState, Lock, TLock, TLockId, Locks);
METAMAP_ACCESSORS_IMPL(TCypressState, Node, ICypressNode, TBranchedNodeId, Nodes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
