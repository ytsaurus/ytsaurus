#include "cypress_state.h"

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
    //        RegisterMethod(this, &TState::AddChunk);

    transactionManager->RegisterHander(this);
}

INode::TConstPtr TCypressState::GetNode(
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
{
    // TODO:
    return NULL;
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
    TCypressNodeBase::TPtr root = ~New<TMapNode>(id);
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
METAMAP_ACCESSORS_IMPL(TCypressState, Node, TCypressNodeBase, TBranchedNodeId, Nodes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
