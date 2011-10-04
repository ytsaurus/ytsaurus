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

Stroka TCypressState::GetPartName() const
{
    return "Cypress";
}

TFuture<TVoid>::TPtr TCypressState::Save(TOutputStream* stream)
{
    YASSERT(false);
    UNUSED(stream);
    return NULL;
}

TFuture<TVoid>::TPtr TCypressState::Load(TInputStream* stream)
{
    YASSERT(false);
    UNUSED(stream);
    return NULL;
}

void TCypressState::Clear()
{ }

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
METAMAP_ACCESSORS_IMPL(TCypressState, Node, TCypressNodeImplBase, TBranchedNodeId, Nodes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
