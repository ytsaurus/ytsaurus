#pragma once

#include "common.h"
#include "node.h"
#include "lock.h"

#include "../misc/id_generator.h"
#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/map.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TCypressState
    : public NMetaState::TMetaStatePart
    , public NTransaction::ITransactionHandler
{
public:
    typedef TIntrusivePtr<TCypressState> TPtr;

    TCypressState(
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState,
        TTransactionManager::TPtr transactionManager);

    METAMAP_ACCESSORS_DECL(Lock, TLock, TLockId);
    METAMAP_ACCESSORS_DECL(Node, TCypressNodeBase, TBranchedNodeId);

    INode::TConstPtr GetNode(
        const TTransactionId& transactionId,
        const TNodeId& nodeId);

private:
    TTransactionManager::TPtr TransactionManager;
    TIdGenerator<TGuid> NodeIdGenerator;
    
    NMetaState::TMetaStateMap<
        TLockId,
        TLock> Locks;
    NMetaState::TMetaStateMap<
        TBranchedNodeId,
        TCypressNodeBase,
        NMetaState::TMetaStateMapPtrTraits<TCypressNodeBase> > Nodes;

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const;
    virtual TFuture<TVoid>::TPtr Save(TOutputStream* stream);
    virtual TFuture<TVoid>::TPtr Load(TInputStream* stream);
    virtual void Clear();

    // ITransactionHandler implementation.
    virtual void OnTransactionStarted(TTransaction& transaction);
    virtual void OnTransactionCommitted(TTransaction& transaction);
    virtual void OnTransactionAborted(TTransaction& transaction);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
