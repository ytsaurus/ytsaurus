#pragma once

#include "common.h"
#include "node.h"
#include "lock.h"

#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/map.h"

//#include "cypress_service_rpc.h"
//
//
//#include "../meta_state/meta_state_service.h"
//#include "../rpc/server.h"
//#include "../ytree/node.h"
//
//#include "../ytree/node.h"

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
    METAMAP_ACCESSORS_DECL(Node, TCypressNodeImplBase, TBranchedNodeId);

private:
    TTransactionManager::TPtr TransactionManager;
    
    NMetaState::TMetaStateMap<
        TLockId,
        TLock> Locks;
    NMetaState::TMetaStateMap<
        TBranchedNodeId,
        TCypressNodeImplBase,
        NMetaState::TMetaStateMapPtrTraits<TCypressNodeImplBase> > Nodes;


    //TMetaStateMap<TNodeId, IRegistryNode, TMetaStateMapPtrTraits<IRegistryNode> > NodeMap;

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
