#pragma once

#include "common.h"
#include "node.h"
#include "lock.h"
#include "cypress_state.pb.h"

#include "../ytree/ypath.h"
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
    typedef TCypressState TThis;
    typedef TIntrusivePtr<TThis> TPtr;

    TCypressState(
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState,
        TTransactionManager::TPtr transactionManager);

    METAMAP_ACCESSORS_DECL(Lock, TLock, TLockId);
    METAMAP_ACCESSORS_DECL(Node, ICypressNode, TBranchedNodeId);

    INode::TPtr FindNode(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    INode::TPtr GetNode(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    IStringNode::TPtr CreateStringNode(const TTransactionId& transactionId);
    IInt64Node::TPtr  CreateInt64Node(const TTransactionId& transactionId);
    IDoubleNode::TPtr CreateDoubleNode(const TTransactionId& transactionId);
    IMapNode::TPtr    CreateMapNode(const TTransactionId& transactionId);

    void GetYPath(
        const TTransactionId& transactionId,
        TYPath path,
        IYsonConsumer* consumer);

    void SetYPath(
        const TTransactionId& transactionId,
        TYPath path,
        TYsonProducer::TPtr producer);

    TVoid SetYPath(const NProto::TMsgSetPath& message);

    void RemoveYPath(
        const TTransactionId& transactionId,
        TYPath path);

    TVoid RemoveYPath(const NProto::TMsgRemovePath& message);

private:
    TTransactionManager::TPtr TransactionManager;
    TIdGenerator<TGuid> NodeIdGenerator;
    
    NMetaState::TMetaStateMap<TLockId, TLock> Locks;
    NMetaState::TMetaStateMap<TBranchedNodeId, ICypressNode> Nodes;

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const;
    virtual TFuture<TVoid>::TPtr Save(TOutputStream* stream);
    virtual TFuture<TVoid>::TPtr Load(TInputStream* stream);
    virtual void Clear();

    // ITransactionHandler implementation.
    virtual void OnTransactionStarted(TTransaction& transaction);
    virtual void OnTransactionCommitted(TTransaction& transaction);
    virtual void OnTransactionAborted(TTransaction& transaction);

    template <class TImpl, class TProxy>
    TIntrusivePtr<TProxy> CreateNode(const TTransactionId& transactionId)
    {
        TBranchedNodeId id(NodeIdGenerator.Next(), transactionId);
        auto* nodeImpl = new TImpl(id);
        YVERIFY(Nodes.Insert(id, nodeImpl));
        return ~New<TProxy>(this, transactionId, id.NodeId);
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
