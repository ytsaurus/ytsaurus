#pragma once

#include "common.h"
#include "node.h"
#include "lock.h"
#include "cypress_manager.pb.h"

#include "../transaction_manager/transaction.h"
#include "../transaction_manager/transaction_manager.h"
#include "../ytree/ypath.h"
#include "../misc/id_generator.h"
#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/map.h"

namespace NYT {
namespace NCypress {

using NTransaction::TTransaction;
using NTransaction::NullTransactionId;
using NTransaction::TTransactionManager;

////////////////////////////////////////////////////////////////////////////////

class TCypressManager
    : public NMetaState::TMetaStatePart
{
public:
    typedef TCypressManager TThis;
    typedef TIntrusivePtr<TThis> TPtr;

    TCypressManager(
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState,
        TTransactionManager::TPtr transactionManager);

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

    METAMAP_ACCESSORS_DECL(Lock, TLock, TLockId);

    TLock* CreateLock(const TNodeId& nodeId, const TTransactionId& transactionId);

    ICypressNode& BranchNode(const ICypressNode& node, const TTransactionId& transactionId);

    void GetYPath(
        const TTransactionId& transactionId,
        TYPath path,
        IYsonConsumer* consumer);

    void SetYPath(
        const TTransactionId& transactionId,
        TYPath path,
        TYsonProducer::TPtr producer);

    TVoid SetYPath(const NProto::TMsgSet& message);

    void RemoveYPath(
        const TTransactionId& transactionId,
        TYPath path);

    TVoid RemoveYPath(const NProto::TMsgRemove& message);

    void LockYPath(
        const TTransactionId& transactionId,
        TYPath path);

    TVoid LockYPath(const NProto::TMsgLock& message);


private:
    TTransactionManager::TPtr TransactionManager;
    TIdGenerator<TNodeId> NodeIdGenerator;
    TIdGenerator<TLockId> LockIdGenerator;
    
    NMetaState::TMetaStateMap<TLockId, TLock> LockMap;
    NMetaState::TMetaStateMap<TBranchedNodeId, ICypressNode> NodeMap;

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const;
    virtual TFuture<TVoid>::TPtr Save(TOutputStream* stream, IInvoker::TPtr invoker);
    virtual TFuture<TVoid>::TPtr Load(TInputStream* stream, IInvoker::TPtr invoker);
    virtual void Clear();

    void OnTransactionCommitted(TTransaction& transaction);
    void OnTransactionAborted(TTransaction& transaction);

    void ReleaseLocks(TTransaction& transaction);
    void MergeBranchedNodes(TTransaction& transaction);
    void RemoveBranchedNodes(TTransaction& transaction);

    template <class TImpl, class TProxy>
    TIntrusivePtr<TProxy> CreateNode(const TTransactionId& transactionId)
    {
        TBranchedNodeId id(NodeIdGenerator.Next(), transactionId);
        auto* nodeImpl = new TImpl(id);
        YVERIFY(NodeMap.Insert(id, nodeImpl));
        return ~New<TProxy>(this, transactionId, id.NodeId);
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
