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

    void RefNode(ICypressNode& node);
    void UnrefNode(ICypressNode & node);

    IStringNode::TPtr CreateStringNodeProxy(const TTransactionId& transactionId);
    IInt64Node::TPtr  CreateInt64NodeProxy(const TTransactionId& transactionId);
    IDoubleNode::TPtr CreateDoubleNodeProxy(const TTransactionId& transactionId);
    IMapNode::TPtr    CreateMapNodeProxy(const TTransactionId& transactionId);
    IListNode::TPtr   CreateListNodeProxy(const TTransactionId& transactionId);

    METAMAP_ACCESSORS_DECL(Lock, TLock, TLockId);

    TLock* CreateLock(const TNodeId& nodeId, const TTransactionId& transactionId);

    ICypressNode& BranchNode(ICypressNode& node, const TTransactionId& transactionId);

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

    void CreateWorld();

    void OnTransactionCommitted(TTransaction& transaction);
    void OnTransactionAborted(TTransaction& transaction);

    void ReleaseLocks(TTransaction& transaction);
    void MergeBranchedNodes(TTransaction& transaction);
    void RemoveBranchedNodes(TTransaction& transaction);
    void CommitCreatedNodes(TTransaction& transaction);
    void RemoveCreatedNodes(TTransaction& transaction);

    template <class TImpl, class TProxy>
    TIntrusivePtr<TProxy> CreateNode(const TTransactionId& transactionId)
    {
        NLog::TLogger& Logger = CypressLogger;

        if (transactionId == NullTransactionId) {
            throw TYTreeException() << "Cannot create a node outside of a transaction";
        }

        auto nodeId = NodeIdGenerator.Next();
        TBranchedNodeId branchedNodeId(nodeId, NullTransactionId);
        auto* nodeImpl = new TImpl(branchedNodeId);
        NodeMap.Insert(branchedNodeId, nodeImpl);
        auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
        transaction.CreatedNodeIds().push_back(nodeId);
        auto proxy = New<TProxy>(this, transactionId, nodeId);

        LOG_INFO("Node created (NodeId: %s, NodeType: %s, TransactionId: %s)",
            ~nodeId.ToString(),
            ~proxy->GetType().ToString(),
            ~transactionId.ToString());

        return proxy;
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
