#pragma once

#include "common.h"
#include "node.h"
#include "node_proxy.h"
#include "lock.h"
#include "cypress_manager.pb.h"

#include "../misc/thread_affinity.h"
#include "../transaction_server/transaction.h"
#include "../transaction_server/transaction_manager.h"
#include "../ytree/ypath_service.h"
#include "../ytree/tree_builder.h"
#include "../misc/id_generator.h"
#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/map.h"
#include "../meta_state/meta_change.h"
#include <yt/ytlib/object_server/object_manager.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeProxy;

class TCypressManager
    : public NMetaState::TMetaStatePart
    , public NYTree::IYPathExecutor
{
public:
    typedef TCypressManager TThis;
    typedef TIntrusivePtr<TThis> TPtr;

    TCypressManager(
        NMetaState::IMetaStateManager* metaStateManager,
        NMetaState::TCompositeMetaState* metaState,
        NTransactionServer::TTransactionManager* transactionManager,
        NObjectServer::TObjectManager* objectManager);

    void RegisterHandler(INodeTypeHandler* handler);
    INodeTypeHandler* GetHandler(ERuntimeNodeType type);

    DECLARE_METAMAP_ACCESSORS(Node, ICypressNode, TBranchedNodeId);

    TNodeId GetRootNodeId();

    NObjectServer::TObjectManager* GetObjectManager() const;

    const ICypressNode* FindTransactionNode(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    const ICypressNode& GetTransactionNode(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    ICypressNode* FindTransactionNodeForUpdate(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    ICypressNode& GetTransactionNodeForUpdate(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    TIntrusivePtr<ICypressNodeProxy> FindNodeProxy(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    TIntrusivePtr<ICypressNodeProxy> GetNodeProxy(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    bool IsLockNeeded(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    TLockId LockTransactionNode(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

    TIntrusivePtr<ICypressNodeProxy> CreateNode(
        ERuntimeNodeType type,
        const TTransactionId& transactionId);

    TIntrusivePtr<ICypressNodeProxy> CreateDynamicNode(
        const TTransactionId& transactionId,
        const Stroka& typeName,
        NYTree::INode* manifest);

    DECLARE_METAMAP_ACCESSORS(Lock, TLock, TLockId);

    TLock& CreateLock(const TNodeId& nodeId, const TTransactionId& transactionId);

    ICypressNode& BranchNode(ICypressNode& node, const TTransactionId& transactionId);

    void ExecuteVerb(NYTree::IYPathService* service, NRpc::IServiceContext* context);

private:
    class TLockTypeHandler;
    class TNodeTypeHandler;

    class TNodeMapTraits
    {
    public:
        TNodeMapTraits(TCypressManager* cypressManager);

        TAutoPtr<ICypressNode> Clone(ICypressNode* value) const;
        void Save(ICypressNode* value, TOutputStream* output) const;
        TAutoPtr<ICypressNode> Load(const TBranchedNodeId& id, TInputStream* input) const;

    private:
        TCypressManager::TPtr CypressManager;

    };
    
    NTransactionServer::TTransactionManager::TPtr TransactionManager;
    NObjectServer::TObjectManager::TPtr ObjectManager;

    NMetaState::TMetaStateMap<TBranchedNodeId, ICypressNode, TNodeMapTraits> NodeMap;
    NMetaState::TMetaStateMap<TLockId, TLock> LockMap;

    yvector<INodeTypeHandler::TPtr> RuntimeTypeToHandler;
    yhash_map<Stroka, INodeTypeHandler::TPtr> TypeNameToHandler;

    yhash_map<TNodeId, INodeBehavior::TPtr> NodeBehaviors;

    i32 RefNode(const TNodeId& nodeId);
    i32 UnrefNode(const TNodeId& nodeId);
    i32 GetNodeRefCounter(const TNodeId& nodeId);

    TVoid DoExecuteLoggedVerb(const NProto::TMsgExecuteVerb& message);
    TVoid DoExecuteVerb(ICypressNodeProxy::TPtr proxy, NRpc::IServiceContext::TPtr context);

    TFuture<TVoid>::TPtr Save(const NMetaState::TCompositeMetaState::TSaveContext& context);
    void Load(TInputStream* input);
    virtual void Clear();

    virtual void OnLeaderRecoveryComplete();
    virtual void OnStopLeading();

    void OnTransactionCommitted(NTransactionServer::TTransaction& transaction);
    void OnTransactionAborted(NTransactionServer::TTransaction& transaction);

    ICypressNodeProxy::TPtr RegisterNode(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        INodeTypeHandler* typeHandler,
        TAutoPtr<ICypressNode> node);

    void ReleaseLocks(NTransactionServer::TTransaction& transaction);
    void MergeBranchedNodes(NTransactionServer::TTransaction& transaction);
    void RemoveBranchedNodes(NTransactionServer::TTransaction& transaction);
    void UnrefOriginatingNodes(NTransactionServer::TTransaction& transaction);
    void CommitCreatedNodes(NTransactionServer::TTransaction& transaction);

    INodeTypeHandler* GetHandler(const ICypressNode& node);

    void CreateNodeBehavior(const ICypressNode& node);
    void DestroyNodeBehavior(const ICypressNode& node);

    template <class TImpl, class TProxy>
    TIntrusivePtr<TProxy> CreateNode(
        const TTransactionId& transactionId,
        ERuntimeNodeType type);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
