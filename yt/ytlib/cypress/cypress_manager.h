#pragma once

#include "public.h"
#include "node.h"
#include "type_handler.h"
#include "node_proxy.h"

#include <ytlib/cell_master/public.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/transaction_server/transaction.h>
#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/tree_builder.h>
#include <ytlib/misc/id_generator.h>
#include <ytlib/meta_state/meta_state_manager.h>
#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/meta_state/mutation.h>
#include <ytlib/object_server/object_manager.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeProxy;

class TCypressManager
    : public NMetaState::TMetaStatePart
{
public:
    typedef TCypressManager TThis;
    typedef TIntrusivePtr<TThis> TPtr;

    explicit TCypressManager(NCellMaster::TBootstrap* bootstrap);

    void RegisterHandler(INodeTypeHandlerPtr handler);
    INodeTypeHandlerPtr FindHandler(EObjectType type);
    INodeTypeHandlerPtr GetHandler(EObjectType type);

    //! Returns the id of the root node.
    /*!
     *  \note
     *  This id depends on cell id.
     */
    TNodeId GetRootNodeId();

    //! Returns a service producer that is absolutely safe to use from any thread.
    /*!
     *  The producer first makes a coarse check to ensure that the peer is leading.
     *  If it passes, then the request is forwarded to the state thread and
     *  another (rigorous) check is made.
     */
    NYTree::TYPathServiceProducer GetRootServiceProducer();

    ICypressNode* FindVersionedNode(
        const TNodeId& nodeId,
        const NTransactionServer::TTransaction* transaction);

    ICypressNode* GetVersionedNode(
        const TNodeId& nodeId,
        const NTransactionServer::TTransaction* transaction);

    ICypressNode* FindVersionedNodeForUpdate(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode = ELockMode::Exclusive);

    ICypressNode* GetVersionedNodeForUpdate(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode = ELockMode::Exclusive);

    TIntrusivePtr<ICypressNodeProxy> FindVersionedNodeProxy(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction = NULL);

    TIntrusivePtr<ICypressNodeProxy> GetVersionedNodeProxy(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction = NULL);

    void LockVersionedNode(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode = ELockMode::Exclusive);

    void RegisterNode(
        NTransactionServer::TTransaction* transaction,
        TAutoPtr<ICypressNode> node);

    NYTree::TYPath GetNodePath(ICypressNodeProxyPtr proxy);
    NYTree::TYPath GetNodePath(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction);
    NYTree::TYPath GetNodePath(const TVersionedNodeId& id);

    DECLARE_METAMAP_ACCESSORS(Node, ICypressNode, TVersionedNodeId);

private:
    class TNodeTypeHandler;
    class TYPathProcessor;
    class TRootProxy;

    class TNodeMapTraits
    {
    public:
        explicit TNodeMapTraits(TCypressManager* cypressManager);

        TAutoPtr<ICypressNode> Create(const TVersionedNodeId& id) const;

    private:
        TCypressManager::TPtr CypressManager;
    };
    
    NCellMaster::TBootstrap* Bootstrap;

    NMetaState::TMetaStateMap<TVersionedNodeId, ICypressNode, TNodeMapTraits> NodeMap;

    std::vector<INodeTypeHandlerPtr> TypeToHandler;

    yhash_map<TNodeId, INodeBehavior::TPtr> NodeBehaviors;

    i32 RefNode(const TNodeId& nodeId);
    i32 UnrefNode(const TNodeId& nodeId);
    i32 GetNodeRefCounter(const TNodeId& nodeId);

    void SaveKeys(TOutputStream* output) const;
    void SaveValues(TOutputStream* output) const;
    void LoadKeys(TInputStream* input);
    void LoadValues(const NCellMaster::TLoadContext& context, TInputStream* input);
    virtual void Clear();

    virtual void OnLeaderRecoveryComplete();
    virtual void OnStopLeading();

    void OnTransactionCommitted(NTransactionServer::TTransaction* transaction);
    void OnTransactionAborted(NTransactionServer::TTransaction* transaction);

    void ReleaseLocks(NTransactionServer::TTransaction* transaction);
    void MergeNodes(NTransactionServer::TTransaction* transaction);
    void MergeNode(
        NTransactionServer::TTransaction* transaction,
        ICypressNode* branchedNode);
    void RemoveBranchedNodes(NTransactionServer::TTransaction* transaction);
    void ReleaseCreatedNodes(NTransactionServer::TTransaction* transaction);
    void PromoteLocks(NTransactionServer::TTransaction* transaction);
    void PromoteLock(TLock* lock, NTransactionServer::TTransaction* parentTransaction);

    INodeTypeHandlerPtr GetHandler(const ICypressNode* node);

    void CreateNodeBehavior(const TNodeId& id);
    void DestroyNodeBehavior(const TNodeId& id);

    void ValidateLock(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode,
        bool* isMandatory);
    void ValidateLock(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode);

    static bool IsParentTransaction(
        NTransactionServer::TTransaction* transaction,
        NTransactionServer::TTransaction* parent);
    static bool IsConcurrentTransaction(
        NTransactionServer::TTransaction* transaction1,
        NTransactionServer::TTransaction* transaction2);

    void AcquireLock(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode mode);
    TLock* DoAcquireLock(
        ICypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode mode);
    void ReleaseLock(
        ICypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction);

   ICypressNode* BranchNode(
       ICypressNode* node,
       NTransactionServer::TTransaction* transaction,
       ELockMode mode);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
