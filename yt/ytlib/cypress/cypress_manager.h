#pragma once

#include "public.h"
#include "node.h"
#include "type_handler.h"
#include "node_proxy.h"
#include "lock.h"

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
#include <ytlib/meta_state/meta_change.h>
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

    TCypressManager(NCellMaster::TBootstrap* bootstrap);

    void RegisterHandler(INodeTypeHandler::TPtr handler);
    INodeTypeHandler::TPtr FindHandler(EObjectType type);
    INodeTypeHandler::TPtr GetHandler(EObjectType type);

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
        NTransactionServer::TTransaction* transaction);

    ICypressNode& GetVersionedNode(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction);

    ICypressNode* FindVersionedNodeForUpdate(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode = ELockMode::Exclusive);

    ICypressNode& GetVersionedNodeForUpdate(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode = ELockMode::Exclusive);

    TIntrusivePtr<ICypressNodeProxy> FindVersionedNodeProxy(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction = NULL);

    TIntrusivePtr<ICypressNodeProxy> GetVersionedNodeProxy(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction = NULL);

    TLockId LockVersionedNode(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode = ELockMode::Exclusive);

    void RegisterNode(
        NTransactionServer::TTransaction* transaction,
        TAutoPtr<ICypressNode> node);

    DECLARE_METAMAP_ACCESSORS(Lock, TLock, TLockId);
    DECLARE_METAMAP_ACCESSORS(Node, ICypressNode, TVersionedNodeId);

private:
    class TLockTypeHandler;
    class TNodeTypeHandler;
    class TYPathProcessor;
    class TRootProxy;

    class TNodeMapTraits
    {
    public:
        TNodeMapTraits(TCypressManager* cypressManager);

        TAutoPtr<ICypressNode> Create(const TVersionedNodeId& id) const;

    private:
        TCypressManager::TPtr CypressManager;
    };
    
    NCellMaster::TBootstrap* Bootstrap;

    NMetaState::TMetaStateMap<TVersionedNodeId, ICypressNode, TNodeMapTraits> NodeMap;
    NMetaState::TMetaStateMap<TLockId, TLock> LockMap;

    yvector<INodeTypeHandler::TPtr> TypeToHandler;

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

    void OnTransactionCommitted(NTransactionServer::TTransaction& transaction);
    void OnTransactionAborted(NTransactionServer::TTransaction& transaction);

    void ReleaseLocks(const NTransactionServer::TTransaction& transaction);
    void MergeBranchedNodes(const NTransactionServer::TTransaction& transaction);
    void MergeBranchedNode(
        const NTransactionServer::TTransaction& transaction,
        const TNodeId& nodeId);
    void RemoveBranchedNodes(const NTransactionServer::TTransaction& transaction);
    void UnrefOriginatingNodes(const NTransactionServer::TTransaction& transaction);

    INodeTypeHandler::TPtr GetHandler(const ICypressNode& node);

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

    static bool IsParentTransaction(NTransactionServer::TTransaction* transaction, NTransactionServer::TTransaction* parent);

    TLockId AcquireLock(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode mode);

    void ReleaseLock(TLock *lock);

   ICypressNode& BranchNode(
       ICypressNode& node,
       NTransactionServer::TTransaction* transaction,
       ELockMode mode);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
