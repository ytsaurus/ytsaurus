#pragma once

#include "public.h"
#include "node.h"
#include "type_handler.h"
#include "node_proxy.h"
#include "lock.h"

#include <core/misc/small_vector.h>
#include <core/misc/id_generator.h>

#include <core/concurrency/thread_affinity.h>

#include <core/ytree/ypath_service.h>
#include <core/ytree/tree_builder.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/entity_map.h>
#include <server/hydra/mutation.h>

#include <server/object_server/object_manager.h>

#include <server/cell_master/automaton.h>

#include <server/transaction_server/transaction.h>
#include <server/transaction_server/transaction_manager.h>

#include <server/security_server/public.h>

#include <server/cypress_server/cypress_manager.pb.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressManager
    : public NCellMaster::TMasterAutomatonPart
{
public:
    explicit TCypressManager(
        TCypressManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Initialize();

    void RegisterHandler(INodeTypeHandlerPtr handler);
    INodeTypeHandlerPtr FindHandler(NObjectClient::EObjectType type);
    INodeTypeHandlerPtr GetHandler(NObjectClient::EObjectType type);
    INodeTypeHandlerPtr GetHandler(const TCypressNodeBase* node);

    NHydra::TMutationPtr CreateUpdateAccessStatisticsMutation(
        const NProto::TReqUpdateAccessStatistics& request);

    typedef NRpc::TTypedServiceRequest<NCypressClient::NProto::TReqCreate> TReqCreate;
    typedef NRpc::TTypedServiceResponse<NCypressClient::NProto::TRspCreate> TRspCreate;

    //! Creates a factory for creating nodes.
    ICypressNodeFactoryPtr CreateNodeFactory(
        NTransactionServer::TTransaction* transaction,
        NSecurityServer::TAccount* account,
        bool preserveAccount);

    //! Creates a new node and registers it.
    TCypressNodeBase* CreateNode(
        INodeTypeHandlerPtr handler,
        ICypressNodeFactoryPtr factory,
        TReqCreate* request,
        TRspCreate* response);

    //! Creates a new node and registers it.
    TCypressNodeBase* CreateNode(const TNodeId& id);

    //! Clones a node and registers its clone.
    TCypressNodeBase* CloneNode(
        TCypressNodeBase* sourceNode,
        ICypressNodeFactoryPtr factory,
        ENodeCloneMode mode);

    //! Returns the root node.
    TCypressNodeBase* GetRootNode() const;

    //! Finds node by id, throws if nothing is found.
    TCypressNodeBase* GetNodeOrThrow(const TVersionedNodeId& id);

    //! Creates a resolver that provides a view in the context of a given transaction.
    NYTree::INodeResolverPtr CreateResolver(NTransactionServer::TTransaction* transaction = nullptr);

    //! Similar to |FindNode| provided by |DECLARE_ENTITY_ACCESSORS| but
    //! specially optimized for the case of null transaction.
    TCypressNodeBase* FindNode(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction);

    TCypressNodeBase* GetVersionedNode(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction);

    ICypressNodeProxyPtr GetNodeProxy(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction = nullptr);

    TCypressNodeBase* LockNode(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request,
        bool recursive = false);

    TLock* CreateLock(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request,
        bool waitable);

    void SetModified(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction);

    void SetAccessed(TCypressNodeBase* trunkNode);

    typedef SmallVector<TCypressNodeBase*, 1> TSubtreeNodes;
    TSubtreeNodes ListSubtreeNodes(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        bool includeRoot = true);

    bool IsOrphaned(TCypressNodeBase* trunkNode);

    DECLARE_ENTITY_MAP_ACCESSORS(Node, TCypressNodeBase, TVersionedNodeId);
    DECLARE_ENTITY_MAP_ACCESSORS(Lock, TLock, TLockId);

private:
    class TNodeFactory;
    class TNodeTypeHandler;
    class TLockTypeHandler;
    class TYPathResolver;

    class TNodeMapTraits
    {
    public:
        explicit TNodeMapTraits(TCypressManager* cypressManager);

        std::unique_ptr<TCypressNodeBase> Create(const TVersionedNodeId& id) const;

    private:
        TCypressManager* CypressManager;

    };

    TCypressManagerConfigPtr Config;

    NHydra::TEntityMap<TVersionedNodeId, TCypressNodeBase, TNodeMapTraits> NodeMap;
    NHydra::TEntityMap<TLockId, TLock> LockMap;

    TEnumIndexedVector<INodeTypeHandlerPtr, NObjectClient::EObjectType> TypeToHandler;

    TNodeId RootNodeId;
    TCypressNodeBase* RootNode;

    TAccessTrackerPtr AccessTracker;

    bool RecomputeKeyColumns;
    
    
    void RegisterNode(TCypressNodeBase* node);

    void DestroyNode(TCypressNodeBase* trunkNode);

    // TAutomatonPart overrides.
    virtual void OnRecoveryComplete() override;

    void DoClear();
    virtual void Clear() override;

    void SaveKeys(NCellMaster::TSaveContext& context) const;
    void SaveValues(NCellMaster::TSaveContext& context) const;
    
    virtual void OnBeforeSnapshotLoaded() override;
    void LoadKeys(NCellMaster::TLoadContext& context);
    void LoadValues(NCellMaster::TLoadContext& context);
    virtual void OnAfterSnapshotLoaded() override;

    void InitBuiltin();

    void OnTransactionCommitted(NTransactionServer::TTransaction* transaction);
    void OnTransactionAborted(NTransactionServer::TTransaction* transaction);

    void ReleaseLocks(NTransactionServer::TTransaction* transaction);
    void MergeNodes(NTransactionServer::TTransaction* transaction);
    void MergeNode(
        NTransactionServer::TTransaction* transaction,
        TCypressNodeBase* branchedNode);
    void RemoveBranchedNodes(NTransactionServer::TTransaction* transaction);
    void RemoveBranchedNode(TCypressNodeBase* branchedNode);
    void PromoteLocks(NTransactionServer::TTransaction* transaction);
    void PromoteLock(TLock* lock, NTransactionServer::TTransaction* parentTransaction);

    TError CheckLock(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request,
        bool checkPending,
        bool* isMandatory);
    bool IsRedundantLockRequest(
        const TTransactionLockState& state,
        const TLockRequest& request);

    static bool IsParentTransaction(
        NTransactionServer::TTransaction* transaction,
        NTransactionServer::TTransaction* parent);
    static bool IsConcurrentTransaction(
        NTransactionServer::TTransaction* requestingTransaction,
        NTransactionServer::TTransaction* existingTransaction);

    TCypressNodeBase* DoAcquireLock(TLock* lock);
    void UpdateNodeLockState(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request);
    TLock* DoCreateLock(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request);
    void CheckPendingLocks(TCypressNodeBase* trunkNode);

    void ListSubtreeNodes(
        TCypressNodeBase* trunkNode,
        NTransactionServer::TTransaction* transaction,
        bool includeRoot,
        TSubtreeNodes* subtreeNodes);

    TCypressNodeBase* BranchNode(
       TCypressNodeBase* originatingNode,
       NTransactionServer::TTransaction* transaction,
       ELockMode mode);

    NYPath::TYPath GetNodePath(
       TCypressNodeBase* trunkNode,
       NTransactionServer::TTransaction* transaction);

    virtual void OnLeaderActive() override;
    virtual void OnStopLeading() override;
    
    void UpdateAccessStatistics(const NProto::TReqUpdateAccessStatistics& request);


    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

DEFINE_REFCOUNTED_TYPE(TCypressManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
