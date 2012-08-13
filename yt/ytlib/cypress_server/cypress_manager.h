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
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeProxy;

class TCypressManager
    : public NMetaState::TMetaStatePart
{
public:
    explicit TCypressManager(NCellMaster::TBootstrap* bootstrap);

    void RegisterHandler(INodeTypeHandlerPtr handler);
    INodeTypeHandlerPtr FindHandler(NObjectServer::EObjectType type);
    INodeTypeHandlerPtr GetHandler(NObjectServer::EObjectType type);

    typedef NRpc::TTypedServiceRequest<NCypressClient::NProto::TReqCreate> TReqCreate;
    typedef NRpc::TTypedServiceResponse<NCypressClient::NProto::TRspCreate> TRspCreate;

    //! Creates a new node, sets its attributes, and also registers it.
    ICypressNode* CreateNode(
        INodeTypeHandlerPtr handler,
        NTransactionServer::TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response,
        NYTree::IAttributeDictionary* attributes);

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

    //! Creates a resolver that provides a view in the context of a given transaction.
    NYTree::IYPathResolverPtr CreateResolver(NTransactionServer::TTransaction* transaction = NULL);

    ICypressNode* FindVersionedNode(
        const TNodeId& nodeId,
        const NTransactionServer::TTransaction* transaction);

    ICypressNode* GetVersionedNode(
        const TNodeId& nodeId,
        const NTransactionServer::TTransaction* transaction);

    ICypressNodeProxyPtr FindVersionedNodeProxy(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction = NULL);

    ICypressNodeProxyPtr GetVersionedNodeProxy(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction = NULL);

    ICypressNodeProxyPtr GetVersionedNodeProxy(
        const TVersionedNodeId& versionedId);

    ICypressNode* LockVersionedNode(
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode = ELockMode::Exclusive);

    ICypressNode* LockVersionedNode(
        ICypressNode* node,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode = ELockMode::Exclusive);

    void RegisterNode(
        NTransactionServer::TTransaction* transaction,
        TAutoPtr<ICypressNode> node,
        NYTree::IAttributeDictionary* attributes = NULL);

    DECLARE_METAMAP_ACCESSORS(Node, ICypressNode, TVersionedNodeId);

private:
    typedef TCypressManager TThis;

    class TNodeTypeHandler;
    class TYPathResolver;

    class TNodeMapTraits
    {
    public:
        explicit TNodeMapTraits(TCypressManager* cypressManager);

        TAutoPtr<ICypressNode> Create(const TVersionedNodeId& id) const;

    private:
        TCypressManager* CypressManager;
    };
    
    NCellMaster::TBootstrap* Bootstrap;

    NMetaState::TMetaStateMap<TVersionedNodeId, ICypressNode, TNodeMapTraits> NodeMap;

    std::vector<INodeTypeHandlerPtr> TypeToHandler;

    yhash_map<TNodeId, INodeBehaviorPtr> NodeBehaviors;

    i32 RefNode(const TNodeId& nodeId);
    i32 UnrefNode(const TNodeId& nodeId);
    i32 GetNodeRefCounter(const TNodeId& nodeId);
    void DestroyNode(const TNodeId& nodeId);

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
        ICypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode,
        bool* isMandatory);
    void ValidateLock(
        ICypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode requestedMode);

    static bool IsParentTransaction(
        NTransactionServer::TTransaction* transaction,
        NTransactionServer::TTransaction* parent);
    static bool IsConcurrentTransaction(
        NTransactionServer::TTransaction* transaction1,
        NTransactionServer::TTransaction* transaction2);

    ICypressNode* AcquireLock(
        ICypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        ELockMode mode);
    TLock* CreateLock(
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

    NYTree::TYPath GetNodePath(
       const TNodeId& nodeId,
       NTransactionServer::TTransaction* transaction);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
