#pragma once

#include "public.h"
#include "access_control_object.h"
#include "access_control_object_namespace.h"
#include "lock.h"
#include "node.h"
#include "node_proxy.h"
#include "type_handler.h"

#include <yt/yt/server/master/cell_master/automaton.h>

#include <yt/yt/server/master/cypress_server/proto/cypress_manager.pb.h>

#include <yt/yt/server/lib/hydra/entity_map.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/id_generator.h>

#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TNodeFactoryOptions
{
    bool PreserveAccount : 1 = false;
    bool PreserveCreationTime : 1 = false;
    bool PreserveModificationTime : 1 = false;
    bool PreserveExpirationTime : 1 = false;
    bool PreserveExpirationTimeout : 1 = false;
    bool PreserveOwner : 1 = false;
    bool PreserveAcl : 1 = false;
    bool PessimisticQuotaCheck : 1 = true;
};

DEFINE_ENUM(EPathRootType,
    (RootNode)
    (PortalExit)
    (SequoiaNode)
    (Other)
);

struct ICypressManager
    : public virtual TRefCounted
{
public:
    virtual void Initialize() = 0;

    virtual void RegisterHandler(INodeTypeHandlerPtr handler) = 0;
    virtual const INodeTypeHandlerPtr& FindHandler(NObjectClient::EObjectType type) = 0;
    virtual const INodeTypeHandlerPtr& GetHandler(NObjectClient::EObjectType type) = 0;
    virtual const INodeTypeHandlerPtr& GetHandler(const TCypressNode* node) = 0;

    //! Creates a new shard without any references to it.
    virtual TCypressShard* CreateShard(TCypressShardId shardId) = 0;

    //! Assigns a given shard to the node. The node must not already be assigned any shard.
    virtual void SetShard(TCypressNode* node, TCypressShard* shard) = 0;

    //! Resets the node's shard. This call is noop if no shard is assigned to the node.
    virtual void ResetShard(TCypressNode* node) = 0;

    //! Increments the node counter of a given shard.
    virtual void UpdateShardNodeCount(TCypressShard* shard, NSecurityServer::TAccount* account, int delta) = 0;

    //! Creates a factory for creating nodes.
    /*
     *  \param serviceTrunkNode pointer to the last committed node
     *  \param unresolvedPathSuffix path from serviceTrunkNode to the node, that needs to be created
     */
    virtual std::unique_ptr<ICypressNodeFactory> CreateNodeFactory(
        TCypressShard* shard,
        NTransactionServer::TTransaction* transaction,
        NSecurityServer::TAccount* account,
        const TNodeFactoryOptions& options,
        TCypressNode* serviceTrunkNode = nullptr,
        NYPath::TYPath unresolvedPathSuffix = {}) = 0;

    //! Creates a new node and registers it.
    virtual TCypressNode* CreateNode(
        const INodeTypeHandlerPtr& handler,
        TNodeId hintId,
        const TCreateNodeContext& context) = 0;

    //! Creates a new node and registers it.
    virtual TCypressNode* InstantiateNode(
        TNodeId id,
        NObjectClient::TCellTag externalCellTag) = 0;

    //! Clones a node and registers its clone.
    virtual TCypressNode* CloneNode(
        TCypressNode* sourceNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        NYTree::IAttributeDictionary* inheritedAttributes,
        TNodeId hintId = NCypressClient::NullObjectId) = 0;

    //! Deserializes a node from a tree snapshot and registers its clone.
    virtual TCypressNode* EndCopyNode(
        TEndCopyContext* context,
        ICypressNodeFactory* factory,
        TNodeId sourceNodeId,
        NYTree::IAttributeDictionary* inheritedAttributes) = 0;

    //! Deserializes an existing #trunkNode node from a tree snapshot.
    virtual void EndCopyNodeInplace(
        TCypressNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory,
        TNodeId sourceNodeId,
        NYTree::IAttributeDictionary* inheritedAttributes) = 0;

    //! Returns the root node.
    virtual TCypressMapNode* GetRootNode() const = 0;

    //! Returns the root shard.
    virtual TCypressShard* GetRootCypressShard() const = 0;

    //! Returns true iff the object is either the global Cypress root or a portal exit.
    virtual bool IsShardRoot(const NObjectServer::TObject* object) const = 0;

    //! Finds node by id, throws if nothing is found.
    virtual TCypressNode* GetNodeOrThrow(TVersionedNodeId id) = 0;

    virtual NYPath::TYPath GetNodePath(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        EPathRootType* pathRootType = nullptr) = 0;

    virtual NYPath::TYPath GetNodePath(
        const ICypressNodeProxy* nodeProxy,
        EPathRootType* pathRootType = nullptr) = 0;

    virtual TCypressNode* ResolvePathToTrunkNode(
        const NYPath::TYPath& path,
        NTransactionServer::TTransaction* transaction = nullptr) = 0;

    virtual ICypressNodeProxyPtr ResolvePathToNodeProxy(
        const NYPath::TYPath& path,
        NTransactionServer::TTransaction* transaction = nullptr) = 0;

    //! Similar to |FindNode| provided by |DECLARE_ENTITY_MAP_ACCESSORS| but
    //! specially optimized for the case of null transaction.
    virtual TCypressNode* FindNode(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual TCypressNode* GetVersionedNode(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual ICypressNodeProxyPtr GetNodeProxy(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction = nullptr) = 0;

    virtual TCypressNode* LockNode(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request,
        bool recursive = false,
        bool dontLockForeign = false) = 0;

    //! Returns |true| if lock can be acquired.
    virtual bool CanLock(
        TCypressNode* trunkNode,
        const TLockRequest& request,
        bool recursive = false) = 0;

    struct TCreateLockResult
    {
        TLock* Lock;
        TCypressNode* BranchedNode;
    };

    virtual TCreateLockResult CreateLock(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request,
        bool waitable) = 0;

    //! Releases and destroys all acquired locks on the specified node for the
    //! specified transaction. Also destroys all pending locks. Adjusts the
    //! version tree of the node correspondingly.
    virtual void UnlockNode(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual void SetModified(TCypressNode* node, NObjectServer::EModificationType modificationType) = 0;
    virtual void SetAccessed(TCypressNode* trunkNode) = 0;
    virtual void SetTouched(TCypressNode* trunkNode) = 0;

    virtual void SetExpirationTime(TCypressNode* node, std::optional<TInstant> time) = 0;
    virtual void MergeExpirationTime(TCypressNode* originatingNode, TCypressNode* branchedNode) = 0;
    virtual void SetExpirationTimeout(TCypressNode* node, std::optional<TDuration> time) = 0;
    virtual void MergeExpirationTimeout(TCypressNode* originatingNode, TCypressNode* branchedNode) = 0;

    using TSubtreeNodes = TCompactVector<TCypressNode*, 1>;
    virtual TSubtreeNodes ListSubtreeNodes(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        bool includeRoot = true) = 0;
    virtual void SetUnreachableSubtreeNodes(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction) = 0;
    virtual void SetReachableSubtreeNodes(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        bool includeRoot = true) = 0;

    virtual bool IsOrphaned(TCypressNode* trunkNode) = 0;

    //! Returns the list consisting of the trunk node
    //! and all of its existing versioned overrides up to #transaction;
    //! #trunkNode is the last element.
    virtual TCypressNodeList GetNodeOriginators(
        NTransactionServer::TTransaction* transaction,
        TCypressNode* trunkNode) = 0;

    //! Same as GetNodeOverrides but #trunkNode is the first element.
    virtual TCypressNodeList GetNodeReverseOriginators(
        NTransactionServer::TTransaction* transaction,
        TCypressNode* trunkNode) = 0;

    virtual const TResolveCachePtr& GetResolveCache() const = 0;

    virtual TFuture<NYson::TYsonString> ComputeRecursiveResourceUsage(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Node, TCypressNode);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Lock, TLock);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Shard, TCypressShard);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(AccessControlObject, TAccessControlObject);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(AccessControlObjectNamespace, TAccessControlObjectNamespace);

    virtual TAccessControlObjectNamespace* CreateAccessControlObjectNamespace(
        const TString& name,
        NCypressClient::TObjectId hintId = NCypressClient::NullObjectId) = 0;
    virtual TAccessControlObjectNamespace* FindAccessControlObjectNamespaceByName(
        const TString& name) const = 0;
    virtual void ZombifyAccessControlObjectNamespace(
        TAccessControlObjectNamespace* accessControlNodeNamespace) = 0;
    //! Returns the number of alive access control namespace objects.
    virtual int GetAccessControlObjectNamespaceCount() const = 0;

    virtual TAccessControlObject* CreateAccessControlObject(
        const TString& name,
        const TString& namespace_,
        NCypressClient::TObjectId hintId = NCypressClient::NullObjectId) = 0;
    virtual void ZombifyAccessControlObject(TAccessControlObject* accessControlNode) = 0;

    DECLARE_INTERFACE_SIGNAL(void(TCypressNode*), NodeCreated);
};

DEFINE_REFCOUNTED_TYPE(ICypressManager)

////////////////////////////////////////////////////////////////////////////////

ICypressManagerPtr CreateCypressManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
