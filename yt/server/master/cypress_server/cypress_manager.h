#pragma once

#include "public.h"
#include "lock.h"
#include "node.h"
#include "node_proxy.h"
#include "type_handler.h"

#include <yt/server/master/cell_master/automaton.h>

#include <yt/server/master/cypress_server/proto/cypress_manager.pb.h>

#include <yt/server/lib/hydra/composite_automaton.h>
#include <yt/server/lib/hydra/entity_map.h>
#include <yt/server/lib/hydra/mutation.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/server/master/security_server/public.h>

#include <yt/server/master/table_server/public.h>

#include <yt/server/master/transaction_server/transaction.h>
#include <yt/server/master/transaction_server/transaction_manager.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/id_generator.h>
#include <yt/core/misc/small_vector.h>

#include <yt/core/ytree/tree_builder.h>
#include <yt/core/ytree/ypath_service.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TNodeFactoryOptions
{
    bool PreserveAccount = false;
    bool PreserveCreationTime = false;
    bool PreserveModificationTime = false;
    bool PreserveExpirationTime = false;
    bool PreserveOwner = false;
    bool PreserveAcl = false;
    bool PessimisticQuotaCheck = true;
};

class TCypressManager
    : public TRefCounted
{
public:
    explicit TCypressManager(NCellMaster::TBootstrap* bootstrap);

    ~TCypressManager();

    void Initialize();

    void RegisterHandler(INodeTypeHandlerPtr handler);
    const INodeTypeHandlerPtr& FindHandler(NObjectClient::EObjectType type);
    const INodeTypeHandlerPtr& GetHandler(NObjectClient::EObjectType type);
    const INodeTypeHandlerPtr& GetHandler(const TCypressNode* node);

    //! Creates a new shard without any references to it.
    TCypressShard* CreateShard(TCypressShardId shardId);

    //! Assigns a given shard to the node. The node must not already be assigned any shard.
    void SetShard(TCypressNode* node, TCypressShard* shard);

    //! Resets the node's shard. This call is noop if no shard is assigned to the node.
    void ResetShard(TCypressNode* node);

    //! Increments the node counter of a given shard.
    void UpdateShardNodeCount(TCypressShard* shard, NSecurityServer::TAccount* account, int delta);

    //! Creates a factory for creating nodes.
    std::unique_ptr<ICypressNodeFactory> CreateNodeFactory(
        TCypressShard* shard,
        NTransactionServer::TTransaction* transaction,
        NSecurityServer::TAccount* account,
        const TNodeFactoryOptions& options);

    //! Creates a new node and registers it.
    TCypressNode* CreateNode(
        const INodeTypeHandlerPtr& handler,
        TNodeId hintId,
        const TCreateNodeContext& context);

    //! Creates a new node and registers it.
    TCypressNode* InstantiateNode(
        TNodeId id,
        NObjectClient::TCellTag externalCellTag);

    //! Clones a node and registers its clone.
    TCypressNode* CloneNode(
        TCypressNode* sourceNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode);

    //! Deserializes a node from a tree snapshot and registers its clone.
    TCypressNode* EndCopyNode(
        TEndCopyContext* context,
        ICypressNodeFactory* factory,
        TNodeId sourceNodeId);

    //! Deserializes an existing #trunkNode node from a tree snapshot.
    void EndCopyNodeInplace(
        TCypressNode* trunkNode,
        TEndCopyContext* context,
        ICypressNodeFactory* factory,
        TNodeId sourceNodeId);

    //! Returns the root node.
    TMapNode* GetRootNode() const;

    //! Finds node by id, throws if nothing is found.
    TCypressNode* GetNodeOrThrow(const TVersionedNodeId& id);

    NYPath::TYPath GetNodePath(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction);

    NYPath::TYPath GetNodePath(
        const ICypressNodeProxy* nodeProxy);

    TCypressNode* ResolvePathToTrunkNode(
        const NYPath::TYPath& path,
        NTransactionServer::TTransaction* transaction = nullptr);

    ICypressNodeProxyPtr ResolvePathToNodeProxy(
        const NYPath::TYPath& path,
        NTransactionServer::TTransaction* transaction = nullptr);

    //! Similar to |FindNode| provided by |DECLARE_ENTITY_MAP_ACCESSORS| but
    //! specially optimized for the case of null transaction.
    TCypressNode* FindNode(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction);

    TCypressNode* GetVersionedNode(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction);

    ICypressNodeProxyPtr GetNodeProxy(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction = nullptr);

    TCypressNode* LockNode(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request,
        bool recursive = false,
        bool dontLockForeign = false);

    TLock* CreateLock(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        const TLockRequest& request,
        bool waitable);

    //! Releases and destroys all acquired locks on the specified node for the
    //! specified transaction. Also destroys all pending locks. Adjusts the
    //! version tree of the node correspondingly.
    void UnlockNode(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction);

    void SetModified(
        TCypressNode* node,
        EModificationType modificationType);

    void SetAccessed(TCypressNode* trunkNode);

    void SetExpirationTime(TCypressNode* node, std::optional<TInstant> time);

    typedef SmallVector<TCypressNode*, 1> TSubtreeNodes;
    TSubtreeNodes ListSubtreeNodes(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction,
        bool includeRoot = true);

    void AbortSubtreeTransactions(
        TCypressNode* trunkNode,
        NTransactionServer::TTransaction* transaction);
    void AbortSubtreeTransactions(NYTree::INodePtr node);

    bool IsOrphaned(TCypressNode* trunkNode);

    //! Returns the list consisting of the trunk node
    //! and all of its existing versioned overrides up to #transaction;
    //! #trunkNode is the last element.
    TCypressNodeList GetNodeOriginators(
        NTransactionServer::TTransaction* transaction,
        TCypressNode* trunkNode);

    //! Same as GetNodeOverrides but #trunkNode is the first element.
    TCypressNodeList GetNodeReverseOriginators(
        NTransactionServer::TTransaction* transaction,
        TCypressNode* trunkNode);

    const NTableServer::TSharedTableSchemaRegistryPtr& GetSharedTableSchemaRegistry() const;
    const TResolveCachePtr& GetResolveCache();

    DECLARE_ENTITY_MAP_ACCESSORS(Node, TCypressNode);
    DECLARE_ENTITY_MAP_ACCESSORS(Lock, TLock);
    DECLARE_ENTITY_MAP_ACCESSORS(Shard, TCypressShard);

    DECLARE_SIGNAL(void(TCypressNode*), NodeCreated);

private:
    class TNodeFactory;
    class TNodeTypeHandler;
    class TLockTypeHandler;

    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TCypressManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
