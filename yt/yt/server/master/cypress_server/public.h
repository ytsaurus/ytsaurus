#pragma once

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

using NCypressClient::TNodeId;
using NCypressClient::TLockId;
using NCypressClient::ELockMode;
using NCypressClient::ELockState;
using NCypressClient::ENodeCloneMode;
using NCypressClient::TVersionedNodeId;
using NCypressClient::TCypressShardId;

using NObjectClient::TTransactionId;

////////////////////////////////////////////////////////////////////////////////

using TAccessControlObjectId = NObjectServer::TObjectId;
using TAccessControlObjectNamespaceId = NObjectServer::TObjectId;

////////////////////////////////////////////////////////////////////////////////

constexpr double MaxExternalCellBias = 16.0;

DECLARE_REFCOUNTED_STRUCT(INodeTypeHandler)
DECLARE_REFCOUNTED_STRUCT(ICypressNodeProxy)
DECLARE_REFCOUNTED_STRUCT(ICypressNodeVisitor)

DECLARE_REFCOUNTED_STRUCT(ICypressManager)
DECLARE_REFCOUNTED_STRUCT(IPortalManager)
DECLARE_REFCOUNTED_STRUCT(IGraftingManager)
DECLARE_REFCOUNTED_STRUCT(ISequoiaActionsExecutor)

DECLARE_REFCOUNTED_STRUCT(TResolveCacheNode)
DECLARE_REFCOUNTED_CLASS(TResolveCache)

struct TNodeFactoryOptions;
struct ICypressNodeFactory;

DECLARE_ENTITY_TYPE(TCypressNode, TVersionedNodeId, NObjectClient::TVersionedObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TLock, TLockId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TCypressShard, NObjectClient::TObjectId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TAccessControlObject, TAccessControlObjectId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TAccessControlObjectNamespace, TAccessControlObjectNamespaceId, NObjectClient::TObjectIdEntropyHash)

DECLARE_MASTER_OBJECT_TYPE(TCypressNode)
DECLARE_MASTER_OBJECT_TYPE(TLock)
DECLARE_MASTER_OBJECT_TYPE(TCypressShard)
DECLARE_MASTER_OBJECT_TYPE(TAccessControlObject)
DECLARE_MASTER_OBJECT_TYPE(TAccessControlObjectNamespace)
DECLARE_MASTER_OBJECT_TYPE(TPortalEntranceNode)
DECLARE_MASTER_OBJECT_TYPE(TPortalExitNode)
DECLARE_MASTER_OBJECT_TYPE(TRootstockNode)
DECLARE_MASTER_OBJECT_TYPE(TScionNode)

using TCypressNodeList = TCompactVector<TCypressNode*, 8>;
using TCypressNodeExpirationMap = std::multimap<TInstant, TCypressNode*>;

struct TLockRequest;

template <class TChild>
class TMapNodeImpl;
using TCypressMapNode = TMapNodeImpl<TCypressNodeRawPtr>;
using TSequoiaMapNode = TMapNodeImpl<TNodeId>;

class TListNode;

template <class T>
class TScalarNode;
using TStringNode  = TScalarNode<TString>;
using TInt64Node   = TScalarNode<i64>;
using TUint64Node  = TScalarNode<ui64>;
using TDoubleNode  = TScalarNode<double>;
using TBooleanNode = TScalarNode<bool>;

class TLinkNode;
class TDocumentNode;
class TPortalEntranceNode;
class TPortalExitNode;
class TCompositeNodeBase;
class TRootstockNode;
class TScionNode;

template <class T>
class TScalarNodeTypeHandler;
using TStringNodeTypeHandler = TScalarNodeTypeHandler<TString>;
using TInt64NodeTypeHandler = TScalarNodeTypeHandler<i64>;
using TUint64NodeTypeHandler = TScalarNodeTypeHandler<ui64>;
using TDoubleNodeTypeHandler = TScalarNodeTypeHandler<double>;
using TBooleanNodeTypeHandler = TScalarNodeTypeHandler<bool>;

struct TCreateNodeContext;

class TSerializeNodeContext;
class TMaterializeNodeContext;
using TCopyPersistenceContext = TCustomPersistenceContext<TSerializeNodeContext, TMaterializeNodeContext>;

DECLARE_REFCOUNTED_CLASS(TCypressManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicCypressManagerConfig)

////////////////////////////////////////////////////////////////////////////////

// Changing the member order requires reign promotion.
DEFINE_ENUM_WITH_UNDERLYING_TYPE(ELockKeyKind, i8,
    ((None)     (0))
    ((Child)    (1))
    ((Attribute)(2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
