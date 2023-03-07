#pragma once

#include <yt/server/lib/hydra/public.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/core/misc/small_vector.h>

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

DECLARE_REFCOUNTED_STRUCT(INodeTypeHandler)
DECLARE_REFCOUNTED_STRUCT(ICypressNodeProxy)
DECLARE_REFCOUNTED_STRUCT(ICypressNodeVisitor)

DECLARE_REFCOUNTED_CLASS(TCypressManager)
DECLARE_REFCOUNTED_CLASS(TPortalManager)

DECLARE_REFCOUNTED_STRUCT(TResolveCacheNode)
DECLARE_REFCOUNTED_CLASS(TResolveCache)

struct TNodeFactoryOptions;
struct ICypressNodeFactory;

DECLARE_ENTITY_TYPE(TCypressNode, TVersionedNodeId, NObjectClient::TDirectVersionedObjectIdHash)
DECLARE_ENTITY_TYPE(TLock, TLockId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TCypressShard, NObjectClient::TObjectId, NObjectClient::TDirectObjectIdHash)

using TCypressNodeList = SmallVector<TCypressNode*, 8>;
using TCypressNodeExpirationMap = std::multimap<TInstant, TCypressNode*>;

struct TLockRequest;

class TMapNode;
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

template <class T>
class TScalarNodeTypeHandler;
using TStringNodeTypeHandler = TScalarNodeTypeHandler<TString>;
using TInt64NodeTypeHandler = TScalarNodeTypeHandler<i64>;
using TUint64NodeTypeHandler = TScalarNodeTypeHandler<ui64>;
using TDoubleNodeTypeHandler = TScalarNodeTypeHandler<double>;
using TBooleanNodeTypeHandler = TScalarNodeTypeHandler<bool>;

struct TCreateNodeContext;

class TBeginCopyContext;
class TEndCopyContext;
using TCopyPersistenceContext = TCustomPersistenceContext<TBeginCopyContext, TEndCopyContext>;

DECLARE_REFCOUNTED_CLASS(TCypressManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicCypressManagerConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELockKeyKind,
    (None)
    (Child)
    (Attribute)
);

DEFINE_ENUM(EModificationType,
    (Attributes)
    (Content)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
