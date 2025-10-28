#pragma once

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBootstrap)

using ISequoiaServiceContext = NYTree::IYPathServiceContext;
using ISequoiaServiceContextPtr = NYTree::IYPathServiceContextPtr;

DECLARE_REFCOUNTED_CLASS(TCypressProxyServiceBase)

DECLARE_REFCOUNTED_STRUCT(INodeProxy)

DECLARE_REFCOUNTED_STRUCT(IObjectService)
DECLARE_REFCOUNTED_STRUCT(ISequoiaService)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

DECLARE_REFCOUNTED_STRUCT(IUserDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TUserDirectory)

DECLARE_REFCOUNTED_CLASS(TPerUserAndWorkloadRequestQueueProvider);

DECLARE_REFCOUNTED_STRUCT(IMasterConnector)

DECLARE_REFCOUNTED_CLASS(TAcdFetcher);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TObjectServiceDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TSequoiaResponseKeeperDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TTestConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressProxyBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressProxyProgramConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressProxyDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TUserDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSequoiaSession)

DECLARE_REFCOUNTED_STRUCT(ISequoiaResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

struct TCypressResolveResult;

// This means that the request was originally sent to a master service that
// doesn't use resolve. This is typical of requests that are concerned either
// with "master as a whole" (e.g. SetMaintennance) or requests that try to
// avoid tasking the master with resolving paths to objects (e.g. VectorizedRead).
struct TMasterResolveResult;

struct TSequoiaResolveResult;

using TResolveResult = std::variant<
    TCypressResolveResult,
    TMasterResolveResult,
    TSequoiaResolveResult
>;

// Cypress node cannot be resolved by ID without proper transaction after trunk
// node was removed. But some verbs (e.g. CheckPermission) are non-transactional
// by design. For such verbs if target ID is not found in resove table but
// target path consist of object ID only this special resolve result is used to
// indicate that (1) there is still no resolve error and (2) we don't know
// reliably if node exists or not.
struct TUnreachableSequoiaResolveResult;

using TMaybeUnreachableResolveResult = std::variant<
    TCypressResolveResult,
    TMasterResolveResult,
    TSequoiaResolveResult,
    TUnreachableSequoiaResolveResult
>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUserWorkloadType,
    (Read)
    (Write)
);

////////////////////////////////////////////////////////////////////////////////

struct TRequestExecutedPayload;
struct TForwardToMasterPayload;

using TInvokeResult = std::variant<
    TRequestExecutedPayload,
    TForwardToMasterPayload
>;

////////////////////////////////////////////////////////////////////////////////

struct TCopyOptions;
struct TMultisetAttributesSubrequest;

struct TCypressNodeDescriptor;
struct TCypressChildDescriptor;
struct TAccessControlDescriptor;

//! Represents a downward path to this node in a Sequoia tree.
//! See #TSequoiaResolveResult::NodeAncestry.
using TNodeAncestry = TRange<TCypressNodeDescriptor>;

////////////////////////////////////////////////////////////////////////////////

/*!
 *  This cache is used to insert rows into {node,path,child}_forks Sequoia
 *  tables. In most cases we've already looked up all necessary records during
 *  either resolve or subtree removal so we can rely on cached information
 *  instead of looking into Sequoia tables again.
 *
 *  NB: Treatment of this cache depends on context. In case of node removal a
 *  record missing from this cache implies a bug somewhere since we haven't
 *  observed the node existance before current request (or there wasn't such
 *  node but we decided to remove nonexistent node instead of reporting resolve
 *  error). In case of node creation record absence means that current Cypress
 *  transaction is the progenitor one for the record.
 */
struct TProgenitorTransactionCache
{
    THashMap<NSequoiaClient::TAbsolutePath, NCypressClient::TTransactionId> Path;
    THashMap<NCypressClient::TNodeId, NCypressClient::TTransactionId> Node;
    // Key: (parent ID, child key).
    THashMap<std::pair<NCypressClient::TNodeId, std::string>, NCypressClient::TTransactionId> Child;
};

////////////////////////////////////////////////////////////////////////////////

struct TSubjectDescriptor;
using TSubjectDescriptorPtr = std::shared_ptr<const TSubjectDescriptor>;

using TGroupDescriptor = TSubjectDescriptor;
using TGroupDescriptorPtr = TSubjectDescriptorPtr;

struct TUserDescriptor;
using TUserDescriptorPtr = std::shared_ptr<const TUserDescriptor>;

////////////////////////////////////////////////////////////////////////////////

using TNodeIdToConstAttributes = THashMap<NCypressClient::TNodeId, NYTree::IConstAttributeDictionaryPtr>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
