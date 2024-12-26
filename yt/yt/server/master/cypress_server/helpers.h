#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/security_server/cluster_resources.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TChild>
using TKeyToCypressNodeImpl =
    THashMap<std::string, TChild, THash<std::string_view>, TEqualTo<std::string_view>>;

} // namespace NDetail

using TKeyToCypressNode = NDetail::TKeyToCypressNodeImpl<TCypressNode*>;
using TKeyToCypressNodeId = NDetail::TKeyToCypressNodeImpl<TNodeId>;

const TKeyToCypressNode& GetMapNodeChildMap(
    const ICypressManagerPtr& cypressManager,
    TCypressMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TKeyToCypressNode* storage);

std::vector<TCypressNode*> GetMapNodeChildList(
    const ICypressManagerPtr& cypressManager,
    TCypressMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

const TKeyToCypressNodeId& GetMapNodeChildMap(
    const ICypressManagerPtr& cypressManager,
    TSequoiaMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TKeyToCypressNodeId* storage);

const std::vector<TCypressNode*>& GetListNodeChildList(
    const ICypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

TCypressNode* FindMapNodeChild(
    const ICypressManagerPtr& cypressManager,
    TCypressMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TStringBuf key);

TNodeId FindMapNodeChild(
    const ICypressManagerPtr& cypressManager,
    TSequoiaMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TStringBuf key);

TCypressNode* GetMapNodeChildOrThrow(
    const ICypressManagerPtr& cypressManager,
    TCypressMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TStringBuf key);

TStringBuf FindMapNodeChildKey(
    TCypressMapNode* parentNode,
    TCypressNode* trunkChildNode);

TCypressNode* FindListNodeChild(
    const ICypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TStringBuf key);

TCypressNode* GetListNodeChildOrThrow(
    const ICypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TStringBuf key);

int FindListNodeChildIndex(
    TListNode* parentNode,
    TCypressNode* trunkChildNode);

THashMap<TString, NYson::TYsonString> GetNodeAttributes(
    const ICypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

std::vector<std::pair<TString, NYson::TYsonString>> SortKeyToAttribute(
    const THashMap<TString, NYson::TYsonString>& keyToAttributeMap);

THashSet<TString> ListNodeAttributes(
    const ICypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

// Use the following Attach/Detach methods with caution
// as they ignore node reachability control.
void AttachChildToNode(
    TCypressNode* trunkParent,
    TCypressNode* child);

void DetachChildFromNode(
    TCypressNode* trunkParent,
    TCypressNode* child);

// NB: #parent is _not_ required to be trunk node.
void AttachChildToSequoiaNodeOrThrow(
    TCypressNode* parent,
    const std::string& childKey,
    TNodeId childId);

void MaybeSetUnreachable(
    const INodeTypeHandlerPtr& handler,
    TCypressNode* node);

bool NodeHasKey(const TCypressNode* node);
std::optional<std::string> FindNodeKey(
    const ICypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

bool NodeHasParentId(const TCypressNode* node);
TNodeId GetNodeParentId(const TCypressNode* node);

bool IsAncestorOf(
    const TCypressNode* trunkAncestor,
    const TCypressNode* trunkDescendant);

TNodeId MakePortalExitNodeId(
    TNodeId entranceNodeId,
    NObjectClient::TCellTag exitCellTag);

TNodeId MakePortalEntranceNodeId(
    TNodeId exitNodeId,
    NObjectClient::TCellTag entranceCellTag);

NObjectClient::TObjectId MakeCypressShardId(
    TNodeId rootNodeId);

TString SuggestCypressShardName(TCypressShard* shard);

void ValidateCompressionCodec(
    const NYson::TYsonString& value,
    const std::optional<THashSet<NCompression::ECodec>>& configuredForbiddenCodecs,
    const std::optional<THashMap<TString, TString>>& configuredForbiddenCodecNameToAlias);

void ValidateErasureCodec(
    const NYson::TYsonString& value,
    const THashSet<NErasure::ECodec>& forbiddenCodecs);
void ValidateErasureCodec(
    NErasure::ECodec codecId,
    const THashSet<NErasure::ECodec>& forbiddenCodecs);

NSecurityServer::TRichClusterResources GetNodeResourceUsage(const TCypressNode* node);

//! Returns closest ancestor with annotation or nullptr if there are no such ancestors.
TCypressNode* FindClosestAncestorWithAnnotation(TCypressNode* node);

//! Returns annotation or std::nullopt if there are no annotations available.
std::optional<TString> GetEffectiveAnnotation(TCypressNode* node);

void ValidateAccessControlObjectNamespaceName(const std::string& name);
void ValidateAccessControlObjectName(const std::string& name);

////////////////////////////////////////////////////////////////////////////////

void MaybeTouchNode(
    const ICypressNodeProxyPtr& nodeProxy,
    const NCypressServer::NProto::TAccessTrackingOptions& protoOptions = {});

TLockRequest CreateLockRequest(
    ELockMode mode,
    const std::optional<TString>& childKey,
    const std::optional<TString>& attributeKey,
    NTransactionServer::TTimestamp timestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
