#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/security_server/cluster_resources.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

const THashMap<TString, TCypressNode*>& GetMapNodeChildMap(
    const ICypressManagerPtr& cypressManager,
    TCypressMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    THashMap<TString, TCypressNode*>* storage);

std::vector<TCypressNode*> GetMapNodeChildList(
    const ICypressManagerPtr& cypressManager,
    TCypressMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

const std::vector<TCypressNode*>& GetListNodeChildList(
    const ICypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

TCypressNode* FindMapNodeChild(
    const ICypressManagerPtr& cypressManager,
    TCypressMapNode* trunkNode,
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

void AttachChild(
    TCypressNode* trunkParent,
    TCypressNode* child);

void DetachChild(
    TCypressNode* trunkParent,
    TCypressNode* child);

void AttachChildToSequoiaNodeOrThrow(
    TCypressNode* trunkParent,
    const TString& childKey,
    TNodeId childId);

bool NodeHasKey(const TCypressNode* node);
std::optional<TString> FindNodeKey(
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
    const std::optional<THashSet<NCompression::ECodec>>& configuredDeprecatedCodecIds,
    const std::optional<THashMap<TString, TString>>& configuredDeprecatedCodecNameToAlias);

NSecurityServer::TRichClusterResources GetNodeResourceUsage(const TCypressNode* node);

//! Returns closest ancestor with annotation or nullptr if there are no such ancestors.
TCypressNode* FindClosestAncestorWithAnnotation(TCypressNode* node);

//! Returns annotation or std::nullopt if there are no annotations available.
std::optional<TString> GetEffectiveAnnotation(TCypressNode* node);

void ValidateAccessControlObjectNamespaceName(const TString& name);
void ValidateAccessControlObjectName(const TString& name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
