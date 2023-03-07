#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/server/master/object_server/public.h>

#include <yt/core/yson/string.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

const THashMap<TString, TCypressNode*>& GetMapNodeChildMap(
    const TCypressManagerPtr& cypressManager,
    TMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    THashMap<TString, TCypressNode*>* storage);

std::vector<TCypressNode*> GetMapNodeChildList(
    const TCypressManagerPtr& cypressManager,
    TMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

const std::vector<TCypressNode*>& GetListNodeChildList(
    const TCypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

TCypressNode* FindMapNodeChild(
    const TCypressManagerPtr& cypressManager,
    TMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TStringBuf key);

TCypressNode* GetMapNodeChildOrThrow(
    const TCypressManagerPtr& cypressManager,
    TMapNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TStringBuf key);

TStringBuf FindMapNodeChildKey(
    TMapNode* parentNode,
    TCypressNode* trunkChildNode);

TCypressNode* FindListNodeChild(
    const TCypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TStringBuf key);

TCypressNode* GetListNodeChildOrThrow(
    const TCypressManagerPtr& cypressManager,
    TListNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TStringBuf key);

int FindListNodeChildIndex(
    TListNode* parentNode,
    TCypressNode* trunkChildNode);

THashMap<TString, NYson::TYsonString> GetNodeAttributes(
    const TCypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

std::vector<std::pair<TString, NYson::TYsonString>> SortKeyToAttribute(
    const THashMap<TString, NYson::TYsonString>& keyToAttributeMap);

THashSet<TString> ListNodeAttributes(
    const TCypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

void AttachChild(
    TCypressNode* trunkParent,
    TCypressNode* child);

void DetachChild(
    TCypressNode* trunkParent,
    TCypressNode* child);

bool NodeHasKey(const TCypressNode* node);
std::optional<TString> FindNodeKey(
    const TCypressManagerPtr& cypressManager,
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
