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
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    THashMap<TString, TCypressNode*>* storage);

std::vector<TCypressNode*> GetMapNodeChildList(
    const TCypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

const std::vector<TCypressNode*>& GetListNodeChildList(
    const TCypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

std::vector<std::pair<TString, TCypressNode*>> SortKeyToChild(
    const THashMap<TString, TCypressNode*>& keyToChildMap);

TCypressNode* FindMapNodeChild(
    const TCypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction,
    TStringBuf key);

TStringBuf FindMapNodeChildKey(
    TMapNode* parentNode,
    TCypressNode* trunkChildNode);

int FindListNodeChildIndex(
    TListNode* parentNode,
    TCypressNode* trunkChildNode);

THashMap<TString, NYson::TYsonString> GetNodeAttributes(
    const TCypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

THashSet<TString> ListNodeAttributes(
    const TCypressManagerPtr& cypressManager,
    TCypressNode* trunkNode,
    NTransactionServer::TTransaction* transaction);

void AttachChild(
    const NObjectServer::TObjectManagerPtr& objectManager,
    TCypressNode* trunkParent,
    TCypressNode* child);

void DetachChild(
    const NObjectServer::TObjectManagerPtr& objectManager,
    TCypressNode* trunkParent,
    TCypressNode* child,
    bool unref);

bool NodeHasKey(const TCypressNode* node);

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
