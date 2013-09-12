#pragma once

#include "public.h"

#include <ytlib/ytree/yson_string.h>

#include <server/transaction_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

yhash_map<Stroka, TCypressNodeBase*> GetMapNodeChildren(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkNode,
    NTransactionServer::TTransaction* transaction);

TCypressNodeBase* FindMapNodeChild(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkNode,
    NTransactionServer::TTransaction* transaction,
    const Stroka& key);

yhash_map<Stroka, NYTree::TYsonString> GetNodeAttributes(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkNode,
    NTransactionServer::TTransaction* transaction);

yhash_set<Stroka> ListNodeAttributes(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkNode,
    NTransactionServer::TTransaction* transaction);

void AttachChild(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkParent,
    TCypressNodeBase* child);

void DetachChild(
    NCellMaster::TBootstrap* bootstrap,
    TCypressNodeBase* trunkParent,
    TCypressNodeBase* child,
    bool unref);

bool NodeHasKey(
    NCellMaster::TBootstrap* bootstrap,
    const TCypressNodeBase* node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
