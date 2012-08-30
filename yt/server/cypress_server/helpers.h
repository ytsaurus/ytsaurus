#pragma once

#include "public.h"

#include <ytlib/ytree/yson_string.h>

#include <server/transaction_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

yhash_map<Stroka, TNodeId> GetMapNodeChildren(
    NCellMaster::TBootstrap* bootstrap,
    const TNodeId& nodeId,
    NTransactionServer::TTransaction* transaction);

TVersionedNodeId FindMapNodeChild(
    NCellMaster::TBootstrap* bootstrap,
    const TNodeId& nodeId,
    NTransactionServer::TTransaction* transaction,
    const Stroka& key);

yhash_map<Stroka, NYTree::TYsonString> GetNodeAttributes(
    NCellMaster::TBootstrap* bootstrap,
    const TNodeId& nodeId,
    NTransactionServer::TTransaction* transaction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
