#pragma once

#include "public.h"

#include <server/cypress_server/public.h>

#include <server/transaction_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateFileNodeProxy(
    NCypressServer::INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    NTransactionServer::TTransaction* transaction,
    TFileNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

