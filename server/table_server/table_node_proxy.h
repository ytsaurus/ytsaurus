#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/cypress_server/public.h>

#include <yt/server/transaction_server/public.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateTableNodeProxy(
    NCypressServer::INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    NTransactionServer::TTransaction* transaction,
    TTableNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

