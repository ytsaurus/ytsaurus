#pragma once

#include "public.h"

#include <server/cypress_server/public.h>

#include <server/transaction_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NJournalServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateJournalNodeProxy(
    NCypressServer::INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    NTransactionServer::TTransaction* transaction,
    TJournalNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalServer
} // namespace NYT

