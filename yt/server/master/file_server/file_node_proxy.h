#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/server/master/object_server/public.h>

namespace NYT::NFileServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateFileNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    NTransactionServer::TTransaction* transaction,
    TFileNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileServer

