#pragma once

#include "public.h"

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/ytlib/cellar_client/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateCellMapProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    NTransactionServer::TTransaction* transaction,
    NCypressServer::TCypressMapNode* trunkNode,
    NCellarClient::ECellarType cellarType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
