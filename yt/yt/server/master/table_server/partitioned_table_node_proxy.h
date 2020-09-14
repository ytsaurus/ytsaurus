#pragma once

#include "public.h"

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/server/master/cell_master/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreatePartitionedTableNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    NTransactionServer::TTransaction* transaction,
    TPartitionedTableNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
