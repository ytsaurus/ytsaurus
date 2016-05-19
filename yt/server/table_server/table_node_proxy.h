#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/cypress_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/server/object_server/public.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateTableNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    NTransactionServer::TTransaction* transaction,
    TTableNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

