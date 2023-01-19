#include "public.h"

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateRootstockProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    NTransactionServer::TTransaction* transaction,
    TRootstockNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
