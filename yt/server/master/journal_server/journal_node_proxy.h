#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/server/master/object_server/public.h>

namespace NYT::NJournalServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateJournalNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    NTransactionServer::TTransaction* transaction,
    TJournalNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer

