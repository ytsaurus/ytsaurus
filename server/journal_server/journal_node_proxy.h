#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/cypress_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/server/object_server/public.h>

namespace NYT::NJournalServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::ICypressNodeProxyPtr CreateJournalNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    NTransactionServer::TTransaction* transaction,
    TJournalNode* trunkNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer

