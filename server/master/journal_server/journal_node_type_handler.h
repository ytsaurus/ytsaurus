#pragma once

#include "public.h"

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/master/cell_master/public.h>

namespace NYT::NJournalServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateJournalTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer

