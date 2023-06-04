#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateSequoiaTransactionService(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
