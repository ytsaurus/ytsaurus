#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateCypressTransactionService(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
