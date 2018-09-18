#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateTransactionService(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
