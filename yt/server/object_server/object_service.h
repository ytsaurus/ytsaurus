#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateObjectService(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
