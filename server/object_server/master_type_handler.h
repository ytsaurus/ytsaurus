#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateMasterTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
