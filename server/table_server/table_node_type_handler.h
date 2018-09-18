#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/cypress_server/public.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateTableTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

