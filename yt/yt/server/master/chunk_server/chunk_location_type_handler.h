#pragma once

#include "private.h"

#include <yt/yt/server/master/object_server/type_handler.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateChunkLocationTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    IDataNodeTrackerInternalPtr nodeTrackerInternal);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
