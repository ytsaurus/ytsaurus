#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELinkType,
    ((Cypress) (0))
    ((Sequoia) (1))
);

INodeTypeHandlerPtr CreateLinkNodeTypeHandler(NCellMaster::TBootstrap* bootstrap, ELinkType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
