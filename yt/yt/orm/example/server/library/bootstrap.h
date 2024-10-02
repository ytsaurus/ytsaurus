#pragma once

#include "public.h"

#include <yt/yt/orm/server/access_control/public.h>
#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/api/public.h>

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

IBootstrap* StartBootstrap(TMasterConfigPtr config, NYTree::INodePtr configNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
