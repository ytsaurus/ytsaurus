#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/server/lib/security_server/public.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

NSecurityServer::IResourceLimitsManagerPtr CreateResourceLimitsManager(
    NTabletNode::TSecurityManagerConfigPtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
