#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateDataNodeNbdService(
    IBootstrap* bootstrap,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
