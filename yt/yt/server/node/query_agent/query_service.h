#pragma once

#include "public.h"

#include <yt/yt/server/node/tablet_node/public.h>

#include <yt/yt/ytlib/query_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateQueryService(
    TQueryAgentConfigPtr config,
    NTabletNode::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
