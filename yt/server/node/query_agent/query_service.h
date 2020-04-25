#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/ytlib/query_client/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateQueryService(
    TQueryAgentConfigPtr config,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
