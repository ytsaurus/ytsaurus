#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/query_client/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateQueryService(
    TQueryAgentConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT
