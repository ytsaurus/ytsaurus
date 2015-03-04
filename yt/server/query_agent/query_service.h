#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/query_client/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateQueryService(
    TQueryAgentConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT
