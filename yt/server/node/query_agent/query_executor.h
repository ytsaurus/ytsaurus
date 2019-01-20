#pragma once

#include "public.h"

#include <yt/server/node/cell_node/public.h>

#include <yt/ytlib/query_client/public.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

NQueryClient::ISubexecutorPtr CreateQuerySubexecutor(
    TQueryAgentConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent

