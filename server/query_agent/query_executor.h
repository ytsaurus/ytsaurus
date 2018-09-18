#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/query_client/public.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

NQueryClient::ISubexecutorPtr CreateQuerySubexecutor(
    TQueryAgentConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

