#pragma once

#include "public.h"

#include <ytlib/query_client/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

NQueryClient::IExecutorPtr CreateQueryExecutor(NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

