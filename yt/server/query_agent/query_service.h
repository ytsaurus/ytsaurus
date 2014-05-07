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
    IInvokerPtr invoker,
    NQueryClient::IExecutorPtr executor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT
