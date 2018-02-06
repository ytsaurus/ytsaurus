#pragma once

#include "public.h"
#include "callbacks.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

IExecutorPtr CreateQueryExecutor(
    NApi::INativeConnectionPtr connection,
    IInvokerPtr invoker,
    TColumnEvaluatorCachePtr columnEvaluatorCache,
    TEvaluatorPtr evaluator,
    NNodeTrackerClient::INodeChannelFactoryPtr nodeChannelFactory,
    TFunctionImplCachePtr functionImplCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
