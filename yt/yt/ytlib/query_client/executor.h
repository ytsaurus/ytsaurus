#pragma once

#include "public.h"
#include "callbacks.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

IExecutorPtr CreateQueryExecutor(
    NApi::NNative::IConnectionPtr connection,
    IInvokerPtr invoker,
    TColumnEvaluatorCachePtr columnEvaluatorCache,
    TEvaluatorPtr evaluator,
    NNodeTrackerClient::INodeChannelFactoryPtr nodeChannelFactory,
    TFunctionImplCachePtr functionImplCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
