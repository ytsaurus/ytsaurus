#pragma once

#include "public.h"
#include "callbacks.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

NQueryClient::IExecutorPtr CreateQueryExecutor(
    NApi::INativeConnectionPtr connection,
    NNodeTrackerClient::INodeChannelFactoryPtr nodeChannelFactory,
    TFunctionImplCachePtr functionImplCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
