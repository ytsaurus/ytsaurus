#pragma once

#include "public.h"

#include <ytlib/query_client/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NQueryClient::IExecutorPtr CreateQueryExecutor(
    IInvokerPtr automatonInvoker,
    IInvokerPtr workerInvoker,
    TTabletManagerPtr tabletManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

