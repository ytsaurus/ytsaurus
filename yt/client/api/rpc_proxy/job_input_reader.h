#pragma once

#include "api_service_proxy.h"

#include <yt/core/concurrency/async_stream.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

// TODOKETE make this a class everyone inherits from
TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateRpcJobInputReader(
    TApiServiceProxy::TReqGetJobInputPtr request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
